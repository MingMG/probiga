"""Database-side payload hashing plus full Python evidence reconstruction.

The writer is not trusted by this audit.  Every stored canonical JSON payload
is hashed once by MySQL's ``SHA2`` and independently by ``CanonicalJson``.
The raw stored columns are then used to reconstruct all five immutable
evidence contracts and both append-only chains.  A mismatch raises instead of
returning a partial success report.

The caller owns the connection and its transaction.  This module opens no
engine and never commits or rolls back.  The database entry point takes shared
row locks, so it is intended for a stopped-writer acceptance window.
External authority is deliberately counted, not certified: source authority
requires its separate verifier and trust root.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from sqlalchemy import text

from server.trading_v2.domain import OrderStatus
from server.trading_v2.execution_evidence import (
    AuthorityStatus,
    CanonicalJson,
    CashEventBinding,
    EvidenceProvenance,
    FillExecutionEvidence,
    HistoryOrigin,
    MarketCalendarEvidence,
    OrderTransitionEvidence,
    OrderTransitionKind,
    QuoteReceiptEvidence,
    QuoteReceiptType,
    validate_cash_event_binding_chain,
    validate_order_transition_chain,
)


MARKET_ZONE = ZoneInfo("Asia/Shanghai")


class V2EvidenceHashAuditError(ValueError):
    """Persisted evidence cannot be independently reproduced."""


@dataclass(frozen=True, slots=True)
class V2EvidenceHashAuditReport:
    table_counts: tuple[tuple[str, int], ...]
    payload_hashes_verified: int
    rows_reconstructed: int
    cash_chains_checked: int
    complete_cash_chains: int
    order_chains_checked: int
    complete_order_chains: int
    external_authority_claims: int
    database_sha2_used: bool
    shared_row_locks_used: bool

    @property
    def audit_passed(self) -> bool:
        counts = dict(self.table_counts)
        expected_payloads = sum(
            counts.get(table, 0) * len(hash_columns)
            for table, hash_columns in EVIDENCE_JSON_HASH_COLUMNS.items()
        )
        return (
            self.database_sha2_used
            and frozenset(counts) == frozenset(EVIDENCE_JSON_HASH_COLUMNS)
            and all(type(count) is int and count > 0 for count in counts.values())
            and self.rows_reconstructed == sum(counts.values())
            and self.payload_hashes_verified == expected_payloads
        )

    @property
    def production_activation_allowed(self) -> bool:
        # Hash correctness is only one activation gate.
        return False


CALENDAR_COLUMNS = (
    "calendar_evidence_id", "market_code", "trade_date", "calendar_version",
    "market_timezone", "calendar_payload_json", "calendar_payload_hash",
    "source_provider", "source_payload_json", "source_payload_hash",
    "source_receipt_id", "source_receipt_hash", "available_at",
    "history_origin", "history_origin_id", "history_origin_at",
    "authority_status", "authority_receipt_hash", "evidence_hash", "created_at",
)
QUOTE_COLUMNS = (
    "quote_evidence_id", "quote_event_id", "stock_code", "trade_date",
    "market_timezone", "quote_at", "received_at", "available_at",
    "source_provider", "source_batch_id", "source_payload_hash",
    "source_receipt_type", "source_receipt_id", "source_receipt_hash",
    "receipt_payload_json", "receipt_payload_hash", "history_origin",
    "history_origin_id", "history_origin_at", "authority_status",
    "authority_receipt_hash", "evidence_hash", "created_at",
)
FILL_COLUMNS = (
    "fill_execution_evidence_id", "fill_id", "order_id", "order_fill_sequence",
    "account_id", "stock_code", "fill_payload_json", "fill_payload_hash",
    "order_payload_json", "order_payload_hash", "quote_event_id",
    "quote_evidence_id", "quote_evidence_hash", "calendar_evidence_id",
    "calendar_evidence_hash", "fee_profile_version", "fee_security_type",
    "fee_effective_from", "fee_effective_to", "fee_created_at",
    "fee_schedule_json", "fee_schedule_hash", "instrument_rule_version",
    "instrument_rule_effective_from", "instrument_rule_effective_to",
    "instrument_rule_created_at", "instrument_rule_json", "instrument_rule_hash",
    "matcher_version", "matcher_request_json", "matcher_request_hash",
    "matcher_response_json", "matcher_output_hash", "accounting_request_json",
    "accounting_request_hash", "settlement_evidence_json",
    "settlement_evidence_hash", "executed_at", "bound_at", "history_origin",
    "history_origin_id", "history_origin_at", "authority_status",
    "authority_receipt_hash", "evidence_hash", "created_at",
)
CASH_COLUMNS = (
    "cash_binding_id", "cash_event_id", "account_id", "account_sequence",
    "cash_event_type", "related_order_id", "related_fill_id", "reversal_of",
    "fill_execution_evidence_id", "fill_execution_evidence_hash",
    "previous_cash_event_id", "previous_binding_id", "previous_binding_hash",
    "cash_event_payload_json", "cash_event_payload_hash", "occurred_at", "bound_at",
    "history_origin", "history_origin_id", "history_origin_at", "authority_status",
    "authority_receipt_hash", "binding_hash", "created_at",
)
ORDER_COLUMNS = (
    "transition_id", "order_id", "account_id", "order_payload_json",
    "order_payload_hash", "transition_sequence", "previous_transition_id",
    "previous_transition_hash", "from_status", "to_status",
    "previous_filled_quantity", "next_filled_quantity", "waiting_reason",
    "transition_kind", "related_fill_id", "fill_execution_evidence_id",
    "fill_execution_evidence_hash", "source_event_type", "source_event_id",
    "source_event_hash", "occurred_at", "recorded_at", "history_origin",
    "history_origin_id", "history_origin_at", "authority_status",
    "authority_receipt_hash", "transition_hash", "created_at",
)


EVIDENCE_JSON_HASH_COLUMNS: Mapping[str, tuple[tuple[str, str], ...]] = {
    "st_market_calendar_evidence_v2": (
        ("calendar_payload_json", "calendar_payload_hash"),
        ("source_payload_json", "source_payload_hash"),
    ),
    "st_quote_receipt_evidence_v2": (
        ("receipt_payload_json", "receipt_payload_hash"),
    ),
    "st_fill_execution_evidence_v2": (
        ("fill_payload_json", "fill_payload_hash"),
        ("order_payload_json", "order_payload_hash"),
        ("fee_schedule_json", "fee_schedule_hash"),
        ("instrument_rule_json", "instrument_rule_hash"),
        ("matcher_request_json", "matcher_request_hash"),
        ("matcher_response_json", "matcher_output_hash"),
        ("accounting_request_json", "accounting_request_hash"),
        ("settlement_evidence_json", "settlement_evidence_hash"),
    ),
    "st_cash_event_binding_v2": (
        ("cash_event_payload_json", "cash_event_payload_hash"),
    ),
    "st_order_transition_v2": (
        ("order_payload_json", "order_payload_hash"),
    ),
}


_TABLES = (
    ("st_market_calendar_evidence_v2", CALENDAR_COLUMNS, "calendar_evidence_id"),
    ("st_quote_receipt_evidence_v2", QUOTE_COLUMNS, "quote_evidence_id"),
    ("st_fill_execution_evidence_v2", FILL_COLUMNS, "fill_execution_evidence_id"),
    ("st_cash_event_binding_v2", CASH_COLUMNS, "account_id, account_sequence"),
    ("st_order_transition_v2", ORDER_COLUMNS, "order_id, transition_sequence"),
)


def _fail(message: str) -> V2EvidenceHashAuditError:
    return V2EvidenceHashAuditError(message)


def _text_value(value: object, name: str) -> str:
    if type(value) is not str or not value or value != value.strip():
        raise _fail(f"{name} must be exact non-blank text")
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


def _provenance(row: Mapping[str, Any], prefix: str) -> EvidenceProvenance:
    try:
        return EvidenceProvenance(
            history_origin=HistoryOrigin(_text_value(
                row["history_origin"], f"{prefix}.history_origin"
            )),
            history_origin_id=_optional_text(
                row["history_origin_id"], f"{prefix}.history_origin_id"
            ),
            history_origin_at=(
                None
                if row["history_origin_at"] is None
                else _datetime_value(
                    row["history_origin_at"], f"{prefix}.history_origin_at"
                )
            ),
            authority_status=AuthorityStatus(_text_value(
                row["authority_status"], f"{prefix}.authority_status"
            )),
            authority_receipt_hash=_optional_hash(
                row["authority_receipt_hash"],
                f"{prefix}.authority_receipt_hash",
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2EvidenceHashAuditError):
            raise
        raise _fail(f"{prefix} provenance cannot be reconstructed") from exc


def _canonical_payloads(
    table: str,
    row: Mapping[str, Any],
    row_number: int,
) -> dict[str, CanonicalJson]:
    payloads: dict[str, CanonicalJson] = {}
    for json_column, hash_column in EVIDENCE_JSON_HASH_COLUMNS[table]:
        name = f"{table}[{row_number}].{json_column}"
        raw = row.get(json_column)
        if type(raw) is not str:
            raise _fail(f"{name} must be exact text")
        try:
            canonical = CanonicalJson(raw)
        except (TypeError, ValueError) as exc:
            raise _fail(f"{name} is not strict canonical JSON") from exc
        stored_hash = _hash_value(
            row.get(hash_column), f"{table}[{row_number}].{hash_column}"
        )
        database_hash = _hash_value(
            row.get(f"__dbhash_{hash_column}"),
            f"{table}[{row_number}].__dbhash_{hash_column}",
        )
        if not canonical.payload_hash == stored_hash == database_hash:
            raise _fail(
                f"{table}[{row_number}] {hash_column} differs from "
                "Python canonical hash or database SHA2"
            )
        payloads[json_column] = canonical
    return payloads


def _expect_storage_time(
    row: Mapping[str, Any],
    column: str,
    expected: datetime,
    name: str,
) -> None:
    if _datetime_value(row[column], f"{name}.{column}") != expected:
        raise _fail(f"{name}.{column} differs from canonical writer time")


def _expect_hash(value: object, expected: str, name: str) -> None:
    if _hash_value(value, name) != expected:
        raise _fail(f"{name} differs from reconstructed evidence")


def _calendar(
    row: Mapping[str, Any],
    payloads: Mapping[str, CanonicalJson],
    number: int,
) -> MarketCalendarEvidence:
    name = f"calendar[{number}]"
    try:
        value = MarketCalendarEvidence(
            market_code=_text_value(row["market_code"], f"{name}.market_code"),
            trade_date=_date_value(row["trade_date"], f"{name}.trade_date"),
            calendar_version=_text_value(
                row["calendar_version"], f"{name}.calendar_version"
            ),
            market_timezone=_text_value(
                row["market_timezone"], f"{name}.market_timezone"
            ),
            calendar_payload=payloads["calendar_payload_json"],
            source_provider=_text_value(
                row["source_provider"], f"{name}.source_provider"
            ),
            source_payload=payloads["source_payload_json"],
            source_receipt_id=_optional_text(
                row["source_receipt_id"], f"{name}.source_receipt_id"
            ),
            source_receipt_hash=_optional_hash(
                row["source_receipt_hash"], f"{name}.source_receipt_hash"
            ),
            available_at=_datetime_value(row["available_at"], f"{name}.available_at"),
            provenance=_provenance(row, name),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2EvidenceHashAuditError):
            raise
        raise _fail(f"{name} cannot be reconstructed") from exc
    _expect_hash(row["calendar_evidence_id"], value.calendar_evidence_id, f"{name}.id")
    _expect_hash(row["evidence_hash"], value.evidence_hash, f"{name}.hash")
    _expect_storage_time(row, "created_at", value.available_at, name)
    return value


def _quote(
    row: Mapping[str, Any],
    payloads: Mapping[str, CanonicalJson],
    number: int,
) -> QuoteReceiptEvidence:
    name = f"quote[{number}]"
    try:
        value = QuoteReceiptEvidence(
            quote_event_id=_hash_value(row["quote_event_id"], f"{name}.quote_event_id"),
            stock_code=_text_value(row["stock_code"], f"{name}.stock_code"),
            trade_date=_date_value(row["trade_date"], f"{name}.trade_date"),
            market_timezone=_text_value(
                row["market_timezone"], f"{name}.market_timezone"
            ),
            quote_at=_datetime_value(row["quote_at"], f"{name}.quote_at"),
            received_at=_datetime_value(row["received_at"], f"{name}.received_at"),
            available_at=_datetime_value(row["available_at"], f"{name}.available_at"),
            source_provider=_text_value(
                row["source_provider"], f"{name}.source_provider"
            ),
            source_batch_id=_text_value(
                row["source_batch_id"], f"{name}.source_batch_id"
            ),
            source_payload_hash=_hash_value(
                row["source_payload_hash"], f"{name}.source_payload_hash"
            ),
            receipt_type=QuoteReceiptType(_text_value(
                row["source_receipt_type"], f"{name}.source_receipt_type"
            )),
            receipt_payload=payloads["receipt_payload_json"],
            source_receipt_id=_optional_text(
                row["source_receipt_id"], f"{name}.source_receipt_id"
            ),
            source_receipt_hash=_optional_hash(
                row["source_receipt_hash"], f"{name}.source_receipt_hash"
            ),
            provenance=_provenance(row, name),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2EvidenceHashAuditError):
            raise
        raise _fail(f"{name} cannot be reconstructed") from exc
    _expect_hash(row["quote_evidence_id"], value.quote_evidence_id, f"{name}.id")
    _expect_hash(row["evidence_hash"], value.evidence_hash, f"{name}.hash")
    _expect_storage_time(row, "created_at", value.available_at, name)
    return value


def _fill(
    row: Mapping[str, Any],
    payloads: Mapping[str, CanonicalJson],
    number: int,
    calendars: Mapping[str, MarketCalendarEvidence],
    quotes: Mapping[str, QuoteReceiptEvidence],
) -> FillExecutionEvidence:
    name = f"fill[{number}]"
    quote_id = _hash_value(row["quote_evidence_id"], f"{name}.quote_evidence_id")
    calendar_id = _hash_value(
        row["calendar_evidence_id"], f"{name}.calendar_evidence_id"
    )
    try:
        quote = quotes[quote_id]
        calendar = calendars[calendar_id]
    except KeyError as exc:
        raise _fail(f"{name} references absent market evidence") from exc
    _expect_hash(row["quote_evidence_hash"], quote.evidence_hash, f"{name}.quote_hash")
    _expect_hash(
        row["calendar_evidence_hash"], calendar.evidence_hash, f"{name}.calendar_hash"
    )
    _expect_hash(row["quote_event_id"], quote.quote_event_id, f"{name}.quote_event_id")
    try:
        value = FillExecutionEvidence(
            fill_id=_text_value(row["fill_id"], f"{name}.fill_id"),
            order_id=_text_value(row["order_id"], f"{name}.order_id"),
            order_fill_sequence=_int_value(
                row["order_fill_sequence"], f"{name}.order_fill_sequence", minimum=1
            ),
            account_id=_text_value(row["account_id"], f"{name}.account_id"),
            stock_code=_text_value(row["stock_code"], f"{name}.stock_code"),
            fill_payload=payloads["fill_payload_json"],
            order_payload=payloads["order_payload_json"],
            quote_evidence=quote,
            calendar_evidence=calendar,
            fee_profile_version=_text_value(
                row["fee_profile_version"], f"{name}.fee_profile_version"
            ),
            fee_security_type=_text_value(
                row["fee_security_type"], f"{name}.fee_security_type"
            ),
            fee_effective_from=_date_value(
                row["fee_effective_from"], f"{name}.fee_effective_from"
            ),
            fee_effective_to=(
                None if row["fee_effective_to"] is None else _date_value(
                    row["fee_effective_to"], f"{name}.fee_effective_to"
                )
            ),
            fee_created_at=_datetime_value(
                row["fee_created_at"], f"{name}.fee_created_at"
            ),
            fee_schedule=payloads["fee_schedule_json"],
            instrument_rule_version=_text_value(
                row["instrument_rule_version"], f"{name}.instrument_rule_version"
            ),
            instrument_rule_effective_from=_date_value(
                row["instrument_rule_effective_from"],
                f"{name}.instrument_rule_effective_from",
            ),
            instrument_rule_effective_to=(
                None if row["instrument_rule_effective_to"] is None else _date_value(
                    row["instrument_rule_effective_to"],
                    f"{name}.instrument_rule_effective_to",
                )
            ),
            instrument_rule_created_at=_datetime_value(
                row["instrument_rule_created_at"],
                f"{name}.instrument_rule_created_at",
            ),
            instrument_rule=payloads["instrument_rule_json"],
            matcher_version=_text_value(
                row["matcher_version"], f"{name}.matcher_version"
            ),
            matcher_request=payloads["matcher_request_json"],
            matcher_response=payloads["matcher_response_json"],
            accounting_request=payloads["accounting_request_json"],
            settlement_evidence=payloads["settlement_evidence_json"],
            executed_at=_datetime_value(row["executed_at"], f"{name}.executed_at"),
            bound_at=_datetime_value(row["bound_at"], f"{name}.bound_at"),
            provenance=_provenance(row, name),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2EvidenceHashAuditError):
            raise
        raise _fail(f"{name} cannot be reconstructed") from exc
    _expect_hash(
        row["fill_execution_evidence_id"],
        value.fill_execution_evidence_id,
        f"{name}.id",
    )
    _expect_hash(row["evidence_hash"], value.evidence_hash, f"{name}.hash")
    _expect_storage_time(row, "created_at", value.bound_at, name)
    return value


def _cash(
    row: Mapping[str, Any],
    payloads: Mapping[str, CanonicalJson],
    number: int,
    fills: Mapping[str, FillExecutionEvidence],
) -> CashEventBinding:
    name = f"cash[{number}]"
    fill_id = _optional_hash(
        row["fill_execution_evidence_id"], f"{name}.fill_execution_evidence_id"
    )
    fill = None
    if fill_id is not None:
        try:
            fill = fills[fill_id]
        except KeyError as exc:
            raise _fail(f"{name} references absent fill evidence") from exc
        _expect_hash(
            row["fill_execution_evidence_hash"],
            fill.evidence_hash,
            f"{name}.fill_execution_evidence_hash",
        )
    elif row["fill_execution_evidence_hash"] is not None:
        raise _fail(f"{name} has a partial fill evidence reference")
    try:
        value = CashEventBinding(
            cash_event_id=_text_value(
                row["cash_event_id"], f"{name}.cash_event_id"
            ),
            account_id=_text_value(row["account_id"], f"{name}.account_id"),
            account_sequence=_int_value(
                row["account_sequence"], f"{name}.account_sequence"
            ),
            cash_event_type=_text_value(
                row["cash_event_type"], f"{name}.cash_event_type"
            ),
            cash_event_payload=payloads["cash_event_payload_json"],
            occurred_at=_datetime_value(row["occurred_at"], f"{name}.occurred_at"),
            bound_at=_datetime_value(row["bound_at"], f"{name}.bound_at"),
            provenance=_provenance(row, name),
            related_order_id=_optional_text(
                row["related_order_id"], f"{name}.related_order_id"
            ),
            related_fill_id=_optional_text(
                row["related_fill_id"], f"{name}.related_fill_id"
            ),
            reversal_of=_optional_text(row["reversal_of"], f"{name}.reversal_of"),
            fill_execution_evidence=fill,
            previous_cash_event_id=_optional_text(
                row["previous_cash_event_id"], f"{name}.previous_cash_event_id"
            ),
            previous_binding_id=_optional_hash(
                row["previous_binding_id"], f"{name}.previous_binding_id"
            ),
            previous_binding_hash=_optional_hash(
                row["previous_binding_hash"], f"{name}.previous_binding_hash"
            ),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2EvidenceHashAuditError):
            raise
        raise _fail(f"{name} cannot be reconstructed") from exc
    _expect_hash(row["cash_binding_id"], value.cash_binding_id, f"{name}.id")
    _expect_hash(row["binding_hash"], value.binding_hash, f"{name}.hash")
    _expect_storage_time(row, "created_at", value.bound_at, name)
    return value


def _order(
    row: Mapping[str, Any],
    payloads: Mapping[str, CanonicalJson],
    number: int,
    fills: Mapping[str, FillExecutionEvidence],
) -> OrderTransitionEvidence:
    name = f"order[{number}]"
    fill_id = _optional_hash(
        row["fill_execution_evidence_id"], f"{name}.fill_execution_evidence_id"
    )
    fill = None
    if fill_id is not None:
        try:
            fill = fills[fill_id]
        except KeyError as exc:
            raise _fail(f"{name} references absent fill evidence") from exc
        _expect_hash(
            row["fill_execution_evidence_hash"],
            fill.evidence_hash,
            f"{name}.fill_execution_evidence_hash",
        )
    elif row["fill_execution_evidence_hash"] is not None:
        raise _fail(f"{name} has a partial fill evidence reference")
    try:
        value = OrderTransitionEvidence(
            order_id=_text_value(row["order_id"], f"{name}.order_id"),
            account_id=_text_value(row["account_id"], f"{name}.account_id"),
            order_payload=payloads["order_payload_json"],
            transition_sequence=_int_value(
                row["transition_sequence"], f"{name}.transition_sequence"
            ),
            previous_transition_id=_optional_hash(
                row["previous_transition_id"], f"{name}.previous_transition_id"
            ),
            previous_transition_hash=_optional_hash(
                row["previous_transition_hash"], f"{name}.previous_transition_hash"
            ),
            from_status=OrderStatus(_text_value(
                row["from_status"], f"{name}.from_status"
            )),
            to_status=OrderStatus(_text_value(row["to_status"], f"{name}.to_status")),
            previous_filled_quantity=_int_value(
                row["previous_filled_quantity"], f"{name}.previous_filled_quantity"
            ),
            next_filled_quantity=_int_value(
                row["next_filled_quantity"], f"{name}.next_filled_quantity"
            ),
            waiting_reason=_optional_text(
                row["waiting_reason"], f"{name}.waiting_reason"
            ),
            transition_kind=OrderTransitionKind(_text_value(
                row["transition_kind"], f"{name}.transition_kind"
            )),
            related_fill_id=_optional_text(
                row["related_fill_id"], f"{name}.related_fill_id"
            ),
            fill_execution_evidence=fill,
            source_event_type=_text_value(
                row["source_event_type"], f"{name}.source_event_type"
            ),
            source_event_id=_text_value(
                row["source_event_id"], f"{name}.source_event_id"
            ),
            source_event_hash=_hash_value(
                row["source_event_hash"], f"{name}.source_event_hash"
            ),
            occurred_at=_datetime_value(row["occurred_at"], f"{name}.occurred_at"),
            recorded_at=_datetime_value(row["recorded_at"], f"{name}.recorded_at"),
            provenance=_provenance(row, name),
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2EvidenceHashAuditError):
            raise
        raise _fail(f"{name} cannot be reconstructed") from exc
    _expect_hash(row["transition_id"], value.transition_id, f"{name}.id")
    _expect_hash(row["transition_hash"], value.transition_hash, f"{name}.hash")
    _expect_storage_time(row, "created_at", value.recorded_at, name)
    return value


def _unique_map(values: tuple[Any, ...], attribute: str, name: str) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for value in values:
        key = getattr(value, attribute)
        if key in result:
            raise _fail(f"duplicate reconstructed {name}: {key}")
        result[key] = value
    return result


def audit_v2_execution_evidence_rows(
    rows_by_table: Mapping[str, tuple[Mapping[str, Any], ...]],
    *,
    database_sha2_used: bool = True,
    shared_row_locks_used: bool = False,
) -> V2EvidenceHashAuditReport:
    """Rebuild all evidence from raw rows already carrying DB SHA2 aliases."""

    if set(rows_by_table) != set(EVIDENCE_JSON_HASH_COLUMNS):
        raise _fail("evidence audit requires exactly the five evidence tables")
    if type(database_sha2_used) is not bool or not database_sha2_used:
        raise _fail("database SHA2 recomputation is mandatory")

    payloads: dict[str, tuple[dict[str, CanonicalJson], ...]] = {}
    payload_hash_count = 0
    for table, _, _ in _TABLES:
        table_rows = rows_by_table[table]
        if type(table_rows) is not tuple:
            raise _fail(f"{table} rows must be exactly tuple")
        parsed = tuple(
            _canonical_payloads(table, row, index)
            for index, row in enumerate(table_rows)
        )
        payloads[table] = parsed
        payload_hash_count += len(table_rows) * len(EVIDENCE_JSON_HASH_COLUMNS[table])

    calendar_rows = rows_by_table["st_market_calendar_evidence_v2"]
    calendars_tuple = tuple(
        _calendar(row, payloads["st_market_calendar_evidence_v2"][index], index)
        for index, row in enumerate(calendar_rows)
    )
    calendars = _unique_map(
        calendars_tuple, "calendar_evidence_id", "calendar evidence"
    )

    quote_rows = rows_by_table["st_quote_receipt_evidence_v2"]
    quotes_tuple = tuple(
        _quote(row, payloads["st_quote_receipt_evidence_v2"][index], index)
        for index, row in enumerate(quote_rows)
    )
    quotes = _unique_map(quotes_tuple, "quote_evidence_id", "quote evidence")

    fill_rows = rows_by_table["st_fill_execution_evidence_v2"]
    fills_tuple = tuple(
        _fill(
            row,
            payloads["st_fill_execution_evidence_v2"][index],
            index,
            calendars,
            quotes,
        )
        for index, row in enumerate(fill_rows)
    )
    fills = _unique_map(
        fills_tuple, "fill_execution_evidence_id", "fill evidence"
    )

    cash_rows = rows_by_table["st_cash_event_binding_v2"]
    cash_tuple = tuple(
        _cash(
            row,
            payloads["st_cash_event_binding_v2"][index],
            index,
            fills,
        )
        for index, row in enumerate(cash_rows)
    )
    _unique_map(cash_tuple, "cash_binding_id", "cash binding")

    order_rows = rows_by_table["st_order_transition_v2"]
    order_tuple = tuple(
        _order(
            row,
            payloads["st_order_transition_v2"][index],
            index,
            fills,
        )
        for index, row in enumerate(order_rows)
    )
    _unique_map(order_tuple, "transition_id", "order transition")

    cash_groups: dict[str, list[CashEventBinding]] = {}
    for item in cash_tuple:
        cash_groups.setdefault(item.account_id, []).append(item)
    complete_cash = 0
    for account_id, items in cash_groups.items():
        ordered = tuple(sorted(items, key=lambda item: item.account_sequence))
        try:
            complete_cash += int(validate_cash_event_binding_chain(ordered))
        except (TypeError, ValueError) as exc:
            raise _fail(f"cash chain {account_id} cannot be reconstructed") from exc

    order_groups: dict[str, list[OrderTransitionEvidence]] = {}
    for item in order_tuple:
        order_groups.setdefault(item.order_id, []).append(item)
    complete_order = 0
    for order_id, items in order_groups.items():
        ordered = tuple(sorted(items, key=lambda item: item.transition_sequence))
        try:
            complete_order += int(validate_order_transition_chain(ordered))
        except (TypeError, ValueError) as exc:
            raise _fail(f"order chain {order_id} cannot be reconstructed") from exc

    all_objects = (*calendars_tuple, *quotes_tuple, *fills_tuple, *cash_tuple, *order_tuple)
    authority_claims = sum(
        getattr(item, "provenance").authority_status
        is AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED
        for item in all_objects
    )
    counts = tuple(
        (table, len(rows_by_table[table])) for table, _, _ in _TABLES
    )
    return V2EvidenceHashAuditReport(
        table_counts=counts,
        payload_hashes_verified=payload_hash_count,
        rows_reconstructed=len(all_objects),
        cash_chains_checked=len(cash_groups),
        complete_cash_chains=complete_cash,
        order_chains_checked=len(order_groups),
        complete_order_chains=complete_order,
        external_authority_claims=authority_claims,
        database_sha2_used=True,
        shared_row_locks_used=shared_row_locks_used,
    )


def _db_hash_expression(json_column: str, hash_column: str) -> str:
    prefix = '{"namespace":"trading-v2.canonical-json.v1","payload":{"value":'
    return (
        "LOWER(SHA2(CAST(CONCAT("
        f"'{prefix}', CONVERT({json_column} USING utf8mb4), '}}}}'"
        ") AS BINARY), 256)) "
        f"AS __dbhash_{hash_column}"
    )


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


def audit_v2_execution_evidence_database(
    connection: Any,
) -> V2EvidenceHashAuditReport:
    """Audit a stopped-writer database on one caller-owned transaction."""

    if connection is None or not callable(getattr(connection, "execute", None)):
        raise _fail("a SQLAlchemy-like connection is required")
    in_transaction = getattr(connection, "in_transaction", None)
    if not callable(in_transaction) or in_transaction() is not True:
        raise _fail("connection must already be in a transaction")

    rows_by_table: dict[str, tuple[Mapping[str, Any], ...]] = {}
    for table, columns, order_by in _TABLES:
        hash_expressions = tuple(
            _db_hash_expression(json_column, hash_column)
            for json_column, hash_column in EVIDENCE_JSON_HASH_COLUMNS[table]
        )
        select_columns = ", ".join((*columns, *hash_expressions))
        result = connection.execute(
            text(
                f"/* v2e:audit_{table} */\n"
                f"SELECT {select_columns} FROM {table} "
                f"ORDER BY {order_by} LOCK IN SHARE MODE"
            )
        )
        rows_by_table[table] = _all_mappings(result, table)
    return audit_v2_execution_evidence_rows(
        rows_by_table,
        database_sha2_used=True,
        shared_row_locks_used=True,
    )


__all__ = [
    "EVIDENCE_JSON_HASH_COLUMNS",
    "V2EvidenceHashAuditError",
    "V2EvidenceHashAuditReport",
    "audit_v2_execution_evidence_database",
    "audit_v2_execution_evidence_rows",
]
