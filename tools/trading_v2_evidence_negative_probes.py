"""Fail-closed negative DML probes for the five V2 evidence tables.

The caller must supply an engine whose every checkout is already bound to the
expected disposable MySQL server and schema.  This module never creates an
engine, changes schema, commits, or deletes cleanup rows.  Every negative DML
attempt is enclosed in a caller-visible explicit transaction and is rolled
back before its result is interpreted.

This module is intentionally independent of the main MySQL acceptance runner
so its SQL construction and transaction behavior can be unit tested without a
database.  A passing fake test is not MySQL behavioral acceptance; errno 1644,
trigger ordering and statement atomicity still require an exact validated
Oracle MySQL isolated-acceptance version.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import re
from types import MappingProxyType
from typing import Any, Mapping, Sequence

from sqlalchemy import text


class EvidenceNegativeProbeError(RuntimeError):
    """Base error for a negative evidence probe."""


class EvidenceNegativeProbeContractError(EvidenceNegativeProbeError):
    """The allowlist, case, baseline, or database result is not exact."""


class EvidenceNegativeProbeGuardError(EvidenceNegativeProbeError):
    """The server did not return the required trigger error."""


class EvidenceNegativeProbeRetentionError(EvidenceNegativeProbeError):
    """A rejected or rolled-back probe changed persistent rows."""


class EvidenceNegativeProbeUnexpectedSuccess(EvidenceNegativeProbeError):
    """A DML path expected to be blocked completed without an error."""


class NegativeProbeOperation(str, Enum):
    INVALID_INSERT = "INVALID_INSERT"
    REPLACE = "REPLACE"
    ON_DUPLICATE_KEY_UPDATE = "ON_DUPLICATE_KEY_UPDATE"


@dataclass(frozen=True, slots=True)
class EvidenceTableProbeMetadata:
    evidence_type: str
    table: str
    columns: tuple[str, ...]
    primary_column: str
    content_hash_column: str
    unique_keys: tuple[tuple[str, ...], ...]
    invalid_identity_groups: tuple[tuple[str, ...], ...]
    invalid_insert_message: str
    replace_message: str
    on_duplicate_key_update_message: str

    def expected_message(self, operation: NegativeProbeOperation) -> str:
        if operation is NegativeProbeOperation.INVALID_INSERT:
            return self.invalid_insert_message
        if operation is NegativeProbeOperation.REPLACE:
            return self.replace_message
        if operation is NegativeProbeOperation.ON_DUPLICATE_KEY_UPDATE:
            return self.on_duplicate_key_update_message
        raise EvidenceNegativeProbeContractError(
            f"unsupported negative probe operation: {operation!r}"
        )


@dataclass(frozen=True, slots=True)
class EvidenceNegativeProbeCase:
    evidence_type: str
    primary_value: str


@dataclass(frozen=True, slots=True)
class EvidenceNegativeProbeStatement:
    operation: NegativeProbeOperation
    sql: str
    parameters: Mapping[str, Any]
    candidate_primary_value: str
    expected_message: str


@dataclass(frozen=True, slots=True)
class EvidenceNegativeProbeResult:
    evidence_type: str
    table: str
    primary_value: str
    candidate_primary_value: str
    operation: NegativeProbeOperation
    mysql_errno: int
    expected_message: str
    row_count_before: int
    row_count_after: int
    baseline_retained: bool


CALENDAR_STORAGE_COLUMNS = (
    "calendar_evidence_id", "market_code", "trade_date", "calendar_version",
    "market_timezone", "calendar_payload_json", "calendar_payload_hash",
    "source_provider", "source_payload_json", "source_payload_hash",
    "source_receipt_id", "source_receipt_hash", "available_at",
    "history_origin", "history_origin_id", "history_origin_at",
    "authority_status", "authority_receipt_hash", "evidence_hash", "created_at",
)
QUOTE_STORAGE_COLUMNS = (
    "quote_evidence_id", "quote_event_id", "stock_code", "trade_date",
    "market_timezone", "quote_at", "received_at", "available_at",
    "source_provider", "source_batch_id", "source_payload_hash",
    "source_receipt_type", "source_receipt_id", "source_receipt_hash",
    "receipt_payload_json", "receipt_payload_hash", "history_origin",
    "history_origin_id", "history_origin_at", "authority_status",
    "authority_receipt_hash", "evidence_hash", "created_at",
)
FILL_STORAGE_COLUMNS = (
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
CASH_STORAGE_COLUMNS = (
    "cash_binding_id", "cash_event_id", "account_id", "account_sequence",
    "cash_event_type", "related_order_id", "related_fill_id", "reversal_of",
    "fill_execution_evidence_id", "fill_execution_evidence_hash",
    "previous_cash_event_id", "previous_binding_id", "previous_binding_hash",
    "cash_event_payload_json", "cash_event_payload_hash", "occurred_at", "bound_at",
    "history_origin", "history_origin_id", "history_origin_at", "authority_status",
    "authority_receipt_hash", "binding_hash", "created_at",
)
ORDER_STORAGE_COLUMNS = (
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


_METADATA = (
    EvidenceTableProbeMetadata(
        evidence_type="MARKET_CALENDAR",
        table="st_market_calendar_evidence_v2",
        columns=CALENDAR_STORAGE_COLUMNS,
        primary_column="calendar_evidence_id",
        content_hash_column="evidence_hash",
        unique_keys=(
            ("calendar_evidence_id",),
            ("evidence_hash",),
            ("calendar_evidence_id", "evidence_hash"),
        ),
        invalid_identity_groups=(("calendar_evidence_id", "evidence_hash"),),
        invalid_insert_message="invalid calendar history origin",
        replace_message="calendar evidence cannot be deleted",
        on_duplicate_key_update_message="calendar evidence is append only",
    ),
    EvidenceTableProbeMetadata(
        evidence_type="QUOTE_RECEIPT",
        table="st_quote_receipt_evidence_v2",
        columns=QUOTE_STORAGE_COLUMNS,
        primary_column="quote_evidence_id",
        content_hash_column="evidence_hash",
        unique_keys=(
            ("quote_evidence_id",),
            ("quote_event_id",),
            ("evidence_hash",),
            ("quote_evidence_id", "quote_event_id", "evidence_hash"),
        ),
        invalid_identity_groups=(
            ("quote_evidence_id", "evidence_hash"),
            ("quote_event_id",),
        ),
        invalid_insert_message="invalid quote history origin",
        replace_message="quote evidence cannot be deleted",
        on_duplicate_key_update_message="quote evidence is append only",
    ),
    EvidenceTableProbeMetadata(
        evidence_type="FILL_EXECUTION",
        table="st_fill_execution_evidence_v2",
        columns=FILL_STORAGE_COLUMNS,
        primary_column="fill_execution_evidence_id",
        content_hash_column="evidence_hash",
        unique_keys=(
            ("fill_execution_evidence_id",),
            ("fill_id",),
            ("order_id", "order_fill_sequence"),
            ("evidence_hash",),
            ("fill_execution_evidence_id", "fill_id", "evidence_hash"),
        ),
        invalid_identity_groups=(
            ("fill_execution_evidence_id", "evidence_hash"),
            ("fill_id",),
            ("order_id",),
        ),
        invalid_insert_message="invalid fill history or authority",
        replace_message="fill evidence cannot be deleted",
        on_duplicate_key_update_message="fill evidence is append only",
    ),
    EvidenceTableProbeMetadata(
        evidence_type="CASH_EVENT",
        table="st_cash_event_binding_v2",
        columns=CASH_STORAGE_COLUMNS,
        primary_column="cash_binding_id",
        content_hash_column="binding_hash",
        unique_keys=(
            ("cash_binding_id",),
            ("cash_event_id",),
            ("account_id", "account_sequence"),
            ("binding_hash",),
            ("cash_binding_id", "cash_event_id", "binding_hash"),
        ),
        invalid_identity_groups=(
            ("cash_binding_id", "binding_hash"),
            ("cash_event_id",),
            ("account_id",),
        ),
        invalid_insert_message="invalid cash history or authority",
        replace_message="cash evidence cannot be deleted",
        on_duplicate_key_update_message="cash evidence is append only",
    ),
    EvidenceTableProbeMetadata(
        evidence_type="ORDER_TRANSITION",
        table="st_order_transition_v2",
        columns=ORDER_STORAGE_COLUMNS,
        primary_column="transition_id",
        content_hash_column="transition_hash",
        unique_keys=(
            ("transition_id",),
            ("order_id", "transition_sequence"),
            ("order_id", "source_event_type", "source_event_id"),
            ("transition_hash",),
            ("transition_id", "transition_hash"),
        ),
        invalid_identity_groups=(
            ("transition_id", "transition_hash"),
            ("order_id",),
        ),
        invalid_insert_message="invalid order history or authority",
        replace_message="order transition cannot be deleted",
        on_duplicate_key_update_message="order transition is append only",
    ),
)


EVIDENCE_TABLE_PROBE_METADATA = MappingProxyType(
    {item.evidence_type: item for item in _METADATA}
)
ALL_NEGATIVE_PROBE_OPERATIONS = tuple(NegativeProbeOperation)
INVALID_HISTORY_ORIGIN = "INVALID_PROBE"

_SAFE_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


def _validate_metadata_contract() -> None:
    expected_types = {
        "MARKET_CALENDAR",
        "QUOTE_RECEIPT",
        "FILL_EXECUTION",
        "CASH_EVENT",
        "ORDER_TRANSITION",
    }
    if set(EVIDENCE_TABLE_PROBE_METADATA) != expected_types:
        raise EvidenceNegativeProbeContractError(
            "negative probe metadata must cover exactly five evidence types"
        )
    if len({item.table for item in _METADATA}) != len(_METADATA):
        raise EvidenceNegativeProbeContractError(
            "negative probe metadata contains duplicate tables"
        )
    for item in _METADATA:
        identifiers = (item.table, *item.columns)
        if any(_SAFE_IDENTIFIER_RE.fullmatch(value) is None for value in identifiers):
            raise EvidenceNegativeProbeContractError(
                f"unsafe SQL identifier in metadata for {item.evidence_type}"
            )
        if len(item.columns) != len(set(item.columns)):
            raise EvidenceNegativeProbeContractError(
                f"duplicate storage column for {item.evidence_type}"
            )
        column_set = set(item.columns)
        if item.primary_column not in column_set:
            raise EvidenceNegativeProbeContractError(
                f"primary column missing for {item.evidence_type}"
            )
        if item.content_hash_column not in column_set:
            raise EvidenceNegativeProbeContractError(
                f"content hash column missing for {item.evidence_type}"
            )
        mutated = {column for group in item.invalid_identity_groups for column in group}
        if "history_origin" not in column_set:
            raise EvidenceNegativeProbeContractError(
                f"history origin missing for {item.evidence_type}"
            )
        for group in (*item.unique_keys, *item.invalid_identity_groups):
            if not group or not set(group) <= column_set:
                raise EvidenceNegativeProbeContractError(
                    f"invalid column group for {item.evidence_type}: {group!r}"
                )
        for unique_key in item.unique_keys:
            if not set(unique_key) & mutated:
                raise EvidenceNegativeProbeContractError(
                    f"invalid candidate does not isolate unique key {unique_key!r}"
                )


_validate_metadata_contract()


def metadata_for_evidence_type(evidence_type: str) -> EvidenceTableProbeMetadata:
    if type(evidence_type) is not str:
        raise EvidenceNegativeProbeContractError("evidence_type must be exact text")
    try:
        return EVIDENCE_TABLE_PROBE_METADATA[evidence_type]
    except KeyError as exc:
        raise EvidenceNegativeProbeContractError(
            f"unsupported evidence type: {evidence_type!r}"
        ) from exc


def _exact_baseline(
    metadata: EvidenceTableProbeMetadata,
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(baseline, Mapping):
        raise EvidenceNegativeProbeContractError("baseline must be a mapping")
    actual = frozenset(baseline)
    expected = frozenset(metadata.columns)
    if actual != expected:
        raise EvidenceNegativeProbeContractError(
            f"{metadata.table} baseline columns differ; "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )
    row = {column: baseline[column] for column in metadata.columns}
    primary = row[metadata.primary_column]
    if type(primary) is not str or _SHA256_RE.fullmatch(primary) is None:
        raise EvidenceNegativeProbeContractError(
            f"{metadata.table} baseline primary key is not lowercase SHA256"
        )
    for group in metadata.invalid_identity_groups:
        if len(group) > 1 and len({row[column] for column in group}) != 1:
            raise EvidenceNegativeProbeContractError(
                f"{metadata.table} baseline identity group differs: {group!r}"
            )
    return row


def _fresh_group_value(
    metadata: EvidenceTableProbeMetadata,
    baseline: Mapping[str, Any],
    group: tuple[str, ...],
) -> str:
    nonce = 0
    while True:
        seed = "|".join(
            (
                "probiga-v2-negative-probe",
                metadata.table,
                str(baseline[metadata.primary_column]),
                ",".join(group),
                str(nonce),
            )
        )
        candidate = hashlib.sha256(seed.encode("utf-8")).hexdigest()
        if all(candidate != baseline[column] for column in group):
            return candidate
        nonce += 1


def build_invalid_candidate(
    metadata: EvidenceTableProbeMetadata,
    baseline: Mapping[str, Any],
) -> dict[str, Any]:
    """Build a fresh-unique row that must hit the history-origin BI guard."""

    row = _exact_baseline(metadata, baseline)
    for group in metadata.invalid_identity_groups:
        candidate = _fresh_group_value(metadata, row, group)
        for column in group:
            row[column] = candidate
    row["history_origin"] = INVALID_HISTORY_ORIGIN
    return row


def _quoted(value: str) -> str:
    if _SAFE_IDENTIFIER_RE.fullmatch(value) is None:
        raise EvidenceNegativeProbeContractError(
            f"unsafe negative probe SQL identifier: {value!r}"
        )
    return f"`{value}`"


def build_negative_probe_statement(
    metadata: EvidenceTableProbeMetadata,
    baseline: Mapping[str, Any],
    operation: NegativeProbeOperation,
) -> EvidenceNegativeProbeStatement:
    """Construct one allowlisted, parameterized, full-row DML statement."""

    if type(metadata) is not EvidenceTableProbeMetadata:
        raise EvidenceNegativeProbeContractError(
            "metadata must be an allowlisted EvidenceTableProbeMetadata"
        )
    registered = EVIDENCE_TABLE_PROBE_METADATA.get(metadata.evidence_type)
    if registered is not metadata:
        raise EvidenceNegativeProbeContractError(
            "metadata must be the registered allowlisted object"
        )
    if type(operation) is not NegativeProbeOperation:
        raise EvidenceNegativeProbeContractError(
            "operation must be NegativeProbeOperation"
        )
    legal_row = _exact_baseline(metadata, baseline)
    parameters = (
        build_invalid_candidate(metadata, legal_row)
        if operation is NegativeProbeOperation.INVALID_INSERT
        else legal_row
    )
    table = _quoted(metadata.table)
    columns = ", ".join(_quoted(column) for column in metadata.columns)
    values = ", ".join(f":{column}" for column in metadata.columns)
    comment = f"/* v2e-negative:{operation.value}:{metadata.table} */\n"
    if operation is NegativeProbeOperation.INVALID_INSERT:
        sql = f"{comment}INSERT INTO {table} ({columns}) VALUES ({values})"
    elif operation is NegativeProbeOperation.REPLACE:
        sql = f"{comment}REPLACE INTO {table} ({columns}) VALUES ({values})"
    else:
        primary = _quoted(metadata.primary_column)
        sql = (
            f"{comment}INSERT INTO {table} ({columns}) VALUES ({values}) "
            f"ON DUPLICATE KEY UPDATE {primary} = VALUES({primary})"
        )
    return EvidenceNegativeProbeStatement(
        operation=operation,
        sql=sql,
        parameters=parameters,
        candidate_primary_value=str(parameters[metadata.primary_column]),
        expected_message=metadata.expected_message(operation),
    )


def mysql_error_code_message(exc: Exception) -> tuple[int | None, str]:
    current: object | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        args = getattr(current, "args", ())
        if isinstance(args, tuple) and args:
            try:
                code = int(args[0])
            except (TypeError, ValueError):
                code = None
            if code is not None:
                message = str(args[1]) if len(args) > 1 else str(current)
                return code, message
        current = getattr(current, "orig", None)
    return None, str(exc)


def require_1644_guard(exc: Exception, expected_message: str) -> None:
    if type(expected_message) is not str or not expected_message:
        raise EvidenceNegativeProbeContractError(
            "expected guard message must be non-empty exact text"
        )
    code, message = mysql_error_code_message(exc)
    if code != 1644 or expected_message.lower() not in message.lower():
        raise EvidenceNegativeProbeGuardError(
            "negative evidence probe did not return SQLSTATE 45000 / errno 1644; "
            f"code={code}; expected_message={expected_message!r}; message={message}"
        ) from exc


def _mapping_first(result: Any, operation: str) -> dict[str, Any] | None:
    try:
        value = result.mappings().first()
    except Exception as exc:
        raise EvidenceNegativeProbeContractError(
            f"{operation} did not return a SQLAlchemy mapping result"
        ) from exc
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise EvidenceNegativeProbeContractError(
            f"{operation} returned a non-mapping row"
        )
    return dict(value)


def _fetch_baseline(
    connection: Any,
    metadata: EvidenceTableProbeMetadata,
    primary_value: str,
    *,
    lock: bool,
) -> dict[str, Any]:
    columns = ", ".join(_quoted(column) for column in metadata.columns)
    suffix = " FOR UPDATE" if lock else ""
    result = connection.execute(
        text(
            f"/* v2e-negative:baseline:{metadata.table} */\n"
            f"SELECT {columns} FROM {_quoted(metadata.table)} "
            f"WHERE {_quoted(metadata.primary_column)} = :primary_value{suffix}"
        ),
        {"primary_value": primary_value},
    )
    row = _mapping_first(result, f"read {metadata.table} baseline")
    if row is None:
        raise EvidenceNegativeProbeContractError(
            f"{metadata.table} baseline row does not exist"
        )
    return _exact_baseline(metadata, row)


def _count_rows(connection: Any, metadata: EvidenceTableProbeMetadata) -> int:
    result = connection.execute(
        text(
            f"/* v2e-negative:count:{metadata.table} */\n"
            f"SELECT COUNT(*) FROM {_quoted(metadata.table)}"
        )
    )
    try:
        value = result.scalar()
    except Exception as exc:
        raise EvidenceNegativeProbeContractError(
            f"count {metadata.table} did not return a scalar"
        ) from exc
    if type(value) is not int or value < 0:
        raise EvidenceNegativeProbeContractError(
            f"count {metadata.table} returned an invalid value: {value!r}"
        )
    return value


def _count_unique_candidate(
    connection: Any,
    metadata: EvidenceTableProbeMetadata,
    unique_key: tuple[str, ...],
    candidate: Mapping[str, Any],
    ordinal: int,
) -> int:
    conditions: list[str] = []
    params: dict[str, Any] = {}
    for index, column in enumerate(unique_key):
        parameter = f"uk_{ordinal}_{index}"
        conditions.append(f"{_quoted(column)} <=> :{parameter}")
        params[parameter] = candidate[column]
    statement = text(
        f"/* v2e-negative:unique:{metadata.table}:{ordinal} */\n"
        f"SELECT COUNT(*) FROM {_quoted(metadata.table)} WHERE "
        + " AND ".join(conditions)
    )
    result = connection.execute(statement, params)
    try:
        value = result.scalar()
    except Exception as exc:
        raise EvidenceNegativeProbeContractError(
            f"unique preflight {metadata.table} did not return a scalar"
        ) from exc
    if type(value) is not int or value < 0:
        raise EvidenceNegativeProbeContractError(
            f"unique preflight {metadata.table} returned {value!r}"
        )
    return value


def _require_fresh_invalid_candidate(
    connection: Any,
    metadata: EvidenceTableProbeMetadata,
    candidate: Mapping[str, Any],
) -> None:
    for ordinal, unique_key in enumerate(metadata.unique_keys):
        count = _count_unique_candidate(
            connection,
            metadata,
            unique_key,
            candidate,
            ordinal,
        )
        if count != 0:
            raise EvidenceNegativeProbeContractError(
                f"{metadata.table} invalid candidate collides with unique key "
                f"{unique_key!r}"
            )


def _begin(connection: Any):
    begin = getattr(connection, "begin", None)
    if not callable(begin):
        raise EvidenceNegativeProbeContractError(
            "identity-bound connection must expose begin()"
        )
    transaction = begin()
    if not callable(getattr(transaction, "rollback", None)):
        raise EvidenceNegativeProbeContractError(
            "explicit transaction must expose rollback()"
        )
    return transaction


def _read_retention(
    engine: Any,
    metadata: EvidenceTableProbeMetadata,
    primary_value: str,
) -> tuple[int, dict[str, Any]]:
    with engine.connect() as connection:
        transaction = _begin(connection)
        try:
            count = _count_rows(connection, metadata)
            row = _fetch_baseline(
                connection,
                metadata,
                primary_value,
                lock=False,
            )
        finally:
            transaction.rollback()
    return count, row


def _validate_cases(
    cases: Sequence[EvidenceNegativeProbeCase],
) -> tuple[EvidenceNegativeProbeCase, ...]:
    try:
        normalized = tuple(cases)
    except TypeError as exc:
        raise EvidenceNegativeProbeContractError("cases must be a sequence") from exc
    if not normalized:
        raise EvidenceNegativeProbeContractError(
            "at least one negative probe case is required"
        )
    seen: set[tuple[str, str]] = set()
    for case in normalized:
        if type(case) is not EvidenceNegativeProbeCase:
            raise EvidenceNegativeProbeContractError(
                "cases must contain EvidenceNegativeProbeCase values"
            )
        metadata_for_evidence_type(case.evidence_type)
        if type(case.primary_value) is not str or _SHA256_RE.fullmatch(
            case.primary_value
        ) is None:
            raise EvidenceNegativeProbeContractError(
                f"{case.evidence_type} primary_value must be lowercase SHA256"
            )
        identity = (case.evidence_type, case.primary_value)
        if identity in seen:
            raise EvidenceNegativeProbeContractError(
                f"duplicate negative probe case: {identity!r}"
            )
        seen.add(identity)
    return normalized


def _validate_engine(engine: Any) -> None:
    if engine is None or not callable(getattr(engine, "connect", None)):
        raise EvidenceNegativeProbeContractError(
            "an identity-bound engine exposing connect() is required"
        )


def run_negative_probe(
    engine: Any,
    case: EvidenceNegativeProbeCase,
    operation: NegativeProbeOperation,
) -> EvidenceNegativeProbeResult:
    """Execute one negative DML attempt and prove rollback retention."""

    _validate_engine(engine)
    normalized_case = _validate_cases((case,))[0]
    if type(operation) is not NegativeProbeOperation:
        raise EvidenceNegativeProbeContractError(
            "operation must be NegativeProbeOperation"
        )
    metadata = metadata_for_evidence_type(normalized_case.evidence_type)
    guard_error: Exception | None = None

    with engine.connect() as connection:
        transaction = _begin(connection)
        try:
            row_count_before = _count_rows(connection, metadata)
            baseline = _fetch_baseline(
                connection,
                metadata,
                normalized_case.primary_value,
                lock=True,
            )
            statement = build_negative_probe_statement(
                metadata,
                baseline,
                operation,
            )
            if operation is NegativeProbeOperation.INVALID_INSERT:
                _require_fresh_invalid_candidate(
                    connection,
                    metadata,
                    statement.parameters,
                )
            try:
                connection.execute(text(statement.sql), dict(statement.parameters))
            except Exception as exc:
                guard_error = exc
        finally:
            # This rollback is unconditional: an unexpected success must never
            # reach a context manager that could commit it.
            transaction.rollback()

    row_count_after, retained = _read_retention(
        engine,
        metadata,
        normalized_case.primary_value,
    )
    if row_count_after != row_count_before:
        raise EvidenceNegativeProbeRetentionError(
            f"{metadata.table} row count changed after {operation.value}; "
            f"before={row_count_before}, after={row_count_after}"
        )
    if retained != baseline:
        raise EvidenceNegativeProbeRetentionError(
            f"{metadata.table} baseline changed after {operation.value}"
        )
    if operation is NegativeProbeOperation.INVALID_INSERT:
        with engine.connect() as connection:
            transaction = _begin(connection)
            try:
                inserted = connection.execute(
                    text(
                        f"/* v2e-negative:candidate:{metadata.table} */\n"
                        f"SELECT COUNT(*) FROM {_quoted(metadata.table)} WHERE "
                        f"{_quoted(metadata.primary_column)} = :primary_value"
                    ),
                    {"primary_value": statement.candidate_primary_value},
                ).scalar()
            finally:
                transaction.rollback()
        if type(inserted) is not int or inserted != 0:
            raise EvidenceNegativeProbeRetentionError(
                f"{metadata.table} invalid candidate was retained"
            )

    if guard_error is None:
        raise EvidenceNegativeProbeUnexpectedSuccess(
            f"{metadata.table} unexpectedly allowed {operation.value}; "
            "the explicit transaction was rolled back"
        )
    require_1644_guard(guard_error, statement.expected_message)
    return EvidenceNegativeProbeResult(
        evidence_type=metadata.evidence_type,
        table=metadata.table,
        primary_value=normalized_case.primary_value,
        candidate_primary_value=statement.candidate_primary_value,
        operation=operation,
        mysql_errno=1644,
        expected_message=statement.expected_message,
        row_count_before=row_count_before,
        row_count_after=row_count_after,
        baseline_retained=True,
    )


def run_negative_probes(
    engine: Any,
    cases: Sequence[EvidenceNegativeProbeCase],
    *,
    operations: Sequence[NegativeProbeOperation] = ALL_NEGATIVE_PROBE_OPERATIONS,
) -> tuple[EvidenceNegativeProbeResult, ...]:
    """Run an exact, non-empty operation matrix in deterministic case order."""

    _validate_engine(engine)
    normalized_cases = _validate_cases(cases)
    try:
        normalized_operations = tuple(operations)
    except TypeError as exc:
        raise EvidenceNegativeProbeContractError(
            "operations must be a sequence"
        ) from exc
    if not normalized_operations:
        raise EvidenceNegativeProbeContractError(
            "at least one negative probe operation is required"
        )
    if any(type(item) is not NegativeProbeOperation for item in normalized_operations):
        raise EvidenceNegativeProbeContractError(
            "operations must contain NegativeProbeOperation values"
        )
    if len(set(normalized_operations)) != len(normalized_operations):
        raise EvidenceNegativeProbeContractError(
            "negative probe operations cannot be duplicated"
        )
    return tuple(
        run_negative_probe(engine, case, operation)
        for case in normalized_cases
        for operation in normalized_operations
    )


def run_invalid_insert_probes(
    engine: Any,
    cases: Sequence[EvidenceNegativeProbeCase],
) -> tuple[EvidenceNegativeProbeResult, ...]:
    return run_negative_probes(
        engine,
        cases,
        operations=(NegativeProbeOperation.INVALID_INSERT,),
    )


def run_replace_probes(
    engine: Any,
    cases: Sequence[EvidenceNegativeProbeCase],
) -> tuple[EvidenceNegativeProbeResult, ...]:
    return run_negative_probes(
        engine,
        cases,
        operations=(NegativeProbeOperation.REPLACE,),
    )


def run_on_duplicate_key_update_probes(
    engine: Any,
    cases: Sequence[EvidenceNegativeProbeCase],
) -> tuple[EvidenceNegativeProbeResult, ...]:
    return run_negative_probes(
        engine,
        cases,
        operations=(NegativeProbeOperation.ON_DUPLICATE_KEY_UPDATE,),
    )


__all__ = [
    "ALL_NEGATIVE_PROBE_OPERATIONS",
    "CALENDAR_STORAGE_COLUMNS",
    "CASH_STORAGE_COLUMNS",
    "EVIDENCE_TABLE_PROBE_METADATA",
    "EvidenceNegativeProbeCase",
    "EvidenceNegativeProbeContractError",
    "EvidenceNegativeProbeError",
    "EvidenceNegativeProbeGuardError",
    "EvidenceNegativeProbeResult",
    "EvidenceNegativeProbeRetentionError",
    "EvidenceNegativeProbeStatement",
    "EvidenceNegativeProbeUnexpectedSuccess",
    "EvidenceTableProbeMetadata",
    "FILL_STORAGE_COLUMNS",
    "INVALID_HISTORY_ORIGIN",
    "NegativeProbeOperation",
    "ORDER_STORAGE_COLUMNS",
    "QUOTE_STORAGE_COLUMNS",
    "build_invalid_candidate",
    "build_negative_probe_statement",
    "metadata_for_evidence_type",
    "mysql_error_code_message",
    "require_1644_guard",
    "run_invalid_insert_probes",
    "run_negative_probe",
    "run_negative_probes",
    "run_on_duplicate_key_update_probes",
    "run_replace_probes",
]
