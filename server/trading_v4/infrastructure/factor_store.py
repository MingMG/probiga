"""Strict caller-transaction storage for Stage-3 factor artifacts.

All three tables are append-only.  A retry is idempotent only when every
stored column agrees with the normalized command; any identifier or natural
key collision with different content fails closed.  Repository methods never
open, commit, or roll back a transaction.  In particular, a MySQL 1205/1213
ends the caller-owned transaction and must be retried from a fresh transaction;
``run_factor_store_transaction`` is the opt-in transaction-owning boundary for
callers that want that bounded retry policy.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping, TypeVar

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError

from ..domain import DataSourceCertification, FactorDefinition, FeatureVector
from ..domain.enums import ResearchStatus
from ..domain.hashes import canonical_primitive
from ..ports.factor_store import (
    EntityFeatureSnapshotRecord,
    FactorDefinitionRecord,
    FactorStoreAppendResult,
    SourceCertificationRecord,
)


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SUPPORTED_DIALECTS = frozenset({"sqlite", "mysql", "mariadb"})
_SOURCE_REPLAY = frozenset(
    {"PIT_CERTIFIED", "FORWARD_ONLY", "DISPLAY_ONLY", "REPLAY_INELIGIBLE"}
)
_SOURCE_STATUS = frozenset({"PENDING", "PASSED", "FAILED", "REVOKED"})
_PIT_SAFE_REVISION_POLICIES = frozenset(
    {
        "APPEND_ONLY_REVISION_CHAIN",
        "BITEMPORAL_REVISION_CHAIN",
        "IMMUTABLE_EVENT_LOG",
    }
)
_AVAILABILITY = frozenset({"ACTIVE", "DEGRADED", "BLOCKED"})
_RESEARCH = frozenset({"BACKTEST_READY", "FORWARD_ONLY", "DISPLAY_ONLY"})
_QUALITY = frozenset({"PASS", "WARN", "FAIL"})
_FACTOR_ROLES = frozenset(
    {"GATE", "STATE", "ALPHA", "RISK", "COST", "PORTFOLIO", "EXPLANATION"}
)
_MISSING_POLICIES = frozenset({"BLOCK", "PROPAGATE_NULL", "DISPLAY_ONLY"})
_SCOPE_TYPES = frozenset({"MARKET", "SECTOR", "INSTRUMENT", "PORTFOLIO"})
_SNAPSHOT_RUN_STATUSES = frozenset({"RUNNING", "VALIDATING"})
_MYSQL_TRANSIENT_LOCK_CODES = frozenset({1205, 1213})


class FactorStoreError(RuntimeError):
    """Base error for strict Stage-3 persistence."""


class FactorStoreTransactionError(FactorStoreError):
    """The supplied connection is not in a caller-owned transaction."""


class FactorStoreConflictError(FactorStoreError):
    """An immutable identifier or natural key already has other content."""


class FactorStoreIntegrityError(FactorStoreError):
    """A parent or persisted row violates the frozen storage contract."""


T = TypeVar("T")


def _active_connection(connection: Any) -> Any:
    if connection is None or not callable(getattr(connection, "execute", None)):
        raise FactorStoreTransactionError("a SQLAlchemy-like connection is required")
    probe = getattr(connection, "in_transaction", None)
    if not callable(probe):
        raise FactorStoreTransactionError("connection must expose in_transaction()")
    try:
        active = probe()
    except Exception as exc:  # pragma: no cover - defensive boundary
        raise FactorStoreTransactionError("transaction state cannot be inspected") from exc
    if type(active) is not bool or not active:
        raise FactorStoreTransactionError(
            "connection must already be in a caller-owned transaction"
        )
    dialect = str(getattr(getattr(connection, "dialect", None), "name", "")).lower()
    if dialect not in _SUPPORTED_DIALECTS:
        raise FactorStoreTransactionError(f"unsupported factor-store dialect: {dialect}")
    return connection


def _text(value: object, *, field: str, maximum: int, allow_empty: bool = False) -> str:
    if type(value) is not str or value != value.strip():
        raise TypeError(f"{field} must be exact text without surrounding whitespace")
    if not value and not allow_empty:
        raise ValueError(f"{field} must not be empty")
    if len(value) > maximum:
        raise ValueError(f"{field} exceeds {maximum} characters")
    return value


def _sha256(value: object, *, field: str) -> str:
    candidate = _text(value, field=field, maximum=64)
    if _SHA256_RE.fullmatch(candidate) is None:
        raise ValueError(f"{field} must be lowercase 64-character SHA-256 hex")
    return candidate


def _utc(value: object, *, field: str) -> datetime:
    if type(value) is not datetime:
        raise TypeError(f"{field} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _db_time(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _stored_time(value: object, *, field: str) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        try:
            result = datetime.fromisoformat(str(value))
        except (TypeError, ValueError) as exc:
            raise FactorStoreIntegrityError(f"stored {field} is not a datetime") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        return result.replace(tzinfo=timezone.utc)
    return result.astimezone(timezone.utc)


def _canonical_json(value: object, *, field: str, expected_type: type) -> str:
    if not isinstance(value, expected_type):
        raise TypeError(f"{field} must be a {expected_type.__name__}")
    try:
        primitive = canonical_primitive(value)
        return json.dumps(
            primitive,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must contain canonical JSON values") from exc


def _json(value: object, *, field: str, expected_type: type) -> Any:
    try:
        parsed = json.loads(str(value))
    except (TypeError, ValueError) as exc:
        raise FactorStoreIntegrityError(f"stored {field} is invalid JSON") from exc
    if not isinstance(parsed, expected_type):
        raise FactorStoreIntegrityError(
            f"stored {field} must be a {expected_type.__name__}"
        )
    if _canonical_json(parsed, field=field, expected_type=expected_type) != str(value):
        raise FactorStoreIntegrityError(f"stored {field} is not canonical JSON")
    return parsed


def _one_result(result: Any) -> Mapping[str, Any] | None:
    rows = result.mappings().all()
    if len(rows) > 1:
        raise FactorStoreIntegrityError("unique lookup returned multiple rows")
    return None if not rows else dict(rows[0])


def _is_duplicate(error: IntegrityError, *, dialect: str) -> bool:
    original = getattr(error, "orig", None)
    arguments = getattr(original, "args", ())
    return bool(
        (dialect in {"mysql", "mariadb"} and bool(arguments) and arguments[0] == 1062)
        or (
            dialect == "sqlite"
            and (
                "unique constraint failed" in str(original or error).casefold()
                or "primary key" in str(original or error).casefold()
            )
        )
    )


def _mysql_errno(error: BaseException) -> int | None:
    original = getattr(error, "orig", None)
    arguments = getattr(original, "args", ())
    if not arguments:
        return None
    try:
        return int(arguments[0])
    except (TypeError, ValueError):
        return None


def run_factor_store_transaction(
    engine: Any,
    operation: Callable[[Any], T],
    *,
    max_attempts: int = 4,
    base_delay_seconds: float = 0.01,
) -> T:
    """Run one replay-safe unit of work in a fresh transaction per attempt.

    Repository methods remain caller-transaction-only.  This helper is the
    explicit higher boundary for a caller that wants MySQL lock timeout/deadlock
    retries.  ``operation`` must contain all database work that belongs to the
    unit and must not perform non-idempotent external side effects: MySQL rolls
    back the whole transaction on error 1213, so retrying only the failed
    statement would violate the repository contract.
    """

    if not callable(getattr(engine, "begin", None)):
        raise TypeError("engine must expose begin()")
    if not callable(operation):
        raise TypeError("operation must be callable")
    if type(max_attempts) is not int or max_attempts < 1:
        raise ValueError("max_attempts must be a positive integer")
    if isinstance(base_delay_seconds, bool) or not isinstance(
        base_delay_seconds, (int, float)
    ) or base_delay_seconds < 0:
        raise ValueError("base_delay_seconds must be non-negative")

    dialect = str(getattr(getattr(engine, "dialect", None), "name", "")).lower()
    for attempt in range(1, max_attempts + 1):
        try:
            with engine.begin() as connection:
                return operation(connection)
        except DBAPIError as exc:
            errno = _mysql_errno(exc)
            if (
                dialect not in {"mysql", "mariadb"}
                or errno not in _MYSQL_TRANSIENT_LOCK_CODES
                or attempt >= max_attempts
            ):
                raise
            if base_delay_seconds:
                time.sleep(base_delay_seconds * attempt)
    raise AssertionError("factor-store transaction retry exhausted unexpectedly")


def _inserted(result: Any) -> bool:
    if type(result.rowcount) is not int or result.rowcount != 1:
        raise FactorStoreIntegrityError("append did not insert exactly one row")
    return True


def _source_parameters(record: SourceCertificationRecord) -> dict[str, Any]:
    if type(record) is not SourceCertificationRecord:
        raise TypeError("record must be exactly SourceCertificationRecord")
    replay = _text(record.replay_eligibility, field="replay_eligibility", maximum=24)
    status = _text(record.certification_status, field="certification_status", maximum=24)
    availability = _text(record.availability_status, field="availability_status", maximum=24)
    research = _text(record.research_status, field="research_status", maximum=24)
    quality = _text(record.quality_status, field="quality_status", maximum=24)
    if replay not in _SOURCE_REPLAY:
        raise ValueError("unsupported replay_eligibility")
    if status not in _SOURCE_STATUS:
        raise ValueError("unsupported certification_status")
    if availability not in _AVAILABILITY or research not in _RESEARCH or quality not in _QUALITY:
        raise ValueError("unsupported source capability status")
    if replay == "PIT_CERTIFIED" and (
        status != "PASSED"
        or availability != "ACTIVE"
        or research != "BACKTEST_READY"
        or quality != "PASS"
    ):
        raise ValueError("PIT_CERTIFIED source requires fully passed capability")
    if replay != "PIT_CERTIFIED" and research == "BACKTEST_READY":
        raise ValueError("BACKTEST_READY source must be PIT_CERTIFIED")
    if status != "PASSED" and (availability == "ACTIVE" or quality == "PASS"):
        raise ValueError("unpassed source cannot be ACTIVE/PASS")
    contract = canonical_primitive(record.contract)
    if not isinstance(contract, dict):
        raise TypeError("contract must be a mapping")
    revision_policy = _text(
        contract.get("revision_policy"),
        field="contract.revision_policy",
        maximum=80,
    )
    if revision_policy != revision_policy.upper():
        raise ValueError("contract.revision_policy must be uppercase")
    if replay == "PIT_CERTIFIED" and revision_policy not in _PIT_SAFE_REVISION_POLICIES:
        raise ValueError("PIT_CERTIFIED source requires a safe revision policy")
    columns = tuple(
        _text(item, field="knowledge_time_columns item", maximum=120)
        for item in record.knowledge_time_columns
    )
    if len(columns) != len(set(columns)):
        raise ValueError("knowledge_time_columns contains duplicates")
    event_column = _text(
        record.event_time_column,
        field="event_time_column",
        maximum=120,
        allow_empty=True,
    )
    if replay == "PIT_CERTIFIED" and (not event_column or not columns):
        raise ValueError("PIT_CERTIFIED source requires event and knowledge columns")
    valid_from = _utc(record.valid_from, field="valid_from")
    valid_to = None if record.valid_to is None else _utc(record.valid_to, field="valid_to")
    certified_at = _utc(record.certified_at, field="certified_at")
    created_at = _utc(record.created_at, field="created_at")
    if valid_to is not None and valid_to <= valid_from:
        raise ValueError("valid_to must be later than valid_from")
    if certified_at > created_at:
        raise ValueError("certified_at cannot follow created_at")
    return {
        "source_key": _text(record.source_key, field="source_key", maximum=120),
        "certification_version": _text(
            record.certification_version, field="certification_version", maximum=120
        ),
        "source_table": _text(record.source_table, field="source_table", maximum=120),
        "event_time_column": event_column,
        "knowledge_time_columns_json": _canonical_json(
            list(columns), field="knowledge_time_columns", expected_type=list
        ),
        "replay_eligibility": replay,
        "certification_status": status,
        "availability_status": availability,
        "research_status": research,
        "quality_status": quality,
        "valid_from": _db_time(valid_from),
        "valid_to": None if valid_to is None else _db_time(valid_to),
        "contract_json": _canonical_json(contract, field="contract", expected_type=Mapping),
        "evidence_hash": _sha256(record.evidence_hash, field="evidence_hash"),
        "certified_by": _text(record.certified_by, field="certified_by", maximum=160),
        "certified_at": _db_time(certified_at),
        "created_at": _db_time(created_at),
    }


def _source_record(row: Mapping[str, Any]) -> SourceCertificationRecord:
    return SourceCertificationRecord(
        source_key=str(row["source_key"]),
        certification_version=str(row["certification_version"]),
        source_table=str(row["source_table"]),
        event_time_column=str(row["event_time_column"]),
        knowledge_time_columns=tuple(
            _json(row["knowledge_time_columns_json"], field="knowledge_time_columns_json", expected_type=list)
        ),
        replay_eligibility=str(row["replay_eligibility"]),
        certification_status=str(row["certification_status"]),
        availability_status=str(row["availability_status"]),
        research_status=str(row["research_status"]),
        quality_status=str(row["quality_status"]),
        valid_from=_stored_time(row["valid_from"], field="valid_from"),
        valid_to=None if row["valid_to"] is None else _stored_time(row["valid_to"], field="valid_to"),
        contract=_json(row["contract_json"], field="contract_json", expected_type=dict),
        evidence_hash=str(row["evidence_hash"]),
        certified_by=str(row["certified_by"]),
        certified_at=_stored_time(row["certified_at"], field="certified_at"),
        created_at=_stored_time(row["created_at"], field="created_at"),
    )


def _factor_parameters(record: FactorDefinitionRecord) -> dict[str, Any]:
    if type(record) is not FactorDefinitionRecord:
        raise TypeError("record must be exactly FactorDefinitionRecord")
    role = _text(record.factor_role, field="factor_role", maximum=32)
    scope_type = _text(record.scope_type, field="scope_type", maximum=32)
    availability = _text(record.availability_status, field="availability_status", maximum=24)
    research = _text(record.research_status, field="research_status", maximum=24)
    quality = _text(record.quality_status, field="quality_status", maximum=24)
    missing_policy = _text(record.missing_policy, field="missing_policy", maximum=24)
    if role not in _FACTOR_ROLES:
        raise ValueError("unsupported factor_role")
    if scope_type not in _SCOPE_TYPES:
        raise ValueError("unsupported scope_type")
    if availability not in _AVAILABILITY or research not in _RESEARCH or quality not in _QUALITY:
        raise ValueError("unsupported factor capability status")
    if missing_policy not in _MISSING_POLICIES:
        raise ValueError("unsupported missing_policy")
    if type(record.pit_eligible) is not bool:
        raise TypeError("pit_eligible must be exactly bool")
    if type(record.max_age_seconds) is not int or record.max_age_seconds < 1:
        raise ValueError("max_age_seconds must be an exact positive int")
    if record.pit_eligible and research != "BACKTEST_READY":
        raise ValueError("PIT eligible factor must be BACKTEST_READY")
    if availability == "ACTIVE" and quality == "FAIL":
        raise ValueError("ACTIVE factor cannot have FAIL quality")
    sources = tuple(
        _text(item, field="required_source_keys item", maximum=120)
        for item in record.required_source_keys
    )
    if not sources or len(sources) != len(set(sources)):
        raise ValueError("required_source_keys must be non-empty and unique")
    certifications: list[dict[str, str]] = []
    for item in record.required_source_certifications:
        if not isinstance(item, Mapping) or set(item) != {
            "source_key",
            "certification_version",
            "evidence_hash",
        }:
            raise ValueError(
                "required source certification references require exactly "
                "source_key, certification_version and evidence_hash"
            )
        certifications.append(
            {
                "source_key": _text(item["source_key"], field="source_key", maximum=120),
                "certification_version": _text(
                    item["certification_version"],
                    field="certification_version",
                    maximum=120,
                ),
                "evidence_hash": _sha256(item["evidence_hash"], field="evidence_hash"),
            }
        )
    certifications.sort(
        key=lambda item: (item["source_key"], item["certification_version"])
    )
    certification_keys = [
        (item["source_key"], item["certification_version"])
        for item in certifications
    ]
    if len(certification_keys) != len(set(certification_keys)):
        raise ValueError("required_source_certifications contains duplicate keys")
    if {item["source_key"] for item in certifications} != set(sources):
        raise ValueError(
            "required_source_certifications must cover required_source_keys exactly"
        )
    available_at = _utc(record.available_at, field="available_at")
    created_at = _utc(record.created_at, field="created_at")
    if available_at > created_at:
        raise ValueError("available_at cannot follow created_at")
    return {
        "factor_key": _text(record.factor_key, field="factor_key", maximum=120),
        "factor_version": _text(record.factor_version, field="factor_version", maximum=120),
        "feature_set_version": _text(
            record.feature_set_version, field="feature_set_version", maximum=120
        ),
        "factor_role": role,
        "scope_type": scope_type,
        "availability_status": availability,
        "research_status": research,
        "quality_status": quality,
        "missing_policy": missing_policy,
        "pit_eligible": int(record.pit_eligible),
        "max_age_seconds": record.max_age_seconds,
        "required_source_keys_json": _canonical_json(
            list(sources), field="required_source_keys", expected_type=list
        ),
        "required_source_certifications_json": _canonical_json(
            certifications,
            field="required_source_certifications",
            expected_type=list,
        ),
        "formula_json": _canonical_json(record.formula, field="formula", expected_type=Mapping),
        "output_schema_json": _canonical_json(
            record.output_schema, field="output_schema", expected_type=Mapping
        ),
        "definition_hash": _sha256(record.definition_hash, field="definition_hash"),
        "available_at": _db_time(available_at),
        "created_at": _db_time(created_at),
    }


def _factor_record(row: Mapping[str, Any]) -> FactorDefinitionRecord:
    pit = row["pit_eligible"]
    try:
        pit_int = int(pit)
    except (TypeError, ValueError) as exc:
        raise FactorStoreIntegrityError("stored pit_eligible is not boolean") from exc
    if pit_int not in (0, 1):
        raise FactorStoreIntegrityError("stored pit_eligible is not boolean")
    return FactorDefinitionRecord(
        factor_key=str(row["factor_key"]),
        factor_version=str(row["factor_version"]),
        feature_set_version=str(row["feature_set_version"]),
        factor_role=str(row["factor_role"]),
        scope_type=str(row["scope_type"]),
        availability_status=str(row["availability_status"]),
        research_status=str(row["research_status"]),
        quality_status=str(row["quality_status"]),
        missing_policy=str(row["missing_policy"]),
        pit_eligible=bool(pit_int),
        max_age_seconds=int(row["max_age_seconds"]),
        required_source_keys=tuple(
            _json(row["required_source_keys_json"], field="required_source_keys_json", expected_type=list)
        ),
        required_source_certifications=tuple(
            _json(
                row["required_source_certifications_json"],
                field="required_source_certifications_json",
                expected_type=list,
            )
        ),
        formula=_json(row["formula_json"], field="formula_json", expected_type=dict),
        output_schema=_json(row["output_schema_json"], field="output_schema_json", expected_type=dict),
        definition_hash=str(row["definition_hash"]),
        available_at=_stored_time(row["available_at"], field="available_at"),
        created_at=_stored_time(row["created_at"], field="created_at"),
    )


def _snapshot_parameters(record: EntityFeatureSnapshotRecord) -> dict[str, Any]:
    if type(record) is not EntityFeatureSnapshotRecord:
        raise TypeError("record must be exactly EntityFeatureSnapshotRecord")
    scope_type = _text(record.scope_type, field="scope_type", maximum=32)
    if scope_type not in _SCOPE_TYPES:
        raise ValueError("unsupported scope_type")
    if type(record.factor_count) is not int or record.factor_count < 1:
        raise ValueError("factor_count must be an exact positive int")
    if len(record.values) != record.factor_count:
        raise ValueError("factor_count must equal the number of feature values")
    if not record.source_certifications:
        raise ValueError("source_certifications must not be empty")
    if any(not isinstance(item, Mapping) for item in record.source_certifications):
        raise TypeError("source_certifications items must be mappings")
    certifications: list[dict[str, str]] = []
    for item in record.source_certifications:
        if set(item) != {"source_key", "certification_version", "evidence_hash"}:
            raise ValueError(
                "source certification references require exactly source_key, "
                "certification_version and evidence_hash"
            )
        certifications.append(
            {
                "source_key": _text(item["source_key"], field="source_key", maximum=120),
                "certification_version": _text(
                    item["certification_version"],
                    field="certification_version",
                    maximum=120,
                ),
                "evidence_hash": _sha256(item["evidence_hash"], field="evidence_hash"),
            }
        )
    certifications.sort(
        key=lambda item: (item["source_key"], item["certification_version"])
    )
    certification_keys = [
        (item["source_key"], item["certification_version"])
        for item in certifications
    ]
    if len(certification_keys) != len(set(certification_keys)):
        raise ValueError("source_certifications contains duplicate registry keys")
    definitions: list[dict[str, str]] = []
    for item in record.factor_definitions:
        if not isinstance(item, Mapping) or set(item) != {
            "factor_key",
            "factor_version",
            "definition_hash",
        }:
            raise ValueError(
                "factor definition references require exactly factor_key, "
                "factor_version and definition_hash"
            )
        definitions.append(
            {
                "factor_key": _text(item["factor_key"], field="factor_key", maximum=120),
                "factor_version": _text(
                    item["factor_version"], field="factor_version", maximum=120
                ),
                "definition_hash": _sha256(
                    item["definition_hash"], field="definition_hash"
                ),
            }
        )
    definitions.sort(key=lambda item: (item["factor_key"], item["factor_version"]))
    definition_keys = [
        (item["factor_key"], item["factor_version"]) for item in definitions
    ]
    if not definitions or len(definition_keys) != len(set(definition_keys)):
        raise ValueError("factor_definitions must be non-empty with unique keys")
    quality_status = _text(record.quality_status, field="quality_status", maximum=24)
    if quality_status not in _QUALITY:
        raise ValueError("unsupported quality_status")
    cutoff = _utc(record.knowledge_cutoff_at, field="knowledge_cutoff_at")
    computed = _utc(record.computed_at, field="computed_at")
    available = _utc(record.available_at, field="available_at")
    created = _utc(record.created_at, field="created_at")
    if not cutoff <= computed <= available <= created:
        raise ValueError("snapshot timestamps must satisfy cutoff <= computed <= available <= created")
    return {
        "snapshot_id": _sha256(record.snapshot_id, field="snapshot_id"),
        "run_uid": _text(record.run_uid, field="run_uid", maximum=64),
        "scope_type": scope_type,
        "scope_id": _text(record.scope_id, field="scope_id", maximum=160),
        "feature_set_version": _text(
            record.feature_set_version, field="feature_set_version", maximum=120
        ),
        "knowledge_cutoff_at": _db_time(cutoff),
        "computed_at": _db_time(computed),
        "available_at": _db_time(available),
        "factor_count": record.factor_count,
        "values_json": _canonical_json(record.values, field="values", expected_type=Mapping),
        "quality_status": quality_status,
        "quality_json": _canonical_json(record.quality, field="quality", expected_type=Mapping),
        "source_certifications_json": _canonical_json(
            certifications, field="source_certifications", expected_type=list
        ),
        "factor_definitions_json": _canonical_json(
            definitions, field="factor_definitions", expected_type=list
        ),
        "source_manifest_hash": _sha256(record.source_manifest_hash, field="source_manifest_hash"),
        "feature_hash": _sha256(record.feature_hash, field="feature_hash"),
        "created_at": _db_time(created),
    }


def _snapshot_record(row: Mapping[str, Any]) -> EntityFeatureSnapshotRecord:
    try:
        count = int(row["factor_count"])
    except (TypeError, ValueError) as exc:
        raise FactorStoreIntegrityError("stored factor_count is not an integer") from exc
    return EntityFeatureSnapshotRecord(
        snapshot_id=str(row["snapshot_id"]),
        run_uid=str(row["run_uid"]),
        scope_type=str(row["scope_type"]),
        scope_id=str(row["scope_id"]),
        feature_set_version=str(row["feature_set_version"]),
        knowledge_cutoff_at=_stored_time(row["knowledge_cutoff_at"], field="knowledge_cutoff_at"),
        computed_at=_stored_time(row["computed_at"], field="computed_at"),
        available_at=_stored_time(row["available_at"], field="available_at"),
        factor_count=count,
        values=_json(row["values_json"], field="values_json", expected_type=dict),
        quality_status=str(row["quality_status"]),
        quality=_json(row["quality_json"], field="quality_json", expected_type=dict),
        source_certifications=tuple(
            _json(row["source_certifications_json"], field="source_certifications_json", expected_type=list)
        ),
        factor_definitions=tuple(
            _json(
                row["factor_definitions_json"],
                field="factor_definitions_json",
                expected_type=list,
            )
        ),
        source_manifest_hash=str(row["source_manifest_hash"]),
        feature_hash=str(row["feature_hash"]),
        created_at=_stored_time(row["created_at"], field="created_at"),
    )


_SOURCE_INSERT = (
    """INSERT INTO st_data_source_certification_v4 (
        source_key, certification_version, source_table, event_time_column,
        knowledge_time_columns_json, replay_eligibility,
        certification_status, availability_status, research_status,
        quality_status, valid_from, valid_to, contract_json, evidence_hash,
        certified_by, certified_at, created_at
    ) VALUES (
        :source_key, :certification_version, :source_table,
        :event_time_column, :knowledge_time_columns_json,
        :replay_eligibility, :certification_status, :availability_status,
        :research_status, :quality_status, :valid_from, :valid_to,
        :contract_json, :evidence_hash, :certified_by, :certified_at,
        :created_at
    )"""
)
_SOURCE_BY_ID = (
    """SELECT source_key, certification_version, source_table,
              event_time_column, knowledge_time_columns_json,
              replay_eligibility, certification_status, availability_status,
              research_status, quality_status, valid_from, valid_to,
              contract_json, evidence_hash, certified_by, certified_at,
              created_at
       FROM st_data_source_certification_v4
       WHERE source_key = :source_key
         AND certification_version = :certification_version"""
)
_SOURCE_BY_ID_FOR_UPDATE = (
    """SELECT source_key, certification_version, source_table,
              event_time_column, knowledge_time_columns_json,
              replay_eligibility, certification_status, availability_status,
              research_status, quality_status, valid_from, valid_to,
              contract_json, evidence_hash, certified_by, certified_at,
              created_at
       FROM st_data_source_certification_v4
       WHERE source_key = :source_key
         AND certification_version = :certification_version
       FOR UPDATE"""
)
_SOURCE_BY_ID_LOCK_IN_SHARE_MODE = (
    """SELECT source_key, certification_version, source_table,
              event_time_column, knowledge_time_columns_json,
              replay_eligibility, certification_status, availability_status,
              research_status, quality_status, valid_from, valid_to,
              contract_json, evidence_hash, certified_by, certified_at,
              created_at
       FROM st_data_source_certification_v4
       WHERE source_key = :source_key
         AND certification_version = :certification_version
       LOCK IN SHARE MODE"""
)
_FACTOR_INSERT = (
    """INSERT INTO st_factor_definition_v4 (
        factor_key, factor_version, feature_set_version, factor_role,
        scope_type, availability_status, research_status, quality_status,
        missing_policy, pit_eligible, max_age_seconds, required_source_keys_json,
        required_source_certifications_json, formula_json, output_schema_json,
        definition_hash, available_at, created_at
    ) VALUES (
        :factor_key, :factor_version, :feature_set_version, :factor_role,
        :scope_type, :availability_status, :research_status, :quality_status,
        :missing_policy, :pit_eligible, :max_age_seconds, :required_source_keys_json,
        :required_source_certifications_json, :formula_json,
        :output_schema_json, :definition_hash, :available_at, :created_at
    )"""
)
_FACTOR_BY_ID = (
    """SELECT factor_key, factor_version, feature_set_version, factor_role,
              scope_type, availability_status, research_status,
              quality_status, missing_policy, pit_eligible, max_age_seconds,
              required_source_keys_json, required_source_certifications_json,
              formula_json, output_schema_json, definition_hash,
              available_at, created_at
       FROM st_factor_definition_v4
       WHERE factor_key = :factor_key AND factor_version = :factor_version"""
)
_FACTOR_BY_NATURAL_KEY = (
    """SELECT factor_key, factor_version, feature_set_version, factor_role,
              scope_type, availability_status, research_status,
              quality_status, missing_policy, pit_eligible, max_age_seconds,
              required_source_keys_json, required_source_certifications_json,
              formula_json, output_schema_json, definition_hash,
              available_at, created_at
       FROM st_factor_definition_v4
       WHERE factor_key = :factor_key
         AND feature_set_version = :feature_set_version"""
)
_FACTOR_BY_NATURAL_KEY_FOR_UPDATE = (
    """SELECT factor_key, factor_version, feature_set_version, factor_role,
              scope_type, availability_status, research_status,
              quality_status, missing_policy, pit_eligible, max_age_seconds,
              required_source_keys_json, required_source_certifications_json,
              formula_json, output_schema_json, definition_hash,
              available_at, created_at
       FROM st_factor_definition_v4
       WHERE factor_key = :factor_key
         AND feature_set_version = :feature_set_version
       FOR UPDATE"""
)
_FACTOR_BY_ID_FOR_UPDATE = (
    """SELECT factor_key, factor_version, feature_set_version, factor_role,
              scope_type, availability_status, research_status,
              quality_status, missing_policy, pit_eligible, max_age_seconds,
              required_source_keys_json, required_source_certifications_json,
              formula_json, output_schema_json, definition_hash,
              available_at, created_at
       FROM st_factor_definition_v4
       WHERE factor_key = :factor_key AND factor_version = :factor_version
       FOR UPDATE"""
)
_FACTOR_BY_NATURAL_KEY_LOCK_IN_SHARE_MODE = (
    """SELECT factor_key, factor_version, feature_set_version, factor_role,
              scope_type, availability_status, research_status,
              quality_status, missing_policy, pit_eligible, max_age_seconds,
              required_source_keys_json, required_source_certifications_json,
              formula_json, output_schema_json, definition_hash,
              available_at, created_at
       FROM st_factor_definition_v4
       WHERE factor_key = :factor_key
         AND feature_set_version = :feature_set_version
       LOCK IN SHARE MODE"""
)
_FACTOR_BY_ID_LOCK_IN_SHARE_MODE = (
    """SELECT factor_key, factor_version, feature_set_version, factor_role,
              scope_type, availability_status, research_status,
              quality_status, missing_policy, pit_eligible, max_age_seconds,
              required_source_keys_json, required_source_certifications_json,
              formula_json, output_schema_json, definition_hash,
              available_at, created_at
       FROM st_factor_definition_v4
       WHERE factor_key = :factor_key AND factor_version = :factor_version
       LOCK IN SHARE MODE"""
)
_SNAPSHOT_INSERT = (
    """INSERT INTO st_entity_feature_snapshot_v4 (
        snapshot_id, run_uid, scope_type, scope_id, feature_set_version,
        knowledge_cutoff_at, computed_at, available_at, factor_count,
        values_json, quality_status, quality_json,
        source_certifications_json, factor_definitions_json,
        source_manifest_hash, feature_hash, created_at
    ) VALUES (
        :snapshot_id, :run_uid, :scope_type, :scope_id,
        :feature_set_version, :knowledge_cutoff_at, :computed_at,
        :available_at, :factor_count, :values_json, :quality_status,
        :quality_json, :source_certifications_json, :factor_definitions_json,
        :source_manifest_hash, :feature_hash, :created_at
    )"""
)
_SNAPSHOT_BY_ID = (
    """SELECT snapshot_id, run_uid, scope_type, scope_id,
              feature_set_version, knowledge_cutoff_at, computed_at,
              available_at, factor_count, values_json, quality_status,
              quality_json, source_certifications_json,
              factor_definitions_json, source_manifest_hash, feature_hash,
              created_at
       FROM st_entity_feature_snapshot_v4
       WHERE snapshot_id = :snapshot_id"""
)
_SNAPSHOT_BY_NATURAL_KEY = (
    """SELECT snapshot_id, run_uid, scope_type, scope_id,
              feature_set_version, knowledge_cutoff_at, computed_at,
              available_at, factor_count, values_json, quality_status,
              quality_json, source_certifications_json,
              factor_definitions_json, source_manifest_hash, feature_hash,
              created_at
       FROM st_entity_feature_snapshot_v4
       WHERE run_uid = :run_uid AND scope_type = :scope_type
         AND scope_id = :scope_id
         AND feature_set_version = :feature_set_version"""
)
_SNAPSHOT_BY_ID_FOR_UPDATE = (
    """SELECT snapshot_id, run_uid, scope_type, scope_id,
              feature_set_version, knowledge_cutoff_at, computed_at,
              available_at, factor_count, values_json, quality_status,
              quality_json, source_certifications_json,
              factor_definitions_json, source_manifest_hash, feature_hash,
              created_at
       FROM st_entity_feature_snapshot_v4
       WHERE snapshot_id = :snapshot_id
       FOR UPDATE"""
)
_SNAPSHOT_BY_NATURAL_KEY_FOR_UPDATE = (
    """SELECT snapshot_id, run_uid, scope_type, scope_id,
              feature_set_version, knowledge_cutoff_at, computed_at,
              available_at, factor_count, values_json, quality_status,
              quality_json, source_certifications_json,
              factor_definitions_json, source_manifest_hash, feature_hash,
              created_at
       FROM st_entity_feature_snapshot_v4
       WHERE run_uid = :run_uid AND scope_type = :scope_type
         AND scope_id = :scope_id
         AND feature_set_version = :feature_set_version
       FOR UPDATE"""
)
_SNAPSHOT_BY_ID_LOCK_IN_SHARE_MODE = (
    """SELECT snapshot_id, run_uid, scope_type, scope_id,
              feature_set_version, knowledge_cutoff_at, computed_at,
              available_at, factor_count, values_json, quality_status,
              quality_json, source_certifications_json,
              factor_definitions_json, source_manifest_hash, feature_hash,
              created_at
       FROM st_entity_feature_snapshot_v4
       WHERE snapshot_id = :snapshot_id
       LOCK IN SHARE MODE"""
)
_SNAPSHOT_BY_NATURAL_KEY_LOCK_IN_SHARE_MODE = (
    """SELECT snapshot_id, run_uid, scope_type, scope_id,
              feature_set_version, knowledge_cutoff_at, computed_at,
              available_at, factor_count, values_json, quality_status,
              quality_json, source_certifications_json,
              factor_definitions_json, source_manifest_hash, feature_hash,
              created_at
       FROM st_entity_feature_snapshot_v4
       WHERE run_uid = :run_uid AND scope_type = :scope_type
         AND scope_id = :scope_id
         AND feature_set_version = :feature_set_version
       LOCK IN SHARE MODE"""
)
_SNAPSHOT_RUN = (
    """SELECT r.run_uid, r.status, c.knowledge_cutoff_at, c.decision_at,
              c.data_snapshot_hash
       FROM st_decision_run_v4 r
       JOIN st_decision_context_v4 c ON c.context_id = r.context_id
       WHERE r.run_uid = :run_uid"""
)
_SNAPSHOT_RUN_FOR_UPDATE = (
    """SELECT r.run_uid, r.status, c.knowledge_cutoff_at, c.decision_at,
              c.data_snapshot_hash
       FROM st_decision_run_v4 r
       JOIN st_decision_context_v4 c ON c.context_id = r.context_id
       WHERE r.run_uid = :run_uid
       FOR UPDATE"""
)


def _same(expected: Mapping[str, Any], actual: Mapping[str, Any]) -> bool:
    for key, value in expected.items():
        stored = actual.get(key)
        if isinstance(value, datetime):
            if _stored_time(stored, field=key).replace(tzinfo=None) != value:
                return False
        elif value is None:
            if stored is not None:
                return False
        elif isinstance(value, int):
            try:
                if int(stored) != value:
                    return False
            except (TypeError, ValueError):
                return False
        elif str(stored) != str(value):
            return False
    return set(actual) == set(expected)


def _source_from_domain(
    value: DataSourceCertification,
    *,
    source_table: object,
    certified_by: object,
    created_at: object,
) -> SourceCertificationRecord:
    if type(value) is not DataSourceCertification:
        raise TypeError("certification must be exactly DataSourceCertification")
    if source_table is None or certified_by is None or created_at is None:
        raise ValueError(
            "domain source persistence requires explicit source_table, "
            "certified_by and created_at"
        )
    valid_from = value.certified_from or value.available_at
    return SourceCertificationRecord(
        source_key=value.source_key,
        certification_version=value.certification_version,
        source_table=_text(source_table, field="source_table", maximum=120),
        event_time_column=value.event_time_field,
        knowledge_time_columns=(
            value.knowledge_time_field,
            value.ingested_at_field,
        ),
        replay_eligibility=value.replay_eligibility.value,
        certification_status=value.certification_status.value,
        availability_status=value.availability_status.value,
        research_status=value.research_status.value,
        quality_status=value.quality_status.value,
        valid_from=valid_from,
        valid_to=value.valid_until,
        contract=value.as_dict(),
        evidence_hash=value.certification_hash,
        certified_by=_text(certified_by, field="certified_by", maximum=160),
        certified_at=value.assessed_at,
        created_at=_utc(created_at, field="created_at"),
    )


def _factor_from_domain(
    value: FactorDefinition,
    *,
    created_at: object,
    source_certifications: object,
) -> FactorDefinitionRecord:
    if type(value) is not FactorDefinition:
        raise TypeError("definition must be exactly FactorDefinition")
    if created_at is None or source_certifications is None:
        raise ValueError(
            "domain factor persistence requires explicit created_at and "
            "source_certifications"
        )
    try:
        certification_refs = tuple(source_certifications)  # type: ignore[arg-type]
    except TypeError as exc:
        raise TypeError("source_certifications must be iterable") from exc
    # The frozen 006 contract defines PIT eligibility solely in terms of the
    # research lifecycle.  Do not substitute ``actionable`` here: actionable
    # also incorporates availability, quality and display-only policy.
    pit_eligible = value.research_status == ResearchStatus.BACKTEST_READY
    return FactorDefinitionRecord(
        factor_key=value.factor_key,
        factor_version=value.factor_version,
        feature_set_version=value.feature_set_version,
        factor_role=value.role.value,
        scope_type=value.scope_type.value,
        availability_status=value.availability_status.value,
        research_status=value.research_status.value,
        quality_status=value.quality_status.value,
        missing_policy=value.missing_policy,
        pit_eligible=pit_eligible,
        max_age_seconds=value.max_age_seconds,
        required_source_keys=tuple(value.required_source_versions),
        required_source_certifications=certification_refs,
        # Preserve the complete domain contract.  The schema's formula and
        # output columns are audit projections, not lossy alternate models.
        formula=value.as_dict(),
        output_schema={"fields": list(value.output_fields)},
        definition_hash=value.definition_hash,
        available_at=value.available_at,
        created_at=_utc(created_at, field="created_at"),
    )


def _snapshot_from_domain(
    value: FeatureVector,
    *,
    snapshot_id: object,
    run_uid: object,
    knowledge_cutoff_at: object,
    computed_at: object,
    available_at: object,
    source_certifications: object,
    factor_definitions: object,
    created_at: object,
) -> EntityFeatureSnapshotRecord:
    if type(value) is not FeatureVector:
        raise TypeError("feature vector must be exactly FeatureVector")
    required = {
        "snapshot_id": snapshot_id,
        "run_uid": run_uid,
        "knowledge_cutoff_at": knowledge_cutoff_at,
        "computed_at": computed_at,
        "available_at": available_at,
        "source_certifications": source_certifications,
        "factor_definitions": factor_definitions,
        "created_at": created_at,
    }
    missing = tuple(name for name, item in required.items() if item is None)
    if missing:
        raise ValueError(
            "domain feature persistence requires explicit " + ", ".join(missing)
        )
    cutoff = _utc(knowledge_cutoff_at, field="knowledge_cutoff_at")
    if value.knowledge_time > cutoff:
        raise ValueError("feature knowledge_time exceeds knowledge_cutoff_at")
    try:
        certifications = tuple(source_certifications)  # type: ignore[arg-type]
    except TypeError as exc:
        raise TypeError("source_certifications must be iterable") from exc
    try:
        definition_refs = tuple(factor_definitions)  # type: ignore[arg-type]
    except TypeError as exc:
        raise TypeError("factor_definitions must be iterable") from exc
    return EntityFeatureSnapshotRecord(
        snapshot_id=_sha256(snapshot_id, field="snapshot_id"),
        run_uid=_text(run_uid, field="run_uid", maximum=64),
        scope_type=value.scope.scope_type.value,
        scope_id=value.scope.scope_id,
        feature_set_version=value.feature_set_version,
        knowledge_cutoff_at=cutoff,
        computed_at=_utc(computed_at, field="computed_at"),
        available_at=_utc(available_at, field="available_at"),
        factor_count=len(value.values),
        values=value.values,
        quality_status=value.quality_status.value,
        quality={
            "capability_name": value.capability_name,
            "feature_builder_version": value.feature_builder_version,
            "feature_knowledge_time": value.as_dict()["knowledge_time"],
            "missing_fields": list(value.missing_fields),
            "reason_codes": list(value.reason_codes),
            "valid_until": value.as_dict()["valid_until"],
            "source_record_ids": list(value.source_record_ids),
            "source_record_hashes": dict(value.source_record_hashes),
        },
        source_certifications=certifications,
        factor_definitions=definition_refs,
        source_manifest_hash=value.source_manifest_hash,
        feature_hash=value.feature_hash,
        created_at=_utc(created_at, field="created_at"),
    )


class FactorStoreRepository:
    """Connection-explicit implementation of :class:`FactorStorePort`."""

    def get_source_certification(self, connection: Any, source_key: str, certification_version: str) -> SourceCertificationRecord | None:
        connection = _active_connection(connection)
        row = _one_result(connection.execute(text(_SOURCE_BY_ID), {"source_key": _text(source_key, field="source_key", maximum=120), "certification_version": _text(certification_version, field="certification_version", maximum=120)}))
        return None if row is None else _source_record(row)

    def append_source_certification(
        self,
        connection: Any,
        record: SourceCertificationRecord | DataSourceCertification,
        *,
        source_table: str | None = None,
        certified_by: str | None = None,
        created_at: datetime | None = None,
    ) -> FactorStoreAppendResult:
        connection = _active_connection(connection)
        if type(record) is DataSourceCertification:
            record = _source_from_domain(
                record,
                source_table=source_table,
                certified_by=certified_by,
                created_at=created_at,
            )
        elif any(item is not None for item in (source_table, certified_by, created_at)):
            raise ValueError("record persistence does not accept domain metadata keywords")
        values = _source_parameters(record)
        dialect = connection.dialect.name.lower()
        # The first probe must remain a non-locking consistent read.  Under
        # MySQL REPEATABLE READ, FOR UPDATE on a missing unique key takes a gap
        # lock; several first writers would then deadlock while upgrading to
        # INSERT.  The final shared locking read is current but does not force
        # duplicate-key losers to upgrade their compatible shared record locks.
        initial = _one_result(connection.execute(text(_SOURCE_BY_ID), values))
        created = False
        if initial is None:
            try:
                created = _inserted(connection.execute(text(_SOURCE_INSERT), values))
            except IntegrityError as exc:
                if not _is_duplicate(exc, dialect=dialect):
                    raise
        if dialect in {"mysql", "mariadb"}:
            row = _one_result(
                connection.execute(text(_SOURCE_BY_ID_LOCK_IN_SHARE_MODE), values)
            )
        else:
            row = _one_result(connection.execute(text(_SOURCE_BY_ID), values))
        if row is None or not _same(values, row):
            raise FactorStoreConflictError("source certification identity has different immutable content")
        return FactorStoreAppendResult(created=created, record=_source_record(row))

    def get_factor_definition(self, connection: Any, factor_key: str, factor_version: str) -> FactorDefinitionRecord | None:
        connection = _active_connection(connection)
        row = _one_result(connection.execute(text(_FACTOR_BY_ID), {"factor_key": _text(factor_key, field="factor_key", maximum=120), "factor_version": _text(factor_version, field="factor_version", maximum=120)}))
        return None if row is None else _factor_record(row)

    @staticmethod
    def _assert_factor_lineage(
        connection: Any,
        values: Mapping[str, Any],
    ) -> None:
        references = _json(
            values["required_source_certifications_json"],
            field="required_source_certifications_json",
            expected_type=list,
        )
        available_at = values["available_at"]
        factor_research = str(values["research_status"])
        for reference in references:
            # Do not take a next-key lock for a missing parent.  A committed
            # parent is subsequently re-read under a shared record lock so
            # validation is still current rather than based on the RR snapshot.
            row = _one_result(connection.execute(text(_SOURCE_BY_ID), reference))
            if row is not None and connection.dialect.name.lower() in {"mysql", "mariadb"}:
                row = _one_result(
                    connection.execute(
                        text(_SOURCE_BY_ID_LOCK_IN_SHARE_MODE), reference
                    )
                )
            if row is None:
                raise FactorStoreIntegrityError(
                    "factor source certification does not exist"
                )
            if str(row["evidence_hash"]) != reference["evidence_hash"]:
                raise FactorStoreIntegrityError(
                    "factor source certification evidence hash conflicts"
                )
            if (
                str(row["certification_status"]) != "PASSED"
                or str(row["availability_status"]) != "ACTIVE"
                or str(row["quality_status"]) != "PASS"
            ):
                raise FactorStoreIntegrityError(
                    "factor source certification is not healthy"
                )
            valid_from = _stored_time(row["valid_from"], field="valid_from").replace(
                tzinfo=None
            )
            valid_to = (
                None
                if row["valid_to"] is None
                else _stored_time(row["valid_to"], field="valid_to").replace(
                    tzinfo=None
                )
            )
            if available_at < valid_from or (
                valid_to is not None and available_at > valid_to
            ):
                raise FactorStoreIntegrityError(
                    "factor source certification is invalid at availability"
                )
            source_mode = (
                str(row["replay_eligibility"]),
                str(row["research_status"]),
            )
            pit_mode = ("PIT_CERTIFIED", "BACKTEST_READY")
            forward_mode = ("FORWARD_ONLY", "FORWARD_ONLY")
            display_mode = ("DISPLAY_ONLY", "DISPLAY_ONLY")
            if factor_research == "BACKTEST_READY":
                allowed = {pit_mode}
            elif factor_research == "FORWARD_ONLY":
                allowed = {pit_mode, forward_mode}
            else:
                allowed = {pit_mode, forward_mode, display_mode}
            if source_mode not in allowed:
                raise FactorStoreIntegrityError(
                    "factor research mode is incompatible with source certification"
                )

    def append_factor_definition(
        self,
        connection: Any,
        record: FactorDefinitionRecord | FactorDefinition,
        *,
        created_at: datetime | None = None,
        source_certifications: tuple[Mapping[str, Any], ...] | None = None,
    ) -> FactorStoreAppendResult:
        connection = _active_connection(connection)
        if type(record) is FactorDefinition:
            record = _factor_from_domain(
                record,
                created_at=created_at,
                source_certifications=source_certifications,
            )
        elif created_at is not None or source_certifications is not None:
            raise ValueError("record persistence does not accept domain metadata keywords")
        values = _factor_parameters(record)
        self._assert_factor_lineage(connection, values)
        dialect = connection.dialect.name.lower()
        initial_identity = _one_result(
            connection.execute(text(_FACTOR_BY_ID), values)
        )
        initial_natural = _one_result(
            connection.execute(text(_FACTOR_BY_NATURAL_KEY), values)
        )
        created = False
        if initial_identity is None and initial_natural is None:
            try:
                created = _inserted(connection.execute(text(_FACTOR_INSERT), values))
            except IntegrityError as exc:
                if not _is_duplicate(exc, dialect=dialect):
                    raise
        if dialect in {"mysql", "mariadb"}:
            identity = _one_result(
                connection.execute(text(_FACTOR_BY_ID_LOCK_IN_SHARE_MODE), values)
            )
            natural = _one_result(
                connection.execute(
                    text(_FACTOR_BY_NATURAL_KEY_LOCK_IN_SHARE_MODE), values
                )
            )
        else:
            identity = _one_result(connection.execute(text(_FACTOR_BY_ID), values))
            natural = _one_result(
                connection.execute(text(_FACTOR_BY_NATURAL_KEY), values)
            )
        if identity is None or natural is None or not _same(values, identity) or not _same(values, natural):
            raise FactorStoreConflictError("factor definition key has different immutable content")
        return FactorStoreAppendResult(created=created, record=_factor_record(identity))

    def get_feature_snapshot(self, connection: Any, snapshot_id: str) -> EntityFeatureSnapshotRecord | None:
        connection = _active_connection(connection)
        row = _one_result(connection.execute(text(_SNAPSHOT_BY_ID), {"snapshot_id": _sha256(snapshot_id, field="snapshot_id")}))
        return None if row is None else _snapshot_record(row)

    @staticmethod
    def _assert_snapshot_run(
        connection: Any,
        values: Mapping[str, Any],
        *,
        allow_committed_replay: bool = False,
    ) -> None:
        statement = (
            _SNAPSHOT_RUN_FOR_UPDATE
            if connection.dialect.name.lower() in {"mysql", "mariadb"}
            else _SNAPSHOT_RUN
        )
        if statement is _SNAPSHOT_RUN_FOR_UPDATE:
            row = _one_result(connection.execute(text(_SNAPSHOT_RUN_FOR_UPDATE), values))
        else:
            row = _one_result(connection.execute(text(_SNAPSHOT_RUN), values))
        if row is None:
            raise FactorStoreIntegrityError("feature snapshot run does not exist")
        statuses = (
            _SNAPSHOT_RUN_STATUSES | {"COMMITTED"}
            if allow_committed_replay
            else _SNAPSHOT_RUN_STATUSES
        )
        if str(row["status"]) not in statuses:
            raise FactorStoreIntegrityError("feature snapshot run is not in an eligible state")
        cutoff = _stored_time(row["knowledge_cutoff_at"], field="run.knowledge_cutoff_at").replace(tzinfo=None)
        decision = _stored_time(row["decision_at"], field="run.decision_at").replace(tzinfo=None)
        if cutoff != values["knowledge_cutoff_at"]:
            raise FactorStoreIntegrityError("feature snapshot cutoff does not match run context")
        if str(row["data_snapshot_hash"]) != values["source_manifest_hash"]:
            raise FactorStoreIntegrityError(
                "feature snapshot source manifest does not match run context"
            )
        if decision > values["computed_at"]:
            raise FactorStoreIntegrityError("feature snapshot was computed before its decision context")
        quality = _json(
            values["quality_json"], field="quality_json", expected_type=dict
        )
        if "valid_until" not in quality:
            raise FactorStoreIntegrityError(
                "feature snapshot quality must bind valid_until"
            )
        valid_until = _stored_time(
            quality["valid_until"], field="feature.valid_until"
        ).replace(tzinfo=None)
        if decision > valid_until:
            raise FactorStoreIntegrityError(
                "feature snapshot expired before decision_time"
            )

    @staticmethod
    def _assert_snapshot_lineage(
        connection: Any,
        values: Mapping[str, Any],
    ) -> None:
        references = _json(
            values["source_certifications_json"],
            field="source_certifications_json",
            expected_type=list,
        )
        cutoff = values["knowledge_cutoff_at"]
        snapshot_quality = str(values["quality_status"])
        quality_details = _json(
            values["quality_json"],
            field="quality_json",
            expected_type=dict,
        )
        raw_reasons = quality_details.get("reason_codes", [])
        if not isinstance(raw_reasons, list) or any(
            type(item) is not str for item in raw_reasons
        ):
            raise FactorStoreIntegrityError(
                "feature snapshot quality reason_codes must be a string array"
            )
        reason_codes = frozenset(raw_reasons)
        statement = (
            _SOURCE_BY_ID_FOR_UPDATE
            if connection.dialect.name.lower() in {"mysql", "mariadb"}
            else _SOURCE_BY_ID
        )
        for reference in references:
            if statement is _SOURCE_BY_ID_FOR_UPDATE:
                row = _one_result(
                    connection.execute(text(_SOURCE_BY_ID_FOR_UPDATE), reference)
                )
            else:
                row = _one_result(connection.execute(text(_SOURCE_BY_ID), reference))
            if row is None:
                raise FactorStoreIntegrityError(
                    "feature snapshot source certification does not exist"
                )
            if str(row["evidence_hash"]) != reference["evidence_hash"]:
                raise FactorStoreIntegrityError(
                    "feature snapshot source certification evidence hash conflicts"
                )
            if (
                str(row["certification_status"]) != "PASSED"
                or str(row["availability_status"]) != "ACTIVE"
                or str(row["quality_status"]) != "PASS"
            ):
                raise FactorStoreIntegrityError(
                    "feature snapshot source certification is not healthy"
                )
            replay = str(row["replay_eligibility"])
            research = str(row["research_status"])
            if replay == "PIT_CERTIFIED" and research == "BACKTEST_READY":
                pass
            elif replay == "FORWARD_ONLY" and research == "FORWARD_ONLY":
                if snapshot_quality == "PASS":
                    raise FactorStoreIntegrityError(
                        "forward-only source cannot support a PASS feature snapshot"
                    )
                if "SOURCE_FORWARD_ONLY" not in reason_codes:
                    raise FactorStoreIntegrityError(
                        "forward-only source requires SOURCE_FORWARD_ONLY reason"
                    )
            else:
                raise FactorStoreIntegrityError(
                    "display-only or replay-ineligible source cannot support "
                    "a computed feature snapshot"
                )
            valid_from = _stored_time(row["valid_from"], field="valid_from").replace(
                tzinfo=None
            )
            valid_to = (
                None
                if row["valid_to"] is None
                else _stored_time(row["valid_to"], field="valid_to").replace(
                    tzinfo=None
                )
            )
            if cutoff < valid_from or (valid_to is not None and cutoff > valid_to):
                raise FactorStoreIntegrityError(
                    "feature snapshot source certification is invalid at cutoff"
                )

    @staticmethod
    def _assert_snapshot_factors(
        connection: Any,
        values: Mapping[str, Any],
    ) -> None:
        references = _json(
            values["factor_definitions_json"],
            field="factor_definitions_json",
            expected_type=list,
        )
        snapshot_sources = {
            (
                item["source_key"],
                item["certification_version"],
                item["evidence_hash"],
            )
            for item in _json(
                values["source_certifications_json"],
                field="source_certifications_json",
                expected_type=list,
            )
        }
        snapshot_values = _json(
            values["values_json"], field="values_json", expected_type=dict
        )
        expected_outputs: set[str] = set()
        required_sources: set[tuple[str, str, str]] = set()
        snapshot_quality = str(values["quality_status"])
        minimum_max_age: int | None = None
        for reference in references:
            if connection.dialect.name.lower() in {"mysql", "mariadb"}:
                row = _one_result(
                    connection.execute(text(_FACTOR_BY_ID_FOR_UPDATE), reference)
                )
            else:
                row = _one_result(connection.execute(text(_FACTOR_BY_ID), reference))
            if row is None:
                raise FactorStoreIntegrityError(
                    "feature snapshot factor definition does not exist"
                )
            if str(row["definition_hash"]) != reference["definition_hash"]:
                raise FactorStoreIntegrityError(
                    "feature snapshot factor definition hash conflicts"
                )
            if str(row["feature_set_version"]) != values["feature_set_version"]:
                raise FactorStoreIntegrityError(
                    "feature snapshot factor has a different feature set"
                )
            if str(row["scope_type"]) != values["scope_type"]:
                raise FactorStoreIntegrityError(
                    "feature snapshot factor has a different scope type"
                )
            factor_available_at = _stored_time(
                row["available_at"], field="factor.available_at"
            ).replace(tzinfo=None)
            if factor_available_at > values["computed_at"]:
                raise FactorStoreIntegrityError(
                    "feature snapshot factor was unavailable when computed"
                )
            if (
                str(row["availability_status"]) != "ACTIVE"
                or str(row["quality_status"]) != "PASS"
            ):
                raise FactorStoreIntegrityError(
                    "feature snapshot factor definition is not healthy"
                )
            research = str(row["research_status"])
            pit_eligible = bool(int(row["pit_eligible"]))
            if research == "BACKTEST_READY" and pit_eligible:
                pass
            elif (
                snapshot_quality != "PASS"
                and research == "FORWARD_ONLY"
                and not pit_eligible
            ):
                pass
            else:
                raise FactorStoreIntegrityError(
                    "feature snapshot factor research mode is not computationally eligible"
                )
            factor_max_age = int(row["max_age_seconds"])
            if factor_max_age < 1:
                raise FactorStoreIntegrityError(
                    "feature snapshot factor max_age_seconds is invalid"
                )
            minimum_max_age = (
                factor_max_age
                if minimum_max_age is None
                else min(minimum_max_age, factor_max_age)
            )

            output_schema = _json(
                row["output_schema_json"],
                field="factor.output_schema_json",
                expected_type=dict,
            )
            fields = output_schema.get("fields")
            if (
                not isinstance(fields, list)
                or not fields
                or any(type(field) is not str or not field.strip() for field in fields)
                or len(fields) != len(set(fields))
            ):
                raise FactorStoreIntegrityError(
                    "factor output schema requires unique non-empty fields"
                )
            overlap = expected_outputs.intersection(fields)
            if overlap:
                raise FactorStoreIntegrityError(
                    "feature snapshot factors define duplicate output fields"
                )
            expected_outputs.update(fields)

            factor_sources = _json(
                row["required_source_certifications_json"],
                field="factor.required_source_certifications_json",
                expected_type=list,
            )
            for source in factor_sources:
                required_sources.add(
                    (
                        source["source_key"],
                        source["certification_version"],
                        source["evidence_hash"],
                    )
                )

        if set(snapshot_values) != expected_outputs:
            raise FactorStoreIntegrityError(
                "feature snapshot values do not match factor output fields"
            )
        if int(values["factor_count"]) != len(snapshot_values):
            raise FactorStoreIntegrityError(
                "feature snapshot factor_count does not match output values"
            )
        if not required_sources.issubset(snapshot_sources):
            raise FactorStoreIntegrityError(
                "feature snapshot does not cover factor source certifications"
            )
        quality = _json(
            values["quality_json"], field="quality_json", expected_type=dict
        )
        if "feature_knowledge_time" not in quality or "valid_until" not in quality:
            raise FactorStoreIntegrityError(
                "feature snapshot quality must bind knowledge and validity times"
            )
        feature_knowledge_time = _stored_time(
            quality["feature_knowledge_time"], field="feature.knowledge_time"
        ).replace(tzinfo=None)
        valid_until = _stored_time(
            quality["valid_until"], field="feature.valid_until"
        ).replace(tzinfo=None)
        if feature_knowledge_time > values["knowledge_cutoff_at"]:
            raise FactorStoreIntegrityError(
                "feature knowledge time exceeds snapshot cutoff"
            )
        assert minimum_max_age is not None
        if valid_until > feature_knowledge_time + timedelta(
            seconds=minimum_max_age
        ):
            raise FactorStoreIntegrityError(
                "feature validity exceeds registered factor max age"
            )

    def append_feature_snapshot(
        self,
        connection: Any,
        record: EntityFeatureSnapshotRecord | FeatureVector,
        *,
        snapshot_id: str | None = None,
        run_uid: str | None = None,
        knowledge_cutoff_at: datetime | None = None,
        computed_at: datetime | None = None,
        available_at: datetime | None = None,
        source_certifications: tuple[Mapping[str, Any], ...] | None = None,
        factor_definitions: tuple[Mapping[str, Any], ...] | None = None,
        created_at: datetime | None = None,
    ) -> FactorStoreAppendResult:
        connection = _active_connection(connection)
        domain_arguments = (
            snapshot_id,
            run_uid,
            knowledge_cutoff_at,
            computed_at,
            available_at,
            source_certifications,
            factor_definitions,
            created_at,
        )
        if type(record) is FeatureVector:
            record = _snapshot_from_domain(
                record,
                snapshot_id=snapshot_id,
                run_uid=run_uid,
                knowledge_cutoff_at=knowledge_cutoff_at,
                computed_at=computed_at,
                available_at=available_at,
                source_certifications=source_certifications,
                factor_definitions=factor_definitions,
                created_at=created_at,
            )
        elif any(item is not None for item in domain_arguments):
            raise ValueError("record persistence does not accept domain metadata keywords")
        values = _snapshot_parameters(record)
        dialect = connection.dialect.name.lower()
        # As with the source/factor appends, missing identity probes must not
        # acquire RR gap locks.  Existing/replayed rows are locked only after a
        # non-locking probe has established that a candidate exists.
        initial_identity = _one_result(connection.execute(text(_SNAPSHOT_BY_ID), values))
        initial_natural = _one_result(
            connection.execute(text(_SNAPSHOT_BY_NATURAL_KEY), values)
        )
        if (
            (initial_identity is not None or initial_natural is not None)
            and dialect in {"mysql", "mariadb"}
        ):
            identity = _one_result(
                connection.execute(
                    text(_SNAPSHOT_BY_ID_LOCK_IN_SHARE_MODE), values
                )
            )
            natural = _one_result(
                connection.execute(
                    text(_SNAPSHOT_BY_NATURAL_KEY_LOCK_IN_SHARE_MODE), values
                )
            )
        else:
            identity = initial_identity
            natural = initial_natural
        if identity is not None or natural is not None:
            if (
                identity is None
                or natural is None
                or not _same(values, identity)
                or not _same(values, natural)
            ):
                raise FactorStoreConflictError(
                    "feature snapshot key has different immutable content"
                )
            self._assert_snapshot_run(
                connection,
                values,
                allow_committed_replay=True,
            )
            self._assert_snapshot_lineage(connection, values)
            self._assert_snapshot_factors(connection, values)
            return FactorStoreAppendResult(
                created=False,
                record=_snapshot_record(identity),
            )
        self._assert_snapshot_run(connection, values)
        self._assert_snapshot_lineage(connection, values)
        self._assert_snapshot_factors(connection, values)
        try:
            created = _inserted(connection.execute(text(_SNAPSHOT_INSERT), values))
        except IntegrityError as exc:
            if not _is_duplicate(exc, dialect=connection.dialect.name.lower()):
                raise
            created = False
        if dialect in {"mysql", "mariadb"}:
            identity = _one_result(
                connection.execute(
                    text(_SNAPSHOT_BY_ID_LOCK_IN_SHARE_MODE), values
                )
            )
            natural = _one_result(
                connection.execute(
                    text(_SNAPSHOT_BY_NATURAL_KEY_LOCK_IN_SHARE_MODE), values
                )
            )
        else:
            identity = _one_result(connection.execute(text(_SNAPSHOT_BY_ID), values))
            natural = _one_result(
                connection.execute(text(_SNAPSHOT_BY_NATURAL_KEY), values)
            )
        if identity is None or natural is None or not _same(values, identity) or not _same(values, natural):
            raise FactorStoreConflictError("feature snapshot key has different immutable content")
        return FactorStoreAppendResult(created=created, record=_snapshot_record(identity))


# A concise adapter name for dependency wiring; both names are public.
SqlFactorStore = FactorStoreRepository


__all__ = [
    "FactorStoreConflictError",
    "FactorStoreError",
    "FactorStoreIntegrityError",
    "FactorStoreRepository",
    "FactorStoreTransactionError",
    "SqlFactorStore",
    "run_factor_store_transaction",
]
