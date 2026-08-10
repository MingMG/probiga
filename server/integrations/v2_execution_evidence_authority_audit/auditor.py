"""Independent stored-row audit for migration-014 execution authority.

The authority writer and its database triggers are not trusted by this audit.
Every recomputable digest is rebuilt in Python and by MySQL ``SHA2`` from the
stored preimage, every signed envelope is verified with its stored Ed25519
trust key, and all immutable parent/revocation relationships are reconstructed.

The caller owns the SQLAlchemy connection and transaction.  This module opens
no engine and never commits or rolls back.  Its database entry point reads the
five migration-014 tables plus the canonical execution/rule parents in a
fixed order under shared row locks and one repeatable snapshot.
"""

from __future__ import annotations

import base64
import binascii
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import json
from typing import Any

from sqlalchemy import text

try:  # Absence is handled at the receipt boundary and remains fail closed.
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
except ImportError:  # pragma: no cover - only a deliberately minimal install
    InvalidSignature = None  # type: ignore[assignment]
    Ed25519PublicKey = None  # type: ignore[assignment]

from server.integrations.v2_execution_evidence_authority import (
    AuthorityClaim,
    AuthorityDecision,
    AuthorityReceiptReference,
    AuthorityTrustKey,
    AuthorityVerificationLevel,
    SignedAuthorityReceipt,
    build_authority_claim,
    build_instrument_rule_authority_claim,
)
from server.integrations.v2_execution_evidence_audit import (
    auditor as execution_auditor,
)
from server.trading_v2.execution_evidence import (
    AuthorityStatus,
    CanonicalJson,
    FillExecutionEvidence,
    MarketCalendarEvidence,
    QuoteReceiptEvidence,
)


TRUST_KEY_TABLE = "st_execution_authority_trust_key_v2"
RECEIPT_TABLE = "st_execution_authority_receipt_v2"
KEY_REVOCATION_TABLE = "st_execution_authority_key_revocation_v2"
RECEIPT_REVOCATION_TABLE = "st_execution_authority_receipt_revocation_v2"
ATTESTATION_TABLE = "st_execution_authority_attestation_v2"

AUTHORITY_AUDIT_TABLES = (
    TRUST_KEY_TABLE,
    RECEIPT_TABLE,
    KEY_REVOCATION_TABLE,
    RECEIPT_REVOCATION_TABLE,
    ATTESTATION_TABLE,
)

TRUST_KEY_COLUMNS = (
    "source_provider",
    "key_id",
    "key_version",
    "algorithm",
    "public_key",
    "public_key_hash",
    "valid_from",
    "valid_to",
    "registered_at",
)
RECEIPT_COLUMNS = (
    "receipt_id",
    "receipt_hash",
    "claim_hash",
    "evidence_type",
    "evidence_id",
    "source_provider",
    "source_payload_hash",
    "receipt_type",
    "key_id",
    "key_version",
    "replay_nonce",
    "issued_at",
    "expires_at",
    "envelope_json",
    "envelope_hash",
    "status",
    "revoked_at",
    "created_at",
)
KEY_REVOCATION_COLUMNS = (
    "source_provider",
    "key_id",
    "key_version",
    "revoked_at",
    "reason_code",
    "revocation_hash",
    "created_at",
)
RECEIPT_REVOCATION_COLUMNS = (
    "receipt_id",
    "receipt_hash",
    "envelope_hash",
    "revoked_at",
    "reason_code",
    "revocation_hash",
    "created_at",
)
ATTESTATION_COLUMNS = (
    "claim_hash",
    "evidence_type",
    "evidence_id",
    "source_provider",
    "source_payload_hash",
    "receipt_type",
    "receipt_id",
    "receipt_hash",
    "available_at",
    "verifier_id",
    "verifier_version",
    "verified_at",
    "verification_level",
    "receipt_envelope_hash",
    "trust_key_id",
    "trust_key_version",
    "replay_nonce",
    "decision_hash",
    "attestation_hash",
    "created_at",
)

AUTHORITY_TABLE_HASH_ALIASES: Mapping[str, tuple[str, ...]] = {
    TRUST_KEY_TABLE: ("__dbhash_public_key_hash",),
    RECEIPT_TABLE: (
        "__dbhash_envelope_hash",
        "__dbhash_signature_message",
    ),
    KEY_REVOCATION_TABLE: ("__dbhash_revocation_hash",),
    RECEIPT_REVOCATION_TABLE: ("__dbhash_revocation_hash",),
    ATTESTATION_TABLE: (
        "__dbhash_decision_hash",
        "__dbhash_attestation_hash",
    ),
}

_TABLE_COLUMNS: Mapping[str, tuple[str, ...]] = {
    TRUST_KEY_TABLE: TRUST_KEY_COLUMNS,
    RECEIPT_TABLE: RECEIPT_COLUMNS,
    KEY_REVOCATION_TABLE: KEY_REVOCATION_COLUMNS,
    RECEIPT_REVOCATION_TABLE: RECEIPT_REVOCATION_COLUMNS,
    ATTESTATION_TABLE: ATTESTATION_COLUMNS,
}
_TABLE_ORDER_BY: Mapping[str, str] = {
    TRUST_KEY_TABLE: "source_provider, key_id, key_version",
    RECEIPT_TABLE: "receipt_id",
    KEY_REVOCATION_TABLE: "source_provider, key_id, key_version",
    RECEIPT_REVOCATION_TABLE: "receipt_id",
    ATTESTATION_TABLE: "attestation_hash",
}
_EVIDENCE_TYPES = frozenset(
    {"MARKET_CALENDAR", "QUOTE_RECEIPT", "INSTRUMENT_RULE"}
)
_QUOTE_RECEIPT_TYPES = frozenset(
    {"QMT_MINUTE", "QMT_REALTIME", "PUBLIC_CONSENSUS", "OTHER"}
)
_CONSISTENT_ISOLATION_LEVELS = frozenset({"REPEATABLE READ", "SERIALIZABLE"})
_INSTRUMENT_RULE_TABLE = "st_instrument_rule_v2"
_INSTRUMENT_RULE_COLUMNS = (
    "stock_code",
    "rule_version",
    "effective_from",
    "effective_to",
    "security_type",
    "exchange_code",
    "can_buy",
    "first_buy_minimum",
    "buy_lot_size",
    "sell_lot_size",
    "settlement_days",
    "tick_size",
    "limit_ratio",
    "special_treatment",
    "suspended",
    "permission_required",
    "permission_confirmed",
    "fee_profile_version",
    "source_snapshot_hash",
    "created_at",
)


class V2AuthorityStoredRowAuditError(ValueError):
    """A migration-014 authority row cannot be independently reproduced."""


@dataclass(frozen=True, slots=True)
class V2AuthorityStoredRowAuditReport:
    table_counts: tuple[tuple[str, int], ...]
    rows_reconstructed: int
    hashes_verified: int
    signatures_verified: int
    database_sha2_used: bool
    shared_row_locks_used: bool

    @property
    def audit_passed(self) -> bool:
        if type(self.table_counts) is not tuple:
            return False
        counts: dict[str, int] = {}
        for item in self.table_counts:
            if (
                type(item) is not tuple
                or len(item) != 2
                or type(item[0]) is not str
                or type(item[1]) is not int
                or item[1] < 0
                or item[0] in counts
            ):
                return False
            counts[item[0]] = item[1]
        expected_hashes = (
            counts.get(TRUST_KEY_TABLE, 0)
            + (2 * counts.get(RECEIPT_TABLE, 0))
            + counts.get(KEY_REVOCATION_TABLE, 0)
            + counts.get(RECEIPT_REVOCATION_TABLE, 0)
            + (2 * counts.get(ATTESTATION_TABLE, 0))
        )
        return (
            self.database_sha2_used is True
            and self.shared_row_locks_used is True
            and set(counts) == set(AUTHORITY_AUDIT_TABLES)
            and tuple(counts) == AUTHORITY_AUDIT_TABLES
            and type(self.rows_reconstructed) is int
            and self.rows_reconstructed == sum(counts.values())
            and type(self.hashes_verified) is int
            and self.hashes_verified == expected_hashes
            and type(self.signatures_verified) is int
            and self.signatures_verified == counts.get(RECEIPT_TABLE, 0)
        )

    @property
    def production_activation_allowed(self) -> bool:
        # Authority integrity is only one production activation gate.
        return False


@dataclass(frozen=True, slots=True)
class V2AuthorityStoredRowAuditParents:
    """Canonical parent facts used to rebuild every persisted authority claim."""

    calendars: Mapping[str, MarketCalendarEvidence]
    quotes: Mapping[str, QuoteReceiptEvidence]
    fills: Mapping[str, FillExecutionEvidence]
    instrument_rules: Mapping[tuple[str, str, date], CanonicalJson]


# Short aliases keep the API discoverable while retaining the explicit name.
V2AuthorityAuditError = V2AuthorityStoredRowAuditError
V2AuthorityAuditReport = V2AuthorityStoredRowAuditReport


@dataclass(frozen=True, slots=True)
class _TrustRecord:
    value: AuthorityTrustKey
    public_key_hash: str
    registered_at: datetime


@dataclass(frozen=True, slots=True)
class _ReceiptRecord:
    value: SignedAuthorityReceipt
    receipt_hash: str
    claim_hash: str
    evidence_type: str
    evidence_id: str
    source_provider: str
    source_payload_hash: str
    receipt_type: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class _RevocationRecord:
    identity: tuple[str, ...]
    revoked_at: datetime
    created_at: datetime
    revocation_hash: str


def _fail(message: str) -> V2AuthorityStoredRowAuditError:
    return V2AuthorityStoredRowAuditError(message)


def _ascii_text(value: object, name: str, *, maximum: int) -> str:
    if (
        type(value) is not str
        or not value
        or value != value.strip()
        or len(value) > maximum
    ):
        raise _fail(f"{name} must be exact non-blank ASCII text <= {maximum}")
    try:
        value.encode("ascii")
    except UnicodeEncodeError as exc:
        raise _fail(f"{name} must be exact non-blank ASCII text <= {maximum}") from exc
    return value


def _hash(value: object, name: str) -> str:
    result = _ascii_text(value, name, maximum=64)
    if len(result) != 64 or result != result.lower() or any(
        item not in "0123456789abcdef" for item in result
    ):
        raise _fail(f"{name} must be lowercase SHA-256")
    return result


def _utc_datetime(value: object, name: str) -> datetime:
    if type(value) is not datetime:
        raise _fail(f"{name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _public_key_bytes(value: object, name: str) -> bytes:
    if type(value) is bytes:
        result = value
    elif type(value) in {bytearray, memoryview}:
        result = bytes(value)
    else:
        raise _fail(f"{name} must be a 32-byte binary Ed25519 key")
    if len(result) != 32:
        raise _fail(f"{name} must be a 32-byte binary Ed25519 key")
    return result


def _empty_parents() -> V2AuthorityStoredRowAuditParents:
    return V2AuthorityStoredRowAuditParents(
        calendars={},
        quotes={},
        fills={},
        instrument_rules={},
    )


def _parent_object_map(
    value: Mapping[str, Any],
    *,
    expected_type: type[Any],
    identity_attribute: str,
    name: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise _fail(f"{name} parents must be a mapping")
    result: dict[str, Any] = {}
    for raw_key, item in value.items():
        key = _hash(raw_key, f"{name} parent key")
        if type(item) is not expected_type:
            raise _fail(f"{name} parent has an invalid type")
        if getattr(item, identity_attribute) != key:
            raise _fail(f"{name} parent map key differs from its identity")
        if key in result:
            raise _fail(f"duplicate {name} parent: {key}")
        result[key] = item
    return result


def _validated_parents(
    parents: V2AuthorityStoredRowAuditParents | None,
) -> V2AuthorityStoredRowAuditParents:
    if parents is None:
        parents = _empty_parents()
    if type(parents) is not V2AuthorityStoredRowAuditParents:
        raise _fail("authority audit parents have an invalid type")
    calendars = _parent_object_map(
        parents.calendars,
        expected_type=MarketCalendarEvidence,
        identity_attribute="calendar_evidence_id",
        name="calendar evidence",
    )
    quotes = _parent_object_map(
        parents.quotes,
        expected_type=QuoteReceiptEvidence,
        identity_attribute="quote_evidence_id",
        name="quote evidence",
    )
    fills = _parent_object_map(
        parents.fills,
        expected_type=FillExecutionEvidence,
        identity_attribute="fill_execution_evidence_id",
        name="fill evidence",
    )
    if not isinstance(parents.instrument_rules, Mapping):
        raise _fail("instrument-rule parents must be a mapping")
    rules: dict[tuple[str, str, date], CanonicalJson] = {}
    for raw_key, payload in parents.instrument_rules.items():
        if (
            type(raw_key) is not tuple
            or len(raw_key) != 3
            or type(raw_key[0]) is not str
            or not raw_key[0]
            or raw_key[0] != raw_key[0].strip()
            or type(raw_key[1]) is not str
            or not raw_key[1]
            or raw_key[1] != raw_key[1].strip()
            or type(raw_key[2]) is not date
        ):
            raise _fail("instrument-rule parent key is invalid")
        if type(payload) is not CanonicalJson:
            raise _fail("instrument-rule parent payload has an invalid type")
        if raw_key in rules:
            raise _fail(f"duplicate instrument-rule parent: {raw_key!r}")
        rules[raw_key] = payload
    return V2AuthorityStoredRowAuditParents(
        calendars=calendars,
        quotes=quotes,
        fills=fills,
        instrument_rules=rules,
    )


def _required_external_claims(
    parents: V2AuthorityStoredRowAuditParents,
) -> dict[str, AuthorityClaim]:
    result: dict[str, AuthorityClaim] = {}
    for evidence in (*parents.calendars.values(), *parents.quotes.values()):
        if (
            evidence.provenance.authority_status
            is not AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED
        ):
            continue
        try:
            claim = build_authority_claim(evidence)
        except (TypeError, ValueError) as exc:
            raise _fail(
                "external-authority core parent cannot reconstruct its claim"
            ) from exc
        if claim.claim_hash in result:
            raise _fail(
                f"duplicate external-authority core claim: {claim.claim_hash}"
            )
        result[claim.claim_hash] = claim
    return result


def _claim_binding(claim: AuthorityClaim) -> dict[str, Any]:
    if type(claim) is not AuthorityClaim:
        raise _fail("authority parent did not reconstruct an exact claim")
    return {
        "claim_hash": claim.claim_hash,
        "evidence_type": claim.evidence_type,
        "evidence_id": claim.evidence_id,
        "source_provider": claim.source_provider,
        "source_payload_hash": claim.source_payload_hash,
        "receipt_type": claim.receipt_type,
        "receipt_id": claim.receipt_id,
        "receipt_hash": claim.receipt_hash,
        "available_at": _utc_datetime(
            claim.available_at, "authority parent claim.available_at"
        ),
    }


def _fill_rule_parent(
    fill: FillExecutionEvidence,
    parents: V2AuthorityStoredRowAuditParents,
    name: str,
) -> None:
    if fill.provenance.authority_status not in {
        AuthorityStatus.UNKNOWN,
        AuthorityStatus.CONTENT_HASH_ONLY,
    }:
        raise _fail(
            f"{name} instrument-rule parent fill cannot claim external authority"
        )
    key = (
        fill.stock_code,
        fill.instrument_rule_version,
        fill.instrument_rule_effective_from,
    )
    stored = parents.instrument_rules.get(key)
    if stored is None:
        raise _fail(f"{name} references an absent canonical instrument rule")
    if (
        stored.json_text != fill.instrument_rule.json_text
        or stored.payload_hash != fill.instrument_rule.payload_hash
    ):
        raise _fail(
            f"{name} fill snapshot differs from its canonical instrument-rule parent"
        )


def _exact_attestation_parent(
    *,
    stored_binding: Mapping[str, Any],
    parents: V2AuthorityStoredRowAuditParents,
    name: str,
) -> tuple[AuthorityClaim, tuple[str, str]]:
    evidence_type = stored_binding["evidence_type"]
    evidence_id = stored_binding["evidence_id"]
    try:
        if evidence_type == "MARKET_CALENDAR":
            evidence = parents.calendars.get(evidence_id)
            if evidence is None:
                raise _fail(f"{name} references an absent calendar parent")
            if (
                evidence.provenance.authority_status
                is not AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED
            ):
                raise _fail(f"{name} calendar parent is not externally authoritative")
            claim = build_authority_claim(evidence)
            parent_identity = (evidence_type, evidence.calendar_evidence_id)
        elif evidence_type == "QUOTE_RECEIPT":
            evidence = parents.quotes.get(evidence_id)
            if evidence is None:
                raise _fail(f"{name} references an absent quote parent")
            if (
                evidence.provenance.authority_status
                is not AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED
            ):
                raise _fail(f"{name} quote parent is not externally authoritative")
            claim = build_authority_claim(evidence)
            parent_identity = (evidence_type, evidence.quote_evidence_id)
        elif evidence_type == "INSTRUMENT_RULE":
            if not (
                evidence_id
                == stored_binding["source_payload_hash"]
                == stored_binding["receipt_hash"]
            ):
                raise _fail(
                    f"{name} instrument-rule hashes do not bind one exact snapshot"
                )
            reference = AuthorityReceiptReference(
                source_provider=stored_binding["source_provider"],
                receipt_id=stored_binding["receipt_id"],
                receipt_hash=stored_binding["receipt_hash"],
            )
            matches: list[tuple[AuthorityClaim, str]] = []
            for fill_id, fill in parents.fills.items():
                if fill.instrument_rule.payload_hash != evidence_id:
                    continue
                _fill_rule_parent(fill, parents, name)
                candidate = build_instrument_rule_authority_claim(fill, reference)
                if candidate.claim_hash == stored_binding["claim_hash"]:
                    matches.append((candidate, fill_id))
            if len(matches) != 1:
                raise _fail(
                    f"{name} instrument-rule claim must match exactly one locked fill "
                    f"parent; matched {len(matches)}"
                )
            claim, fill_id = matches[0]
            parent_identity = (evidence_type, fill_id)
        else:  # guarded by the stored-row enum check, retained fail closed.
            raise _fail(f"{name}.evidence_type is unsupported")
    except (TypeError, ValueError) as exc:
        if isinstance(exc, V2AuthorityStoredRowAuditError):
            raise
        raise _fail(f"{name} cannot reconstruct its exact authority parent") from exc
    if _claim_binding(claim) != dict(stored_binding):
        raise _fail(f"{name} differs from its recomputed authority parent claim")
    return claim, parent_identity


def _canonical(value: Any) -> Any:
    if type(value) is datetime:
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if type(value) is date:
        return value.isoformat()
    if isinstance(value, Enum):
        return _canonical(value.value)
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise _fail("canonical authority payload keys must be text")
        return {key: _canonical(value[key]) for key in sorted(value)}
    if type(value) in {tuple, list}:
        return [_canonical(item) for item in value]
    if value is None or type(value) in {str, int, bool}:
        return value
    raise _fail(f"unsupported canonical authority value: {type(value).__name__}")


def _encoded(namespace: str, payload: dict[str, Any]) -> bytes:
    try:
        return json.dumps(
            {"namespace": namespace, "payload": _canonical(payload)},
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, RecursionError) as exc:
        if isinstance(exc, V2AuthorityStoredRowAuditError):
            raise
        raise _fail("canonical authority preimage cannot be encoded") from exc


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    return hashlib.sha256(_encoded(namespace, payload)).hexdigest()


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _reject_json_number(value: str) -> None:
    raise ValueError(f"non-integer JSON number is forbidden: {value}")


def _strict_canonical_json(value: object, name: str) -> Any:
    if type(value) is not str or not value or len(value) > 1_000_000:
        raise _fail(f"{name} must be canonical JSON text")
    try:
        parsed = json.loads(
            value,
            parse_float=_reject_json_number,
            parse_constant=_reject_json_number,
            object_pairs_hook=_unique_object,
        )
        normalized = json.dumps(
            _canonical(parsed),
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError, json.JSONDecodeError, RecursionError) as exc:
        if isinstance(exc, V2AuthorityStoredRowAuditError):
            raise
        raise _fail(f"{name} must be strict canonical JSON") from exc
    if value != normalized:
        raise _fail(f"{name} must already be strict canonical JSON")
    return parsed


def _base64url_64(value: object, name: str) -> bytes:
    result = _ascii_text(value, name, maximum=1024)
    try:
        decoded = base64.urlsafe_b64decode(
            result + "=" * ((4 - len(result) % 4) % 4)
        )
    except (ValueError, binascii.Error) as exc:
        raise _fail(f"{name} must be canonical base64url") from exc
    canonical = base64.urlsafe_b64encode(decoded).decode("ascii").rstrip("=")
    if len(decoded) != 64 or canonical != result:
        raise _fail(f"{name} must be a canonical 64-byte base64url signature")
    return decoded


def _expect_hash_triplet(
    *, stored: object, database: object, expected: str, name: str
) -> None:
    if not _hash(stored, name) == _hash(database, f"{name}.database_sha2") == expected:
        raise _fail(f"{name} differs from Python reconstruction or database SHA2")


def _expected_row_keys(table: str) -> set[str]:
    return set(_TABLE_COLUMNS[table]) | set(AUTHORITY_TABLE_HASH_ALIASES[table])


def _validated_rows(
    rows_by_table: Mapping[str, tuple[Mapping[str, Any], ...]],
) -> dict[str, tuple[dict[str, Any], ...]]:
    if not isinstance(rows_by_table, Mapping) or set(rows_by_table) != set(
        AUTHORITY_AUDIT_TABLES
    ):
        raise _fail("authority audit requires exactly the five migration-014 tables")
    validated: dict[str, tuple[dict[str, Any], ...]] = {}
    for table in AUTHORITY_AUDIT_TABLES:
        values = rows_by_table[table]
        if type(values) is not tuple:
            raise _fail(f"{table} rows must be exactly tuple")
        expected = _expected_row_keys(table)
        copied: list[dict[str, Any]] = []
        for number, row in enumerate(values):
            if not isinstance(row, Mapping):
                raise _fail(f"{table}[{number}] must be a mapping row")
            if set(row) != expected:
                raise _fail(f"{table}[{number}] columns differ from the audit SELECT")
            copied.append(dict(row))
        validated[table] = tuple(copied)
    return validated


def _assert_ordered(
    rows: tuple[dict[str, Any], ...],
    columns: tuple[str, ...],
    table: str,
) -> None:
    try:
        keys = tuple(tuple(row[column] for column in columns) for row in rows)
        if keys != tuple(sorted(keys)):
            raise _fail(f"{table} rows are not in deterministic whole-table order")
    except TypeError as exc:
        raise _fail(f"{table} ordering columns cannot be compared") from exc


def _add_unique(seen: set[Any], value: Any, name: str) -> None:
    if value in seen:
        raise _fail(f"duplicate {name}: {value!r}")
    seen.add(value)


def _validate_receipt_type(evidence_type: str, receipt_type: str, name: str) -> None:
    valid = (
        receipt_type == "CALENDAR_OTHER"
        if evidence_type == "MARKET_CALENDAR"
        else receipt_type == "INSTRUMENT_RULE"
        if evidence_type == "INSTRUMENT_RULE"
        else receipt_type in _QUOTE_RECEIPT_TYPES
    )
    if not valid:
        raise _fail(f"{name} is not valid for {evidence_type}")


def _db_time(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _pipe_digest(namespace: str, values: tuple[str, ...]) -> str:
    return hashlib.sha256((namespace + "|" + "|".join(values)).encode("utf-8")).hexdigest()


def _signature_payload(receipt: SignedAuthorityReceipt) -> dict[str, Any]:
    return {
        "claim_hash": receipt.claim_hash,
        "source_provider": receipt.source_provider,
        "receipt_id": receipt.receipt_id,
        "receipt_hash": receipt.receipt_hash,
        "key_id": receipt.key_id,
        "key_version": receipt.key_version,
        "replay_nonce": receipt.replay_nonce,
        "issued_at": receipt.issued_at,
        "expires_at": receipt.expires_at,
    }


def _decision_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "claim_hash": row["claim_hash"],
        "verified": True,
        "verifier_id": row["verifier_id"],
        "verifier_version": row["verifier_version"],
        "verified_at": _utc_datetime(row["verified_at"], "verified_at"),
        "reason_code": "VERIFIED",
        "verification_level": AuthorityVerificationLevel.CRYPTOGRAPHIC,
        "receipt_envelope_hash": row["receipt_envelope_hash"],
        "trust_key_id": row["trust_key_id"],
        "trust_key_version": row["trust_key_version"],
        "replay_nonce": row["replay_nonce"],
    }


def _attestation_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "claim_hash": row["claim_hash"],
        "evidence_type": row["evidence_type"],
        "evidence_id": row["evidence_id"],
        "source_provider": row["source_provider"],
        "source_payload_hash": row["source_payload_hash"],
        "receipt_type": row["receipt_type"],
        "receipt_id": row["receipt_id"],
        "receipt_hash": row["receipt_hash"],
        "available_at": _utc_datetime(row["available_at"], "available_at"),
        "verifier_id": row["verifier_id"],
        "verifier_version": row["verifier_version"],
        "verified_at": _utc_datetime(row["verified_at"], "verified_at"),
        "verification_level": AuthorityVerificationLevel.CRYPTOGRAPHIC,
        "receipt_envelope_hash": row["receipt_envelope_hash"],
        "trust_key_id": row["trust_key_id"],
        "trust_key_version": row["trust_key_version"],
        "replay_nonce": row["replay_nonce"],
        "decision_hash": row["decision_hash"],
    }


def _audit_trust_keys(
    rows: tuple[dict[str, Any], ...],
) -> dict[tuple[str, str, str], _TrustRecord]:
    _assert_ordered(rows, ("source_provider", "key_id", "key_version"), TRUST_KEY_TABLE)
    result: dict[tuple[str, str, str], _TrustRecord] = {}
    public_key_hashes: set[str] = set()
    for number, row in enumerate(rows):
        name = f"{TRUST_KEY_TABLE}[{number}]"
        provider = _ascii_text(row["source_provider"], f"{name}.source_provider", maximum=128)
        key_id = _ascii_text(row["key_id"], f"{name}.key_id", maximum=128)
        key_version = _ascii_text(row["key_version"], f"{name}.key_version", maximum=128)
        identity = (provider, key_id, key_version)
        if identity in result:
            raise _fail(f"duplicate authority trust key: {identity!r}")
        algorithm = _ascii_text(row["algorithm"], f"{name}.algorithm", maximum=16)
        if algorithm != "Ed25519":
            raise _fail(f"{name}.algorithm must be Ed25519")
        public_key = _public_key_bytes(row["public_key"], f"{name}.public_key")
        public_key_hash = hashlib.sha256(public_key).hexdigest()
        _expect_hash_triplet(
            stored=row["public_key_hash"],
            database=row["__dbhash_public_key_hash"],
            expected=public_key_hash,
            name=f"{name}.public_key_hash",
        )
        _add_unique(public_key_hashes, public_key_hash, "authority public key hash")
        valid_from = _utc_datetime(row["valid_from"], f"{name}.valid_from")
        valid_to = (
            None
            if row["valid_to"] is None
            else _utc_datetime(row["valid_to"], f"{name}.valid_to")
        )
        registered_at = _utc_datetime(row["registered_at"], f"{name}.registered_at")
        if valid_to is not None and valid_from >= valid_to:
            raise _fail(f"{name}.valid_to must follow valid_from")
        try:
            reconstructed = AuthorityTrustKey(
                source_provider=provider,
                key_id=key_id,
                key_version=key_version,
                public_key=public_key,
                valid_from=valid_from,
                valid_to=valid_to,
            )
        except (TypeError, ValueError) as exc:
            raise _fail(f"{name} cannot be reconstructed as an Ed25519 trust key") from exc
        result[identity] = _TrustRecord(
            value=reconstructed,
            public_key_hash=public_key_hash,
            registered_at=registered_at,
        )
    return result


def _audit_receipts(
    rows: tuple[dict[str, Any], ...],
    trust_keys: Mapping[tuple[str, str, str], _TrustRecord],
) -> dict[str, _ReceiptRecord]:
    _assert_ordered(rows, ("receipt_id",), RECEIPT_TABLE)
    result: dict[str, _ReceiptRecord] = {}
    claim_hashes: set[str] = set()
    envelope_hashes: set[str] = set()
    replay_bindings: set[tuple[str, str, str, str]] = set()
    receipt_bindings: set[tuple[str, str, str]] = set()
    revocation_bindings: set[tuple[str, str, str]] = set()
    for number, row in enumerate(rows):
        name = f"{RECEIPT_TABLE}[{number}]"
        receipt_id = _ascii_text(row["receipt_id"], f"{name}.receipt_id", maximum=128)
        if receipt_id in result:
            raise _fail(f"duplicate authority receipt_id: {receipt_id!r}")
        receipt_hash = _hash(row["receipt_hash"], f"{name}.receipt_hash")
        claim_hash = _hash(row["claim_hash"], f"{name}.claim_hash")
        evidence_type = _ascii_text(row["evidence_type"], f"{name}.evidence_type", maximum=40)
        if evidence_type not in _EVIDENCE_TYPES:
            raise _fail(f"{name}.evidence_type is unsupported")
        evidence_id = _hash(row["evidence_id"], f"{name}.evidence_id")
        provider = _ascii_text(row["source_provider"], f"{name}.source_provider", maximum=128)
        source_payload_hash = _hash(
            row["source_payload_hash"], f"{name}.source_payload_hash"
        )
        receipt_type = _ascii_text(row["receipt_type"], f"{name}.receipt_type", maximum=40)
        _validate_receipt_type(evidence_type, receipt_type, f"{name}.receipt_type")
        if evidence_type in {"MARKET_CALENDAR", "INSTRUMENT_RULE"} and (
            receipt_hash != source_payload_hash
        ):
            raise _fail(f"{name} receipt_hash must bind the exact source payload")
        key_id = _ascii_text(row["key_id"], f"{name}.key_id", maximum=128)
        key_version = _ascii_text(row["key_version"], f"{name}.key_version", maximum=128)
        replay_nonce = _ascii_text(row["replay_nonce"], f"{name}.replay_nonce", maximum=128)
        issued_at = _utc_datetime(row["issued_at"], f"{name}.issued_at")
        expires_at = _utc_datetime(row["expires_at"], f"{name}.expires_at")
        created_at = _utc_datetime(row["created_at"], f"{name}.created_at")
        if not issued_at <= created_at < expires_at or issued_at >= expires_at:
            raise _fail(f"{name} has an invalid issue/create/expiry chronology")
        status = _ascii_text(row["status"], f"{name}.status", maximum=16)
        if status != "ACTIVE" or row["revoked_at"] is not None:
            raise _fail(f"{name} must retain immutable ACTIVE/null-revoked state")

        parsed_envelope = _strict_canonical_json(
            row["envelope_json"], f"{name}.envelope_json"
        )
        try:
            receipt = SignedAuthorityReceipt.from_json(row["envelope_json"])
        except (TypeError, ValueError) as exc:
            raise _fail(f"{name}.envelope_json cannot be reconstructed") from exc
        if receipt.envelope_json != row["envelope_json"]:
            raise _fail(f"{name}.envelope_json differs from canonical reconstruction")
        bindings = {
            "claim_hash": claim_hash,
            "source_provider": provider,
            "receipt_id": receipt_id,
            "receipt_hash": receipt_hash,
            "key_id": key_id,
            "key_version": key_version,
            "replay_nonce": replay_nonce,
        }
        if any(getattr(receipt, column) != value for column, value in bindings.items()):
            raise _fail(f"{name} columns differ from the signed envelope")
        if (
            _utc_datetime(receipt.issued_at, f"{name}.envelope.issued_at") != issued_at
            or _utc_datetime(receipt.expires_at, f"{name}.envelope.expires_at")
            != expires_at
        ):
            raise _fail(f"{name} timestamps differ from the signed envelope")

        expected_envelope_hash = _digest(
            "trading-v2.canonical-json.v1", {"value": parsed_envelope}
        )
        if receipt.envelope_hash != expected_envelope_hash:
            raise _fail(f"{name}.envelope_hash domain reconstruction drifted")
        _expect_hash_triplet(
            stored=row["envelope_hash"],
            database=row["__dbhash_envelope_hash"],
            expected=expected_envelope_hash,
            name=f"{name}.envelope_hash",
        )

        signature_message = _encoded(
            "trading-v2.authority-receipt-signature.v1",
            _signature_payload(receipt),
        )
        if receipt.signature_message != signature_message:
            raise _fail(f"{name} signature preimage differs from independent reconstruction")
        signature_message_hash = hashlib.sha256(signature_message).hexdigest()
        if _hash(
            row["__dbhash_signature_message"],
            f"{name}.__dbhash_signature_message",
        ) != signature_message_hash:
            raise _fail(f"{name} signature preimage differs from database SHA2")

        key_identity = (provider, key_id, key_version)
        trust = trust_keys.get(key_identity)
        if trust is None:
            raise _fail(f"{name} references an absent authority trust key")
        if trust.registered_at > created_at:
            raise _fail(f"{name} predates trust-key registration")
        if issued_at < trust.value.valid_from or (
            trust.value.valid_to is not None and issued_at >= trust.value.valid_to
        ):
            raise _fail(f"{name} was issued outside trust-key validity")
        if Ed25519PublicKey is None or InvalidSignature is None:
            raise _fail("cryptography with Ed25519 support is mandatory")
        signature = _base64url_64(receipt.signature, f"{name}.signature")
        try:
            verifier = Ed25519PublicKey.from_public_bytes(trust.value.public_key)
            verifier.verify(signature, signature_message)
        except Exception as exc:
            raise _fail(f"{name} Ed25519 signature is invalid") from exc

        _add_unique(claim_hashes, claim_hash, "authority receipt claim_hash")
        _add_unique(envelope_hashes, expected_envelope_hash, "authority envelope_hash")
        _add_unique(
            replay_bindings,
            (provider, key_id, key_version, replay_nonce),
            "authority receipt replay binding",
        )
        _add_unique(
            receipt_bindings,
            (receipt_id, receipt_hash, claim_hash),
            "authority receipt claim binding",
        )
        _add_unique(
            revocation_bindings,
            (receipt_id, receipt_hash, expected_envelope_hash),
            "authority receipt revocation binding",
        )
        result[receipt_id] = _ReceiptRecord(
            value=receipt,
            receipt_hash=receipt_hash,
            claim_hash=claim_hash,
            evidence_type=evidence_type,
            evidence_id=evidence_id,
            source_provider=provider,
            source_payload_hash=source_payload_hash,
            receipt_type=receipt_type,
            created_at=created_at,
        )
    return result


def _audit_key_revocations(
    rows: tuple[dict[str, Any], ...],
    trust_keys: Mapping[tuple[str, str, str], _TrustRecord],
) -> dict[tuple[str, str, str], _RevocationRecord]:
    _assert_ordered(
        rows, ("source_provider", "key_id", "key_version"), KEY_REVOCATION_TABLE
    )
    result: dict[tuple[str, str, str], _RevocationRecord] = {}
    hashes: set[str] = set()
    for number, row in enumerate(rows):
        name = f"{KEY_REVOCATION_TABLE}[{number}]"
        identity = (
            _ascii_text(row["source_provider"], f"{name}.source_provider", maximum=128),
            _ascii_text(row["key_id"], f"{name}.key_id", maximum=128),
            _ascii_text(row["key_version"], f"{name}.key_version", maximum=128),
        )
        if identity in result:
            raise _fail(f"duplicate authority key revocation: {identity!r}")
        parent = trust_keys.get(identity)
        if parent is None:
            raise _fail(f"{name} references an absent trust key")
        revoked_at = _utc_datetime(row["revoked_at"], f"{name}.revoked_at")
        created_at = _utc_datetime(row["created_at"], f"{name}.created_at")
        if revoked_at > created_at or parent.registered_at > created_at:
            raise _fail(f"{name} has invalid parent/revocation chronology")
        reason = _ascii_text(row["reason_code"], f"{name}.reason_code", maximum=64)
        expected = _pipe_digest(
            "trading-v2.authority-key-revocation.v1",
            (*identity, _db_time(revoked_at), reason),
        )
        _expect_hash_triplet(
            stored=row["revocation_hash"],
            database=row["__dbhash_revocation_hash"],
            expected=expected,
            name=f"{name}.revocation_hash",
        )
        _add_unique(hashes, expected, "authority key revocation_hash")
        result[identity] = _RevocationRecord(identity, revoked_at, created_at, expected)
    return result


def _audit_receipt_revocations(
    rows: tuple[dict[str, Any], ...],
    receipts: Mapping[str, _ReceiptRecord],
) -> dict[str, _RevocationRecord]:
    _assert_ordered(rows, ("receipt_id",), RECEIPT_REVOCATION_TABLE)
    result: dict[str, _RevocationRecord] = {}
    hashes: set[str] = set()
    for number, row in enumerate(rows):
        name = f"{RECEIPT_REVOCATION_TABLE}[{number}]"
        receipt_id = _ascii_text(row["receipt_id"], f"{name}.receipt_id", maximum=128)
        if receipt_id in result:
            raise _fail(f"duplicate authority receipt revocation: {receipt_id!r}")
        parent = receipts.get(receipt_id)
        if parent is None:
            raise _fail(f"{name} references an absent receipt")
        receipt_hash = _hash(row["receipt_hash"], f"{name}.receipt_hash")
        envelope_hash = _hash(row["envelope_hash"], f"{name}.envelope_hash")
        if (
            receipt_hash != parent.receipt_hash
            or envelope_hash != parent.value.envelope_hash
        ):
            raise _fail(f"{name} differs from its exact receipt binding")
        revoked_at = _utc_datetime(row["revoked_at"], f"{name}.revoked_at")
        created_at = _utc_datetime(row["created_at"], f"{name}.created_at")
        if revoked_at > created_at or parent.created_at > created_at:
            raise _fail(f"{name} has invalid parent/revocation chronology")
        reason = _ascii_text(row["reason_code"], f"{name}.reason_code", maximum=64)
        expected = _pipe_digest(
            "trading-v2.authority-receipt-revocation.v1",
            (receipt_id, receipt_hash, envelope_hash, _db_time(revoked_at), reason),
        )
        _expect_hash_triplet(
            stored=row["revocation_hash"],
            database=row["__dbhash_revocation_hash"],
            expected=expected,
            name=f"{name}.revocation_hash",
        )
        _add_unique(hashes, expected, "authority receipt revocation_hash")
        result[receipt_id] = _RevocationRecord(
            (receipt_id, receipt_hash, envelope_hash), revoked_at, created_at, expected
        )
    return result


def _audit_attestations(
    rows: tuple[dict[str, Any], ...],
    trust_keys: Mapping[tuple[str, str, str], _TrustRecord],
    receipts: Mapping[str, _ReceiptRecord],
    key_revocations: Mapping[tuple[str, str, str], _RevocationRecord],
    receipt_revocations: Mapping[str, _RevocationRecord],
    parents: V2AuthorityStoredRowAuditParents,
    required_external_claims: Mapping[str, AuthorityClaim],
) -> None:
    _assert_ordered(rows, ("attestation_hash",), ATTESTATION_TABLE)
    claim_hashes: set[str] = set()
    attestation_hashes: set[str] = set()
    attestation_bindings: set[tuple[str, str]] = set()
    replay_bindings: set[tuple[str, str, str, str]] = set()
    parent_bindings: set[tuple[str, str]] = set()
    covered_external_claims: set[str] = set()
    for number, row in enumerate(rows):
        name = f"{ATTESTATION_TABLE}[{number}]"
        claim_hash = _hash(row["claim_hash"], f"{name}.claim_hash")
        evidence_type = _ascii_text(row["evidence_type"], f"{name}.evidence_type", maximum=40)
        if evidence_type not in _EVIDENCE_TYPES:
            raise _fail(f"{name}.evidence_type is unsupported")
        evidence_id = _hash(row["evidence_id"], f"{name}.evidence_id")
        provider = _ascii_text(row["source_provider"], f"{name}.source_provider", maximum=128)
        source_payload_hash = _hash(
            row["source_payload_hash"], f"{name}.source_payload_hash"
        )
        receipt_type = _ascii_text(row["receipt_type"], f"{name}.receipt_type", maximum=40)
        _validate_receipt_type(evidence_type, receipt_type, f"{name}.receipt_type")
        receipt_id = _ascii_text(row["receipt_id"], f"{name}.receipt_id", maximum=128)
        receipt_hash = _hash(row["receipt_hash"], f"{name}.receipt_hash")
        available_at = _utc_datetime(row["available_at"], f"{name}.available_at")
        verifier_id = _ascii_text(row["verifier_id"], f"{name}.verifier_id", maximum=128)
        verifier_version = _ascii_text(
            row["verifier_version"], f"{name}.verifier_version", maximum=128
        )
        verified_at = _utc_datetime(row["verified_at"], f"{name}.verified_at")
        created_at = _utc_datetime(row["created_at"], f"{name}.created_at")
        level = _ascii_text(
            row["verification_level"], f"{name}.verification_level", maximum=32
        )
        if level != AuthorityVerificationLevel.CRYPTOGRAPHIC.value:
            raise _fail(f"{name}.verification_level must be CRYPTOGRAPHIC")
        envelope_hash = _hash(
            row["receipt_envelope_hash"], f"{name}.receipt_envelope_hash"
        )
        key_id = _ascii_text(row["trust_key_id"], f"{name}.trust_key_id", maximum=128)
        key_version = _ascii_text(
            row["trust_key_version"], f"{name}.trust_key_version", maximum=128
        )
        replay_nonce = _ascii_text(row["replay_nonce"], f"{name}.replay_nonce", maximum=128)
        if verified_at < available_at or created_at != verified_at:
            raise _fail(f"{name} has invalid available/verified/created chronology")

        stored_claim_binding = {
            "claim_hash": claim_hash,
            "evidence_type": evidence_type,
            "evidence_id": evidence_id,
            "source_provider": provider,
            "source_payload_hash": source_payload_hash,
            "receipt_type": receipt_type,
            "receipt_id": receipt_id,
            "receipt_hash": receipt_hash,
            "available_at": available_at,
        }
        parent_claim, parent_identity = _exact_attestation_parent(
            stored_binding=stored_claim_binding,
            parents=parents,
            name=name,
        )

        parent = receipts.get(receipt_id)
        if parent is None:
            raise _fail(f"{name} references an absent authority receipt")
        expected_parent_values = {
            "claim_hash": parent.claim_hash,
            "evidence_type": parent.evidence_type,
            "evidence_id": parent.evidence_id,
            "source_provider": parent.source_provider,
            "source_payload_hash": parent.source_payload_hash,
            "receipt_type": parent.receipt_type,
            "receipt_hash": parent.receipt_hash,
            "receipt_envelope_hash": parent.value.envelope_hash,
            "trust_key_id": parent.value.key_id,
            "trust_key_version": parent.value.key_version,
            "replay_nonce": parent.value.replay_nonce,
        }
        actual_parent_values = {
            "claim_hash": claim_hash,
            "evidence_type": evidence_type,
            "evidence_id": evidence_id,
            "source_provider": provider,
            "source_payload_hash": source_payload_hash,
            "receipt_type": receipt_type,
            "receipt_hash": receipt_hash,
            "receipt_envelope_hash": envelope_hash,
            "trust_key_id": key_id,
            "trust_key_version": key_version,
            "replay_nonce": replay_nonce,
        }
        if actual_parent_values != expected_parent_values:
            raise _fail(f"{name} differs from its exact receipt binding")
        if (
            parent.created_at > available_at
            or _utc_datetime(parent.value.issued_at, f"{name}.parent.issued_at")
            > available_at
            or _utc_datetime(parent.value.expires_at, f"{name}.parent.expires_at")
            <= verified_at
        ):
            raise _fail(f"{name} is outside the receipt's attestation window")

        key_identity = (provider, key_id, key_version)
        trust = trust_keys.get(key_identity)
        if trust is None:
            raise _fail(f"{name} references an absent authority trust key")
        issued_at = _utc_datetime(parent.value.issued_at, f"{name}.parent.issued_at")
        if (
            trust.registered_at > available_at
            or issued_at < trust.value.valid_from
            or (trust.value.valid_to is not None and issued_at >= trust.value.valid_to)
        ):
            raise _fail(f"{name} is outside the trust-key attestation window")
        key_revocation = key_revocations.get(key_identity)
        if key_revocation is not None and key_revocation.created_at < created_at:
            raise _fail(f"{name} was recorded after its trust key was revoked")
        receipt_revocation = receipt_revocations.get(receipt_id)
        if receipt_revocation is not None and receipt_revocation.created_at < created_at:
            raise _fail(f"{name} was recorded after its receipt was revoked")

        decision_input = _decision_payload(row)
        try:
            decision = AuthorityDecision(
                claim_hash=claim_hash,
                verified=True,
                verifier_id=verifier_id,
                verifier_version=verifier_version,
                verified_at=verified_at,
                reason_code="VERIFIED",
                verification_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
                receipt_envelope_hash=envelope_hash,
                trust_key_id=key_id,
                trust_key_version=key_version,
                replay_nonce=replay_nonce,
            )
        except (TypeError, ValueError) as exc:
            raise _fail(f"{name} cannot reconstruct its cryptographic decision") from exc
        expected_decision_hash = _digest(
            "trading-v2.authority-decision.v1", decision_input
        )
        if decision.decision_hash != expected_decision_hash:
            raise _fail(f"{name}.decision_hash domain reconstruction drifted")
        _expect_hash_triplet(
            stored=row["decision_hash"],
            database=row["__dbhash_decision_hash"],
            expected=expected_decision_hash,
            name=f"{name}.decision_hash",
        )

        expected_attestation_hash = _digest(
            "trading-v2.authority-attestation.v1", _attestation_payload(row)
        )
        _expect_hash_triplet(
            stored=row["attestation_hash"],
            database=row["__dbhash_attestation_hash"],
            expected=expected_attestation_hash,
            name=f"{name}.attestation_hash",
        )
        _add_unique(claim_hashes, claim_hash, "authority attestation claim_hash")
        _add_unique(
            attestation_hashes,
            expected_attestation_hash,
            "authority attestation_hash",
        )
        _add_unique(
            attestation_bindings,
            (claim_hash, expected_attestation_hash),
            "authority attestation binding",
        )
        _add_unique(
            replay_bindings,
            (provider, key_id, key_version, replay_nonce),
            "authority attestation replay binding",
        )
        _add_unique(
            parent_bindings,
            parent_identity,
            "authority attestation parent coverage",
        )
        if parent_claim.claim_hash in required_external_claims:
            covered_external_claims.add(parent_claim.claim_hash)

    missing = sorted(set(required_external_claims) - covered_external_claims)
    if missing:
        raise _fail(
            "externally authoritative calendar/quote parents are missing exact "
            f"attestations: {missing!r}"
        )


def audit_v2_execution_evidence_authority_rows(
    rows_by_table: Mapping[str, tuple[Mapping[str, Any], ...]],
    *,
    parents: V2AuthorityStoredRowAuditParents | None = None,
    database_sha2_used: bool = True,
    shared_row_locks_used: bool = False,
) -> V2AuthorityStoredRowAuditReport:
    """Audit raw migration-014 rows already carrying DB ``SHA2`` aliases."""

    if type(database_sha2_used) is not bool or database_sha2_used is not True:
        raise _fail("independent database SHA2 recomputation is mandatory")
    if type(shared_row_locks_used) is not bool:
        raise _fail("shared_row_locks_used must be exactly bool")
    rows = _validated_rows(rows_by_table)
    exact_parents = _validated_parents(parents)
    required_external_claims = _required_external_claims(exact_parents)

    trust_keys = _audit_trust_keys(rows[TRUST_KEY_TABLE])
    receipts = _audit_receipts(rows[RECEIPT_TABLE], trust_keys)
    key_revocations = _audit_key_revocations(
        rows[KEY_REVOCATION_TABLE], trust_keys
    )
    receipt_revocations = _audit_receipt_revocations(
        rows[RECEIPT_REVOCATION_TABLE], receipts
    )

    # A later revocation preserves the historical validity of an immutable
    # receipt.  A revocation definitely stored first (strictly earlier at
    # DATETIME(6) precision) makes the receipt impossible under migration 014.
    for receipt_id, receipt in receipts.items():
        key_identity = (
            receipt.source_provider,
            receipt.value.key_id,
            receipt.value.key_version,
        )
        revocation = key_revocations.get(key_identity)
        if revocation is not None and revocation.created_at < receipt.created_at:
            raise _fail(
                f"authority receipt {receipt_id!r} was stored after key revocation"
            )

    _audit_attestations(
        rows[ATTESTATION_TABLE],
        trust_keys,
        receipts,
        key_revocations,
        receipt_revocations,
        exact_parents,
        required_external_claims,
    )
    counts = tuple((table, len(rows[table])) for table in AUTHORITY_AUDIT_TABLES)
    count_map = dict(counts)
    return V2AuthorityStoredRowAuditReport(
        table_counts=counts,
        rows_reconstructed=sum(count_map.values()),
        hashes_verified=(
            count_map[TRUST_KEY_TABLE]
            + 2 * count_map[RECEIPT_TABLE]
            + count_map[KEY_REVOCATION_TABLE]
            + count_map[RECEIPT_REVOCATION_TABLE]
            + 2 * count_map[ATTESTATION_TABLE]
        ),
        signatures_verified=count_map[RECEIPT_TABLE],
        database_sha2_used=True,
        shared_row_locks_used=shared_row_locks_used,
    )


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_json_text(column: str) -> str:
    return f"JSON_QUOTE(CONVERT({column} USING utf8mb4))"


def _sql_json_time(column: str) -> str:
    return (
        "JSON_QUOTE(DATE_FORMAT("
        + column
        + ", '%Y-%m-%dT%H:%i:%s.%f+00:00'))"
    )


def _sql_canonical_hash(
    namespace: str,
    fields: tuple[tuple[str, str], ...],
    alias: str,
) -> str:
    arguments: list[str] = [
        _sql_literal(
            '{"namespace":'
            + json.dumps(namespace, ensure_ascii=False, separators=(",", ":"))
            + ',"payload":{'
        )
    ]
    for index, (key, expression) in enumerate(fields):
        prefix = ("," if index else "") + json.dumps(
            key, ensure_ascii=False, separators=(",", ":")
        ) + ":"
        arguments.extend((_sql_literal(prefix), expression))
    arguments.append(_sql_literal("}}"))
    return (
        "LOWER(SHA2(CAST(CONCAT("
        + ", ".join(arguments)
        + f") AS BINARY), 256)) AS {alias}"
    )


def _sql_pipe_hash(
    namespace: str,
    expressions: tuple[str, ...],
    alias: str,
) -> str:
    arguments: list[str] = [_sql_literal(namespace + "|")]
    for index, expression in enumerate(expressions):
        if index:
            arguments.append(_sql_literal("|"))
        arguments.append(expression)
    return (
        "LOWER(SHA2(CAST(CONCAT("
        + ", ".join(arguments)
        + f") AS BINARY), 256)) AS {alias}"
    )


def _database_hash_expressions(table: str) -> tuple[str, ...]:
    if table == TRUST_KEY_TABLE:
        return ("LOWER(SHA2(public_key, 256)) AS __dbhash_public_key_hash",)
    if table == RECEIPT_TABLE:
        envelope = (
            "LOWER(SHA2(CAST(CONCAT("
            "'{\"namespace\":\"trading-v2.canonical-json.v1\","
            "\"payload\":{\"value\":', "
            "CONVERT(envelope_json USING utf8mb4), '}}'"
            ") AS BINARY), 256)) AS __dbhash_envelope_hash"
        )
        signature = _sql_canonical_hash(
            "trading-v2.authority-receipt-signature.v1",
            (
                ("claim_hash", _sql_json_text("claim_hash")),
                ("expires_at", _sql_json_time("expires_at")),
                ("issued_at", _sql_json_time("issued_at")),
                ("key_id", _sql_json_text("key_id")),
                ("key_version", _sql_json_text("key_version")),
                ("receipt_hash", _sql_json_text("receipt_hash")),
                ("receipt_id", _sql_json_text("receipt_id")),
                ("replay_nonce", _sql_json_text("replay_nonce")),
                ("source_provider", _sql_json_text("source_provider")),
            ),
            "__dbhash_signature_message",
        )
        return envelope, signature
    if table == KEY_REVOCATION_TABLE:
        return (
            _sql_pipe_hash(
                "trading-v2.authority-key-revocation.v1",
                (
                    "source_provider",
                    "key_id",
                    "key_version",
                    "DATE_FORMAT(revoked_at, '%Y-%m-%dT%H:%i:%s.%f+00:00')",
                    "reason_code",
                ),
                "__dbhash_revocation_hash",
            ),
        )
    if table == RECEIPT_REVOCATION_TABLE:
        return (
            _sql_pipe_hash(
                "trading-v2.authority-receipt-revocation.v1",
                (
                    "receipt_id",
                    "receipt_hash",
                    "envelope_hash",
                    "DATE_FORMAT(revoked_at, '%Y-%m-%dT%H:%i:%s.%f+00:00')",
                    "reason_code",
                ),
                "__dbhash_revocation_hash",
            ),
        )
    if table == ATTESTATION_TABLE:
        decision = _sql_canonical_hash(
            "trading-v2.authority-decision.v1",
            (
                ("claim_hash", _sql_json_text("claim_hash")),
                ("reason_code", _sql_literal('"VERIFIED"')),
                ("receipt_envelope_hash", _sql_json_text("receipt_envelope_hash")),
                ("replay_nonce", _sql_json_text("replay_nonce")),
                ("trust_key_id", _sql_json_text("trust_key_id")),
                ("trust_key_version", _sql_json_text("trust_key_version")),
                ("verification_level", _sql_json_text("verification_level")),
                ("verified", _sql_literal("true")),
                ("verified_at", _sql_json_time("verified_at")),
                ("verifier_id", _sql_json_text("verifier_id")),
                ("verifier_version", _sql_json_text("verifier_version")),
            ),
            "__dbhash_decision_hash",
        )
        attestation = _sql_canonical_hash(
            "trading-v2.authority-attestation.v1",
            (
                ("available_at", _sql_json_time("available_at")),
                ("claim_hash", _sql_json_text("claim_hash")),
                ("decision_hash", _sql_json_text("decision_hash")),
                ("evidence_id", _sql_json_text("evidence_id")),
                ("evidence_type", _sql_json_text("evidence_type")),
                ("receipt_envelope_hash", _sql_json_text("receipt_envelope_hash")),
                ("receipt_hash", _sql_json_text("receipt_hash")),
                ("receipt_id", _sql_json_text("receipt_id")),
                ("receipt_type", _sql_json_text("receipt_type")),
                ("replay_nonce", _sql_json_text("replay_nonce")),
                ("source_payload_hash", _sql_json_text("source_payload_hash")),
                ("source_provider", _sql_json_text("source_provider")),
                ("trust_key_id", _sql_json_text("trust_key_id")),
                ("trust_key_version", _sql_json_text("trust_key_version")),
                ("verification_level", _sql_json_text("verification_level")),
                ("verified_at", _sql_json_time("verified_at")),
                ("verifier_id", _sql_json_text("verifier_id")),
                ("verifier_version", _sql_json_text("verifier_version")),
            ),
            "__dbhash_attestation_hash",
        )
        return decision, attestation
    raise _fail(f"unknown authority audit table: {table}")


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


def _require_consistent_isolation(connection: Any) -> str:
    probe = getattr(connection, "get_isolation_level", None)
    if not callable(probe):
        raise _fail("connection must expose get_isolation_level()")
    try:
        raw = probe()
    except Exception as exc:
        raise _fail("transaction isolation level cannot be inspected") from exc
    if type(raw) is not str or not raw.strip():
        raise _fail("transaction isolation level must be exact text")
    normalized = " ".join(
        raw.upper().replace("_", " ").replace("-", " ").split()
    )
    if normalized not in _CONSISTENT_ISOLATION_LEVELS:
        raise _fail(
            "authority database audit requires REPEATABLE READ or SERIALIZABLE "
            "transaction isolation"
        )
    return normalized


def _database_boolean(value: object, name: str) -> bool:
    if type(value) is bool:
        return value
    if type(value) is int and value in {0, 1}:
        return bool(value)
    raise _fail(f"{name} must be a database boolean")


def _database_decimal_text(value: object, scale: int, name: str) -> str:
    if type(value) is not Decimal or not value.is_finite():
        raise _fail(f"{name} must be an exact finite Decimal")
    quantum = Decimal(1).scaleb(-scale)
    try:
        quantized = value.quantize(quantum)
    except InvalidOperation as exc:
        raise _fail(f"{name} cannot be quantized") from exc
    if quantized != value:
        raise _fail(f"{name} exceeds scale {scale}")
    return format(quantized, f".{scale}f")


def _instrument_rule_parent(
    row: Mapping[str, Any], number: int
) -> tuple[tuple[str, str, date], CanonicalJson]:
    name = f"{_INSTRUMENT_RULE_TABLE}[{number}]"
    if set(row) != set(_INSTRUMENT_RULE_COLUMNS):
        raise _fail(f"{name} columns differ from the canonical rule SELECT")
    try:
        stock_code = execution_auditor._text_value(  # noqa: SLF001
            row["stock_code"], f"{name}.stock_code"
        )
        rule_version = execution_auditor._text_value(  # noqa: SLF001
            row["rule_version"], f"{name}.rule_version"
        )
        effective_from = execution_auditor._date_value(  # noqa: SLF001
            row["effective_from"], f"{name}.effective_from"
        )
        value = CanonicalJson.from_value(
            {
                "buy_lot_size": execution_auditor._int_value(  # noqa: SLF001
                    row["buy_lot_size"], f"{name}.buy_lot_size", minimum=1
                ),
                "can_buy": _database_boolean(row["can_buy"], f"{name}.can_buy"),
                "created_at": execution_auditor._datetime_value(  # noqa: SLF001
                    row["created_at"], f"{name}.created_at"
                ),
                "effective_from": effective_from,
                "effective_to": (
                    None
                    if row["effective_to"] is None
                    else execution_auditor._date_value(  # noqa: SLF001
                        row["effective_to"], f"{name}.effective_to"
                    )
                ),
                "exchange_code": execution_auditor._text_value(  # noqa: SLF001
                    row["exchange_code"], f"{name}.exchange_code"
                ),
                "fee_profile_version": execution_auditor._text_value(  # noqa: SLF001
                    row["fee_profile_version"], f"{name}.fee_profile_version"
                ),
                "first_buy_minimum": execution_auditor._int_value(  # noqa: SLF001
                    row["first_buy_minimum"],
                    f"{name}.first_buy_minimum",
                    minimum=1,
                ),
                "limit_ratio": (
                    None
                    if row["limit_ratio"] is None
                    else _database_decimal_text(
                        row["limit_ratio"], 8, f"{name}.limit_ratio"
                    )
                ),
                "permission_confirmed": _database_boolean(
                    row["permission_confirmed"], f"{name}.permission_confirmed"
                ),
                "permission_required": execution_auditor._text_value(  # noqa: SLF001
                    row["permission_required"], f"{name}.permission_required"
                ),
                "rule_version": rule_version,
                "security_type": execution_auditor._text_value(  # noqa: SLF001
                    row["security_type"], f"{name}.security_type"
                ),
                "sell_lot_size": execution_auditor._int_value(  # noqa: SLF001
                    row["sell_lot_size"], f"{name}.sell_lot_size", minimum=1
                ),
                "settlement_days": execution_auditor._int_value(  # noqa: SLF001
                    row["settlement_days"], f"{name}.settlement_days"
                ),
                "source_snapshot_hash": execution_auditor._hash_value(  # noqa: SLF001
                    row["source_snapshot_hash"], f"{name}.source_snapshot_hash"
                ),
                "special_treatment": _database_boolean(
                    row["special_treatment"], f"{name}.special_treatment"
                ),
                "stock_code": stock_code,
                "suspended": _database_boolean(
                    row["suspended"], f"{name}.suspended"
                ),
                "tick_size": _database_decimal_text(
                    row["tick_size"], 6, f"{name}.tick_size"
                ),
            }
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2AuthorityStoredRowAuditError):
            raise
        raise _fail(f"{name} cannot reconstruct its canonical snapshot") from exc
    return (stock_code, rule_version, effective_from), value


def _load_authority_parents(connection: Any) -> V2AuthorityStoredRowAuditParents:
    core_rows: dict[str, tuple[Mapping[str, Any], ...]] = {}
    for table, columns, order_by in execution_auditor._TABLES:  # noqa: SLF001
        hash_expressions = tuple(
            execution_auditor._db_hash_expression(  # noqa: SLF001
                json_column, hash_column
            )
            for json_column, hash_column in execution_auditor.EVIDENCE_JSON_HASH_COLUMNS[
                table
            ]
        )
        result = connection.execute(
            text(
                f"/* v2e:authority_parent_{table} */\n"
                f"SELECT {', '.join((*columns, *hash_expressions))} FROM {table} "
                f"ORDER BY {order_by} LOCK IN SHARE MODE"
            )
        )
        core_rows[table] = _all_mappings(result, table)

    try:
        execution_auditor.audit_v2_execution_evidence_rows(
            core_rows,
            database_sha2_used=True,
            shared_row_locks_used=True,
        )
        payloads: dict[str, tuple[dict[str, CanonicalJson], ...]] = {}
        for table, _, _ in execution_auditor._TABLES:  # noqa: SLF001
            payloads[table] = tuple(
                execution_auditor._canonical_payloads(  # noqa: SLF001
                    table, row, index
                )
                for index, row in enumerate(core_rows[table])
            )
        calendar_table = "st_market_calendar_evidence_v2"
        calendars_tuple = tuple(
            execution_auditor._calendar(  # noqa: SLF001
                row, payloads[calendar_table][index], index
            )
            for index, row in enumerate(core_rows[calendar_table])
        )
        calendars = execution_auditor._unique_map(  # noqa: SLF001
            calendars_tuple, "calendar_evidence_id", "calendar evidence"
        )
        quote_table = "st_quote_receipt_evidence_v2"
        quotes_tuple = tuple(
            execution_auditor._quote(  # noqa: SLF001
                row, payloads[quote_table][index], index
            )
            for index, row in enumerate(core_rows[quote_table])
        )
        quotes = execution_auditor._unique_map(  # noqa: SLF001
            quotes_tuple, "quote_evidence_id", "quote evidence"
        )
        fill_table = "st_fill_execution_evidence_v2"
        fills_tuple = tuple(
            execution_auditor._fill(  # noqa: SLF001
                row,
                payloads[fill_table][index],
                index,
                calendars,
                quotes,
            )
            for index, row in enumerate(core_rows[fill_table])
        )
        fills = execution_auditor._unique_map(  # noqa: SLF001
            fills_tuple, "fill_execution_evidence_id", "fill evidence"
        )
    except (KeyError, TypeError, ValueError) as exc:
        if isinstance(exc, V2AuthorityStoredRowAuditError):
            raise
        raise _fail("canonical execution parents failed independent reconstruction") from exc

    rule_result = connection.execute(
        text(
            f"/* v2e:authority_parent_{_INSTRUMENT_RULE_TABLE} */\n"
            f"SELECT {', '.join(_INSTRUMENT_RULE_COLUMNS)} "
            f"FROM {_INSTRUMENT_RULE_TABLE} "
            "ORDER BY stock_code, rule_version, effective_from "
            "LOCK IN SHARE MODE"
        )
    )
    rule_rows = _all_mappings(rule_result, _INSTRUMENT_RULE_TABLE)
    rules: dict[tuple[str, str, date], CanonicalJson] = {}
    ordered_rule_keys: list[tuple[str, str, date]] = []
    for number, row in enumerate(rule_rows):
        key, payload = _instrument_rule_parent(row, number)
        if key in rules:
            raise _fail(f"duplicate canonical instrument-rule parent: {key!r}")
        ordered_rule_keys.append(key)
        rules[key] = payload
    if ordered_rule_keys != sorted(ordered_rule_keys):
        raise _fail("canonical instrument-rule parents are not deterministically ordered")
    return V2AuthorityStoredRowAuditParents(
        calendars=calendars,
        quotes=quotes,
        fills=fills,
        instrument_rules=rules,
    )


def audit_v2_execution_evidence_authority_database(
    connection: Any,
) -> V2AuthorityStoredRowAuditReport:
    """Audit all migration-014 rows on one caller-owned transaction."""

    if connection is None or not callable(getattr(connection, "execute", None)):
        raise _fail("a SQLAlchemy-like connection is required")
    in_transaction = getattr(connection, "in_transaction", None)
    if not callable(in_transaction) or in_transaction() is not True:
        raise _fail("connection must already be in a transaction")
    _require_consistent_isolation(connection)

    rows_by_table: dict[str, tuple[Mapping[str, Any], ...]] = {}
    for table in AUTHORITY_AUDIT_TABLES:
        select_columns = ", ".join(
            (*_TABLE_COLUMNS[table], *_database_hash_expressions(table))
        )
        result = connection.execute(
            text(
                f"/* v2e:authority_audit_{table} */\n"
                f"SELECT {select_columns} FROM {table} "
                f"ORDER BY {_TABLE_ORDER_BY[table]} LOCK IN SHARE MODE"
            )
        )
        rows_by_table[table] = _all_mappings(result, table)
    parents = _load_authority_parents(connection)
    return audit_v2_execution_evidence_authority_rows(
        rows_by_table,
        parents=parents,
        database_sha2_used=True,
        shared_row_locks_used=True,
    )


__all__ = [
    "ATTESTATION_COLUMNS",
    "AUTHORITY_AUDIT_TABLES",
    "AUTHORITY_TABLE_HASH_ALIASES",
    "KEY_REVOCATION_COLUMNS",
    "RECEIPT_COLUMNS",
    "RECEIPT_REVOCATION_COLUMNS",
    "TRUST_KEY_COLUMNS",
    "V2AuthorityAuditError",
    "V2AuthorityAuditReport",
    "V2AuthorityStoredRowAuditError",
    "V2AuthorityStoredRowAuditParents",
    "V2AuthorityStoredRowAuditReport",
    "audit_v2_execution_evidence_authority_database",
    "audit_v2_execution_evidence_authority_rows",
]
