"""Fail-closed operational writes for the migration-014 authority registry.

The boundary accepts only a caller-owned connection already in a transaction.
It neither opens an engine nor commits or rolls back.  Every conflict must
escape the caller's transaction; a failed INSERT is never followed by a query.
"""

from __future__ import annotations

import base64
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import hashlib
from typing import Any, Mapping

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from server.integrations.v2_execution_evidence_authority import (
    AuthorityClaim,
    AuthorityTrustKey,
    SignedAuthorityReceipt,
)
from server.trading_v2.execution_evidence_schema_gate import (
    V2EvidenceMaintenanceFenceError,
    assert_v2_evidence_maintenance_fence_inactive,
)


_EVIDENCE_RECEIPT_TYPES = {
    "MARKET_CALENDAR": frozenset({"CALENDAR_OTHER"}),
    "QUOTE_RECEIPT": frozenset(
        {"QMT_MINUTE", "QMT_REALTIME", "PUBLIC_CONSENSUS", "OTHER"}
    ),
    "INSTRUMENT_RULE": frozenset({"INSTRUMENT_RULE"}),
}


class AuthorityRegistryError(RuntimeError):
    """Base failure at the controlled registry boundary."""


class AuthorityRegistryTransactionError(AuthorityRegistryError):
    """The supplied object is not an active caller-owned transaction."""


class AuthorityRegistryValidationError(AuthorityRegistryError, ValueError):
    """Caller input or stored parent state is not canonical and safe."""


class AuthorityRegistryConflictError(AuthorityRegistryError):
    """An immutable identity is already bound to different content."""


class AuthorityRegistryWriteStatus(str, Enum):
    INSERTED = "INSERTED"
    IDEMPOTENT = "IDEMPOTENT"


@dataclass(frozen=True, slots=True)
class AuthorityRegistryWriteResult:
    status: AuthorityRegistryWriteStatus
    operation: str
    identity: str
    content_hash: str
    database_owned_at: datetime


def _text(value: object, name: str, *, maximum: int) -> str:
    if type(value) is not str:
        raise AuthorityRegistryValidationError(f"{name} must be exactly str")
    normalized = value.strip()
    if not normalized or len(normalized) > maximum:
        raise AuthorityRegistryValidationError(f"{name} is invalid")
    try:
        normalized.encode("ascii")
    except UnicodeEncodeError as exc:
        raise AuthorityRegistryValidationError(f"{name} must be ASCII") from exc
    return normalized


def _hash(value: object, name: str) -> str:
    normalized = _text(value, name, maximum=64).lower()
    if len(normalized) != 64 or any(c not in "0123456789abcdef" for c in normalized):
        raise AuthorityRegistryValidationError(f"{name} must be lowercase SHA-256")
    return normalized


def _utc(value: object, name: str) -> datetime:
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise AuthorityRegistryValidationError(
            f"{name} must be an aware datetime"
        )
    return value.astimezone(timezone.utc)


def _db_time(value: datetime) -> datetime:
    return _utc(value, "database datetime").replace(tzinfo=None)


def _stored_time(value: object, name: str) -> datetime:
    if type(value) is not datetime:
        raise AuthorityRegistryValidationError(f"{name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _binary(value: object, name: str) -> bytes:
    if isinstance(value, memoryview):
        value = value.tobytes()
    elif isinstance(value, bytearray):
        value = bytes(value)
    if type(value) is not bytes or len(value) != 32:
        raise AuthorityRegistryValidationError(
            f"{name} must be exactly 32 bytes"
        )
    return value


def _pipe_hash(namespace: str, parts: tuple[str, ...]) -> str:
    return hashlib.sha256((namespace + "|" + "|".join(parts)).encode("utf-8")).hexdigest()


def _time_text(value: datetime) -> str:
    return _utc(value, "revoked_at").isoformat(timespec="microseconds")


@dataclass(frozen=True, slots=True)
class AuthorityTrustKeyRegistration:
    source_provider: str
    key_id: str
    key_version: str
    public_key: bytes
    valid_from: datetime
    valid_to: datetime | None = None
    public_key_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name in ("source_provider", "key_id", "key_version"):
            object.__setattr__(self, name, _text(getattr(self, name), name, maximum=128))
        key = _binary(self.public_key, "public_key")
        valid_from = _utc(self.valid_from, "valid_from")
        valid_to = None if self.valid_to is None else _utc(self.valid_to, "valid_to")
        if valid_to is not None and valid_from >= valid_to:
            raise AuthorityRegistryValidationError("valid_to must follow valid_from")
        try:
            Ed25519PublicKey.from_public_bytes(key)
        except (TypeError, ValueError) as exc:
            raise AuthorityRegistryValidationError("public_key is not Ed25519") from exc
        object.__setattr__(self, "public_key", key)
        object.__setattr__(self, "valid_from", valid_from)
        object.__setattr__(self, "valid_to", valid_to)
        object.__setattr__(self, "public_key_hash", hashlib.sha256(key).hexdigest())

    @classmethod
    def from_trust_key(cls, value: AuthorityTrustKey) -> "AuthorityTrustKeyRegistration":
        if type(value) is not AuthorityTrustKey or value.revoked_at is not None:
            raise AuthorityRegistryValidationError(
                "registration requires an unrevoked canonical AuthorityTrustKey"
            )
        return cls(
            source_provider=value.source_provider,
            key_id=value.key_id,
            key_version=value.key_version,
            public_key=value.public_key,
            valid_from=value.valid_from,
            valid_to=value.valid_to,
        )


def _canonical_claim(value: object) -> AuthorityClaim:
    if type(value) is not AuthorityClaim:
        raise AuthorityRegistryValidationError("claim must be exactly AuthorityClaim")
    return AuthorityClaim(
        evidence_type=value.evidence_type,
        evidence_id=value.evidence_id,
        source_provider=value.source_provider,
        source_payload_hash=value.source_payload_hash,
        receipt_type=value.receipt_type,
        receipt_id=value.receipt_id,
        receipt_hash=value.receipt_hash,
        available_at=_utc(value.available_at, "claim.available_at"),
        trade_date=value.trade_date,
        event_at=None if value.event_at is None else _utc(value.event_at, "claim.event_at"),
        received_at=(
            None
            if value.received_at is None
            else _utc(value.received_at, "claim.received_at")
        ),
    )


def _canonical_receipt(value: object) -> SignedAuthorityReceipt:
    if type(value) is not SignedAuthorityReceipt:
        raise AuthorityRegistryValidationError(
            "receipt must be exactly SignedAuthorityReceipt"
        )
    try:
        reconstructed = SignedAuthorityReceipt.from_json(value.envelope_json)
    except (TypeError, ValueError) as exc:
        raise AuthorityRegistryValidationError(
            "receipt envelope is not canonical"
        ) from exc
    if reconstructed.envelope_json != value.envelope_json or (
        reconstructed.envelope_hash != value.envelope_hash
    ):
        raise AuthorityRegistryValidationError(
            "receipt differs from its canonical envelope"
        )
    return reconstructed


@dataclass(frozen=True, slots=True)
class AuthorityReceiptRegistration:
    claim: AuthorityClaim
    receipt: SignedAuthorityReceipt

    def __post_init__(self) -> None:
        claim = _canonical_claim(self.claim)
        receipt = _canonical_receipt(self.receipt)
        allowed = _EVIDENCE_RECEIPT_TYPES.get(claim.evidence_type)
        if allowed is None or claim.receipt_type not in allowed:
            raise AuthorityRegistryValidationError(
                "evidence_type/receipt_type is not operationally whitelisted"
            )
        if (
            receipt.claim_hash != claim.claim_hash
            or receipt.source_provider != claim.source_provider
            or receipt.receipt_id != claim.receipt_id
            or receipt.receipt_hash != claim.receipt_hash
        ):
            raise AuthorityRegistryValidationError(
                "signed receipt does not bind the exact authority claim"
            )
        if claim.evidence_type in {"MARKET_CALENDAR", "INSTRUMENT_RULE"} and (
            claim.receipt_hash != claim.source_payload_hash
        ):
            raise AuthorityRegistryValidationError(
                "receipt_hash must bind the exact source payload"
            )
        if not receipt.issued_at <= claim.available_at < receipt.expires_at:
            raise AuthorityRegistryValidationError(
                "claim availability must be inside the signed receipt window"
            )
        object.__setattr__(self, "claim", claim)
        object.__setattr__(self, "receipt", receipt)


@dataclass(frozen=True, slots=True)
class AuthorityKeyRevocation:
    source_provider: str
    key_id: str
    key_version: str
    revoked_at: datetime
    reason_code: str
    revocation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name in ("source_provider", "key_id", "key_version"):
            object.__setattr__(self, name, _text(getattr(self, name), name, maximum=128))
        revoked_at = _utc(self.revoked_at, "revoked_at")
        reason = _text(self.reason_code, "reason_code", maximum=64)
        object.__setattr__(self, "revoked_at", revoked_at)
        object.__setattr__(self, "reason_code", reason)
        object.__setattr__(
            self,
            "revocation_hash",
            _pipe_hash(
                "trading-v2.authority-key-revocation.v1",
                (
                    self.source_provider,
                    self.key_id,
                    self.key_version,
                    _time_text(revoked_at),
                    reason,
                ),
            ),
        )


@dataclass(frozen=True, slots=True)
class AuthorityReceiptRevocation:
    receipt_id: str
    receipt_hash: str
    envelope_hash: str
    revoked_at: datetime
    reason_code: str
    revocation_hash: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "receipt_id", _text(self.receipt_id, "receipt_id", maximum=128))
        object.__setattr__(self, "receipt_hash", _hash(self.receipt_hash, "receipt_hash"))
        object.__setattr__(self, "envelope_hash", _hash(self.envelope_hash, "envelope_hash"))
        revoked_at = _utc(self.revoked_at, "revoked_at")
        reason = _text(self.reason_code, "reason_code", maximum=64)
        object.__setattr__(self, "revoked_at", revoked_at)
        object.__setattr__(self, "reason_code", reason)
        object.__setattr__(
            self,
            "revocation_hash",
            _pipe_hash(
                "trading-v2.authority-receipt-revocation.v1",
                (
                    self.receipt_id,
                    self.receipt_hash,
                    self.envelope_hash,
                    _time_text(revoked_at),
                    reason,
                ),
            ),
        )


def _active_connection(connection: Any) -> Any:
    if connection is None or not callable(getattr(connection, "execute", None)):
        raise AuthorityRegistryTransactionError(
            "a SQLAlchemy-like connection is required"
        )
    probe = getattr(connection, "in_transaction", None)
    if not callable(probe):
        raise AuthorityRegistryTransactionError(
            "connection must expose in_transaction()"
        )
    try:
        active = probe()
    except Exception as exc:
        raise AuthorityRegistryTransactionError(
            "transaction state cannot be inspected"
        ) from exc
    if type(active) is not bool or not active:
        raise AuthorityRegistryTransactionError(
            "connection must already be in a transaction"
        )
    try:
        assert_v2_evidence_maintenance_fence_inactive(connection)
    except V2EvidenceMaintenanceFenceError as exc:
        raise AuthorityRegistryTransactionError(
            "authority registry writes are blocked by the maintenance fence"
        ) from exc
    return connection


def _rows(result: Any, *, operation: str) -> tuple[dict[str, Any], ...]:
    try:
        values = result.mappings().all()
    except Exception as exc:
        raise AuthorityRegistryValidationError(
            f"{operation} did not return mapping rows"
        ) from exc
    if type(values) is not list:
        values = list(values)
    if any(not isinstance(item, Mapping) for item in values):
        raise AuthorityRegistryValidationError(
            f"{operation} returned a non-mapping row"
        )
    return tuple(dict(item) for item in values)


def _query_rows(
    connection: Any,
    tag: str,
    sql: str,
    params: Mapping[str, Any],
) -> tuple[dict[str, Any], ...]:
    result = connection.execute(text(f"/* v2ar:{tag} */\n{sql}"), dict(params))
    return _rows(result, operation=tag)


def _database_now(connection: Any) -> datetime:
    rows = _query_rows(
        connection,
        "database_now",
        "SELECT UTC_TIMESTAMP(6) AS database_now",
        {},
    )
    if len(rows) != 1 or set(rows[0]) != {"database_now"}:
        raise AuthorityRegistryValidationError(
            "database UTC clock did not return exactly one timestamp"
        )
    return _stored_time(rows[0]["database_now"], "database_now")


def _exact_columns(row: Mapping[str, Any], columns: tuple[str, ...], name: str) -> None:
    if set(row) != set(columns):
        raise AuthorityRegistryValidationError(f"{name} columns differ")


def _same_value(actual: object, expected: object) -> bool:
    if isinstance(actual, memoryview):
        actual = actual.tobytes()
    elif isinstance(actual, bytearray):
        actual = bytes(actual)
    if type(expected) is datetime:
        if type(actual) is not datetime:
            return False
        if expected.tzinfo is None:
            if actual.tzinfo is not None and actual.utcoffset() is not None:
                actual = actual.astimezone(timezone.utc)
            return actual.replace(tzinfo=None) == expected
        return _stored_time(actual, "stored datetime") == expected.astimezone(timezone.utc)
    return actual == expected


def _require_exact_candidate(
    rows: tuple[dict[str, Any], ...],
    *,
    columns: tuple[str, ...],
    stable: Mapping[str, Any],
    owned_column: str,
    name: str,
) -> datetime | None:
    if not rows:
        return None
    if len(rows) != 1:
        raise AuthorityRegistryConflictError(
            f"{name} identities resolve to different rows; caller must roll back"
        )
    row = rows[0]
    _exact_columns(row, columns, name)
    differences = [
        column
        for column, expected in stable.items()
        if not _same_value(row[column], expected)
    ]
    if differences:
        raise AuthorityRegistryConflictError(
            f"{name} differs in {', '.join(differences)}; caller must roll back"
        )
    return _stored_time(row[owned_column], f"{name}.{owned_column}")


def _classify_integrity_error(exc: IntegrityError, name: str) -> None:
    detail = str(getattr(exc, "orig", exc)).casefold()
    if "duplicate" not in detail and "1062" not in detail:
        raise exc
    raise AuthorityRegistryConflictError(
        f"{name} primary/alternate/nonce/hash identity was concurrently bound; "
        "caller must roll back and retry in a new transaction"
    ) from exc


def _insert(
    connection: Any,
    *,
    tag: str,
    table: str,
    stable: Mapping[str, Any],
    owned_column: str,
) -> None:
    columns = (*stable.keys(), owned_column)
    values = [f":{column}" for column in stable]
    values.append("UTC_TIMESTAMP(6)")
    try:
        result = connection.execute(
            text(
                f"/* v2ar:{tag} */\nINSERT INTO {table} "
                f"({', '.join(columns)}) VALUES ({', '.join(values)})"
            ),
            dict(stable),
        )
    except IntegrityError as exc:
        _classify_integrity_error(exc, table)
    if int(getattr(result, "rowcount", -1)) != 1:
        raise AuthorityRegistryValidationError(
            f"{table} insert did not affect exactly one row"
        )


TRUST_COLUMNS = (
    "source_provider", "key_id", "key_version", "algorithm", "public_key",
    "public_key_hash", "valid_from", "valid_to", "registered_at",
)
RECEIPT_COLUMNS = (
    "receipt_id", "receipt_hash", "claim_hash", "evidence_type", "evidence_id",
    "source_provider", "source_payload_hash", "receipt_type", "key_id",
    "key_version", "replay_nonce", "issued_at", "expires_at", "envelope_json",
    "envelope_hash", "status", "revoked_at", "created_at",
)
KEY_REVOCATION_COLUMNS = (
    "source_provider", "key_id", "key_version", "revoked_at", "reason_code",
    "revocation_hash", "created_at",
)
RECEIPT_REVOCATION_COLUMNS = (
    "receipt_id", "receipt_hash", "envelope_hash", "revoked_at", "reason_code",
    "revocation_hash", "created_at",
)


def _result(
    status: AuthorityRegistryWriteStatus,
    operation: str,
    identity: str,
    content_hash: str,
    database_owned_at: datetime,
) -> AuthorityRegistryWriteResult:
    return AuthorityRegistryWriteResult(
        status=status,
        operation=operation,
        identity=identity,
        content_hash=content_hash,
        database_owned_at=database_owned_at,
    )


def append_authority_trust_key(
    connection: Any,
    registration: AuthorityTrustKeyRegistration,
) -> AuthorityRegistryWriteResult:
    connection = _active_connection(connection)
    if type(registration) is not AuthorityTrustKeyRegistration:
        raise AuthorityRegistryValidationError(
            "registration must be exactly AuthorityTrustKeyRegistration"
        )
    stable = {
        "source_provider": registration.source_provider,
        "key_id": registration.key_id,
        "key_version": registration.key_version,
        "algorithm": "Ed25519",
        "public_key": registration.public_key,
        "public_key_hash": registration.public_key_hash,
        "valid_from": _db_time(registration.valid_from),
        "valid_to": None if registration.valid_to is None else _db_time(registration.valid_to),
    }
    candidates = _query_rows(
        connection,
        "trust_key_candidates",
        f"SELECT {', '.join(TRUST_COLUMNS)} "
        "FROM st_execution_authority_trust_key_v2 "
        "WHERE (BINARY source_provider = BINARY :source_provider "
        "AND BINARY key_id = BINARY :key_id "
        "AND BINARY key_version = BINARY :key_version) "
        "OR BINARY public_key_hash = BINARY :public_key_hash FOR UPDATE",
        stable,
    )
    registered_at = _require_exact_candidate(
        candidates,
        columns=TRUST_COLUMNS,
        stable=stable,
        owned_column="registered_at",
        name="authority trust key",
    )
    identity = "/".join(
        (registration.source_provider, registration.key_id, registration.key_version)
    )
    if registered_at is not None:
        return _result(
            AuthorityRegistryWriteStatus.IDEMPOTENT,
            "TRUST_KEY",
            identity,
            registration.public_key_hash,
            registered_at,
        )
    _insert(
        connection,
        tag="insert_trust_key",
        table="st_execution_authority_trust_key_v2",
        stable=stable,
        owned_column="registered_at",
    )
    readback = _query_rows(
        connection,
        "trust_key_readback",
        f"SELECT {', '.join(TRUST_COLUMNS)} "
        "FROM st_execution_authority_trust_key_v2 "
        "WHERE BINARY source_provider = BINARY :source_provider "
        "AND BINARY key_id = BINARY :key_id "
        "AND BINARY key_version = BINARY :key_version FOR UPDATE",
        stable,
    )
    registered_at = _require_exact_candidate(
        readback,
        columns=TRUST_COLUMNS,
        stable=stable,
        owned_column="registered_at",
        name="authority trust key readback",
    )
    if registered_at is None:
        raise AuthorityRegistryValidationError("authority trust key cannot be read back")
    return _result(
        AuthorityRegistryWriteStatus.INSERTED,
        "TRUST_KEY",
        identity,
        registration.public_key_hash,
        registered_at,
    )


def _load_receipt_key(
    connection: Any,
    registration: AuthorityReceiptRegistration,
    *,
    require_active: bool,
) -> tuple[AuthorityTrustKey, datetime]:
    receipt = registration.receipt
    rows = _query_rows(
        connection,
        "receipt_trust_key",
        "SELECT k.source_provider, k.key_id, k.key_version, k.algorithm, "
        "k.public_key, k.public_key_hash, k.valid_from, k.valid_to, "
        "k.registered_at, kr.revocation_hash AS key_revocation_hash "
        "FROM st_execution_authority_trust_key_v2 k "
        "LEFT JOIN st_execution_authority_key_revocation_v2 kr "
        "ON BINARY kr.source_provider = BINARY k.source_provider "
        "AND BINARY kr.key_id = BINARY k.key_id "
        "AND BINARY kr.key_version = BINARY k.key_version "
        "WHERE BINARY k.source_provider = BINARY :source_provider "
        "AND BINARY k.key_id = BINARY :key_id "
        "AND BINARY k.key_version = BINARY :key_version FOR UPDATE",
        {
            "source_provider": receipt.source_provider,
            "key_id": receipt.key_id,
            "key_version": receipt.key_version,
        },
    )
    expected_columns = (*TRUST_COLUMNS, "key_revocation_hash")
    if len(rows) != 1:
        raise AuthorityRegistryValidationError(
            "exactly one registered trust key is required"
        )
    row = rows[0]
    _exact_columns(row, expected_columns, "receipt trust key")
    if row["algorithm"] != "Ed25519":
        raise AuthorityRegistryValidationError("trust key algorithm is not Ed25519")
    public_key = _binary(row["public_key"], "registered public_key")
    public_key_hash = hashlib.sha256(public_key).hexdigest()
    if _hash(row["public_key_hash"], "registered public_key_hash") != public_key_hash:
        raise AuthorityRegistryValidationError("registered public key hash differs")
    if require_active and row["key_revocation_hash"] is not None:
        raise AuthorityRegistryValidationError("trust key is revoked")
    key = AuthorityTrustKey(
        source_provider=row["source_provider"],
        key_id=row["key_id"],
        key_version=row["key_version"],
        public_key=public_key,
        valid_from=_stored_time(row["valid_from"], "key.valid_from"),
        valid_to=(
            None
            if row["valid_to"] is None
            else _stored_time(row["valid_to"], "key.valid_to")
        ),
    )
    registered_at = _stored_time(row["registered_at"], "key.registered_at")
    if receipt.issued_at < key.valid_from or (
        key.valid_to is not None and receipt.issued_at >= key.valid_to
    ):
        raise AuthorityRegistryValidationError(
            "signed receipt is outside the trust key validity window"
        )
    try:
        signature = base64.urlsafe_b64decode(
            receipt.signature + "=" * (-len(receipt.signature) % 4)
        )
        Ed25519PublicKey.from_public_bytes(public_key).verify(
            signature,
            receipt.signature_message,
        )
    except (InvalidSignature, TypeError, ValueError) as exc:
        raise AuthorityRegistryValidationError(
            "Ed25519 receipt signature is invalid"
        ) from exc
    return key, registered_at


def _receipt_stable(registration: AuthorityReceiptRegistration) -> dict[str, Any]:
    claim = registration.claim
    receipt = registration.receipt
    return {
        "receipt_id": receipt.receipt_id,
        "receipt_hash": receipt.receipt_hash,
        "claim_hash": claim.claim_hash,
        "evidence_type": claim.evidence_type,
        "evidence_id": claim.evidence_id,
        "source_provider": claim.source_provider,
        "source_payload_hash": claim.source_payload_hash,
        "receipt_type": claim.receipt_type,
        "key_id": receipt.key_id,
        "key_version": receipt.key_version,
        "replay_nonce": receipt.replay_nonce,
        "issued_at": _db_time(receipt.issued_at),
        "expires_at": _db_time(receipt.expires_at),
        "envelope_json": receipt.envelope_json,
        "envelope_hash": receipt.envelope_hash,
        "status": "ACTIVE",
        "revoked_at": None,
    }


def append_authority_receipt(
    connection: Any,
    registration: AuthorityReceiptRegistration,
) -> AuthorityRegistryWriteResult:
    connection = _active_connection(connection)
    if type(registration) is not AuthorityReceiptRegistration:
        raise AuthorityRegistryValidationError(
            "registration must be exactly AuthorityReceiptRegistration"
        )
    stable = _receipt_stable(registration)
    candidates = _query_rows(
        connection,
        "receipt_candidates",
        f"SELECT {', '.join(RECEIPT_COLUMNS)} "
        "FROM st_execution_authority_receipt_v2 "
        "WHERE BINARY receipt_id = BINARY :receipt_id "
        "OR BINARY claim_hash = BINARY :claim_hash "
        "OR BINARY envelope_hash = BINARY :envelope_hash "
        "OR (BINARY source_provider = BINARY :source_provider "
        "AND BINARY key_id = BINARY :key_id "
        "AND BINARY key_version = BINARY :key_version "
        "AND BINARY replay_nonce = BINARY :replay_nonce) FOR UPDATE",
        stable,
    )
    created_at = _require_exact_candidate(
        candidates,
        columns=RECEIPT_COLUMNS,
        stable=stable,
        owned_column="created_at",
        name="authority receipt",
    )
    _key, registered_at = _load_receipt_key(
        connection,
        registration,
        require_active=created_at is None,
    )
    claim_available_at = registration.claim.available_at
    if created_at is not None:
        if not registration.receipt.issued_at <= created_at < registration.receipt.expires_at:
            raise AuthorityRegistryValidationError(
                "stored receipt has invalid DB-owned creation chronology"
            )
        if registered_at > created_at:
            raise AuthorityRegistryValidationError(
                "stored receipt predates its trust key registration"
            )
        if registered_at > claim_available_at or created_at > claim_available_at:
            raise AuthorityRegistryValidationError(
                "stored receipt or trust key became available after the claim"
            )
        return _result(
            AuthorityRegistryWriteStatus.IDEMPOTENT,
            "RECEIPT",
            registration.receipt.receipt_id,
            registration.receipt.envelope_hash,
            created_at,
        )
    observed_now = _database_now(connection)
    if not registration.receipt.issued_at <= observed_now < registration.receipt.expires_at:
        raise AuthorityRegistryValidationError(
            "database UTC clock is outside the signed receipt window"
        )
    if registered_at > observed_now:
        raise AuthorityRegistryValidationError(
            "trust key registration is later than the database UTC clock"
        )
    if registered_at > claim_available_at or observed_now > claim_available_at:
        raise AuthorityRegistryValidationError(
            "receipt and trust key must be registered by claim.available_at"
        )
    _insert(
        connection,
        tag="insert_receipt",
        table="st_execution_authority_receipt_v2",
        stable=stable,
        owned_column="created_at",
    )
    readback = _query_rows(
        connection,
        "receipt_readback",
        f"SELECT {', '.join(RECEIPT_COLUMNS)} "
        "FROM st_execution_authority_receipt_v2 "
        "WHERE BINARY receipt_id = BINARY :receipt_id FOR UPDATE",
        stable,
    )
    created_at = _require_exact_candidate(
        readback,
        columns=RECEIPT_COLUMNS,
        stable=stable,
        owned_column="created_at",
        name="authority receipt readback",
    )
    if created_at is None:
        raise AuthorityRegistryValidationError("authority receipt cannot be read back")
    if not registration.receipt.issued_at <= created_at < registration.receipt.expires_at:
        raise AuthorityRegistryValidationError(
            "receipt DB-owned creation time is outside its validity window"
        )
    if registered_at > created_at:
        raise AuthorityRegistryValidationError(
            "receipt DB-owned creation time predates key registration"
        )
    if registered_at > claim_available_at or created_at > claim_available_at:
        raise AuthorityRegistryValidationError(
            "receipt or trust key DB-owned time exceeds claim.available_at"
        )
    if created_at < observed_now:
        raise AuthorityRegistryValidationError(
            "receipt DB-owned creation time predates the observed database clock"
        )
    return _result(
        AuthorityRegistryWriteStatus.INSERTED,
        "RECEIPT",
        registration.receipt.receipt_id,
        registration.receipt.envelope_hash,
        created_at,
    )


def _load_key_parent(connection: Any, value: AuthorityKeyRevocation) -> datetime:
    rows = _query_rows(
        connection,
        "key_revocation_parent",
        f"SELECT {', '.join(TRUST_COLUMNS)} "
        "FROM st_execution_authority_trust_key_v2 "
        "WHERE BINARY source_provider = BINARY :source_provider "
        "AND BINARY key_id = BINARY :key_id "
        "AND BINARY key_version = BINARY :key_version FOR UPDATE",
        {
            "source_provider": value.source_provider,
            "key_id": value.key_id,
            "key_version": value.key_version,
        },
    )
    if len(rows) != 1:
        raise AuthorityRegistryValidationError(
            "key revocation requires exactly one trust-key parent"
        )
    _exact_columns(rows[0], TRUST_COLUMNS, "key revocation parent")
    return _stored_time(rows[0]["registered_at"], "key parent registered_at")


def append_authority_key_revocation(
    connection: Any,
    revocation: AuthorityKeyRevocation,
) -> AuthorityRegistryWriteResult:
    connection = _active_connection(connection)
    if type(revocation) is not AuthorityKeyRevocation:
        raise AuthorityRegistryValidationError(
            "revocation must be exactly AuthorityKeyRevocation"
        )
    stable = {
        "source_provider": revocation.source_provider,
        "key_id": revocation.key_id,
        "key_version": revocation.key_version,
        "revoked_at": _db_time(revocation.revoked_at),
        "reason_code": revocation.reason_code,
        "revocation_hash": revocation.revocation_hash,
    }
    candidates = _query_rows(
        connection,
        "key_revocation_candidates",
        f"SELECT {', '.join(KEY_REVOCATION_COLUMNS)} "
        "FROM st_execution_authority_key_revocation_v2 "
        "WHERE (BINARY source_provider = BINARY :source_provider "
        "AND BINARY key_id = BINARY :key_id "
        "AND BINARY key_version = BINARY :key_version) "
        "OR BINARY revocation_hash = BINARY :revocation_hash FOR UPDATE",
        stable,
    )
    created_at = _require_exact_candidate(
        candidates,
        columns=KEY_REVOCATION_COLUMNS,
        stable=stable,
        owned_column="created_at",
        name="authority key revocation",
    )
    identity = "/".join(
        (revocation.source_provider, revocation.key_id, revocation.key_version)
    )
    if created_at is not None:
        registered_at = _load_key_parent(connection, revocation)
        if revocation.revoked_at > created_at:
            raise AuthorityRegistryValidationError(
                "stored key revocation is later than its DB-owned creation time"
            )
        if registered_at > created_at:
            raise AuthorityRegistryValidationError(
                "stored key revocation predates its trust-key parent"
            )
        return _result(
            AuthorityRegistryWriteStatus.IDEMPOTENT,
            "KEY_REVOCATION",
            identity,
            revocation.revocation_hash,
            created_at,
        )
    registered_at = _load_key_parent(connection, revocation)
    observed_now = _database_now(connection)
    if revocation.revoked_at > observed_now or registered_at > observed_now:
        raise AuthorityRegistryValidationError(
            "key revocation is in the future or its parent is not yet registered"
        )
    _insert(
        connection,
        tag="insert_key_revocation",
        table="st_execution_authority_key_revocation_v2",
        stable=stable,
        owned_column="created_at",
    )
    readback = _query_rows(
        connection,
        "key_revocation_readback",
        f"SELECT {', '.join(KEY_REVOCATION_COLUMNS)} "
        "FROM st_execution_authority_key_revocation_v2 "
        "WHERE BINARY source_provider = BINARY :source_provider "
        "AND BINARY key_id = BINARY :key_id "
        "AND BINARY key_version = BINARY :key_version FOR UPDATE",
        stable,
    )
    created_at = _require_exact_candidate(
        readback,
        columns=KEY_REVOCATION_COLUMNS,
        stable=stable,
        owned_column="created_at",
        name="authority key revocation readback",
    )
    if created_at is None:
        raise AuthorityRegistryValidationError(
            "authority key revocation cannot be read back"
        )
    if revocation.revoked_at > created_at or registered_at > created_at:
        raise AuthorityRegistryValidationError(
            "key revocation DB-owned chronology differs"
        )
    if created_at < observed_now:
        raise AuthorityRegistryValidationError(
            "key revocation creation predates the observed database clock"
        )
    return _result(
        AuthorityRegistryWriteStatus.INSERTED,
        "KEY_REVOCATION",
        identity,
        revocation.revocation_hash,
        created_at,
    )


def _load_receipt_parent(
    connection: Any,
    value: AuthorityReceiptRevocation,
) -> datetime:
    rows = _query_rows(
        connection,
        "receipt_revocation_parent",
        f"SELECT {', '.join(RECEIPT_COLUMNS)} "
        "FROM st_execution_authority_receipt_v2 "
        "WHERE BINARY receipt_id = BINARY :receipt_id "
        "AND BINARY receipt_hash = BINARY :receipt_hash "
        "AND BINARY envelope_hash = BINARY :envelope_hash FOR UPDATE",
        {
            "receipt_id": value.receipt_id,
            "receipt_hash": value.receipt_hash,
            "envelope_hash": value.envelope_hash,
        },
    )
    if len(rows) != 1:
        raise AuthorityRegistryValidationError(
            "receipt revocation requires exactly one exact receipt parent"
        )
    _exact_columns(rows[0], RECEIPT_COLUMNS, "receipt revocation parent")
    if rows[0]["status"] != "ACTIVE" or rows[0]["revoked_at"] is not None:
        raise AuthorityRegistryValidationError("receipt parent state is invalid")
    return _stored_time(rows[0]["created_at"], "receipt parent created_at")


def append_authority_receipt_revocation(
    connection: Any,
    revocation: AuthorityReceiptRevocation,
) -> AuthorityRegistryWriteResult:
    connection = _active_connection(connection)
    if type(revocation) is not AuthorityReceiptRevocation:
        raise AuthorityRegistryValidationError(
            "revocation must be exactly AuthorityReceiptRevocation"
        )
    stable = {
        "receipt_id": revocation.receipt_id,
        "receipt_hash": revocation.receipt_hash,
        "envelope_hash": revocation.envelope_hash,
        "revoked_at": _db_time(revocation.revoked_at),
        "reason_code": revocation.reason_code,
        "revocation_hash": revocation.revocation_hash,
    }
    candidates = _query_rows(
        connection,
        "receipt_revocation_candidates",
        f"SELECT {', '.join(RECEIPT_REVOCATION_COLUMNS)} "
        "FROM st_execution_authority_receipt_revocation_v2 "
        "WHERE BINARY receipt_id = BINARY :receipt_id "
        "OR BINARY revocation_hash = BINARY :revocation_hash FOR UPDATE",
        stable,
    )
    created_at = _require_exact_candidate(
        candidates,
        columns=RECEIPT_REVOCATION_COLUMNS,
        stable=stable,
        owned_column="created_at",
        name="authority receipt revocation",
    )
    if created_at is not None:
        parent_created_at = _load_receipt_parent(connection, revocation)
        if revocation.revoked_at > created_at:
            raise AuthorityRegistryValidationError(
                "stored receipt revocation is later than its DB-owned creation time"
            )
        if parent_created_at > created_at:
            raise AuthorityRegistryValidationError(
                "stored receipt revocation predates its receipt parent"
            )
        return _result(
            AuthorityRegistryWriteStatus.IDEMPOTENT,
            "RECEIPT_REVOCATION",
            revocation.receipt_id,
            revocation.revocation_hash,
            created_at,
        )
    parent_created_at = _load_receipt_parent(connection, revocation)
    observed_now = _database_now(connection)
    if revocation.revoked_at > observed_now or parent_created_at > observed_now:
        raise AuthorityRegistryValidationError(
            "receipt revocation is in the future or its parent is not yet registered"
        )
    _insert(
        connection,
        tag="insert_receipt_revocation",
        table="st_execution_authority_receipt_revocation_v2",
        stable=stable,
        owned_column="created_at",
    )
    readback = _query_rows(
        connection,
        "receipt_revocation_readback",
        f"SELECT {', '.join(RECEIPT_REVOCATION_COLUMNS)} "
        "FROM st_execution_authority_receipt_revocation_v2 "
        "WHERE BINARY receipt_id = BINARY :receipt_id FOR UPDATE",
        stable,
    )
    created_at = _require_exact_candidate(
        readback,
        columns=RECEIPT_REVOCATION_COLUMNS,
        stable=stable,
        owned_column="created_at",
        name="authority receipt revocation readback",
    )
    if created_at is None:
        raise AuthorityRegistryValidationError(
            "authority receipt revocation cannot be read back"
        )
    if revocation.revoked_at > created_at or parent_created_at > created_at:
        raise AuthorityRegistryValidationError(
            "receipt revocation DB-owned chronology differs"
        )
    if created_at < observed_now:
        raise AuthorityRegistryValidationError(
            "receipt revocation creation predates the observed database clock"
        )
    return _result(
        AuthorityRegistryWriteStatus.INSERTED,
        "RECEIPT_REVOCATION",
        revocation.receipt_id,
        revocation.revocation_hash,
        created_at,
    )


__all__ = [
    "AuthorityKeyRevocation",
    "AuthorityReceiptRegistration",
    "AuthorityReceiptRevocation",
    "AuthorityRegistryConflictError",
    "AuthorityRegistryError",
    "AuthorityRegistryTransactionError",
    "AuthorityRegistryValidationError",
    "AuthorityRegistryWriteResult",
    "AuthorityRegistryWriteStatus",
    "AuthorityTrustKeyRegistration",
    "append_authority_key_revocation",
    "append_authority_receipt",
    "append_authority_receipt_revocation",
    "append_authority_trust_key",
]
