"""Explicit external-authority boundary for V2 execution evidence.

Hashes prove content identity, not source authority.  Production writers may
accept ``EXTERNAL_RECEIPT_VERIFIED`` only through the MySQL registry-backed
verifier, which loads the signed receipt, trust key and append-only revocations
on the caller transaction.  The default verifier denies everything.  This
module owns no engine and no transaction.
"""

from __future__ import annotations

import base64
import binascii
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
import hashlib
import json
from collections.abc import Callable
from typing import Any, Mapping, Protocol, runtime_checkable
from zoneinfo import ZoneInfo

try:  # Imported lazily by the verifier; absence remains fail closed.
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric.ed25519 import (
        Ed25519PublicKey,
    )
except ImportError:  # pragma: no cover - exercised only in a minimal install
    InvalidSignature = None  # type: ignore[assignment]
    Ed25519PublicKey = None  # type: ignore[assignment]

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from server.trading_v2.execution_evidence import (
    CanonicalJson,
    FillExecutionEvidence,
    MarketCalendarEvidence,
    QuoteReceiptEvidence,
    QuoteReceiptType,
)
from server.trading_v2.execution_evidence_schema_gate import (
    V2EvidenceMaintenanceFenceError,
    assert_v2_evidence_maintenance_fence_inactive,
)


MARKET_ZONE = ZoneInfo("Asia/Shanghai")


class AuthorityVerificationError(ValueError):
    """An external authority claim was absent, unsupported, or unverified."""


class AuthorityNonceReplayError(AuthorityVerificationError):
    """A provider/key/version nonce is already bound to another claim."""


class AuthorityAttestationConflictError(AuthorityVerificationError):
    """An authority claim or attestation identity carries different content."""


class AuthorityVerificationLevel(str, Enum):
    DENIED = "DENIED"
    REGISTRY_ONLY = "REGISTRY_ONLY"
    CRYPTOGRAPHIC = "CRYPTOGRAPHIC"


def _required_text(value: object, name: str, *, maximum: int = 1000) -> str:
    if type(value) is not str:
        raise AuthorityVerificationError(f"{name} must be exactly str")
    normalized = value.strip()
    if not normalized or len(normalized) > maximum:
        raise AuthorityVerificationError(f"{name} is invalid")
    return normalized


def _identity_text(value: object, name: str, *, maximum: int = 128) -> str:
    normalized = _required_text(value, name, maximum=maximum)
    try:
        normalized.encode("ascii")
    except UnicodeEncodeError as exc:
        raise AuthorityVerificationError(f"{name} must be ASCII identity text") from exc
    return normalized


def _sha256(value: object, name: str) -> str:
    normalized = _required_text(value, name, maximum=64).lower()
    if len(normalized) != 64 or any(
        item not in "0123456789abcdef" for item in normalized
    ):
        raise AuthorityVerificationError(f"{name} must be lowercase SHA-256")
    return normalized


def _aware(value: object, name: str) -> datetime:
    if type(value) is not datetime:
        raise AuthorityVerificationError(f"{name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise AuthorityVerificationError(f"{name} must be timezone-aware")
    return value


def _canonical(value: Any) -> Any:
    if type(value) is datetime:
        return value.astimezone(timezone.utc).isoformat(timespec="microseconds")
    if type(value) is date:
        return value.isoformat()
    if isinstance(value, Enum):
        return _canonical(value.value)
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise AuthorityVerificationError("authority metadata keys must be text")
        return {key: _canonical(value[key]) for key in sorted(value)}
    if type(value) in {tuple, list}:
        return [_canonical(item) for item in value]
    if value is None or type(value) in {str, int, bool}:
        return value
    raise AuthorityVerificationError(
        f"unsupported authority claim value: {type(value).__name__}"
    )


def _digest(namespace: str, payload: dict[str, Any]) -> str:
    encoded = _encoded(namespace, payload)
    return hashlib.sha256(encoded).hexdigest()


def _encoded(namespace: str, payload: dict[str, Any]) -> bytes:
    return json.dumps(
        {"namespace": namespace, "payload": _canonical(payload)},
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _db_datetime(value: datetime) -> datetime:
    # Authority receipts are self-signed in canonical UTC.  Keep their
    # registry/attestation timestamps in UTC DATETIME(6), unlike the legacy V2
    # market-local fact tables, so the SQL row can be bound byte-for-byte to
    # the signed envelope without relying on MySQL timezone tables.
    return _aware(value, "database datetime").astimezone(timezone.utc).replace(
        tzinfo=None
    )


def _stored_utc_datetime(value: object, name: str) -> datetime:
    if type(value) is not datetime:
        raise AuthorityVerificationError(f"{name} must be exactly datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@dataclass(frozen=True, slots=True)
class AuthorityClaim:
    evidence_type: str
    evidence_id: str
    source_provider: str
    source_payload_hash: str
    receipt_type: str
    receipt_id: str
    receipt_hash: str
    available_at: datetime
    trade_date: date
    event_at: datetime | None = None
    received_at: datetime | None = None
    claim_hash: str = field(init=False)

    def __post_init__(self) -> None:
        for name in (
            "evidence_type",
            "source_provider",
            "receipt_type",
            "receipt_id",
        ):
            object.__setattr__(
                self,
                name,
                _identity_text(getattr(self, name), name),
            )
        for name in (
            "evidence_id",
            "source_payload_hash",
            "receipt_hash",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        _aware(self.available_at, "available_at")
        if type(self.trade_date) is not date:
            raise AuthorityVerificationError("trade_date must be exactly date")
        for name in ("event_at", "received_at"):
            value = getattr(self, name)
            if value is not None:
                _aware(value, name)
                if value > self.available_at:
                    raise AuthorityVerificationError(
                        f"{name} cannot follow available_at"
                    )
        object.__setattr__(
            self,
            "claim_hash",
            _digest(
                "trading-v2.authority-claim.v1",
                {
                    "evidence_type": self.evidence_type,
                    "evidence_id": self.evidence_id,
                    "source_provider": self.source_provider,
                    "source_payload_hash": self.source_payload_hash,
                    "receipt_type": self.receipt_type,
                    "receipt_id": self.receipt_id,
                    "receipt_hash": self.receipt_hash,
                    "available_at": self.available_at,
                    "trade_date": self.trade_date,
                    "event_at": self.event_at,
                    "received_at": self.received_at,
                },
            ),
        )


@dataclass(frozen=True, slots=True)
class AuthorityDecision:
    claim_hash: str
    verified: bool
    verifier_id: str
    verifier_version: str
    verified_at: datetime
    reason_code: str
    verification_level: AuthorityVerificationLevel | None = None
    receipt_envelope_hash: str | None = None
    trust_key_id: str | None = None
    trust_key_version: str | None = None
    replay_nonce: str | None = None
    decision_hash: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "claim_hash", _sha256(self.claim_hash, "claim_hash"))
        if type(self.verified) is not bool:
            raise AuthorityVerificationError("verified must be exactly bool")
        for name in ("verifier_id", "verifier_version", "reason_code"):
            object.__setattr__(
                self,
                name,
                _identity_text(getattr(self, name), name),
            )
        _aware(self.verified_at, "verified_at")
        if self.verified and self.reason_code != "VERIFIED":
            raise AuthorityVerificationError(
                "a verified decision requires reason_code VERIFIED"
            )
        if not self.verified and self.reason_code == "VERIFIED":
            raise AuthorityVerificationError(
                "a denied decision cannot use reason_code VERIFIED"
            )
        level = self.verification_level
        if level is None:
            level = (
                AuthorityVerificationLevel.REGISTRY_ONLY
                if self.verified
                else AuthorityVerificationLevel.DENIED
            )
        if type(level) is not AuthorityVerificationLevel:
            raise AuthorityVerificationError(
                "verification_level must be exactly AuthorityVerificationLevel"
            )
        if self.verified == (level is AuthorityVerificationLevel.DENIED):
            raise AuthorityVerificationError(
                "verification level differs from decision outcome"
            )
        object.__setattr__(self, "verification_level", level)
        envelope_hash = (
            None
            if self.receipt_envelope_hash is None
            else _sha256(self.receipt_envelope_hash, "receipt_envelope_hash")
        )
        key_id = (
            None
            if self.trust_key_id is None
            else _identity_text(self.trust_key_id, "trust_key_id")
        )
        key_version = (
            None
            if self.trust_key_version is None
            else _identity_text(self.trust_key_version, "trust_key_version")
        )
        replay_nonce = (
            None
            if self.replay_nonce is None
            else _identity_text(self.replay_nonce, "replay_nonce")
        )
        crypto_metadata = (envelope_hash, key_id, key_version, replay_nonce)
        if level is AuthorityVerificationLevel.CRYPTOGRAPHIC:
            if any(item is None for item in crypto_metadata):
                raise AuthorityVerificationError(
                    "cryptographic decisions require envelope, key, and nonce"
                )
        elif any(item is not None for item in crypto_metadata):
            raise AuthorityVerificationError(
                "only cryptographic decisions may carry trust metadata"
            )
        object.__setattr__(self, "receipt_envelope_hash", envelope_hash)
        object.__setattr__(self, "trust_key_id", key_id)
        object.__setattr__(self, "trust_key_version", key_version)
        object.__setattr__(self, "replay_nonce", replay_nonce)
        object.__setattr__(
            self,
            "decision_hash",
            _digest(
                "trading-v2.authority-decision.v1",
                {
                    "claim_hash": self.claim_hash,
                    "verified": self.verified,
                    "verifier_id": self.verifier_id,
                    "verifier_version": self.verifier_version,
                    "verified_at": self.verified_at,
                    "reason_code": self.reason_code,
                    "verification_level": level,
                    "receipt_envelope_hash": envelope_hash,
                    "trust_key_id": key_id,
                    "trust_key_version": key_version,
                    "replay_nonce": replay_nonce,
                },
            ),
        )


@runtime_checkable
class EvidenceAuthorityVerifier(Protocol):
    def verify(self, connection: Any, claim: AuthorityClaim) -> AuthorityDecision:
        """Verify one exact claim without committing or opening a connection."""


@dataclass(frozen=True, slots=True)
class AuthorityReceiptReference:
    """Explicit receipt coordinates for a nested authoritative payload."""

    source_provider: str
    receipt_id: str
    receipt_hash: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "source_provider",
            _identity_text(self.source_provider, "source_provider"),
        )
        object.__setattr__(
            self,
            "receipt_id",
            _identity_text(self.receipt_id, "receipt_id"),
        )
        object.__setattr__(
            self, "receipt_hash", _sha256(self.receipt_hash, "receipt_hash")
        )


def authority_receipt_signature_message(
    *,
    claim_hash: str,
    source_provider: str,
    receipt_id: str,
    receipt_hash: str,
    key_id: str,
    key_version: str,
    replay_nonce: str,
    issued_at: datetime,
    expires_at: datetime,
) -> bytes:
    """Return the exact Ed25519 message for one signed receipt."""

    normalized = {
        "claim_hash": _sha256(claim_hash, "claim_hash"),
        "source_provider": _identity_text(source_provider, "source_provider"),
        "receipt_id": _identity_text(receipt_id, "receipt_id"),
        "receipt_hash": _sha256(receipt_hash, "receipt_hash"),
        "key_id": _identity_text(key_id, "key_id"),
        "key_version": _identity_text(key_version, "key_version"),
        "replay_nonce": _identity_text(replay_nonce, "replay_nonce"),
        "issued_at": _aware(issued_at, "issued_at"),
        "expires_at": _aware(expires_at, "expires_at"),
    }
    if normalized["issued_at"] >= normalized["expires_at"]:
        raise AuthorityVerificationError(
            "authority receipt expiry must follow issue time"
        )
    return _encoded("trading-v2.authority-receipt-signature.v1", normalized)


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _decode_base64url(value: object, name: str, *, length: int) -> bytes:
    text_value = _required_text(value, name, maximum=1024)
    try:
        decoded = base64.urlsafe_b64decode(
            text_value + "=" * ((4 - len(text_value) % 4) % 4)
        )
    except (ValueError, binascii.Error) as exc:
        raise AuthorityVerificationError(f"{name} is not base64url") from exc
    if len(decoded) != length or _base64url(decoded) != text_value:
        raise AuthorityVerificationError(f"{name} is not canonical base64url")
    return decoded


@dataclass(frozen=True, slots=True)
class SignedAuthorityReceipt:
    claim_hash: str
    source_provider: str
    receipt_id: str
    receipt_hash: str
    key_id: str
    key_version: str
    replay_nonce: str
    issued_at: datetime
    expires_at: datetime
    signature: str
    envelope_hash: str = field(init=False)
    envelope_json: str = field(init=False)

    def __post_init__(self) -> None:
        # Reuse the message constructor as the single normalization boundary.
        authority_receipt_signature_message(
            claim_hash=self.claim_hash,
            source_provider=self.source_provider,
            receipt_id=self.receipt_id,
            receipt_hash=self.receipt_hash,
            key_id=self.key_id,
            key_version=self.key_version,
            replay_nonce=self.replay_nonce,
            issued_at=self.issued_at,
            expires_at=self.expires_at,
        )
        object.__setattr__(self, "claim_hash", _sha256(self.claim_hash, "claim_hash"))
        object.__setattr__(
            self,
            "source_provider",
            _identity_text(self.source_provider, "source_provider"),
        )
        object.__setattr__(
            self,
            "receipt_id",
            _identity_text(self.receipt_id, "receipt_id"),
        )
        object.__setattr__(
            self, "receipt_hash", _sha256(self.receipt_hash, "receipt_hash")
        )
        for name in ("key_id", "key_version", "replay_nonce"):
            object.__setattr__(
                self,
                name,
                _identity_text(getattr(self, name), name),
            )
        _aware(self.issued_at, "issued_at")
        _aware(self.expires_at, "expires_at")
        _decode_base64url(self.signature, "signature", length=64)
        envelope_value = {
            "algorithm": "Ed25519",
            "claim_hash": self.claim_hash,
            "source_provider": self.source_provider,
            "receipt_id": self.receipt_id,
            "receipt_hash": self.receipt_hash,
            "key_id": self.key_id,
            "key_version": self.key_version,
            "replay_nonce": self.replay_nonce,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "signature": self.signature,
        }
        canonical = CanonicalJson.from_value(envelope_value)
        object.__setattr__(self, "envelope_json", canonical.json_text)
        object.__setattr__(self, "envelope_hash", canonical.payload_hash)

    @property
    def signature_message(self) -> bytes:
        return authority_receipt_signature_message(
            claim_hash=self.claim_hash,
            source_provider=self.source_provider,
            receipt_id=self.receipt_id,
            receipt_hash=self.receipt_hash,
            key_id=self.key_id,
            key_version=self.key_version,
            replay_nonce=self.replay_nonce,
            issued_at=self.issued_at,
            expires_at=self.expires_at,
        )

    @classmethod
    def from_json(cls, value: object) -> "SignedAuthorityReceipt":
        if type(value) is not str:
            raise AuthorityVerificationError(
                "authority receipt envelope must be canonical JSON text"
            )
        try:
            payload = CanonicalJson(value).value()
        except (TypeError, ValueError) as exc:
            raise AuthorityVerificationError(
                "authority receipt envelope is not canonical JSON"
            ) from exc
        expected = {
            "algorithm",
            "claim_hash",
            "source_provider",
            "receipt_id",
            "receipt_hash",
            "key_id",
            "key_version",
            "replay_nonce",
            "issued_at",
            "expires_at",
            "signature",
        }
        if type(payload) is not dict or set(payload) != expected:
            raise AuthorityVerificationError(
                "authority receipt envelope key set differs"
            )
        if payload["algorithm"] != "Ed25519":
            raise AuthorityVerificationError(
                "authority receipt algorithm is unsupported"
            )
        try:
            issued_at = datetime.fromisoformat(payload["issued_at"])
            expires_at = datetime.fromisoformat(payload["expires_at"])
        except (TypeError, ValueError) as exc:
            raise AuthorityVerificationError(
                "authority receipt timestamps are invalid"
            ) from exc
        return cls(
            claim_hash=payload["claim_hash"],
            source_provider=payload["source_provider"],
            receipt_id=payload["receipt_id"],
            receipt_hash=payload["receipt_hash"],
            key_id=payload["key_id"],
            key_version=payload["key_version"],
            replay_nonce=payload["replay_nonce"],
            issued_at=issued_at,
            expires_at=expires_at,
            signature=payload["signature"],
        )


@dataclass(frozen=True, slots=True)
class AuthorityTrustKey:
    source_provider: str
    key_id: str
    key_version: str
    public_key: bytes
    valid_from: datetime
    valid_to: datetime | None = None
    revoked_at: datetime | None = None

    def __post_init__(self) -> None:
        for name in ("source_provider", "key_id", "key_version"):
            object.__setattr__(
                self,
                name,
                _identity_text(getattr(self, name), name),
            )
        if type(self.public_key) is not bytes or len(self.public_key) != 32:
            raise AuthorityVerificationError(
                "Ed25519 public_key must be exactly 32 bytes"
            )
        _aware(self.valid_from, "valid_from")
        valid_to = self.valid_to
        revoked_at = self.revoked_at
        if valid_to is not None:
            _aware(valid_to, "valid_to")
            if valid_to <= self.valid_from:
                raise AuthorityVerificationError(
                    "trust key valid_to must follow valid_from"
                )
        if revoked_at is not None:
            _aware(revoked_at, "revoked_at")


@runtime_checkable
class AuthorityReceiptLoader(Protocol):
    def load(
        self,
        connection: Any,
        claim: AuthorityClaim,
    ) -> SignedAuthorityReceipt:
        """Load one immutable, replay-unique receipt on the caller transaction."""


class MySQLAuthorityReceiptLoader:
    """Load a signed receipt from the append-only authority registry."""

    def load(
        self,
        connection: Any,
        claim: AuthorityClaim,
    ) -> SignedAuthorityReceipt:
        result = connection.execute(
            text(
                """
                SELECT envelope_json, envelope_hash
                FROM st_execution_authority_receipt_v2
                WHERE BINARY receipt_id = BINARY :receipt_id
                  AND BINARY receipt_hash = BINARY :receipt_hash
                  AND BINARY claim_hash = BINARY :claim_hash
                  AND BINARY evidence_type = BINARY :evidence_type
                  AND BINARY evidence_id = BINARY :evidence_id
                  AND BINARY source_provider = BINARY :source_provider
                  AND BINARY source_payload_hash = BINARY :source_payload_hash
                  AND status = 'ACTIVE'
                  AND revoked_at IS NULL
                  AND created_at <= :available_at
                FOR UPDATE
                """
            ),
            {
                "receipt_id": claim.receipt_id,
                "receipt_hash": claim.receipt_hash,
                "claim_hash": claim.claim_hash,
                "evidence_type": claim.evidence_type,
                "evidence_id": claim.evidence_id,
                "source_provider": claim.source_provider,
                "source_payload_hash": claim.source_payload_hash,
                "available_at": _db_datetime(claim.available_at),
            },
        )
        try:
            rows = result.mappings().all()
        except Exception as exc:
            raise AuthorityVerificationError(
                "authority receipt registry returned invalid rows"
            ) from exc
        if len(rows) != 1 or not isinstance(rows[0], Mapping):
            raise AuthorityVerificationError(
                "exactly one active authority receipt is required"
            )
        row = dict(rows[0])
        if set(row) != {"envelope_json", "envelope_hash"}:
            raise AuthorityVerificationError(
                "authority receipt registry columns differ"
            )
        receipt = SignedAuthorityReceipt.from_json(row["envelope_json"])
        if _sha256(row["envelope_hash"], "envelope_hash") != receipt.envelope_hash:
            raise AuthorityVerificationError(
                "authority receipt envelope hash differs"
            )
        return receipt


class Ed25519AuthorityVerifier:
    """Verify provider/key/version, signature, expiry, revocation and replay binding."""

    verifier_id = "ed25519-authority-receipt"
    verifier_version = "v1"

    def __init__(
        self,
        *,
        loader: AuthorityReceiptLoader,
        trust_keys: tuple[AuthorityTrustKey, ...],
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not isinstance(loader, AuthorityReceiptLoader):
            raise AuthorityVerificationError(
                "an explicit authority receipt loader is required"
            )
        if type(trust_keys) is not tuple or not trust_keys:
            raise AuthorityVerificationError("at least one trust key is required")
        keys: dict[tuple[str, str, str], AuthorityTrustKey] = {}
        for item in trust_keys:
            if type(item) is not AuthorityTrustKey:
                raise AuthorityVerificationError(
                    "trust_keys must contain AuthorityTrustKey values"
                )
            identity = (item.source_provider, item.key_id, item.key_version)
            if identity in keys:
                raise AuthorityVerificationError("duplicate authority trust key")
            keys[identity] = item
        self._loader = loader
        self._keys = keys
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def verify(self, connection: Any, claim: AuthorityClaim) -> AuthorityDecision:
        if Ed25519PublicKey is None or InvalidSignature is None:
            return self._denied(claim, "CRYPTOGRAPHY_UNAVAILABLE")
        receipt = self._loader.load(connection, claim)
        if (
            receipt.claim_hash != claim.claim_hash
            or receipt.source_provider != claim.source_provider
            or receipt.receipt_id != claim.receipt_id
            or receipt.receipt_hash != claim.receipt_hash
        ):
            return self._denied(claim, "RECEIPT_CLAIM_MISMATCH")
        now = _aware(self._clock(), "verifier clock")
        if receipt.issued_at > claim.available_at:
            return self._denied(claim, "RECEIPT_ISSUED_AFTER_AVAILABILITY", now)
        if receipt.expires_at <= claim.available_at or receipt.expires_at <= now:
            return self._denied(claim, "RECEIPT_EXPIRED", now)
        if now < receipt.issued_at:
            return self._denied(claim, "VERIFIER_CLOCK_BEFORE_ISSUE", now)
        key = self._keys.get(
            (receipt.source_provider, receipt.key_id, receipt.key_version)
        )
        if key is None:
            return self._denied(claim, "TRUST_KEY_UNKNOWN", now)
        if receipt.issued_at < key.valid_from or (
            key.valid_to is not None and receipt.issued_at >= key.valid_to
        ):
            return self._denied(claim, "TRUST_KEY_OUTSIDE_VALIDITY", now)
        if key.revoked_at is not None and now >= key.revoked_at:
            return self._denied(claim, "TRUST_KEY_REVOKED", now)
        try:
            Ed25519PublicKey.from_public_bytes(key.public_key).verify(
                _decode_base64url(receipt.signature, "signature", length=64),
                receipt.signature_message,
            )
        except (InvalidSignature, ValueError):
            return self._denied(claim, "SIGNATURE_INVALID", now)
        return AuthorityDecision(
            claim_hash=claim.claim_hash,
            verified=True,
            verifier_id=self.verifier_id,
            verifier_version=self.verifier_version,
            verified_at=now,
            reason_code="VERIFIED",
            verification_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
            receipt_envelope_hash=receipt.envelope_hash,
            trust_key_id=receipt.key_id,
            trust_key_version=receipt.key_version,
            replay_nonce=receipt.replay_nonce,
        )

    def _denied(
        self,
        claim: AuthorityClaim,
        reason: str,
        verified_at: datetime | None = None,
    ) -> AuthorityDecision:
        return AuthorityDecision(
            claim_hash=claim.claim_hash,
            verified=False,
            verifier_id=self.verifier_id,
            verifier_version=self.verifier_version,
            verified_at=verified_at or _aware(self._clock(), "verifier clock"),
            reason_code=reason,
            verification_level=AuthorityVerificationLevel.DENIED,
        )


@dataclass(frozen=True, slots=True)
class _RegistryAuthorityRecord:
    receipt: SignedAuthorityReceipt
    trust_key: AuthorityTrustKey
    receipt_revoked_at: datetime | None


class _LoadedAuthorityReceipt:
    def __init__(self, receipt: SignedAuthorityReceipt) -> None:
        self._receipt = receipt

    def load(self, connection: Any, claim: AuthorityClaim) -> SignedAuthorityReceipt:
        del connection, claim
        return self._receipt


_REGISTRY_COLUMNS = (
    "envelope_json",
    "envelope_hash",
    "public_key",
    "public_key_hash",
    "key_valid_from",
    "key_valid_to",
    "key_revoked_at",
    "receipt_revoked_at",
)


def _public_key_bytes(value: object) -> bytes:
    if isinstance(value, memoryview):
        value = value.tobytes()
    elif isinstance(value, bytearray):
        value = bytes(value)
    if type(value) is not bytes or len(value) != 32:
        raise AuthorityVerificationError(
            "registered Ed25519 public_key must be exactly 32 bytes"
        )
    return value


def _load_registry_authority_record(
    connection: Any,
    claim: AuthorityClaim,
) -> _RegistryAuthorityRecord | None:
    """Load one receipt, its registered key and append-only revocations."""

    result = connection.execute(
        text(
            """
            SELECT r.envelope_json AS envelope_json,
                   r.envelope_hash AS envelope_hash,
                   k.public_key AS public_key,
                   k.public_key_hash AS public_key_hash,
                   k.valid_from AS key_valid_from,
                   k.valid_to AS key_valid_to,
                   kr.revoked_at AS key_revoked_at,
                   rr.revoked_at AS receipt_revoked_at
            FROM st_execution_authority_receipt_v2 r
            INNER JOIN st_execution_authority_trust_key_v2 k
              ON BINARY k.source_provider = BINARY r.source_provider
             AND BINARY k.key_id = BINARY r.key_id
             AND BINARY k.key_version = BINARY r.key_version
             AND k.algorithm = 'Ed25519'
             AND k.registered_at <= :available_at
            LEFT JOIN st_execution_authority_key_revocation_v2 kr
              ON BINARY kr.source_provider = BINARY k.source_provider
             AND BINARY kr.key_id = BINARY k.key_id
             AND BINARY kr.key_version = BINARY k.key_version
            LEFT JOIN st_execution_authority_receipt_revocation_v2 rr
              ON BINARY rr.receipt_id = BINARY r.receipt_id
             AND BINARY rr.receipt_hash = BINARY r.receipt_hash
             AND BINARY rr.envelope_hash = BINARY r.envelope_hash
            WHERE BINARY r.receipt_id = BINARY :receipt_id
              AND BINARY r.receipt_hash = BINARY :receipt_hash
              AND BINARY r.claim_hash = BINARY :claim_hash
              AND BINARY r.evidence_type = BINARY :evidence_type
              AND BINARY r.evidence_id = BINARY :evidence_id
              AND BINARY r.source_provider = BINARY :source_provider
              AND BINARY r.source_payload_hash = BINARY :source_payload_hash
              AND BINARY r.receipt_type = BINARY :receipt_type
              AND r.status = 'ACTIVE'
              AND r.revoked_at IS NULL
              AND r.created_at <= :available_at
            FOR UPDATE
            """
        ),
        {
            "receipt_id": claim.receipt_id,
            "receipt_hash": claim.receipt_hash,
            "claim_hash": claim.claim_hash,
            "evidence_type": claim.evidence_type,
            "evidence_id": claim.evidence_id,
            "source_provider": claim.source_provider,
            "source_payload_hash": claim.source_payload_hash,
            "receipt_type": claim.receipt_type,
            "available_at": _db_datetime(claim.available_at),
        },
    )
    try:
        rows = result.mappings().all()
    except Exception as exc:
        raise AuthorityVerificationError(
            "authority trust registry returned invalid rows"
        ) from exc
    if not rows:
        return None
    if len(rows) != 1 or not isinstance(rows[0], Mapping):
        raise AuthorityVerificationError(
            "exactly one registered authority key/receipt binding is required"
        )
    row = dict(rows[0])
    if set(row) != set(_REGISTRY_COLUMNS):
        raise AuthorityVerificationError("authority trust registry columns differ")
    receipt = SignedAuthorityReceipt.from_json(row["envelope_json"])
    if _sha256(row["envelope_hash"], "envelope_hash") != receipt.envelope_hash:
        raise AuthorityVerificationError("authority receipt envelope hash differs")
    public_key = _public_key_bytes(row["public_key"])
    if hashlib.sha256(public_key).hexdigest() != _sha256(
        row["public_key_hash"], "public_key_hash"
    ):
        raise AuthorityVerificationError("registered authority public key hash differs")
    valid_to = row["key_valid_to"]
    key_revoked_at = row["key_revoked_at"]
    receipt_revoked_at = row["receipt_revoked_at"]
    trust_key = AuthorityTrustKey(
        source_provider=receipt.source_provider,
        key_id=receipt.key_id,
        key_version=receipt.key_version,
        public_key=public_key,
        valid_from=_stored_utc_datetime(row["key_valid_from"], "key_valid_from"),
        valid_to=(
            None
            if valid_to is None
            else _stored_utc_datetime(valid_to, "key_valid_to")
        ),
        revoked_at=(
            None
            if key_revoked_at is None
            else _stored_utc_datetime(key_revoked_at, "key_revoked_at")
        ),
    )
    return _RegistryAuthorityRecord(
        receipt=receipt,
        trust_key=trust_key,
        receipt_revoked_at=(
            None
            if receipt_revoked_at is None
            else _stored_utc_datetime(
                receipt_revoked_at, "receipt_revoked_at"
            )
        ),
    )


class MySQLRegistryBackedAuthorityVerifier:
    """Ed25519 verifier whose receipt, key and revocations come only from MySQL."""

    verifier_id = Ed25519AuthorityVerifier.verifier_id
    verifier_version = Ed25519AuthorityVerifier.verifier_version

    def __init__(self, *, clock: Callable[[], datetime] | None = None) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def _denied(
        self,
        claim: AuthorityClaim,
        reason: str,
        now: datetime,
    ) -> AuthorityDecision:
        return AuthorityDecision(
            claim_hash=claim.claim_hash,
            verified=False,
            verifier_id=self.verifier_id,
            verifier_version=self.verifier_version,
            verified_at=now,
            reason_code=reason,
            verification_level=AuthorityVerificationLevel.DENIED,
        )

    def _record(
        self,
        connection: Any,
        claim: AuthorityClaim,
        now: datetime,
    ) -> _RegistryAuthorityRecord | AuthorityDecision:
        record = _load_registry_authority_record(connection, claim)
        if record is None:
            return self._denied(claim, "TRUST_REGISTRY_BINDING_MISSING", now)
        if record.receipt_revoked_at is not None:
            return self._denied(claim, "AUTHORITY_RECEIPT_REVOKED", now)
        if record.trust_key.revoked_at is not None:
            return self._denied(claim, "TRUST_KEY_REVOKED", now)
        return record

    def verify(self, connection: Any, claim: AuthorityClaim) -> AuthorityDecision:
        now = _aware(self._clock(), "verifier clock")
        loaded = self._record(connection, claim, now)
        if type(loaded) is AuthorityDecision:
            return loaded
        verifier = Ed25519AuthorityVerifier(
            loader=_LoadedAuthorityReceipt(loaded.receipt),
            trust_keys=(loaded.trust_key,),
            clock=lambda: now,
        )
        return verifier.verify(connection, claim)

    def require_registered_claim_active(
        self,
        connection: Any,
        claim: AuthorityClaim,
    ) -> None:
        """Lock and recheck a registered claim without re-expiring old proof."""

        now = _aware(self._clock(), "verifier clock")
        loaded = self._record(connection, claim, now)
        if type(loaded) is AuthorityDecision:
            raise AuthorityVerificationError(
                f"stored authority is no longer active: {loaded.reason_code}"
            )
        verifier = Ed25519AuthorityVerifier(
            loader=_LoadedAuthorityReceipt(loaded.receipt),
            trust_keys=(loaded.trust_key,),
            clock=lambda: claim.available_at,
        )
        decision = verifier.verify(connection, claim)
        if not decision.verified:
            raise AuthorityVerificationError(
                "stored authority receipt no longer proves the exact claim: "
                f"{decision.reason_code}"
            )


class AuthorityAttestationStatus(str, Enum):
    INSERTED = "INSERTED"
    IDEMPOTENT = "IDEMPOTENT"


@dataclass(frozen=True, slots=True)
class AuthorityAttestationResult:
    status: AuthorityAttestationStatus
    claim_hash: str
    attestation_hash: str


_ATTESTATION_COLUMNS = (
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


def _attestation_storage(
    claim: AuthorityClaim,
    decision: AuthorityDecision,
) -> tuple[dict[str, Any], str]:
    payload = {
        "claim_hash": claim.claim_hash,
        "evidence_type": claim.evidence_type,
        "evidence_id": claim.evidence_id,
        "source_provider": claim.source_provider,
        "source_payload_hash": claim.source_payload_hash,
        "receipt_type": claim.receipt_type,
        "receipt_id": claim.receipt_id,
        "receipt_hash": claim.receipt_hash,
        "available_at": claim.available_at,
        "verifier_id": decision.verifier_id,
        "verifier_version": decision.verifier_version,
        "verified_at": decision.verified_at,
        "verification_level": decision.verification_level,
        "receipt_envelope_hash": decision.receipt_envelope_hash,
        "trust_key_id": decision.trust_key_id,
        "trust_key_version": decision.trust_key_version,
        "replay_nonce": decision.replay_nonce,
        "decision_hash": decision.decision_hash,
    }
    attestation_hash = _digest(
        "trading-v2.authority-attestation.v1", payload
    )
    return (
        {
            "claim_hash": claim.claim_hash,
            "evidence_type": claim.evidence_type,
            "evidence_id": claim.evidence_id,
            "source_provider": claim.source_provider,
            "source_payload_hash": claim.source_payload_hash,
            "receipt_type": claim.receipt_type,
            "receipt_id": claim.receipt_id,
            "receipt_hash": claim.receipt_hash,
            "available_at": _db_datetime(claim.available_at),
            "verifier_id": decision.verifier_id,
            "verifier_version": decision.verifier_version,
            "verified_at": _db_datetime(decision.verified_at),
            "verification_level": decision.verification_level.value,
            "receipt_envelope_hash": decision.receipt_envelope_hash,
            "trust_key_id": decision.trust_key_id,
            "trust_key_version": decision.trust_key_version,
            "replay_nonce": decision.replay_nonce,
            "decision_hash": decision.decision_hash,
            "attestation_hash": attestation_hash,
            "created_at": _db_datetime(decision.verified_at),
        },
        attestation_hash,
    )


def load_authority_attestation(
    connection: Any,
    claim: AuthorityClaim,
) -> AuthorityAttestationResult | None:
    """Return an exact prior cryptographic attestation for idempotent replay.

    Historical proof is checked from its immutable stored decision instead of
    being re-timestamped or invalidated by a later key rotation.  Any row drift
    fails closed; absence returns ``None`` so a new claim must be live-verified.
    """

    if connection is None or not callable(getattr(connection, "execute", None)):
        raise AuthorityVerificationError(
            "authority attestation requires a SQLAlchemy-like connection"
        )
    if type(claim) is not AuthorityClaim:
        raise AuthorityVerificationError(
            "authority attestation requires a canonical claim"
        )
    select_sql = (
        "SELECT "
        + ", ".join(_ATTESTATION_COLUMNS)
        + " FROM st_execution_authority_attestation_v2 "
        "WHERE claim_hash = :claim_hash FOR UPDATE"
    )
    result = connection.execute(
        text(select_sql), {"claim_hash": claim.claim_hash}
    )
    try:
        stored = result.mappings().first()
    except Exception as exc:
        raise AuthorityVerificationError(
            "authority attestation lookup returned invalid rows"
        ) from exc
    if stored is None:
        return None
    if not isinstance(stored, Mapping) or set(stored) != set(_ATTESTATION_COLUMNS):
        raise AuthorityVerificationError(
            "stored authority attestation columns differ"
        )
    row = dict(stored)
    stable = {
        "claim_hash": claim.claim_hash,
        "evidence_type": claim.evidence_type,
        "evidence_id": claim.evidence_id,
        "source_provider": claim.source_provider,
        "source_payload_hash": claim.source_payload_hash,
        "receipt_type": claim.receipt_type,
        "receipt_id": claim.receipt_id,
        "receipt_hash": claim.receipt_hash,
        "available_at": _db_datetime(claim.available_at),
        "verification_level": AuthorityVerificationLevel.CRYPTOGRAPHIC.value,
    }
    if any(row[name] != value for name, value in stable.items()):
        raise AuthorityVerificationError(
            "stored authority attestation is bound to different evidence"
        )
    decision = AuthorityDecision(
        claim_hash=claim.claim_hash,
        verified=True,
        verifier_id=row["verifier_id"],
        verifier_version=row["verifier_version"],
        verified_at=_stored_utc_datetime(row["verified_at"], "verified_at"),
        reason_code="VERIFIED",
        verification_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
        receipt_envelope_hash=row["receipt_envelope_hash"],
        trust_key_id=row["trust_key_id"],
        trust_key_version=row["trust_key_version"],
        replay_nonce=row["replay_nonce"],
    )
    expected, attestation_hash = _attestation_storage(claim, decision)
    if row != expected:
        raise AuthorityVerificationError(
            "stored authority attestation hash or decision differs"
        )
    return AuthorityAttestationResult(
        AuthorityAttestationStatus.IDEMPOTENT,
        claim.claim_hash,
        attestation_hash,
    )


def _classify_attestation_integrity_error(exc: IntegrityError) -> None:
    detail = str(getattr(exc, "orig", exc)).casefold()
    if "duplicate" not in detail and "1062" not in detail:
        raise exc
    if "uk_authority_attestation_v2_replay" in detail:
        raise AuthorityNonceReplayError(
            "authority replay nonce is already bound to another claim; "
            "the caller transaction must be rolled back"
        ) from exc
    raise AuthorityAttestationConflictError(
        "authority claim or attestation identity already carries different "
        "content; the caller transaction must be rolled back"
    ) from exc


def append_authority_attestation(
    connection: Any,
    claim: AuthorityClaim,
    decision: AuthorityDecision,
) -> AuthorityAttestationResult:
    """Persist one cryptographic decision in the caller-owned transaction."""

    if connection is None or not callable(getattr(connection, "execute", None)):
        raise AuthorityVerificationError(
            "authority attestation requires a SQLAlchemy-like connection"
        )
    in_transaction = getattr(connection, "in_transaction", None)
    if not callable(in_transaction) or in_transaction() is not True:
        raise AuthorityVerificationError(
            "authority attestation requires an active caller transaction"
        )
    try:
        assert_v2_evidence_maintenance_fence_inactive(connection)
    except V2EvidenceMaintenanceFenceError as exc:
        raise AuthorityVerificationError(
            "authority attestation is blocked by the maintenance fence"
        ) from exc
    if type(claim) is not AuthorityClaim or type(decision) is not AuthorityDecision:
        raise AuthorityVerificationError(
            "authority attestation requires canonical claim and decision"
        )
    if (
        not decision.verified
        or decision.claim_hash != claim.claim_hash
        or decision.verification_level
        is not AuthorityVerificationLevel.CRYPTOGRAPHIC
    ):
        raise AuthorityVerificationError(
            "only a bound cryptographic decision can be attested"
        )
    existing = load_authority_attestation(connection, claim)
    if existing is not None:
        return existing
    expected, attestation_hash = _attestation_storage(claim, decision)
    select_sql = (
        "SELECT "
        + ", ".join(_ATTESTATION_COLUMNS)
        + " FROM st_execution_authority_attestation_v2 "
        "WHERE claim_hash = :claim_hash FOR UPDATE"
    )
    placeholders = ", ".join(f":{column}" for column in _ATTESTATION_COLUMNS)
    try:
        inserted = connection.execute(
            text(
                "INSERT INTO st_execution_authority_attestation_v2 ("
                + ", ".join(_ATTESTATION_COLUMNS)
                + f") VALUES ({placeholders})"
            ),
            expected,
        )
    except IntegrityError as exc:
        _classify_attestation_integrity_error(exc)
    if int(getattr(inserted, "rowcount", -1)) != 1:
        raise AuthorityVerificationError(
            "authority attestation insert did not affect exactly one row"
        )
    readback_result = connection.execute(
        text(select_sql), {"claim_hash": claim.claim_hash}
    )
    try:
        readback = readback_result.mappings().first()
    except Exception as exc:
        raise AuthorityVerificationError(
            "authority attestation readback returned invalid rows"
        ) from exc
    if not isinstance(readback, Mapping) or dict(readback) != expected:
        raise AuthorityVerificationError(
            "authority attestation readback differs"
        )
    return AuthorityAttestationResult(
        AuthorityAttestationStatus.INSERTED,
        claim.claim_hash,
        attestation_hash,
    )


class DenyAllAuthorityVerifier:
    def verify(self, connection: Any, claim: AuthorityClaim) -> AuthorityDecision:
        del connection
        return AuthorityDecision(
            claim_hash=claim.claim_hash,
            verified=False,
            verifier_id="deny-all",
            verifier_version="v1",
            verified_at=claim.available_at,
            reason_code="AUTHORITY_NOT_CONFIGURED",
        )


class MySQLReceiptRegistryAuthorityVerifier:
    """Verify registered quote receipts on the caller-owned connection."""

    verifier_id = "mysql-v2-receipt-registry"
    verifier_version = "v1"

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    _STATEMENTS = {
        QuoteReceiptType.QMT_MINUTE.value: """
            SELECT COUNT(*)
            FROM st_qmt_minute_sync_receipt_v2 r
            WHERE r.receipt_id = :receipt_id
              AND r.trade_date = :trade_date
              AND BINARY r.source_provider = BINARY :source_provider
              AND :event_at BETWEEN r.first_trade_time AND r.last_trade_time
              AND r.created_at <= :available_at
              AND r.quality_status = 'PASS'
              AND r.forward_eligible = 1
            FOR UPDATE
        """,
        QuoteReceiptType.QMT_REALTIME.value: """
            SELECT COUNT(*)
            FROM st_qmt_realtime_sync_receipt_v2 r
            WHERE r.receipt_id = :receipt_id
              AND BINARY r.source_provider = BINARY :source_provider
              AND r.source_generated_at <= :event_at
              AND r.heartbeat_at >= :event_at
              AND r.published_at <= :available_at
              AND r.created_at <= :available_at
              AND r.quality_status = 'PASS'
            FOR UPDATE
        """,
        QuoteReceiptType.PUBLIC_CONSENSUS.value: """
            SELECT COUNT(*)
            FROM st_public_quote_receipt_v2 r
            WHERE r.batch_id = :receipt_id
              AND r.trade_date = :trade_date
              AND r.quote_at = :event_at
              AND r.received_at = :received_at
              AND BINARY r.source_provider = BINARY :source_provider
              AND r.created_at <= :available_at
              AND r.quality_status = 'PASS'
            FOR UPDATE
        """,
    }

    def verify(self, connection: Any, claim: AuthorityClaim) -> AuthorityDecision:
        if claim.evidence_type != "QUOTE_RECEIPT":
            return self._decision(claim, False, "UNSUPPORTED_EVIDENCE_TYPE")
        statement = self._STATEMENTS.get(claim.receipt_type)
        if statement is None or claim.event_at is None:
            return self._decision(claim, False, "UNSUPPORTED_RECEIPT_TYPE")
        result = connection.execute(
            text(statement),
            {
                "receipt_id": claim.receipt_id,
                "trade_date": claim.trade_date,
                "source_provider": claim.source_provider,
                "event_at": claim.event_at.astimezone(MARKET_ZONE).replace(
                    tzinfo=None
                ),
                "received_at": (
                    None
                    if claim.received_at is None
                    else claim.received_at.astimezone(MARKET_ZONE).replace(
                        tzinfo=None
                    )
                ),
                "available_at": claim.available_at.astimezone(
                    MARKET_ZONE
                ).replace(tzinfo=None),
            },
        ).scalar()
        if type(result) is not int:
            raise AuthorityVerificationError(
                "authority registry count must be exactly int"
            )
        return self._decision(
            claim,
            result == 1,
            "VERIFIED" if result == 1 else "RECEIPT_REGISTRY_MISMATCH",
        )

    def _decision(
        self,
        claim: AuthorityClaim,
        verified: bool,
        reason_code: str,
    ) -> AuthorityDecision:
        verified_at = _aware(self._clock(), "verifier clock")
        return AuthorityDecision(
            claim_hash=claim.claim_hash,
            verified=verified,
            verifier_id=self.verifier_id,
            verifier_version=self.verifier_version,
            verified_at=verified_at,
            reason_code=reason_code,
            verification_level=(
                AuthorityVerificationLevel.REGISTRY_ONLY
                if verified
                else AuthorityVerificationLevel.DENIED
            ),
        )


def build_authority_claim(evidence: object) -> AuthorityClaim:
    if type(evidence) is MarketCalendarEvidence:
        if evidence.source_receipt_id is None or evidence.source_receipt_hash is None:
            raise AuthorityVerificationError(
                "authoritative calendar evidence requires a receipt pair"
            )
        return AuthorityClaim(
            evidence_type="MARKET_CALENDAR",
            evidence_id=evidence.calendar_evidence_id,
            source_provider=evidence.source_provider,
            source_payload_hash=evidence.source_payload.payload_hash,
            receipt_type="CALENDAR_OTHER",
            receipt_id=evidence.source_receipt_id,
            receipt_hash=evidence.source_receipt_hash,
            available_at=evidence.available_at,
            trade_date=evidence.trade_date,
        )
    if type(evidence) is QuoteReceiptEvidence:
        if evidence.source_receipt_id is None or evidence.source_receipt_hash is None:
            raise AuthorityVerificationError(
                "authoritative quote evidence requires a receipt pair"
            )
        return AuthorityClaim(
            evidence_type="QUOTE_RECEIPT",
            evidence_id=evidence.quote_evidence_id,
            source_provider=evidence.source_provider,
            source_payload_hash=evidence.source_payload_hash,
            receipt_type=evidence.receipt_type.value,
            receipt_id=evidence.source_receipt_id,
            receipt_hash=evidence.source_receipt_hash,
            available_at=evidence.available_at,
            trade_date=evidence.trade_date,
            event_at=evidence.quote_at,
            received_at=evidence.received_at,
        )
    raise AuthorityVerificationError(
        f"unsupported authoritative evidence type: {type(evidence).__name__}"
    )


def build_instrument_rule_authority_claim(
    evidence: FillExecutionEvidence,
    reference: AuthorityReceiptReference,
) -> AuthorityClaim:
    """Bind a fill's exact instrument-rule snapshot to an external receipt."""

    if type(evidence) is not FillExecutionEvidence:
        raise AuthorityVerificationError(
            "instrument rule authority requires FillExecutionEvidence"
        )
    if type(reference) is not AuthorityReceiptReference:
        raise AuthorityVerificationError(
            "instrument rule authority requires AuthorityReceiptReference"
        )
    rule_hash = evidence.instrument_rule.payload_hash
    if reference.receipt_hash != rule_hash:
        raise AuthorityVerificationError(
            "instrument rule receipt must hash the exact rule snapshot"
        )
    return AuthorityClaim(
        evidence_type="INSTRUMENT_RULE",
        evidence_id=rule_hash,
        source_provider=reference.source_provider,
        source_payload_hash=rule_hash,
        receipt_type="INSTRUMENT_RULE",
        receipt_id=reference.receipt_id,
        receipt_hash=reference.receipt_hash,
        available_at=evidence.executed_at,
        trade_date=evidence.quote_evidence.trade_date,
        event_at=evidence.instrument_rule_created_at,
    )


def require_verified_authority(
    connection: Any,
    evidence: object,
    verifier: EvidenceAuthorityVerifier | None,
    *,
    minimum_level: AuthorityVerificationLevel = (
        AuthorityVerificationLevel.REGISTRY_ONLY
    ),
) -> AuthorityDecision:
    if type(minimum_level) is not AuthorityVerificationLevel:
        raise AuthorityVerificationError(
            "minimum_level must be exactly AuthorityVerificationLevel"
        )
    if verifier is None or not isinstance(verifier, EvidenceAuthorityVerifier):
        raise AuthorityVerificationError(
            "external authority requires an explicit verifier"
        )
    claim = build_authority_claim(evidence)
    decision = verifier.verify(connection, claim)
    if type(decision) is not AuthorityDecision:
        raise AuthorityVerificationError(
            "authority verifier returned an invalid decision type"
        )
    if decision.claim_hash != claim.claim_hash:
        raise AuthorityVerificationError(
            "authority decision is bound to another claim"
        )
    if not decision.verified:
        raise AuthorityVerificationError(
            f"external authority was denied: {decision.reason_code}"
        )
    if decision.verified_at < evidence.available_at:
        raise AuthorityVerificationError(
            "authority cannot be verified before evidence became available"
        )
    levels = {
        AuthorityVerificationLevel.DENIED: 0,
        AuthorityVerificationLevel.REGISTRY_ONLY: 1,
        AuthorityVerificationLevel.CRYPTOGRAPHIC: 2,
    }
    if levels[decision.verification_level] < levels[minimum_level]:
        raise AuthorityVerificationError(
            "authority verification level is below the required trust level"
        )
    return decision


def require_verified_instrument_rule_authority(
    connection: Any,
    evidence: FillExecutionEvidence,
    reference: AuthorityReceiptReference,
    verifier: EvidenceAuthorityVerifier | None,
    *,
    minimum_level: AuthorityVerificationLevel = (
        AuthorityVerificationLevel.CRYPTOGRAPHIC
    ),
) -> AuthorityDecision:
    if verifier is None or not isinstance(verifier, EvidenceAuthorityVerifier):
        raise AuthorityVerificationError(
            "instrument rule authority requires an explicit verifier"
        )
    claim = build_instrument_rule_authority_claim(evidence, reference)
    decision = verifier.verify(connection, claim)
    if type(decision) is not AuthorityDecision:
        raise AuthorityVerificationError(
            "authority verifier returned an invalid decision type"
        )
    if decision.claim_hash != claim.claim_hash or not decision.verified:
        raise AuthorityVerificationError(
            "instrument rule authority claim was not verified"
        )
    levels = {
        AuthorityVerificationLevel.DENIED: 0,
        AuthorityVerificationLevel.REGISTRY_ONLY: 1,
        AuthorityVerificationLevel.CRYPTOGRAPHIC: 2,
    }
    if levels[decision.verification_level] < levels[minimum_level]:
        raise AuthorityVerificationError(
            "instrument rule verification level is below the required trust level"
        )
    if decision.verified_at < evidence.executed_at:
        raise AuthorityVerificationError(
            "instrument rule authority cannot be verified before execution"
        )
    return decision
