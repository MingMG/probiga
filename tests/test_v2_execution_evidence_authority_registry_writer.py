from __future__ import annotations

import base64
from datetime import date, datetime, timedelta, timezone
import hashlib
import inspect
import re
from typing import Any, Mapping

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy.exc import IntegrityError

from server.integrations.v2_execution_evidence_authority import (
    AuthorityClaim,
    SignedAuthorityReceipt,
    authority_receipt_signature_message,
)
from server.integrations.v2_execution_evidence_authority_registry_writer import (
    AuthorityKeyRevocation,
    AuthorityReceiptRegistration,
    AuthorityReceiptRevocation,
    AuthorityRegistryConflictError,
    AuthorityRegistryTransactionError,
    AuthorityRegistryValidationError,
    AuthorityRegistryWriteStatus,
    AuthorityTrustKeyRegistration,
    append_authority_key_revocation,
    append_authority_receipt,
    append_authority_receipt_revocation,
    append_authority_trust_key,
)
from server.integrations.v2_execution_evidence_authority_registry_writer import (
    writer,
)


UTC = timezone.utc
NOW = datetime(2026, 8, 4, 2, 3, 4, 567890, tzinfo=UTC)


class _Mappings:
    def __init__(self, rows: list[Mapping[str, Any]]) -> None:
        self._rows = rows

    def all(self) -> list[Mapping[str, Any]]:
        return self._rows

    def __iter__(self):
        return iter(self._rows)


class _Result:
    def __init__(self, rows: list[Mapping[str, Any]] | None = None, rowcount: int = -1) -> None:
        self._rows = rows or []
        self.rowcount = rowcount

    def mappings(self) -> _Mappings:
        return _Mappings(self._rows)


class RegistryConnection:
    def __init__(self, *, active: bool = True, fence: str = "INACTIVE") -> None:
        self.active = active
        self.fence = fence
        self.calls: list[str] = []
        self.lifecycle_calls: list[str] = []
        self.tables: dict[str, list[dict[str, Any]]] = {
            "st_execution_authority_trust_key_v2": [],
            "st_execution_authority_receipt_v2": [],
            "st_execution_authority_key_revocation_v2": [],
            "st_execution_authority_receipt_revocation_v2": [],
        }

    def in_transaction(self) -> bool:
        return self.active

    def commit(self) -> None:
        self.lifecycle_calls.append("commit")

    def rollback(self) -> None:
        self.lifecycle_calls.append("rollback")

    @staticmethod
    def _tag(sql: str) -> str:
        match = re.search(r"/\* v2ar:([^*]+) \*/", sql)
        return "maintenance_fence" if match is None else match.group(1)

    @staticmethod
    def _matches(row: Mapping[str, Any], params: Mapping[str, Any], names: tuple[str, ...]) -> bool:
        return all(row.get(name) == params.get(name) for name in names)

    def _candidate_rows(self, tag: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
        if tag == "database_now":
            return [{"database_now": NOW.replace(tzinfo=None)}]
        if tag in {"trust_key_candidates", "trust_key_readback", "key_revocation_parent"}:
            rows = self.tables["st_execution_authority_trust_key_v2"]
            return [
                dict(row)
                for row in rows
                if self._matches(row, params, ("source_provider", "key_id", "key_version"))
                or (tag == "trust_key_candidates" and row["public_key_hash"] == params["public_key_hash"])
            ]
        if tag == "receipt_trust_key":
            rows = self.tables["st_execution_authority_trust_key_v2"]
            matches = [
                dict(row)
                for row in rows
                if self._matches(row, params, ("source_provider", "key_id", "key_version"))
            ]
            for row in matches:
                revocations = self.tables["st_execution_authority_key_revocation_v2"]
                found = next(
                    (
                        item
                        for item in revocations
                        if self._matches(item, row, ("source_provider", "key_id", "key_version"))
                    ),
                    None,
                )
                row["key_revocation_hash"] = None if found is None else found["revocation_hash"]
            return matches
        if tag in {"receipt_candidates", "receipt_readback", "receipt_revocation_parent"}:
            rows = self.tables["st_execution_authority_receipt_v2"]
            if tag == "receipt_candidates":
                return [
                    dict(row)
                    for row in rows
                    if row["receipt_id"] == params["receipt_id"]
                    or row["claim_hash"] == params["claim_hash"]
                    or row["envelope_hash"] == params["envelope_hash"]
                    or self._matches(
                        row,
                        params,
                        ("source_provider", "key_id", "key_version", "replay_nonce"),
                    )
                ]
            if tag == "receipt_readback":
                return [dict(row) for row in rows if row["receipt_id"] == params["receipt_id"]]
            return [
                dict(row)
                for row in rows
                if self._matches(row, params, ("receipt_id", "receipt_hash", "envelope_hash"))
            ]
        if tag in {"key_revocation_candidates", "key_revocation_readback"}:
            rows = self.tables["st_execution_authority_key_revocation_v2"]
            return [
                dict(row)
                for row in rows
                if self._matches(row, params, ("source_provider", "key_id", "key_version"))
                or (tag == "key_revocation_candidates" and row["revocation_hash"] == params["revocation_hash"])
            ]
        if tag in {"receipt_revocation_candidates", "receipt_revocation_readback"}:
            rows = self.tables["st_execution_authority_receipt_revocation_v2"]
            return [
                dict(row)
                for row in rows
                if row["receipt_id"] == params["receipt_id"]
                or (
                    tag == "receipt_revocation_candidates"
                    and row["revocation_hash"] == params["revocation_hash"]
                )
            ]
        raise AssertionError(f"unexpected query tag: {tag}")

    def execute(self, statement: Any, params: Mapping[str, Any] | None = None) -> _Result:
        sql = str(statement)
        params = dict(params or {})
        if "schema_migration_v2_maintenance_fence" in sql:
            self.calls.append("maintenance_fence")
            return _Result([{"fence_name": "execution_evidence_011_015", "state": self.fence}])
        tag = self._tag(sql)
        self.calls.append(tag)
        if tag.startswith("insert_"):
            table_by_tag = {
                "insert_trust_key": ("st_execution_authority_trust_key_v2", "registered_at"),
                "insert_receipt": ("st_execution_authority_receipt_v2", "created_at"),
                "insert_key_revocation": ("st_execution_authority_key_revocation_v2", "created_at"),
                "insert_receipt_revocation": ("st_execution_authority_receipt_revocation_v2", "created_at"),
            }
            table, owned = table_by_tag[tag]
            self.tables[table].append({**params, owned: NOW.replace(tzinfo=None)})
            return _Result(rowcount=1)
        return _Result(self._candidate_rows(tag, params))


def _private(seed: bytes = b"authority-registry-writer") -> Ed25519PrivateKey:
    return Ed25519PrivateKey.from_private_bytes(hashlib.sha256(seed).digest())


def _trust(private: Ed25519PrivateKey | None = None, **changes: Any) -> AuthorityTrustKeyRegistration:
    private = private or _private()
    values = {
        "source_provider": "calendar-provider",
        "key_id": "calendar-key",
        "key_version": "v1",
        "public_key": private.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        ),
        "valid_from": NOW - timedelta(days=1),
        "valid_to": NOW + timedelta(days=1),
    }
    values.update(changes)
    return AuthorityTrustKeyRegistration(**values)


def _receipt(
    private: Ed25519PrivateKey | None = None,
    *,
    receipt_id: str = "calendar-receipt",
    nonce: str = "nonce-1",
    receipt_type: str = "CALENDAR_OTHER",
    signature_private: Ed25519PrivateKey | None = None,
    available_at: datetime | None = None,
) -> AuthorityReceiptRegistration:
    private = private or _private()
    payload_hash = hashlib.sha256(b"calendar-payload").hexdigest()
    claim = AuthorityClaim(
        evidence_type="MARKET_CALENDAR",
        evidence_id=hashlib.sha256(receipt_id.encode()).hexdigest(),
        source_provider="calendar-provider",
        source_payload_hash=payload_hash,
        receipt_type=receipt_type,
        receipt_id=receipt_id,
        receipt_hash=payload_hash,
        available_at=available_at or NOW + timedelta(minutes=1),
        trade_date=date(2026, 8, 4),
    )
    fields = {
        "claim_hash": claim.claim_hash,
        "source_provider": claim.source_provider,
        "receipt_id": claim.receipt_id,
        "receipt_hash": claim.receipt_hash,
        "key_id": "calendar-key",
        "key_version": "v1",
        "replay_nonce": nonce,
        "issued_at": NOW - timedelta(minutes=1),
        "expires_at": NOW + timedelta(hours=1),
    }
    message = authority_receipt_signature_message(**fields)
    signature = (signature_private or private).sign(message)
    return AuthorityReceiptRegistration(
        claim=claim,
        receipt=SignedAuthorityReceipt(
            **fields,
            signature=base64.urlsafe_b64encode(signature).decode("ascii").rstrip("="),
        ),
    )


def _seed_receipt(connection: RegistryConnection) -> AuthorityReceiptRegistration:
    private = _private()
    append_authority_trust_key(connection, _trust(private))
    registration = _receipt(private)
    append_authority_receipt(connection, registration)
    return registration


def test_requires_active_caller_transaction_and_never_owns_lifecycle() -> None:
    connection = RegistryConnection(active=False)
    with pytest.raises(AuthorityRegistryTransactionError, match="already be in a transaction"):
        append_authority_trust_key(connection, _trust())
    assert connection.calls == []
    assert connection.lifecycle_calls == []


def test_maintenance_fence_blocks_before_registry_queries() -> None:
    connection = RegistryConnection(fence="ACTIVE")
    with pytest.raises(AuthorityRegistryTransactionError, match="maintenance fence"):
        append_authority_trust_key(connection, _trust())
    assert connection.calls == ["maintenance_fence"]


def test_trust_key_insert_exact_readback_and_idempotent_replay() -> None:
    connection = RegistryConnection()
    registration = _trust()
    inserted = append_authority_trust_key(connection, registration)
    replayed = append_authority_trust_key(connection, registration)
    assert inserted.status is AuthorityRegistryWriteStatus.INSERTED
    assert replayed.status is AuthorityRegistryWriteStatus.IDEMPOTENT
    assert inserted.database_owned_at == NOW
    assert replayed.database_owned_at == NOW
    assert connection.lifecycle_calls == []


def test_trust_key_alternate_hash_with_different_identity_is_conflict() -> None:
    connection = RegistryConnection()
    append_authority_trust_key(connection, _trust())
    with pytest.raises(AuthorityRegistryConflictError, match="caller must roll back"):
        append_authority_trust_key(connection, _trust(key_id="different-key"))


def test_receipt_signature_binding_insert_replay_and_nonce_conflict() -> None:
    connection = RegistryConnection()
    private = _private()
    append_authority_trust_key(connection, _trust(private))
    registration = _receipt(private)
    inserted = append_authority_receipt(connection, registration)
    replayed = append_authority_receipt(connection, registration)
    assert inserted.status is AuthorityRegistryWriteStatus.INSERTED
    assert replayed.status is AuthorityRegistryWriteStatus.IDEMPOTENT

    conflicting = _receipt(private, receipt_id="other-receipt", nonce="nonce-1")
    with pytest.raises(AuthorityRegistryConflictError, match="caller must roll back"):
        append_authority_receipt(connection, conflicting)


def test_receipt_rejects_invalid_signature_and_non_whitelisted_type() -> None:
    connection = RegistryConnection()
    private = _private()
    append_authority_trust_key(connection, _trust(private))
    bad_signature = _receipt(private, signature_private=_private(b"wrong"))
    with pytest.raises(AuthorityRegistryValidationError, match="signature is invalid"):
        append_authority_receipt(connection, bad_signature)

    with pytest.raises(AuthorityRegistryValidationError, match="whitelisted"):
        _receipt(private, receipt_type="OTHER")


def test_receipt_rejects_registry_write_after_claim_availability() -> None:
    connection = RegistryConnection()
    private = _private()
    append_authority_trust_key(connection, _trust(private))
    registration = _receipt(
        private,
        available_at=NOW - timedelta(seconds=1),
    )

    with pytest.raises(
        AuthorityRegistryValidationError,
        match="registered by claim.available_at",
    ):
        append_authority_receipt(connection, registration)

    assert connection.tables["st_execution_authority_receipt_v2"] == []


def test_key_and_receipt_revocations_insert_and_exact_replay() -> None:
    connection = RegistryConnection()
    registration = _seed_receipt(connection)
    receipt_revocation = AuthorityReceiptRevocation(
        receipt_id=registration.receipt.receipt_id,
        receipt_hash=registration.receipt.receipt_hash,
        envelope_hash=registration.receipt.envelope_hash,
        revoked_at=NOW - timedelta(seconds=1),
        reason_code="COMPROMISED_RECEIPT",
    )
    first_receipt = append_authority_receipt_revocation(connection, receipt_revocation)
    replay_receipt = append_authority_receipt_revocation(connection, receipt_revocation)
    assert first_receipt.status is AuthorityRegistryWriteStatus.INSERTED
    assert replay_receipt.status is AuthorityRegistryWriteStatus.IDEMPOTENT

    key_revocation = AuthorityKeyRevocation(
        source_provider="calendar-provider",
        key_id="calendar-key",
        key_version="v1",
        revoked_at=NOW - timedelta(seconds=1),
        reason_code="KEY_COMPROMISED",
    )
    first_key = append_authority_key_revocation(connection, key_revocation)
    replay_key = append_authority_key_revocation(connection, key_revocation)
    assert first_key.status is AuthorityRegistryWriteStatus.INSERTED
    assert replay_key.status is AuthorityRegistryWriteStatus.IDEMPOTENT
    assert connection.lifecycle_calls == []


def test_revocation_hashes_match_migration_pipe_contract() -> None:
    key = AuthorityKeyRevocation(
        source_provider="provider",
        key_id="key",
        key_version="v1",
        revoked_at=NOW,
        reason_code="ROTATED",
    )
    expected = hashlib.sha256(
        (
            "trading-v2.authority-key-revocation.v1|provider|key|v1|"
            "2026-08-04T02:03:04.567890+00:00|ROTATED"
        ).encode()
    ).hexdigest()
    assert key.revocation_hash == expected


def test_duplicate_insert_race_requires_new_transaction_and_no_readback() -> None:
    class RaceConnection(RegistryConnection):
        def execute(self, statement: Any, params: Mapping[str, Any] | None = None) -> _Result:
            if "v2ar:insert_trust_key" in str(statement):
                self.calls.append("insert_trust_key")
                raise IntegrityError("insert", params, Exception("1062 duplicate"))
            return super().execute(statement, params)

    connection = RaceConnection()
    with pytest.raises(AuthorityRegistryConflictError, match="new transaction"):
        append_authority_trust_key(connection, _trust())
    assert connection.calls == [
        "maintenance_fence",
        "trust_key_candidates",
        "insert_trust_key",
    ]


def test_core_writer_has_no_engine_or_transaction_lifecycle_ownership() -> None:
    source = inspect.getsource(writer)
    assert "create_engine" not in source
    assert ".commit(" not in source
    assert ".rollback(" not in source
    assert ".begin(" not in source
