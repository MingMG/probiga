from __future__ import annotations

import base64
import hashlib
from dataclasses import replace
from datetime import timedelta, timezone
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy.exc import IntegrityError

from server.integrations.v2_execution_evidence_authority import (
    AuthorityAttestationStatus,
    AuthorityAttestationConflictError,
    AuthorityDecision,
    AuthorityNonceReplayError,
    AuthorityTrustKey,
    AuthorityVerificationLevel,
    AuthorityVerificationError,
    DenyAllAuthorityVerifier,
    Ed25519AuthorityVerifier,
    MySQLAuthorityReceiptLoader,
    MySQLRegistryBackedAuthorityVerifier,
    MySQLReceiptRegistryAuthorityVerifier,
    SignedAuthorityReceipt,
    append_authority_attestation,
    authority_receipt_signature_message,
    build_authority_claim,
    require_verified_authority,
)
from server.trading_v2.execution_evidence import (
    AuthorityStatus,
    QuoteReceiptType,
)
from tools.trading_v2_evidence_behavioral_scenario import (
    build_behavioral_scenario,
)


class _ScalarResult:
    def __init__(self, value: int) -> None:
        self.value = value

    def scalar(self):
        return self.value


class _RegistryConnection:
    def __init__(self, count: int) -> None:
        self.count = count
        self.calls: list[tuple[str, dict[str, object]]] = []

    def execute(self, statement, parameters):
        self.calls.append((" ".join(str(statement).split()), dict(parameters)))
        return _ScalarResult(self.count)


class _MappingsResult:
    def __init__(self, rows=(), *, rowcount=-1) -> None:
        self._rows = tuple(dict(item) for item in rows)
        self.rowcount = rowcount

    def mappings(self):
        return self

    def all(self):
        return tuple(dict(item) for item in self._rows)

    def first(self):
        return None if not self._rows else dict(self._rows[0])

    def __iter__(self):
        return iter(self.all())


class _AuthorityConnection:
    def __init__(
        self,
        *,
        receipt_row=None,
        registry_row=None,
        fence_state="INACTIVE",
    ) -> None:
        self.receipt_row = receipt_row
        self.registry_row = registry_row
        self.attestation_row = None
        self.fence_state = fence_state
        self.calls: list[tuple[str, dict[str, object]]] = []

    def in_transaction(self):
        return True

    def execute(self, statement, parameters):
        sql = " ".join(str(statement).split())
        params = dict(parameters)
        self.calls.append((sql, params))
        if "schema_migration_v2_maintenance_fence" in sql:
            return _MappingsResult(
                (
                    {
                        "fence_name": "execution_evidence_011_015",
                        "state": self.fence_state,
                    },
                )
            )
        if "INNER JOIN st_execution_authority_trust_key_v2" in sql:
            rows = () if self.registry_row is None else (self.registry_row,)
            return _MappingsResult(rows)
        if "FROM st_execution_authority_receipt_v2" in sql:
            rows = () if self.receipt_row is None else (self.receipt_row,)
            return _MappingsResult(rows)
        if sql.startswith("SELECT") and "st_execution_authority_attestation_v2" in sql:
            rows = () if self.attestation_row is None else (self.attestation_row,)
            return _MappingsResult(rows)
        if sql.startswith("INSERT INTO st_execution_authority_attestation_v2"):
            self.attestation_row = dict(params)
            return _MappingsResult(rowcount=1)
        raise AssertionError(sql)


def _authoritative_quote():
    scenario = build_behavioral_scenario()
    quote = next(
        case.evidence
        for case in scenario.cases
        if case.evidence_type == "QUOTE_RECEIPT"
    )
    assert quote.receipt_type is QuoteReceiptType.OTHER
    receipt_hash = quote.receipt_payload.payload_hash
    provenance = replace(
        quote.provenance,
        authority_status=AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED,
        authority_receipt_hash=receipt_hash,
    )
    return replace(
        quote,
        source_receipt_hash=receipt_hash,
        provenance=provenance,
    )


def test_claim_binds_exact_authoritative_quote_fields():
    quote = _authoritative_quote()

    claim = build_authority_claim(quote)

    assert claim.evidence_type == "QUOTE_RECEIPT"
    assert claim.evidence_id == quote.quote_evidence_id
    assert claim.receipt_id == quote.source_receipt_id
    assert claim.receipt_hash == quote.source_receipt_hash
    assert len(claim.claim_hash) == 64


def test_default_deny_and_wrong_claim_decision_fail_closed():
    quote = _authoritative_quote()
    connection = object()

    with pytest.raises(AuthorityVerificationError, match="denied"):
        require_verified_authority(
            connection,
            quote,
            DenyAllAuthorityVerifier(),
        )

    class WrongClaimVerifier:
        def verify(self, _connection, claim):
            return AuthorityDecision(
                claim_hash="0" * 64,
                verified=True,
                verifier_id="test",
                verifier_version="v1",
                verified_at=claim.available_at,
                reason_code="VERIFIED",
            )

    with pytest.raises(AuthorityVerificationError, match="another claim"):
        require_verified_authority(connection, quote, WrongClaimVerifier())


def test_explicit_bound_verifier_can_authorize_after_availability():
    quote = _authoritative_quote()

    class BoundVerifier:
        def verify(self, _connection, claim):
            return AuthorityDecision(
                claim_hash=claim.claim_hash,
                verified=True,
                verifier_id="test-trust-root",
                verifier_version="key-v3",
                verified_at=claim.available_at + timedelta(microseconds=1),
                reason_code="VERIFIED",
            )

    decision = require_verified_authority(object(), quote, BoundVerifier())
    assert decision.verified is True
    assert decision.verifier_version == "key-v3"


def test_registry_verifier_requires_supported_passed_exact_receipt():
    quote = _authoritative_quote()
    quote = replace(quote, receipt_type=QuoteReceiptType.QMT_REALTIME)
    claim = build_authority_claim(quote)
    connection = _RegistryConnection(1)
    verifier = MySQLReceiptRegistryAuthorityVerifier(
        clock=lambda: quote.available_at + timedelta(microseconds=1)
    )

    decision = verifier.verify(connection, claim)

    assert decision.verified is True
    assert decision.reason_code == "VERIFIED"
    assert len(connection.calls) == 1
    sql, params = connection.calls[0]
    assert "st_qmt_realtime_sync_receipt_v2" in sql
    assert "FOR UPDATE" in sql
    assert params["receipt_id"] == quote.source_receipt_id

    denied = MySQLReceiptRegistryAuthorityVerifier(
        clock=lambda: quote.available_at + timedelta(microseconds=1)
    ).verify(_RegistryConnection(0), claim)
    assert denied.verified is False
    assert denied.reason_code == "RECEIPT_REGISTRY_MISMATCH"


def test_registry_does_not_upgrade_other_or_calendar_claims():
    quote = _authoritative_quote()
    claim = build_authority_claim(quote)
    verifier = MySQLReceiptRegistryAuthorityVerifier(
        clock=lambda: quote.available_at + timedelta(microseconds=1)
    )

    decision = verifier.verify(_RegistryConnection(1), claim)

    assert decision.verified is False
    assert decision.reason_code == "UNSUPPORTED_RECEIPT_TYPE"


def test_verification_time_before_availability_is_rejected():
    quote = _authoritative_quote()

    class EarlyVerifier:
        def verify(self, _connection, claim):
            return AuthorityDecision(
                claim_hash=claim.claim_hash,
                verified=True,
                verifier_id="test",
                verifier_version="v1",
                verified_at=claim.available_at - timedelta(microseconds=1),
                reason_code="VERIFIED",
            )

    with pytest.raises(AuthorityVerificationError, match="before"):
        require_verified_authority(object(), quote, EarlyVerifier())


def test_non_protocol_or_invalid_decision_is_rejected():
    quote = _authoritative_quote()
    with pytest.raises(AuthorityVerificationError, match="explicit verifier"):
        require_verified_authority(object(), quote, SimpleNamespace())

    class InvalidVerifier:
        def verify(self, _connection, _claim):
            return object()

    with pytest.raises(AuthorityVerificationError, match="decision type"):
        require_verified_authority(object(), quote, InvalidVerifier())


def _signed_receipt(claim, private_key, *, nonce="nonce-1", expires=None):
    issued_at = claim.available_at - timedelta(seconds=1)
    expires_at = expires or (claim.available_at + timedelta(hours=1))
    message = authority_receipt_signature_message(
        claim_hash=claim.claim_hash,
        source_provider=claim.source_provider,
        receipt_id=claim.receipt_id,
        receipt_hash=claim.receipt_hash,
        key_id="provider-key",
        key_version="2026-08",
        replay_nonce=nonce,
        issued_at=issued_at,
        expires_at=expires_at,
    )
    signature = base64.urlsafe_b64encode(private_key.sign(message)).decode(
        "ascii"
    ).rstrip("=")
    return SignedAuthorityReceipt(
        claim_hash=claim.claim_hash,
        source_provider=claim.source_provider,
        receipt_id=claim.receipt_id,
        receipt_hash=claim.receipt_hash,
        key_id="provider-key",
        key_version="2026-08",
        replay_nonce=nonce,
        issued_at=issued_at,
        expires_at=expires_at,
        signature=signature,
    )


class _ReceiptLoader:
    def __init__(self, receipt):
        self.receipt = receipt

    def load(self, _connection, _claim):
        return self.receipt


def _trust_key(private_key, quote, *, revoked_at=None):
    public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return AuthorityTrustKey(
        source_provider=quote.source_provider,
        key_id="provider-key",
        key_version="2026-08",
        public_key=public_key,
        valid_from=quote.available_at - timedelta(days=1),
        valid_to=quote.available_at + timedelta(days=1),
        revoked_at=revoked_at,
    )


def _registry_row(
    receipt,
    private_key,
    quote,
    *,
    key_revoked_at=None,
    receipt_revoked_at=None,
):
    public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )

    def stored(value):
        return (
            None
            if value is None
            else value.astimezone(timezone.utc).replace(tzinfo=None)
        )

    return {
        "envelope_json": receipt.envelope_json,
        "envelope_hash": receipt.envelope_hash,
        "public_key": public_key,
        "public_key_hash": hashlib.sha256(public_key).hexdigest(),
        "key_valid_from": stored(quote.available_at - timedelta(days=1)),
        "key_valid_to": stored(quote.available_at + timedelta(days=1)),
        "key_revoked_at": stored(key_revoked_at),
        "receipt_revoked_at": stored(receipt_revoked_at),
    }


def test_ed25519_verifier_binds_key_signature_expiry_and_nonce():
    quote = _authoritative_quote()
    claim = build_authority_claim(quote)
    private_key = Ed25519PrivateKey.generate()
    receipt = _signed_receipt(claim, private_key)
    verifier = Ed25519AuthorityVerifier(
        loader=_ReceiptLoader(receipt),
        trust_keys=(_trust_key(private_key, quote),),
        clock=lambda: quote.available_at + timedelta(seconds=1),
    )

    decision = require_verified_authority(
        object(),
        quote,
        verifier,
        minimum_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
    )

    assert decision.verification_level is AuthorityVerificationLevel.CRYPTOGRAPHIC
    assert decision.receipt_envelope_hash == receipt.envelope_hash
    assert decision.trust_key_id == "provider-key"
    assert decision.replay_nonce == "nonce-1"
    assert SignedAuthorityReceipt.from_json(receipt.envelope_json) == receipt


def test_registry_only_decision_cannot_satisfy_cryptographic_gate():
    quote = replace(
        _authoritative_quote(), receipt_type=QuoteReceiptType.QMT_REALTIME
    )
    verifier = MySQLReceiptRegistryAuthorityVerifier(
        clock=lambda: quote.available_at + timedelta(seconds=1)
    )

    with pytest.raises(AuthorityVerificationError, match="below"):
        require_verified_authority(
            _RegistryConnection(1),
            quote,
            verifier,
            minimum_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
        )


def test_signature_tamper_expiry_revocation_and_claim_replay_are_denied():
    quote = _authoritative_quote()
    claim = build_authority_claim(quote)
    private_key = Ed25519PrivateKey.generate()
    receipt = _signed_receipt(claim, private_key)
    bad_signature = ("A" if receipt.signature[0] != "A" else "B") + receipt.signature[1:]
    tampered = replace(receipt, signature=bad_signature)
    verifier = Ed25519AuthorityVerifier(
        loader=_ReceiptLoader(tampered),
        trust_keys=(_trust_key(private_key, quote),),
        clock=lambda: quote.available_at + timedelta(seconds=1),
    )
    with pytest.raises(AuthorityVerificationError, match="SIGNATURE_INVALID"):
        require_verified_authority(object(), quote, verifier)

    expired_receipt = _signed_receipt(
        claim,
        private_key,
        expires=quote.available_at + timedelta(microseconds=1),
    )
    expired = Ed25519AuthorityVerifier(
        loader=_ReceiptLoader(expired_receipt),
        trust_keys=(_trust_key(private_key, quote),),
        clock=lambda: quote.available_at + timedelta(seconds=1),
    )
    with pytest.raises(AuthorityVerificationError, match="RECEIPT_EXPIRED"):
        require_verified_authority(object(), quote, expired)

    revoked = Ed25519AuthorityVerifier(
        loader=_ReceiptLoader(receipt),
        trust_keys=(
            _trust_key(
                private_key,
                quote,
                revoked_at=quote.available_at + timedelta(microseconds=1),
            ),
        ),
        clock=lambda: quote.available_at + timedelta(seconds=1),
    )
    with pytest.raises(AuthorityVerificationError, match="TRUST_KEY_REVOKED"):
        require_verified_authority(object(), quote, revoked)

    another_quote = replace(quote, source_receipt_id="another-receipt")
    replay = Ed25519AuthorityVerifier(
        loader=_ReceiptLoader(receipt),
        trust_keys=(_trust_key(private_key, another_quote),),
        clock=lambda: another_quote.available_at + timedelta(seconds=1),
    )
    with pytest.raises(AuthorityVerificationError, match="RECEIPT_CLAIM_MISMATCH"):
        require_verified_authority(object(), another_quote, replay)


def test_mysql_receipt_loader_requires_one_exact_locked_canonical_envelope():
    quote = _authoritative_quote()
    claim = build_authority_claim(quote)
    private_key = Ed25519PrivateKey.generate()
    receipt = _signed_receipt(claim, private_key)
    connection = _AuthorityConnection(
        receipt_row={
            "envelope_json": receipt.envelope_json,
            "envelope_hash": receipt.envelope_hash,
        }
    )

    loaded = MySQLAuthorityReceiptLoader().load(connection, claim)

    assert loaded == receipt
    sql, params = connection.calls[0]
    assert sql.endswith("FOR UPDATE")
    assert "status = 'ACTIVE'" in sql
    assert "revoked_at IS NULL" in sql
    assert params["claim_hash"] == claim.claim_hash
    assert params["evidence_id"] == claim.evidence_id

    connection.receipt_row = {
        "envelope_json": receipt.envelope_json,
        "envelope_hash": "0" * 64,
    }
    with pytest.raises(AuthorityVerificationError, match="hash differs"):
        MySQLAuthorityReceiptLoader().load(connection, claim)


def test_mysql_registry_backed_verifier_loads_key_and_revocations_atomically():
    quote = _authoritative_quote()
    claim = build_authority_claim(quote)
    private_key = Ed25519PrivateKey.generate()
    receipt = _signed_receipt(claim, private_key)
    connection = _AuthorityConnection(
        registry_row=_registry_row(receipt, private_key, quote)
    )
    verifier = MySQLRegistryBackedAuthorityVerifier(
        clock=lambda: quote.available_at + timedelta(seconds=1)
    )

    decision = require_verified_authority(
        connection,
        quote,
        verifier,
        minimum_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
    )

    assert decision.verified is True
    sql, params = connection.calls[0]
    assert "st_execution_authority_trust_key_v2" in sql
    assert "st_execution_authority_key_revocation_v2" in sql
    assert "st_execution_authority_receipt_revocation_v2" in sql
    assert sql.endswith("FOR UPDATE")
    assert params["claim_hash"] == claim.claim_hash


@pytest.mark.parametrize(
    ("field", "reason"),
    (
        ("key_revoked_at", "TRUST_KEY_REVOKED"),
        ("receipt_revoked_at", "AUTHORITY_RECEIPT_REVOKED"),
    ),
)
def test_mysql_registry_backed_verifier_rejects_append_only_revocation(
    field,
    reason,
):
    quote = _authoritative_quote()
    claim = build_authority_claim(quote)
    private_key = Ed25519PrivateKey.generate()
    receipt = _signed_receipt(claim, private_key)
    revoked_at = quote.available_at + timedelta(microseconds=1)
    row = _registry_row(
        receipt,
        private_key,
        quote,
        **{field: revoked_at},
    )
    verifier = MySQLRegistryBackedAuthorityVerifier(
        clock=lambda: quote.available_at + timedelta(seconds=1)
    )

    with pytest.raises(AuthorityVerificationError, match=reason):
        require_verified_authority(
            _AuthorityConnection(registry_row=row),
            quote,
            verifier,
            minimum_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
        )


def test_authority_identity_text_rejects_non_ascii():
    quote = _authoritative_quote()
    claim = build_authority_claim(quote)
    with pytest.raises(AuthorityVerificationError, match="ASCII identity"):
        authority_receipt_signature_message(
            claim_hash=claim.claim_hash,
            source_provider=claim.source_provider,
            receipt_id="receipt-\u6536\u636e",
            receipt_hash=claim.receipt_hash,
            key_id="provider-key",
            key_version="2026-08",
            replay_nonce="nonce-1",
            issued_at=claim.available_at - timedelta(seconds=1),
            expires_at=claim.available_at + timedelta(hours=1),
        )


def test_cryptographic_attestation_is_append_only_idempotent_and_conflict_safe():
    quote = _authoritative_quote()
    claim = build_authority_claim(quote)
    private_key = Ed25519PrivateKey.generate()
    receipt = _signed_receipt(claim, private_key)
    verifier = Ed25519AuthorityVerifier(
        loader=_ReceiptLoader(receipt),
        trust_keys=(_trust_key(private_key, quote),),
        clock=lambda: quote.available_at + timedelta(seconds=1),
    )
    decision = require_verified_authority(
        object(),
        quote,
        verifier,
        minimum_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
    )
    connection = _AuthorityConnection()

    inserted = append_authority_attestation(connection, claim, decision)
    replayed = append_authority_attestation(connection, claim, decision)

    assert inserted.status is AuthorityAttestationStatus.INSERTED
    assert replayed.status is AuthorityAttestationStatus.IDEMPOTENT
    assert inserted.attestation_hash == replayed.attestation_hash
    assert connection.attestation_row["verification_level"] == "CRYPTOGRAPHIC"
    assert connection.attestation_row["receipt_envelope_hash"] == receipt.envelope_hash
    assert connection.attestation_row["replay_nonce"] == receipt.replay_nonce

    connection.attestation_row = {
        **connection.attestation_row,
        "decision_hash": "f" * 64,
    }
    with pytest.raises(AuthorityVerificationError, match="hash or decision differs"):
        append_authority_attestation(connection, claim, decision)


def test_active_maintenance_fence_blocks_authority_attestation() -> None:
    quote = _authoritative_quote()
    claim = build_authority_claim(quote)
    private_key = Ed25519PrivateKey.generate()
    receipt = _signed_receipt(claim, private_key)
    decision = require_verified_authority(
        object(),
        quote,
        Ed25519AuthorityVerifier(
            loader=_ReceiptLoader(receipt),
            trust_keys=(_trust_key(private_key, quote),),
            clock=lambda: quote.available_at + timedelta(seconds=1),
        ),
        minimum_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
    )
    connection = _AuthorityConnection(fence_state="ACTIVE")

    with pytest.raises(AuthorityVerificationError, match="maintenance fence"):
        append_authority_attestation(connection, claim, decision)

    assert connection.attestation_row is None


@pytest.mark.parametrize(
    ("key_name", "error_type"),
    (
        (
            "uk_authority_attestation_v2_replay",
            AuthorityNonceReplayError,
        ),
        ("PRIMARY", AuthorityAttestationConflictError),
    ),
)
def test_attestation_duplicate_errors_are_classified_without_failed_tx_reads(
    key_name,
    error_type,
):
    quote = _authoritative_quote()
    claim = build_authority_claim(quote)
    decision = AuthorityDecision(
        claim_hash=claim.claim_hash,
        verified=True,
        verifier_id="ed25519-authority-receipt",
        verifier_version="v1",
        verified_at=quote.available_at + timedelta(seconds=1),
        reason_code="VERIFIED",
        verification_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
        receipt_envelope_hash="a" * 64,
        trust_key_id="provider-key",
        trust_key_version="2026-08",
        replay_nonce="nonce-1",
    )

    class DuplicateConnection(_AuthorityConnection):
        def execute(self, statement, parameters):
            sql = " ".join(str(statement).split())
            if sql.startswith("INSERT INTO st_execution_authority_attestation_v2"):
                raise IntegrityError(
                    sql,
                    dict(parameters),
                    Exception(
                        "1062 Duplicate entry for key " + repr(key_name)
                    ),
                )
            return super().execute(statement, parameters)

    connection = DuplicateConnection()
    with pytest.raises(error_type):
        append_authority_attestation(connection, claim, decision)
    assert sum(
        sql.startswith("SELECT")
        and "st_execution_authority_attestation_v2" in sql
        for sql, _ in connection.calls
    ) == 1
    assert sum(
        "schema_migration_v2_maintenance_fence" in sql
        for sql, _ in connection.calls
    ) == 1


def test_attestation_rejects_missing_transaction_or_registry_only_decision():
    quote = _authoritative_quote()
    claim = build_authority_claim(quote)
    registry_decision = AuthorityDecision(
        claim_hash=claim.claim_hash,
        verified=True,
        verifier_id="registry",
        verifier_version="v1",
        verified_at=quote.available_at + timedelta(microseconds=1),
        reason_code="VERIFIED",
    )

    with pytest.raises(AuthorityVerificationError, match="active caller transaction"):
        append_authority_attestation(
            SimpleNamespace(
                in_transaction=lambda: False,
                execute=lambda *_args, **_kwargs: None,
            ),
            claim,
            registry_decision,
        )

    with pytest.raises(AuthorityVerificationError, match="cryptographic"):
        append_authority_attestation(_AuthorityConnection(), claim, registry_decision)
