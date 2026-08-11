from __future__ import annotations

import base64
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from server.integrations.v2_execution_evidence_authority import (
    AuthorityClaim,
    AuthorityDecision,
    AuthorityReceiptReference,
    AuthorityVerificationLevel,
    SignedAuthorityReceipt,
    authority_receipt_signature_message,
    build_authority_claim,
    build_instrument_rule_authority_claim,
)
from server.integrations.v2_execution_evidence_authority.verifier import (
    _attestation_storage,
)
from server.integrations.v2_execution_evidence_authority_audit import (
    AUTHORITY_AUDIT_TABLES,
    V2AuthorityStoredRowAuditError,
    V2AuthorityStoredRowAuditParents,
    audit_v2_execution_evidence_authority_database,
    audit_v2_execution_evidence_authority_rows,
)
from server.trading_v2.execution_evidence import AuthorityStatus, CanonicalJson
from tools.trading_v2_evidence_behavioral_scenario import (
    build_behavioral_scenario,
)


CORE_AUDIT_TABLES = (
    "st_market_calendar_evidence_v2",
    "st_quote_receipt_evidence_v2",
    "st_fill_execution_evidence_v2",
    "st_cash_event_binding_v2",
    "st_order_transition_v2",
)
INSTRUMENT_RULE_TABLE = "st_instrument_rule_v2"


def _stored(value: datetime) -> datetime:
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _pipe_hash(namespace: str, *items: str) -> str:
    return hashlib.sha256(
        (namespace + "|" + "|".join(items)).encode("utf-8")
    ).hexdigest()


def _authoritative_quote():
    scenario = build_behavioral_scenario()
    quote = next(
        case.evidence
        for case in scenario.cases
        if case.evidence_type == "QUOTE_RECEIPT"
    )
    receipt_hash = quote.receipt_payload.payload_hash
    return replace(
        quote,
        source_receipt_hash=receipt_hash,
        provenance=replace(
            quote.provenance,
            authority_status=AuthorityStatus.EXTERNAL_RECEIPT_VERIFIED,
            authority_receipt_hash=receipt_hash,
        ),
    )


def _rows(
    *,
    claim: AuthorityClaim | None = None,
    parents: V2AuthorityStoredRowAuditParents | None = None,
):
    private_key = Ed25519PrivateKey.generate()
    public_key = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    quote = _authoritative_quote()
    if claim is None:
        claim = build_authority_claim(quote)
    if parents is None:
        parents = V2AuthorityStoredRowAuditParents(
            calendars={},
            quotes={quote.quote_evidence_id: quote},
            fills={},
            instrument_rules={},
        )
    available_at = claim.available_at
    issued_at = available_at - timedelta(seconds=10)
    expires_at = available_at + timedelta(hours=1)
    message = authority_receipt_signature_message(
        claim_hash=claim.claim_hash,
        source_provider=claim.source_provider,
        receipt_id=claim.receipt_id,
        receipt_hash=claim.receipt_hash,
        key_id="provider-key",
        key_version="2026-08",
        replay_nonce="nonce-1",
        issued_at=issued_at,
        expires_at=expires_at,
    )
    signature = base64.urlsafe_b64encode(private_key.sign(message)).decode(
        "ascii"
    ).rstrip("=")
    receipt = SignedAuthorityReceipt(
        claim_hash=claim.claim_hash,
        source_provider=claim.source_provider,
        receipt_id=claim.receipt_id,
        receipt_hash=claim.receipt_hash,
        key_id="provider-key",
        key_version="2026-08",
        replay_nonce="nonce-1",
        issued_at=issued_at,
        expires_at=expires_at,
        signature=signature,
    )
    receipt_created_at = available_at - timedelta(seconds=5)
    decision = AuthorityDecision(
        claim_hash=claim.claim_hash,
        verified=True,
        verifier_id="ed25519-authority-receipt",
        verifier_version="v1",
        verified_at=available_at + timedelta(seconds=1),
        reason_code="VERIFIED",
        verification_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
        receipt_envelope_hash=receipt.envelope_hash,
        trust_key_id=receipt.key_id,
        trust_key_version=receipt.key_version,
        replay_nonce=receipt.replay_nonce,
    )
    attestation, attestation_hash = _attestation_storage(claim, decision)
    key_revoked_at = available_at + timedelta(seconds=5)
    key_revocation_created_at = available_at + timedelta(seconds=6)
    key_revocation_hash = _pipe_hash(
        "trading-v2.authority-key-revocation.v1",
        claim.source_provider,
        receipt.key_id,
        receipt.key_version,
        key_revoked_at.astimezone(timezone.utc).isoformat(timespec="microseconds"),
        "KEY_ROTATED",
    )
    receipt_revoked_at = available_at + timedelta(seconds=7)
    receipt_revocation_created_at = available_at + timedelta(seconds=8)
    receipt_revocation_hash = _pipe_hash(
        "trading-v2.authority-receipt-revocation.v1",
        receipt.receipt_id,
        receipt.receipt_hash,
        receipt.envelope_hash,
        receipt_revoked_at.astimezone(timezone.utc).isoformat(
            timespec="microseconds"
        ),
        "SOURCE_RETRACTED",
    )
    rows = {
        "st_execution_authority_trust_key_v2": (
            {
                "source_provider": claim.source_provider,
                "key_id": receipt.key_id,
                "key_version": receipt.key_version,
                "algorithm": "Ed25519",
                "public_key": public_key,
                "public_key_hash": hashlib.sha256(public_key).hexdigest(),
                "valid_from": _stored(available_at - timedelta(days=1)),
                "valid_to": _stored(available_at + timedelta(days=1)),
                "registered_at": _stored(available_at - timedelta(hours=1)),
                "__dbhash_public_key_hash": hashlib.sha256(public_key).hexdigest(),
            },
        ),
        "st_execution_authority_receipt_v2": (
            {
                "receipt_id": receipt.receipt_id,
                "receipt_hash": receipt.receipt_hash,
                "claim_hash": receipt.claim_hash,
                "evidence_type": claim.evidence_type,
                "evidence_id": claim.evidence_id,
                "source_provider": claim.source_provider,
                "source_payload_hash": claim.source_payload_hash,
                "receipt_type": claim.receipt_type,
                "key_id": receipt.key_id,
                "key_version": receipt.key_version,
                "replay_nonce": receipt.replay_nonce,
                "issued_at": _stored(receipt.issued_at),
                "expires_at": _stored(receipt.expires_at),
                "envelope_json": receipt.envelope_json,
                "envelope_hash": receipt.envelope_hash,
                "status": "ACTIVE",
                "revoked_at": None,
                "created_at": _stored(receipt_created_at),
                "__dbhash_envelope_hash": receipt.envelope_hash,
                "__dbhash_signature_message": hashlib.sha256(
                    receipt.signature_message
                ).hexdigest(),
            },
        ),
        "st_execution_authority_key_revocation_v2": (
            {
                "source_provider": claim.source_provider,
                "key_id": receipt.key_id,
                "key_version": receipt.key_version,
                "revoked_at": _stored(key_revoked_at),
                "reason_code": "KEY_ROTATED",
                "revocation_hash": key_revocation_hash,
                "created_at": _stored(key_revocation_created_at),
                "__dbhash_revocation_hash": key_revocation_hash,
            },
        ),
        "st_execution_authority_receipt_revocation_v2": (
            {
                "receipt_id": receipt.receipt_id,
                "receipt_hash": receipt.receipt_hash,
                "envelope_hash": receipt.envelope_hash,
                "revoked_at": _stored(receipt_revoked_at),
                "reason_code": "SOURCE_RETRACTED",
                "revocation_hash": receipt_revocation_hash,
                "created_at": _stored(receipt_revocation_created_at),
                "__dbhash_revocation_hash": receipt_revocation_hash,
            },
        ),
        "st_execution_authority_attestation_v2": (
            {
                **attestation,
                "__dbhash_decision_hash": decision.decision_hash,
                "__dbhash_attestation_hash": attestation_hash,
            },
        ),
    }
    return rows, claim, receipt, parents


def _copy(rows):
    return {
        table: tuple(dict(row) for row in values)
        for table, values in rows.items()
    }


class _Result:
    def __init__(self, rows):
        self.rows = tuple(dict(row) for row in rows)

    def mappings(self):
        return self

    def all(self):
        return tuple(dict(row) for row in self.rows)


class _Connection:
    def __init__(
        self,
        rows,
        *,
        core_rows=None,
        rule_rows=(),
        active=True,
        isolation="REPEATABLE READ",
    ):
        self.rows = rows
        self.core_rows = (
            {table: () for table in CORE_AUDIT_TABLES}
            if core_rows is None
            else core_rows
        )
        self.rule_rows = tuple(rule_rows)
        self.active = active
        self.isolation = isolation
        self.statements = []

    def in_transaction(self):
        return self.active

    def get_isolation_level(self):
        return self.isolation

    def execute(self, statement):
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        tables = (*AUTHORITY_AUDIT_TABLES, *CORE_AUDIT_TABLES, INSTRUMENT_RULE_TABLE)
        matches = [table for table in tables if f"FROM {table} " in sql]
        assert len(matches) == 1, sql
        table = matches[0]
        if table in AUTHORITY_AUDIT_TABLES:
            return _Result(self.rows[table])
        if table in CORE_AUDIT_TABLES:
            return _Result(self.core_rows[table])
        return _Result(self.rule_rows)


def test_full_nonempty_authority_inventory_is_reconstructed():
    rows, _, _, parents = _rows()

    report = audit_v2_execution_evidence_authority_rows(
        rows, parents=parents, shared_row_locks_used=True
    )

    assert report.audit_passed
    assert report.table_counts == tuple((table, 1) for table in AUTHORITY_AUDIT_TABLES)
    assert report.rows_reconstructed == 5
    assert report.hashes_verified == 7
    assert report.signatures_verified == 1
    assert report.database_sha2_used is True
    assert report.shared_row_locks_used is True
    assert report.production_activation_allowed is False


def test_self_consistent_attestation_for_wrong_core_claim_fails_closed():
    _, claim, _, parents = _rows()
    wrong_claim = replace(
        claim,
        trade_date=claim.trade_date - timedelta(days=1),
    )
    rows, _, _, _ = _rows(claim=wrong_claim, parents=parents)

    with pytest.raises(V2AuthorityStoredRowAuditError, match="recomputed"):
        audit_v2_execution_evidence_authority_rows(rows, parents=parents)


def test_orphan_and_missing_core_attestations_fail_closed():
    _, claim, _, parents = _rows()
    orphan_claim = replace(claim, evidence_id="d" * 64)
    orphan_rows, _, _, _ = _rows(claim=orphan_claim, parents=parents)
    with pytest.raises(V2AuthorityStoredRowAuditError, match="absent quote parent"):
        audit_v2_execution_evidence_authority_rows(
            orphan_rows, parents=parents
        )

    rows, _, _, _ = _rows(parents=parents)
    rows = _copy(rows)
    rows["st_execution_authority_attestation_v2"] = ()
    with pytest.raises(V2AuthorityStoredRowAuditError, match="missing exact"):
        audit_v2_execution_evidence_authority_rows(rows, parents=parents)


def test_duplicate_attestation_coverage_fails_closed():
    original, _, _, parents = _rows()
    rows = _copy(original)
    rows["st_execution_authority_attestation_v2"] *= 2

    with pytest.raises(V2AuthorityStoredRowAuditError, match="duplicate"):
        audit_v2_execution_evidence_authority_rows(rows, parents=parents)


def _instrument_rule_case():
    scenario = build_behavioral_scenario()
    fill = next(
        case.evidence
        for case in scenario.cases
        if case.evidence_type == "FILL_EXECUTION"
    )
    reference = AuthorityReceiptReference(
        source_provider="instrument-rule-provider",
        receipt_id="instrument-rule-receipt-1",
        receipt_hash=fill.instrument_rule.payload_hash,
    )
    claim = build_instrument_rule_authority_claim(fill, reference)
    key = (
        fill.stock_code,
        fill.instrument_rule_version,
        fill.instrument_rule_effective_from,
    )
    parents = V2AuthorityStoredRowAuditParents(
        calendars={},
        quotes={},
        fills={fill.fill_execution_evidence_id: fill},
        instrument_rules={key: fill.instrument_rule},
    )
    return fill, claim, key, parents


def test_instrument_rule_attestation_reconstructs_one_fill_and_rule_parent():
    _, claim, _, parents = _instrument_rule_case()
    rows, _, _, _ = _rows(claim=claim, parents=parents)

    report = audit_v2_execution_evidence_authority_rows(
        rows, parents=parents, shared_row_locks_used=True
    )

    assert report.audit_passed


def test_instrument_rule_requires_exact_canonical_rule_and_unique_fill():
    fill, claim, key, parents = _instrument_rule_case()
    rows, _, _, _ = _rows(claim=claim, parents=parents)
    wrong_rule_parents = replace(
        parents,
        instrument_rules={key: CanonicalJson.from_value({"wrong": True})},
    )
    with pytest.raises(V2AuthorityStoredRowAuditError, match="differs"):
        audit_v2_execution_evidence_authority_rows(
            rows, parents=wrong_rule_parents
        )

    second_payload = fill.fill_payload.value()
    assert type(second_payload) is dict
    second_payload["fill_id"] = "mysql57-fill-ambiguous"
    second_accounting_request = fill.accounting_request.value()
    assert type(second_accounting_request) is dict
    second_accounting_request["fill_id"] = second_payload["fill_id"]
    second_fill = replace(
        fill,
        fill_id=second_payload["fill_id"],
        fill_payload=CanonicalJson.from_value(second_payload),
        accounting_request=CanonicalJson.from_value(second_accounting_request),
    )
    ambiguous_parents = replace(
        parents,
        fills={
            fill.fill_execution_evidence_id: fill,
            second_fill.fill_execution_evidence_id: second_fill,
        },
    )
    with pytest.raises(V2AuthorityStoredRowAuditError, match="matched 2"):
        audit_v2_execution_evidence_authority_rows(
            rows, parents=ambiguous_parents
        )


def test_all_five_authority_tables_may_legitimately_be_empty():
    rows = {table: () for table in AUTHORITY_AUDIT_TABLES}

    report = audit_v2_execution_evidence_authority_rows(
        rows, shared_row_locks_used=True
    )

    assert report.audit_passed
    assert report.rows_reconstructed == 0
    assert report.hashes_verified == 0
    assert report.signatures_verified == 0


def test_row_reconstruction_without_shared_locks_is_not_a_passing_audit():
    rows = {table: () for table in AUTHORITY_AUDIT_TABLES}

    report = audit_v2_execution_evidence_authority_rows(rows)

    assert report.audit_passed is False
    assert report.shared_row_locks_used is False


def test_database_audit_locks_authority_and_canonical_parent_inventories():
    rows = {table: () for table in AUTHORITY_AUDIT_TABLES}
    connection = _Connection(rows)

    report = audit_v2_execution_evidence_authority_database(connection)

    assert report.audit_passed
    assert report.shared_row_locks_used is True
    assert len(connection.statements) == 11
    for table, sql in zip(
        AUTHORITY_AUDIT_TABLES, connection.statements[:5], strict=True
    ):
        assert f"FROM {table}" in sql
        assert "SHA2(" in sql
        assert "ORDER BY" in sql
        assert "LOCK IN SHARE MODE" in sql
    assert sum("SHA2(" in sql for sql in connection.statements) == 10
    assert f"FROM {INSTRUMENT_RULE_TABLE}" in connection.statements[-1]
    assert "LOCK IN SHARE MODE" in connection.statements[-1]


def test_empty_database_still_executes_all_eleven_shared_lock_queries():
    rows = {table: () for table in AUTHORITY_AUDIT_TABLES}
    connection = _Connection(rows)

    report = audit_v2_execution_evidence_authority_database(connection)

    assert report.audit_passed
    assert report.rows_reconstructed == 0
    assert len(connection.statements) == 11
    assert all("LOCK IN SHARE MODE" in sql for sql in connection.statements)


def test_database_audit_requires_active_caller_transaction():
    with pytest.raises(V2AuthorityStoredRowAuditError, match="transaction"):
        audit_v2_execution_evidence_authority_database(
            SimpleNamespace(execute=lambda *_: None, in_transaction=lambda: False)
        )


def test_database_audit_rejects_read_committed_before_selecting_rows():
    rows = {table: () for table in AUTHORITY_AUDIT_TABLES}
    connection = _Connection(rows, isolation="READ COMMITTED")

    with pytest.raises(V2AuthorityStoredRowAuditError, match="REPEATABLE READ"):
        audit_v2_execution_evidence_authority_database(connection)

    assert connection.statements == []


@pytest.mark.parametrize(
    ("table", "column"),
    (
        ("st_execution_authority_trust_key_v2", "__dbhash_public_key_hash"),
        ("st_execution_authority_receipt_v2", "envelope_hash"),
        ("st_execution_authority_receipt_v2", "__dbhash_signature_message"),
        ("st_execution_authority_key_revocation_v2", "revocation_hash"),
        ("st_execution_authority_receipt_revocation_v2", "__dbhash_revocation_hash"),
        ("st_execution_authority_attestation_v2", "decision_hash"),
        ("st_execution_authority_attestation_v2", "__dbhash_attestation_hash"),
    ),
)
def test_python_or_database_hash_drift_fails_closed(table, column):
    original, _, _, parents = _rows()
    rows = _copy(original)
    rows[table][0][column] = "0" * 64

    with pytest.raises(V2AuthorityStoredRowAuditError):
        audit_v2_execution_evidence_authority_rows(rows, parents=parents)


def test_signature_is_verified_not_merely_hashed():
    original, _, receipt, parents = _rows()
    rows = _copy(original)
    wrong_private_key = Ed25519PrivateKey.generate()
    wrong_signature = base64.urlsafe_b64encode(
        wrong_private_key.sign(receipt.signature_message)
    ).decode("ascii").rstrip("=")
    wrong_receipt = SignedAuthorityReceipt(
        claim_hash=receipt.claim_hash,
        source_provider=receipt.source_provider,
        receipt_id=receipt.receipt_id,
        receipt_hash=receipt.receipt_hash,
        key_id=receipt.key_id,
        key_version=receipt.key_version,
        replay_nonce=receipt.replay_nonce,
        issued_at=receipt.issued_at,
        expires_at=receipt.expires_at,
        signature=wrong_signature,
    )
    receipt_row = rows["st_execution_authority_receipt_v2"][0]
    receipt_row["envelope_json"] = wrong_receipt.envelope_json
    receipt_row["envelope_hash"] = wrong_receipt.envelope_hash
    receipt_row["__dbhash_envelope_hash"] = wrong_receipt.envelope_hash
    rows["st_execution_authority_receipt_revocation_v2"] = ()
    rows["st_execution_authority_attestation_v2"] = ()

    with pytest.raises(V2AuthorityStoredRowAuditError, match="signature"):
        audit_v2_execution_evidence_authority_rows(rows, parents=parents)


def test_missing_parent_and_duplicate_identity_fail_closed():
    original, _, _, parents = _rows()
    missing = _copy(original)
    missing["st_execution_authority_trust_key_v2"] = ()
    with pytest.raises(V2AuthorityStoredRowAuditError, match="absent"):
        audit_v2_execution_evidence_authority_rows(missing, parents=parents)

    duplicate = _copy(original)
    duplicate["st_execution_authority_trust_key_v2"] *= 2
    with pytest.raises(V2AuthorityStoredRowAuditError, match="duplicate"):
        audit_v2_execution_evidence_authority_rows(duplicate, parents=parents)


def test_revocation_stored_before_receipt_or_attestation_fails_closed():
    original, claim, _, parents = _rows()
    rows = _copy(original)
    revocation = rows["st_execution_authority_key_revocation_v2"][0]
    revoked_at = claim.available_at - timedelta(seconds=7)
    revocation["revoked_at"] = _stored(revoked_at)
    revocation["created_at"] = _stored(
        claim.available_at - timedelta(seconds=6)
    )
    digest = _pipe_hash(
        "trading-v2.authority-key-revocation.v1",
        revocation["source_provider"],
        revocation["key_id"],
        revocation["key_version"],
        revoked_at.astimezone(timezone.utc).isoformat(timespec="microseconds"),
        revocation["reason_code"],
    )
    revocation["revocation_hash"] = digest
    revocation["__dbhash_revocation_hash"] = digest

    with pytest.raises(V2AuthorityStoredRowAuditError, match="after key revocation"):
        audit_v2_execution_evidence_authority_rows(rows, parents=parents)


def test_exact_table_and_row_column_inventory_is_required():
    original, _, _, parents = _rows()
    rows = _copy(original)
    rows["st_execution_authority_receipt_v2"][0]["unexpected"] = 1
    with pytest.raises(V2AuthorityStoredRowAuditError, match="columns differ"):
        audit_v2_execution_evidence_authority_rows(rows, parents=parents)

    rows = _copy(original)
    rows.pop("st_execution_authority_attestation_v2")
    with pytest.raises(V2AuthorityStoredRowAuditError, match="exactly the five"):
        audit_v2_execution_evidence_authority_rows(rows, parents=parents)


def test_invalid_enum_time_and_hex_are_rejected():
    original, claim, _, parents = _rows()
    enum_rows = _copy(original)
    enum_rows["st_execution_authority_receipt_v2"][0]["evidence_type"] = "UNKNOWN"
    with pytest.raises(V2AuthorityStoredRowAuditError, match="unsupported"):
        audit_v2_execution_evidence_authority_rows(enum_rows, parents=parents)

    time_rows = _copy(original)
    time_rows["st_execution_authority_receipt_v2"][0]["expires_at"] = _stored(
        claim.available_at - timedelta(hours=1)
    )
    with pytest.raises(V2AuthorityStoredRowAuditError, match="chronology"):
        audit_v2_execution_evidence_authority_rows(time_rows, parents=parents)

    hex_rows = _copy(original)
    hex_rows["st_execution_authority_receipt_v2"][0]["claim_hash"] = "A" * 64
    with pytest.raises(V2AuthorityStoredRowAuditError, match="lowercase SHA-256"):
        audit_v2_execution_evidence_authority_rows(hex_rows, parents=parents)


def test_database_sha2_proof_cannot_be_disabled():
    rows, _, _, parents = _rows()
    with pytest.raises(V2AuthorityStoredRowAuditError, match="mandatory"):
        audit_v2_execution_evidence_authority_rows(
            rows, parents=parents, database_sha2_used=False
        )
