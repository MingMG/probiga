from __future__ import annotations

from copy import deepcopy

import pytest

from server.integrations.v2_execution_evidence_audit import (
    EVIDENCE_JSON_HASH_COLUMNS,
    V2EvidenceHashAuditError,
    audit_v2_execution_evidence_database,
    audit_v2_execution_evidence_rows,
)
from server.integrations.v2_execution_evidence_writer.writer import (
    _calendar_storage,
    _cash_storage,
    _fill_storage,
    _order_storage,
    _quote_storage,
)
from server.trading_v2.execution_evidence import CanonicalJson
from tools.trading_v2_evidence_behavioral_scenario import (
    build_behavioral_scenario,
)


def _rows():
    scenario = build_behavioral_scenario()
    storage = {
        "MARKET_CALENDAR": (
            "st_market_calendar_evidence_v2",
            _calendar_storage,
        ),
        "QUOTE_RECEIPT": (
            "st_quote_receipt_evidence_v2",
            _quote_storage,
        ),
        "FILL_EXECUTION": (
            "st_fill_execution_evidence_v2",
            _fill_storage,
        ),
        "CASH_EVENT": ("st_cash_event_binding_v2", _cash_storage),
        "ORDER_TRANSITION": ("st_order_transition_v2", _order_storage),
    }
    result = {table: [] for table in EVIDENCE_JSON_HASH_COLUMNS}
    for case in scenario.cases:
        table, projector = storage[case.evidence_type]
        row = dict(projector(case.evidence))
        for json_column, hash_column in EVIDENCE_JSON_HASH_COLUMNS[table]:
            row[f"__dbhash_{hash_column}"] = CanonicalJson(
                row[json_column]
            ).payload_hash
        result[table].append(row)
    return {table: tuple(rows) for table, rows in result.items()}


class _Mappings:
    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return _Mappings(self._rows)


class _Connection:
    def __init__(self, rows, *, active=True):
        self.rows = rows
        self.active = active
        self.statements = []

    def in_transaction(self):
        return self.active

    def execute(self, statement):
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        for table, rows in self.rows.items():
            if f"FROM {table}" in sql:
                return _Result(rows)
        raise AssertionError(sql)


def test_full_five_table_audit_recomputes_payloads_and_rebuilds_chains():
    report = audit_v2_execution_evidence_rows(_rows())

    assert report.audit_passed is True
    assert report.rows_reconstructed == 5
    assert report.payload_hashes_verified == 13
    assert report.cash_chains_checked == 1
    assert report.order_chains_checked == 1
    assert report.complete_cash_chains == 0
    assert report.complete_order_chains == 0
    assert report.external_authority_claims == 0
    assert report.production_activation_allowed is False


def test_database_entry_point_uses_mysql_sha2_and_shared_locks():
    connection = _Connection(_rows())

    report = audit_v2_execution_evidence_database(connection)

    assert report.audit_passed is True
    assert report.shared_row_locks_used is True
    assert len(connection.statements) == 5
    assert all("SHA2(" in sql for sql in connection.statements)
    assert all("LOCK IN SHARE MODE" in sql for sql in connection.statements)


def test_database_entry_point_requires_caller_owned_transaction():
    with pytest.raises(V2EvidenceHashAuditError, match="already be in a transaction"):
        audit_v2_execution_evidence_database(_Connection(_rows(), active=False))


def test_database_and_python_hash_disagreement_fails_closed():
    rows = deepcopy(_rows())
    row = rows["st_fill_execution_evidence_v2"][0]
    row["__dbhash_matcher_output_hash"] = "0" * 64

    with pytest.raises(V2EvidenceHashAuditError, match="database SHA2"):
        audit_v2_execution_evidence_rows(rows)


def test_full_evidence_hash_is_rebuilt_instead_of_trusted():
    rows = deepcopy(_rows())
    row = rows["st_order_transition_v2"][0]
    row["transition_hash"] = "0" * 64

    with pytest.raises(V2EvidenceHashAuditError, match="reconstructed evidence"):
        audit_v2_execution_evidence_rows(rows)


def test_missing_upstream_reference_fails_closed():
    rows = deepcopy(_rows())
    rows["st_quote_receipt_evidence_v2"] = ()

    with pytest.raises(V2EvidenceHashAuditError, match="absent market evidence"):
        audit_v2_execution_evidence_rows(rows)


def test_exact_five_table_inventory_is_required():
    rows = _rows()
    rows.pop("st_cash_event_binding_v2")

    with pytest.raises(V2EvidenceHashAuditError, match="exactly the five"):
        audit_v2_execution_evidence_rows(rows)


def test_vacuous_audit_fails_and_lock_proof_remains_explicit():
    empty = {table: () for table in EVIDENCE_JSON_HASH_COLUMNS}
    report = audit_v2_execution_evidence_rows(empty)
    assert report.audit_passed is False

    unlocked = audit_v2_execution_evidence_rows(
        _rows(),
        shared_row_locks_used=False,
    )
    assert unlocked.audit_passed is True
    assert unlocked.shared_row_locks_used is False
