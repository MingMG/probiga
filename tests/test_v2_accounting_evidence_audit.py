from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from typing import Any, Mapping

import pytest

from server.integrations.v2_accounting_evidence_audit import (
    ACCOUNTING_AUDIT_HASH_FIELDS,
    ACCOUNTING_AUDIT_PARENT_KINDS,
    ACCOUNTING_AUDIT_TABLES,
    FINALIZATION_TABLE,
    LOT_EFFECT_TABLE,
    OUTCOME_TABLE,
    V2AccountingEvidenceAuditError,
    V2AccountingEvidenceAuditParents,
    V2AccountingEvidenceAuditReport,
    audit_v2_accounting_evidence_database,
    audit_v2_accounting_evidence_rows,
    expected_accounting_hash_verifications,
)
from server.integrations.v2_accounting_evidence_writer import writer as writer_impl
from server.trading_v2.accounting_evidence import (
    FillAccountingOutcome,
    finalize_fill_accounting_outcome,
)
from tests.test_trading_v2_accounting_evidence import (
    _buy_outcome,
    _canonical_rows,
    _sell_outcome,
)


def _stored_fixture(
    outcome: FillAccountingOutcome,
) -> tuple[
    dict[str, tuple[dict[str, Any], ...]],
    V2AccountingEvidenceAuditParents,
]:
    finalization = finalize_fill_accounting_outcome(outcome)
    outcome_row = dict(writer_impl._outcome_storage(outcome))
    outcome_row.update(
        {
            "__dbhash_provenance_hash": outcome.provenance.provenance_hash,
            "__dbhash_lot_effect_root_hash": outcome.lot_effect_root_hash,
            "__dbhash_outcome_hash": outcome.outcome_hash,
        }
    )
    effect_rows: list[dict[str, Any]] = []
    for effect in outcome.lot_effects:
        row = dict(writer_impl._effect_storage(outcome, effect))
        row.update(
            {
                "__dbhash_provenance_hash": effect.provenance.provenance_hash,
                "__dbhash_lot_effect_root_hash": effect.lot_effect_root_hash,
                "__dbhash_before_lot_hash": (
                    None
                    if effect.before_lot is None
                    else effect.before_lot.snapshot_hash
                ),
                "__dbhash_after_lot_hash": effect.after_lot.snapshot_hash,
                "__dbhash_effect_hash": effect.effect_hash,
            }
        )
        effect_rows.append(row)
    finalization_row = dict(writer_impl._finalization_storage(finalization))
    finalization_row.update(
        {
            "__dbhash_provenance_hash": outcome.provenance.provenance_hash,
            "__dbhash_lot_effects_hash": outcome.lot_effects_hash,
            "__dbhash_finalization_hash": finalization.finalization_hash,
        }
    )

    canonical = _canonical_rows(outcome)
    fill = outcome.fill_execution_evidence
    fill_facts = {fill.fill_id: dict(canonical["lock_fill"])}
    for effect in outcome.lot_effects:
        opened_fill_id = effect.after_lot.opened_fill_id
        if opened_fill_id not in fill_facts:
            historical = dict(canonical["lock_fill"])
            historical["fill_id"] = opened_fill_id
            fill_facts[opened_fill_id] = historical
    parents = V2AccountingEvidenceAuditParents(
        fills={fill.fill_execution_evidence_id: fill},
        cash_bindings={outcome.cash_binding.cash_binding_id: outcome.cash_binding},
        order_transitions={
            outcome.order_transition.transition_id: outcome.order_transition
        },
        accounts={fill.account_id: dict(canonical["lock_account"])},
        orders={fill.order_id: dict(canonical["lock_order"])},
        fill_facts=fill_facts,
        cash_facts={
            outcome.cash_binding.cash_event_id: dict(canonical["lock_cash"])
        },
        lots={
            effect.after_lot.lot_id: dict(row)
            for effect, row in zip(
                outcome.lot_effects,
                canonical["lock_lots"],
                strict=True,
            )
        },
    )
    return (
        {
            OUTCOME_TABLE: (outcome_row,),
            LOT_EFFECT_TABLE: tuple(effect_rows),
            FINALIZATION_TABLE: (finalization_row,),
        },
        parents,
    )


def _audit(
    rows: Mapping[str, tuple[Mapping[str, Any], ...]],
    parents: V2AccountingEvidenceAuditParents,
):
    return audit_v2_accounting_evidence_rows(
        rows,
        parents=parents,
        database_sha2_used=True,
        shared_row_locks_used=True,
    )


@pytest.mark.parametrize(
    ("factory", "expected_rows", "expected_hashes", "expected_lot_chains"),
    (
        (_buy_outcome, 3, 11, 1),
        (_sell_outcome, 4, 16, 2),
    ),
)
def test_reconstructs_complete_buy_and_sell_accounting_evidence(
    factory,
    expected_rows: int,
    expected_hashes: int,
    expected_lot_chains: int,
) -> None:
    rows, parents = _stored_fixture(factory())
    report = _audit(rows, parents)

    assert report.audit_passed is True
    assert tuple(table for table, _ in report.table_counts) == ACCOUNTING_AUDIT_TABLES
    assert report.rows_reconstructed == expected_rows
    assert report.hashes_verified == expected_hashes
    assert report.hash_verifications == expected_accounting_hash_verifications(rows)
    assert report.finalized_outcomes == 1
    assert report.lot_chains_checked == expected_lot_chains
    assert report.parent_rows_checked >= 5
    assert report.production_activation_allowed is False
    assert report.actionable_output_allowed is False


def test_report_hash_counts_are_derived_from_declared_fields() -> None:
    rows, parents = _stored_fixture(_buy_outcome())
    report = _audit(rows, parents)

    assert report.hash_verifications == tuple(
        (
            table,
            len(rows[table]) * len(ACCOUNTING_AUDIT_HASH_FIELDS[table]),
        )
        for table in ACCOUNTING_AUDIT_TABLES
    )
    # BUY_CREATE has no before snapshot, but independently proving the stored
    # and DB-recomputed nullable hash are both NULL is still one verification.
    assert dict(report.hash_verifications)[LOT_EFFECT_TABLE] == 5


def test_report_rejects_redteam_duplicate_key_dict_collapse() -> None:
    rows, parents = _stored_fixture(_buy_outcome())
    report = _audit(rows, parents)
    forged = replace(
        report,
        table_counts=(
            (OUTCOME_TABLE, 0),
            (LOT_EFFECT_TABLE, 1),
            (FINALIZATION_TABLE, 1),
            (OUTCOME_TABLE, 1),
        ),
    )

    # The old implementation converted this vector to dict: all three names
    # and the last OUTCOME count survived, concealing the duplicate entry.
    assert tuple(dict(forged.table_counts)) == ACCOUNTING_AUDIT_TABLES
    assert forged.audit_passed is False


@pytest.mark.parametrize(
    "replacement",
    (
        [],
        ((OUTCOME_TABLE, 1), (FINALIZATION_TABLE, 1), (LOT_EFFECT_TABLE, 1)),
        ([OUTCOME_TABLE, 1], (LOT_EFFECT_TABLE, 1), (FINALIZATION_TABLE, 1)),
        ((1, 1), (LOT_EFFECT_TABLE, 1), (FINALIZATION_TABLE, 1)),
        ((OUTCOME_TABLE, True), (LOT_EFFECT_TABLE, 1), (FINALIZATION_TABLE, 1)),
        ((OUTCOME_TABLE, -1), (LOT_EFFECT_TABLE, 1), (FINALIZATION_TABLE, 1)),
    ),
)
def test_report_requires_exact_ordered_table_count_tuples(replacement: object) -> None:
    rows, parents = _stored_fixture(_buy_outcome())
    report = replace(_audit(rows, parents), table_counts=replacement)  # type: ignore[arg-type]
    assert report.audit_passed is False


def test_report_rejects_duplicate_and_underreported_hash_vectors() -> None:
    rows, parents = _stored_fixture(_buy_outcome())
    report = _audit(rows, parents)
    duplicate = replace(
        report,
        hash_verifications=(
            (OUTCOME_TABLE, 0),
            (LOT_EFFECT_TABLE, 5),
            (FINALIZATION_TABLE, 2),
            (OUTCOME_TABLE, 4),
        ),
    )
    underreported = replace(
        report,
        hash_verifications=(
            (OUTCOME_TABLE, 3),
            (LOT_EFFECT_TABLE, 5),
            (FINALIZATION_TABLE, 2),
        ),
        hashes_verified=10,
    )

    assert duplicate.audit_passed is False
    assert underreported.audit_passed is False


@pytest.mark.parametrize(
    "field",
    (
        "hashes_verified",
        "rows_reconstructed",
        "finalized_outcomes",
        "lot_chains_checked",
        "parent_rows_checked",
    ),
)
@pytest.mark.parametrize("replacement", (True, -1))
def test_report_requires_exact_nonnegative_integer_metrics(
    field: str,
    replacement: object,
) -> None:
    rows, parents = _stored_fixture(_buy_outcome())
    report = replace(_audit(rows, parents), **{field: replacement})
    assert report.audit_passed is False


@pytest.mark.parametrize(
    ("field", "replacement"),
    (
        ("database_sha2_used", 1),
        ("shared_row_locks_used", 1),
        ("finalized_outcome_ids", ()),
        ("lot_chain_ids", ("duplicated", "duplicated")),
        ("parent_row_checks", ()),
    ),
)
def test_report_requires_exact_booleans_and_proof_details(
    field: str,
    replacement: object,
) -> None:
    rows, parents = _stored_fixture(_buy_outcome())
    report = replace(_audit(rows, parents), **{field: replacement})
    assert report.audit_passed is False


def test_migration_recomputes_accounting_metrics_without_trusting_property(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from server.db import migrations_v2

    rows, parents = _stored_fixture(_buy_outcome())
    report = _audit(rows, parents)
    forged = replace(
        report,
        hash_verifications=(
            (OUTCOME_TABLE, 3),
            (LOT_EFFECT_TABLE, 5),
            (FINALIZATION_TABLE, 2),
        ),
        hashes_verified=10,
    )
    monkeypatch.setattr(
        V2AccountingEvidenceAuditReport,
        "audit_passed",
        property(lambda _self: True),
    )

    assert forged.audit_passed is True
    assert migrations_v2._accounting_audit_metrics_are_complete(
        forged,
        audit_tables=ACCOUNTING_AUDIT_TABLES,
        hash_fields=ACCOUNTING_AUDIT_HASH_FIELDS,
        parent_kinds=ACCOUNTING_AUDIT_PARENT_KINDS,
    ) is False


def test_empty_tables_are_valid_and_hash_counts_are_exact() -> None:
    rows = {table: () for table in ACCOUNTING_AUDIT_TABLES}
    parents = V2AccountingEvidenceAuditParents(
        fills={},
        cash_bindings={},
        order_transitions={},
        accounts={},
        orders={},
        fill_facts={},
        cash_facts={},
        lots={},
    )
    report = _audit(rows, parents)

    assert report.audit_passed is True
    assert report.rows_reconstructed == 0
    assert report.hashes_verified == 0
    assert report.finalized_outcomes == 0


@pytest.mark.parametrize(
    ("table", "column", "replacement", "message"),
    (
        (OUTCOME_TABLE, "__dbhash_outcome_hash", "0" * 64, "dbhash_outcome"),
        (OUTCOME_TABLE, "account_cash_before", Decimal("1.234"), "scale 2"),
        (LOT_EFFECT_TABLE, "__dbhash_after_lot_hash", "1" * 64, "after_lot"),
        (LOT_EFFECT_TABLE, "effect_kind", "UNKNOWN", "cannot be reconstructed"),
        (FINALIZATION_TABLE, "effect_hashes_json", "[]", "manifest differs"),
        (FINALIZATION_TABLE, "finalization_status", "PENDING", "cannot be reconstructed"),
    ),
)
def test_hash_enum_decimal_and_manifest_drift_fail_closed(
    table: str,
    column: str,
    replacement: object,
    message: str,
) -> None:
    rows, parents = _stored_fixture(_buy_outcome())
    changed = dict(rows[table][0])
    changed[column] = replacement
    rows[table] = (changed, *rows[table][1:])

    with pytest.raises(V2AccountingEvidenceAuditError, match=message):
        _audit(rows, parents)


def test_noncanonical_lot_json_and_duplicate_effects_fail_closed() -> None:
    rows, parents = _stored_fixture(_buy_outcome())
    changed = dict(rows[LOT_EFFECT_TABLE][0])
    changed["after_lot_json"] = changed["after_lot_json"] + " "
    rows[LOT_EFFECT_TABLE] = (changed,)
    with pytest.raises(V2AccountingEvidenceAuditError, match="canonical JSON"):
        _audit(rows, parents)

    rows, parents = _stored_fixture(_buy_outcome())
    rows[LOT_EFFECT_TABLE] = (
        rows[LOT_EFFECT_TABLE][0],
        dict(rows[LOT_EFFECT_TABLE][0]),
    )
    with pytest.raises(V2AccountingEvidenceAuditError, match="duplicate reconstructed"):
        _audit(rows, parents)


def test_missing_parent_or_orphan_child_fails_closed() -> None:
    rows, parents = _stored_fixture(_buy_outcome())
    missing_fill = V2AccountingEvidenceAuditParents(
        fills={},
        cash_bindings=parents.cash_bindings,
        order_transitions=parents.order_transitions,
        accounts=parents.accounts,
        orders=parents.orders,
        fill_facts=parents.fill_facts,
        cash_facts=parents.cash_facts,
        lots=parents.lots,
    )
    with pytest.raises(V2AccountingEvidenceAuditError, match="absent fill evidence"):
        _audit(rows, missing_fill)

    rows, parents = _stored_fixture(_buy_outcome())
    rows[OUTCOME_TABLE] = ()
    with pytest.raises(V2AccountingEvidenceAuditError, match="absent accounting outcome"):
        _audit(rows, parents)


def test_outcome_without_final_marker_is_not_complete() -> None:
    rows, parents = _stored_fixture(_buy_outcome())
    rows[FINALIZATION_TABLE] = ()

    with pytest.raises(V2AccountingEvidenceAuditError, match="without FINAL marker"):
        _audit(rows, parents)


def test_canonical_fill_and_latest_lot_parent_drift_fail_closed() -> None:
    rows, parents = _stored_fixture(_buy_outcome())
    fill_id = next(iter(parents.fill_facts))
    changed_fill = dict(parents.fill_facts[fill_id])
    changed_fill["price"] = Decimal("99.000000")
    drifted_fill_parents = V2AccountingEvidenceAuditParents(
        fills=parents.fills,
        cash_bindings=parents.cash_bindings,
        order_transitions=parents.order_transitions,
        accounts=parents.accounts,
        orders=parents.orders,
        fill_facts={**parents.fill_facts, fill_id: changed_fill},
        cash_facts=parents.cash_facts,
        lots=parents.lots,
    )
    with pytest.raises(V2AccountingEvidenceAuditError, match="parent projection"):
        _audit(rows, drifted_fill_parents)

    rows, parents = _stored_fixture(_buy_outcome())
    lot_id = next(iter(parents.lots))
    changed_lot = dict(parents.lots[lot_id])
    changed_lot["protective_stop"] = Decimal("8.000000")
    drifted_lot_parents = V2AccountingEvidenceAuditParents(
        fills=parents.fills,
        cash_bindings=parents.cash_bindings,
        order_transitions=parents.order_transitions,
        accounts=parents.accounts,
        orders=parents.orders,
        fill_facts=parents.fill_facts,
        cash_facts=parents.cash_facts,
        lots={**parents.lots, lot_id: changed_lot},
    )
    with pytest.raises(V2AccountingEvidenceAuditError, match="latest accounting effect"):
        _audit(rows, drifted_lot_parents)


class _Mappings:
    def all(self) -> list[Mapping[str, Any]]:
        return []


class _Result:
    def mappings(self) -> _Mappings:
        return _Mappings()


class _EmptyConnection:
    def __init__(self, *, active: bool = True) -> None:
        self.active = active
        self.calls: list[str] = []

    def in_transaction(self) -> bool:
        return self.active

    def execute(self, statement: Any, params: Mapping[str, Any] | None = None) -> _Result:
        assert params is None
        self.calls.append(str(statement))
        return _Result()


def test_database_entrypoint_uses_caller_transaction_sha2_and_shared_locks() -> None:
    connection = _EmptyConnection()
    report = audit_v2_accounting_evidence_database(connection)

    assert report.audit_passed is True
    assert len(connection.calls) == 8  # three accounting and five evidence parents
    assert all("LOCK IN SHARE MODE" in sql for sql in connection.calls)
    assert all("SHA2" in sql for sql in connection.calls)
    assert any("lot-accounting-effect.v1" in sql for sql in connection.calls)
    assert any("fill-accounting-outcome.v1" in sql for sql in connection.calls)
    assert any("fill-accounting-finalization.v1" in sql for sql in connection.calls)


def test_database_entrypoint_rejects_connection_without_active_transaction() -> None:
    with pytest.raises(V2AccountingEvidenceAuditError, match="already be in a transaction"):
        audit_v2_accounting_evidence_database(_EmptyConnection(active=False))


def test_rows_contract_requires_exact_tables_and_database_sha2() -> None:
    rows, parents = _stored_fixture(_buy_outcome())
    with pytest.raises(V2AccountingEvidenceAuditError, match="exactly the three"):
        audit_v2_accounting_evidence_rows(
            {OUTCOME_TABLE: rows[OUTCOME_TABLE]},
            parents=parents,
            shared_row_locks_used=True,
        )
    with pytest.raises(V2AccountingEvidenceAuditError, match="SHA2"):
        audit_v2_accounting_evidence_rows(
            rows,
            parents=parents,
            database_sha2_used=False,
            shared_row_locks_used=True,
        )
