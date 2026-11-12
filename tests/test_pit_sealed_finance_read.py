from datetime import date, datetime

import pytest
from sqlalchemy import event, text

from server.common import pit_facts as pit
from server.common.pit_sealed_finance_read import load_sealed_finance_facts
from tests.test_pit_finance_event_facts import (
    _engine,
    _finance_data_exclusion,
    _install_finance_test_catalog,
    _install_prior_finance_coverage,
)
from tests.test_pit_finance_compressed_read import _mysql_transport


@pytest.mark.parametrize("exclude", [True, False])
def test_exact_finance_features_reuse_seal_and_skip_unbound_payloads(exclude):
    engine = _engine()
    _install_finance_test_catalog(engine)
    _install_prior_finance_coverage(engine)
    source = {
        "stock_code": "000001", "report_date": "2026-03-31", "report_type": "Q1",
        "notice_date": "2026-04-25", "roe_wtd": 14.5,
    }
    older = {**source, "report_date": "2025-12-31", "report_type": "Q4"}
    known = "2026-08-30 01:10:00"
    bound = pit.append_finance_revision(engine, source, known_at=known)
    unrelated = pit.append_finance_revision(engine, older, known_at=known)
    coverage = pit.append_source_coverage(
        engine, fact_kind="finance", stock_code="000001", window_start="1900-01-01",
        window_end="2026-08-30", known_at=known, covered_through_at=known,
        watermark_kind="CAPTURED_AT", watermark_evidence={"source_call": "success"},
        source_rows=[older, source], fact_bindings=[
            {"revision_id": bound.revision_id, "content_hash": bound.content_hash},
            {"revision_id": unrelated.revision_id, "content_hash": unrelated.content_hash},
        ], source="adata.finance.core_index", batch_id="sealed-report",
    )
    target = date(2026, 8, 30)
    completed = datetime(2026, 8, 30, 1, 30)
    decision = datetime(2026, 8, 30, 1, 45)
    codes = ["000001", "002731"]
    seal = pit.append_finance_atomic_batch_seal(
        engine, as_of_date=target, completed_known_at=completed,
        data_exclusions=[_finance_data_exclusion()] if exclude else [],
    )
    after = pit.append_finance_revision(
        engine, {**source, "roe_wtd": 99}, known_at="2026-08-30 01:35:00",
    )
    verified = pit.load_finance_atomic_batch_seal(
        engine, codes=codes, decision_at=decision, as_of_date=target,
        seal_coverage_id=seal["seal_coverage_id"],
    )
    mysql = _mysql_transport(engine)
    selection = pit.SealedFinanceInputs(
        engine=mysql, decision_at=decision, fact_cutoff_at=completed, as_of_date=target,
        seal_id=verified["seal_coverage_id"], batch_root_sha256=verified["batch_root_sha256"],
        members=verified["members"], exclusions=pit._finance_data_exclusions_from_baseline(
            verified, codes=codes, target=target,
        ),
    )
    payload_reads = set()

    def record(_connection, _cursor, statement, _params, context, _many):
        if "COMPRESS(payload_json)" in statement:
            payload_reads.update(
                value for key, value in context.compiled_parameters[0].items()
                if key.startswith("row_ids_")
            )

    event.listen(engine, "before_cursor_execute", record)
    args = dict(selection=selection, codes=codes, decision_at=decision, fact_cutoff_at=completed, as_of_date=target)
    try:
        result = load_sealed_finance_facts(mysql, **args)
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert result.facts["000001"]["finance_revision_id"] == bound.revision_id
    assert result.facts["000001"]["roe_wtd"] == 14.5
    assert result.facts["000001"]["finance_publication_precision"] == "DATE_ONLY"
    assert result.status_for("000001") == pit.PIT_AVAILABLE
    if exclude:
        assert result.status_for("002731") == pit.PIT_DATA_BLOCKED
        assert result.reason_for("002731") == "PIT_FINANCE_DATA_EXCLUDED:FINANCE_FETCH_FAILED"
        assert result.coverage_by_code["002731"]["seal_coverage_id"] == seal["seal_coverage_id"]
    else:
        assert result.status_for("002731") == pit.PIT_AVAILABLE
        assert result.reason_for("002731") == "PIT_FINANCE_EXPECTED_UNAVAILABLE"
        assert result.coverage_by_code["002731"]["coverage_id"] == verified["members"]["002731"]["coverage_id"]
    assert {coverage.coverage_id, bound.revision_id} <= payload_reads
    assert not {unrelated.revision_id, after.revision_id, seal["seal_coverage_id"]} & payload_reads

    with pytest.raises(ValueError, match="SELECTION_DIFFERS"):
        load_sealed_finance_facts(mysql, **{**args, "fact_cutoff_at": decision})

    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER trg_pit_finance_revision_immutable_bu"))
        connection.execute(text(f"UPDATE {pit.FINANCE_REVISION_TABLE} SET payload_json='{{}}' WHERE revision_id=:id"),
                           {"id": bound.revision_id})
    with pytest.raises(ValueError, match="revision content hash differs"):
        load_sealed_finance_facts(mysql, **args)
