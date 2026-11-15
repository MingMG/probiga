from dataclasses import replace
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, event, text

from server.common import pit_facts as pit
from server.common.pit_sealed_finance_read import load_sealed_finance_facts
from tests.test_pit_finance_event_facts import (
    _engine,
    _finance_data_exclusion,
    _install_finance_test_catalog,
    _install_prior_finance_coverage,
)
from server.common.formal_analysis_store import FormalAnalysisStore
from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
from tests.test_pit_finance_compressed_read import _mysql_transport


def _formal_store(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'finance-checkpoint.sqlite'}")
    privileged_migrate_formal_analysis_schema(engine)
    return FormalAnalysisStore(engine)


@pytest.mark.parametrize("exclude,masked_date", [(True, False), (False, False), (True, True)])
def test_exact_finance_features_reuse_seal_and_skip_unbound_payloads(tmp_path, exclude, masked_date):
    engine = _engine()
    _install_finance_test_catalog(engine)
    _install_prior_finance_coverage(engine)
    source = {
        "stock_code": "000001", "report_date": "2026-03-31", "report_type": "Q1",
        "notice_date": "2026-04-25", "roe_wtd": 14.5,
    }
    if masked_date:
        source["published_at"] = None
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
    original_connect = mysql.connect

    def connect():
        connection = original_connect()
        original_execute = connection.execute

        def execute(statement, parameters):
            sql = statement.text.replace(" FORCE INDEX (idx_pit_finance_decision)", "")
            if sql != statement.text:
                statement = text(sql).bindparams(*statement._bindparams.values()).execution_options(
                    **statement.get_execution_options()
                )
            return original_execute(statement, parameters)

        connection.execute = execute
        return connection

    mysql.connect = connect
    selection = pit.SealedFinanceInputs(
        engine=mysql, decision_at=decision, fact_cutoff_at=completed, as_of_date=target,
        seal_id=verified["seal_coverage_id"], batch_root_sha256=verified["batch_root_sha256"],
        members=verified["members"], exclusions=pit._finance_data_exclusions_from_baseline(
            verified, codes=codes, target=target,
        ),
    )
    payload_reads = set()
    projected_reads = set()
    report_dates = set()

    def record(_connection, _cursor, statement, _params, context, _many):
        if "COMPRESS(payload_json)" in statement:
            payload_reads.update(
                value for key, value in context.compiled_parameters[0].items()
                if key.startswith("row_ids_")
            )
        if "JSON_EXTRACT(payload_json, '$.fact_bindings')" in statement:
            projected_reads.update(
                value for key, value in context.compiled_parameters[0].items()
                if key.startswith("keys_")
            )
        if f"FROM {pit.FINANCE_REVISION_TABLE} WHERE stock_code IN" in statement:
            report_dates.add(context.compiled_parameters[0]["report"])

    event.listen(engine, "before_cursor_execute", record)
    args = dict(selection=selection, codes=codes, decision_at=decision, fact_cutoff_at=completed, as_of_date=target)
    try:
        result = load_sealed_finance_facts(mysql, **args)
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert result.facts["000001"]["finance_revision_id"] == bound.revision_id
    assert result.facts["000001"]["roe_wtd"] == 14.5
    assert result.facts["000001"]["finance_publication_precision"] == "DATE_ONLY"
    assert result.facts["000001"]["finance_published_at"] == "2026-04-25"
    assert result.facts["000001"]["finance_publication_time_verified"] is False
    assert result.status_for("000001") == pit.PIT_AVAILABLE
    if exclude:
        assert result.status_for("002731") == pit.PIT_DATA_BLOCKED
        assert result.reason_for("002731") == "PIT_FINANCE_DATA_EXCLUDED:FINANCE_FETCH_FAILED"
        assert result.coverage_by_code["002731"]["seal_coverage_id"] == seal["seal_coverage_id"]
    else:
        assert result.status_for("002731") == pit.PIT_AVAILABLE
        assert result.reason_for("002731") == "PIT_FINANCE_EXPECTED_UNAVAILABLE"
        assert result.coverage_by_code["002731"]["coverage_id"] == verified["members"]["002731"]["coverage_id"]
    assert projected_reads == {coverage.coverage_id}
    assert report_dates == {date(2026, 3, 31)}
    assert bound.revision_id in payload_reads
    assert not {coverage.coverage_id, unrelated.revision_id, after.revision_id, seal["seal_coverage_id"]} & payload_reads

    checkpoint = _formal_store(tmp_path)
    formal = {"formal_store": checkpoint, "formal_generation_sha256": "c" * 64}
    initial_formal = load_sealed_finance_facts(mysql, **args, **formal)
    assert initial_formal == result
    payload_reads.clear()
    projected_reads.clear()
    report_dates.clear()
    event.listen(engine, "before_cursor_execute", record)
    try:
        resumed_formal = load_sealed_finance_facts(mysql, **args, **formal)
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert resumed_formal == result
    assert list(resumed_formal.facts["000001"]) == list(result.facts["000001"])
    assert not payload_reads and not projected_reads and not report_dates

    with pytest.raises(ValueError, match="SELECTION_DIFFERS"):
        load_sealed_finance_facts(mysql, **{**args, "fact_cutoff_at": decision})

    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER trg_pit_finance_revision_immutable_bu"))
        connection.execute(text(f"UPDATE {pit.FINANCE_REVISION_TABLE} SET payload_json='{{}}' WHERE revision_id=:id"),
                           {"id": bound.revision_id})
    with pytest.raises(ValueError, match="revision content hash differs"):
        load_sealed_finance_facts(mysql, **args)

    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER trg_pit_source_coverage_immutable_bu"))
        connection.execute(text(f"""
            UPDATE {pit.SOURCE_COVERAGE_TABLE}
            SET payload_json=json_set(payload_json, '$.fact_bindings[0].content_hash', :tampered)
            WHERE coverage_id=:id
        """), {"id": coverage.coverage_id, "tampered": "a" * 64})
    with pytest.raises(ValueError, match="FACT_SET_DIFFERS"):
        load_sealed_finance_facts(mysql, **args)


def test_later_finance_slot_materializes_only_changed_report_binding(tmp_path):
    engine = _engine()
    _install_finance_test_catalog(engine)
    store = _formal_store(tmp_path)
    codes = ["000001", "002731"]
    target = date(2026, 8, 30)

    def report(code, roe):
        return {"stock_code": code, "report_date": "2026-03-31", "report_type": "Q1",
                "notice_date": "2026-04-25", "roe_wtd": roe}

    def record_coverage(row, revision, known):
        return pit.append_source_coverage(
            engine, fact_kind="finance", stock_code=row["stock_code"], window_start="1900-01-01",
            window_end=target, known_at=known, covered_through_at=known,
            watermark_kind="CAPTURED_AT", watermark_evidence={"source_call": "success"},
            source_rows=[row], fact_bindings=[{
                "revision_id": revision.revision_id, "content_hash": revision.content_hash,
            }], source="adata.finance.core_index", batch_id="cross-slot-report",
        )

    def slot(h, decision_minute):
        cutoff = datetime(2026, 8, 30, h, 30)
        decision = datetime(2026, 8, 30, h, decision_minute)
        seal = pit.append_finance_atomic_batch_seal(engine, as_of_date=target, completed_known_at=cutoff)
        verified = pit.load_finance_atomic_batch_seal(
            engine, codes=codes, decision_at=decision, as_of_date=target,
            seal_coverage_id=seal["seal_coverage_id"],
        )
        selection = pit.SealedFinanceInputs(
            engine=engine, decision_at=decision, fact_cutoff_at=cutoff, as_of_date=target,
            seal_id=verified["seal_coverage_id"], batch_root_sha256=verified["batch_root_sha256"],
            members=verified["members"], exclusions={},
        )
        return dict(selection=selection, codes=codes, decision_at=decision, fact_cutoff_at=cutoff, as_of_date=target)

    first = {code: report(code, 10.5) for code in codes}
    prior = {
        code: pit.append_finance_revision(engine, first[code], known_at="2026-08-30 01:00:00")
        for code in codes
    }
    for code in codes:
        record_coverage(first[code], prior[code], "2026-08-30 01:05:00")
    first_slot = slot(1, 45)
    baseline = load_sealed_finance_facts(engine, **first_slot, formal_store=store, formal_generation_sha256="1" * 64)

    record_coverage(first["000001"], prior["000001"], "2026-08-30 02:05:00")
    changed = report("002731", 16.75)
    changed_revision = pit.append_finance_revision(engine, changed, known_at="2026-08-30 02:10:00")
    record_coverage(changed, changed_revision, "2026-08-30 02:15:00")
    second_slot = slot(2, 45)
    old_member = first_slot["selection"].members["000001"]
    new_member = second_slot["selection"].members["000001"]
    assert old_member["coverage_id"] != new_member["coverage_id"]
    assert old_member["fact_set_hash"] == new_member["fact_set_hash"]

    metadata_codes = set()
    payload_ids = set()
    checked_coverage_ids = set()

    def count_reads(_connection, _cursor, statement, _params, context, _many):
        keys = {
            value for key, value in context.compiled_parameters[0].items()
            if key.startswith("keys_")
        }
        if "JSON_EXTRACT(payload_json, '$.fact_bindings')" in statement:
            checked_coverage_ids.update(keys)
        if f"FROM {pit.FINANCE_REVISION_TABLE} WHERE stock_code IN" in statement:
            metadata_codes.update(keys)
        if f"FROM {pit.FINANCE_REVISION_TABLE} WHERE revision_id IN" in statement:
            payload_ids.update(keys)

    event.listen(engine, "before_cursor_execute", count_reads)
    try:
        incremental = load_sealed_finance_facts(
            engine, **second_slot, formal_store=store, formal_generation_sha256="2" * 64,
        )
    finally:
        event.remove(engine, "before_cursor_execute", count_reads)
    direct = load_sealed_finance_facts(engine, **second_slot)
    assert incremental == direct
    assert incremental.manifest_hash != baseline.manifest_hash
    assert all(list(incremental.facts[code]) == list(direct.facts[code]) for code in codes)
    assert incremental.facts["000001"]["finance_decision_at"] != baseline.facts["000001"]["finance_decision_at"]
    assert incremental.facts["002731"]["roe_wtd"] == 16.75
    assert metadata_codes == {"002731"}
    assert prior["000001"].revision_id not in payload_ids and changed_revision.revision_id in payload_ids
    assert checked_coverage_ids == {item["coverage_id"] for item in second_slot["selection"].members.values()}

    backdated_decision = datetime(2026, 8, 30, 1, 40)
    original_selection = first_slot["selection"]
    backdated_slot = {
        **first_slot, "decision_at": backdated_decision,
        "selection": replace(original_selection, decision_at=backdated_decision),
    }
    metadata_codes.clear()
    event.listen(engine, "before_cursor_execute", count_reads)
    try:
        backdated = load_sealed_finance_facts(
            engine, **backdated_slot, formal_store=store, formal_generation_sha256="3" * 64,
        )
    finally:
        event.remove(engine, "before_cursor_execute", count_reads)
    assert backdated == load_sealed_finance_facts(engine, **backdated_slot)
    assert metadata_codes == set(codes)

    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER trg_pit_source_coverage_immutable_bu"))
        connection.execute(text(f"""
            UPDATE {pit.SOURCE_COVERAGE_TABLE}
            SET payload_json=json_set(payload_json, '$.fact_bindings[0].content_hash', :tampered)
            WHERE coverage_id=:id
        """), {"id": new_member["coverage_id"], "tampered": "a" * 64})
    with pytest.raises(ValueError, match="FACT_SET_DIFFERS"):
        load_sealed_finance_facts(engine, **second_slot, formal_store=store, formal_generation_sha256="4" * 64)
