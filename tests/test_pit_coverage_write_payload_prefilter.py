import pytest
from sqlalchemy import create_engine, event, text

from server.common.pit_facts import SOURCE_COVERAGE_TABLE, append_source_coverages, ensure_pit_fact_schema


@pytest.fixture
def coverage_store():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    ensure_pit_fact_schema(engine)
    payload_reads = []

    @event.listens_for(engine, "before_cursor_execute")
    def capture_payload_select(_conn, _cursor, sql, parameters, _context, _executemany):
        if sql.lstrip().upper().startswith("SELECT") and SOURCE_COVERAGE_TABLE in sql and "payload_json" in sql:
            payload_reads.append(tuple(parameters))

    yield engine, payload_reads
    engine.dispose()


def _coverage(code, cutoff, *, provider_evidence="original"):
    return {
        "fact_kind": "event",
        "stock_code": code,
        "window_start": "2026-10-12",
        "window_end": cutoff[:10],
        "known_at": cutoff,
        "covered_through_at": cutoff,
        "watermark_kind": "CAPTURED_AT",
        "watermark_evidence": {"capture": provider_evidence},
        "source_rows": [],
        "fact_bindings": [],
        "source": "eastmoney.notice",
        "batch_id": "test-" + cutoff[:10],
    }


@pytest.mark.parametrize("codes", [("000001",), ("000001", "000002")])
def test_new_cutoff_appends_revisions_without_old_payload_transfer(coverage_store, codes):
    engine, payload_reads = coverage_store
    prior = append_source_coverages(engine, [_coverage(code, "2026-11-10T18:30:00") for code in codes])
    next_day = append_source_coverages(engine, [_coverage(code, "2026-11-11T18:30:00") for code in codes])

    assert payload_reads == []
    for old, new in zip(prior, next_day, strict=True):
        assert old.source_response_hash == new.source_response_hash
        assert old.fact_set_hash == new.fact_set_hash
        assert new.supersedes_coverage_id == old.coverage_id
        assert new.revision_no == old.revision_no + 1 and not new.idempotent


def test_matching_metadata_still_requires_exact_replay_bytes(coverage_store):
    engine, payload_reads = coverage_store
    at = "2026-11-10T18:30:00"
    original_rows = [_coverage("000001", at), _coverage("000002", at), _coverage("000003", at)]
    old_first, old_second, old_third = append_source_coverages(engine, original_rows)
    same, altered, later = append_source_coverages(engine, [
        original_rows[0], _coverage("000002", at, provider_evidence="changed"),
        _coverage("000003", "2026-11-11T18:30:00"),
    ])

    assert len(payload_reads) == 1
    assert set(payload_reads[0]) == {old_first.coverage_id, old_second.coverage_id}
    assert same.idempotent and same.coverage_id == old_first.coverage_id
    assert not altered.idempotent and altered.supersedes_coverage_id == old_second.coverage_id
    assert not later.idempotent and later.supersedes_coverage_id == old_third.coverage_id

    payload_reads.clear()
    repeated, = append_source_coverages(engine, [_coverage("000002", at, provider_evidence="changed")])
    assert payload_reads == [(altered.coverage_id,)]
    assert repeated.idempotent and repeated.coverage_id == altered.coverage_id
    with engine.connect() as connection:
        assert connection.execute(text(f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE}")).scalar_one() == 5
