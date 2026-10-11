"""Real immutable PIT writes and reads for the targeted exposure monitor.

Only the official provider's HTTP boundary is replaced.  Receipts are produced
and sealed by the production provider; SQLite uses the real append-only schema,
writers, hash validation and point-in-time reader.
"""

from copy import deepcopy
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from server.common.announcement_provider import CninfoMarketAnnouncementProvider
from server.common.pit_facts import (
    CNINFO_EVENT_SOURCE,
    EVENT_CAPTURE_PURPOSE_EXPOSURE_MONITOR,
    EVENT_REVISION_TABLE,
    SOURCE_COVERAGE_TABLE,
    TIME_UNVERIFIED,
    _append_revision,
    append_event_revision,
    append_source_coverage,
    canonical_hash,
    canonical_json,
    ensure_pit_fact_schema,
    load_event_facts,
)
from server.common.qmt_announcement_pit import parse_qmt_announcement_frame


CUTOFF = datetime(2026, 10, 11, 23, 0)
KNOWN = CUTOFF + timedelta(seconds=10)
START = date(2026, 9, 11)
CODE = "000001"


class _Response:
    def __init__(self, body):
        self.body = body
        self.encoding = ""

    def raise_for_status(self):
        return None

    def json(self):
        return deepcopy(self.body)


class _OfficialHttpFixture:
    def __init__(self, rows_by_code):
        self.rows_by_code = rows_by_code
        self.master_reads = 0

    def get(self, _url):
        self.master_reads += 1
        return _Response({"stockList": [
            {"code": code, "orgId": f"org-{code}", "category": "A"}
            for code in sorted(self.rows_by_code)
        ]})

    def post(self, _url, *, data):
        code, org_id = str(data["stock"]).split(",", 1)
        assert org_id == f"org-{code}"
        assert int(data["pageNum"]) == 1
        rows = self.rows_by_code[code]
        assert len(rows) <= 30
        return _Response({
            "pageNum": 1, "totalRecordNum": len(rows),
            "totalpages": len(rows) // 30, "hasMore": False,
            "announcements": rows or None,
        })


@pytest.fixture
def engine():
    database = create_engine("sqlite+pysqlite:///:memory:", future=True)
    assert ensure_pit_fact_schema(database)["valid"]
    yield database
    database.dispose()


def _captured(rows_by_code):
    client = _OfficialHttpFixture(rows_by_code)
    provider = CninfoMarketAnnouncementProvider(
        client=client, minimum_request_interval=0, now_fn=lambda: KNOWN,
    )
    provider.bind_capture_deadline(remaining_seconds=30)
    fetched = {
        code: provider.fetch(
            stock_code=code, qmt_code=f"{code}.SZ",
            requested_start_time=START.strftime("%Y%m%d") + "000000",
            requested_end_time=CUTOFF.strftime("%Y%m%d%H%M%S"),
        )
        for code in rows_by_code
    }
    receipts = provider.finalize_receipts({
        code: result.receipt for code, result in fetched.items()
    })
    assert client.master_reads == 2
    return fetched, receipts


def _evidence(receipt, *, rows=(), catalog_count=1):
    response_hash = canonical_hash({
        "schema": "probiga.pit-event-source-response.v1", "rows": list(rows),
    })
    return {
        "provider": CNINFO_EVENT_SOURCE, "period": "announcement",
        "capture_purpose": EVENT_CAPTURE_PURPOSE_EXPOSURE_MONITOR,
        "primary_provider": CNINFO_EVENT_SOURCE, "fallback_reason": "",
        "provider_receipt": deepcopy(receipt),
        "provider_receipt_hash": canonical_hash(receipt),
        "fact_cutoff_at": CUTOFF.isoformat(timespec="microseconds"),
        "decision_at": KNOWN.isoformat(timespec="microseconds"),
        "received_at": KNOWN.isoformat(timespec="microseconds"),
        "query_end_time": CUTOFF.strftime("%Y%m%d%H%M%S"),
        "source_response_hash": response_hash,
        "global_batch_root_hash": canonical_hash({"fixture": "batch"}),
        "catalog_batch_id": "isolated-catalog",
        "catalog_manifest_hash": canonical_hash({"fixture": "catalog"}),
        "catalog_member_set_hash": canonical_hash({"fixture": "members"}),
        "catalog_member_count": catalog_count,
        "window_start": START.isoformat(),
        "window_end": CUTOFF.date().isoformat(),
    }


def _append_empty(engine, evidence, *, code=CODE):
    return append_source_coverage(
        engine, fact_kind="event", stock_code=code,
        window_start=START, window_end=CUTOFF.date(), known_at=KNOWN,
        received_at=KNOWN, covered_through_at=CUTOFF,
        watermark_kind="QUERY_CUTOFF", watermark_evidence=evidence,
        source_rows=[], fact_bindings=[], source=CNINFO_EVENT_SOURCE,
        batch_id="isolated-exposure-monitor",
    )


def _read(engine, *, codes=(CODE,), decision=KNOWN, cutoff=CUTOFF):
    return load_event_facts(
        engine, codes=codes, decision_at=decision, fact_cutoff_at=cutoff,
        start_date=START, end_date=CUTOFF.date(),
    )


def _date_only_event():
    source_time = datetime(2026, 10, 9, tzinfo=ZoneInfo("Asia/Shanghai"))
    fetched, _receipts = _captured({CODE: [{
        "secCode": CODE, "announcementId": "date-only-disclosure",
        "announcementTime": int(source_time.timestamp() * 1000),
        "announcementTitle": "source date without an exact release time",
        "adjunctUrl": "finalpage/2026-10-09/disclosure.PDF",
    }]})
    events = parse_qmt_announcement_frame(
        stock_code=CODE, qmt_code=f"{CODE}.SZ",
        frame=pd.DataFrame(fetched[CODE].rows), fact_cutoff_at=CUTOFF,
        window_start=START, source=CNINFO_EVENT_SOURCE,
    )
    assert len(events) == 1
    assert events[0]["published_at"] is None
    assert events[0]["source_published_text"] == "2026-10-09"
    return events[0]


def test_exposure_monitor_primary_cninfo_empty_receipts_read_as_authoritative_empty(engine):
    codes = tuple(f"{index:06d}" for index in range(1, 28))
    empty_codes = codes[:20]
    published = datetime(2026, 10, 9, 20, 44, tzinfo=ZoneInfo("Asia/Shanghai"))
    fetched, receipts = _captured({
        code: [] if code in empty_codes else [{
            "secCode": code, "announcementId": f"disclosure-{code}",
            "announcementTime": int(published.timestamp() * 1000),
            "announcementTitle": "known exact-time source disclosure",
            "adjunctUrl": f"finalpage/2026-10-09/{code}.PDF",
        }]
        for code in codes
    })
    for code in empty_codes:
        coverage = _append_empty(
            engine, _evidence(receipts[code], catalog_count=len(codes)), code=code,
        )
        assert coverage.coverage_id
    for code in codes[20:]:
        events = parse_qmt_announcement_frame(
            stock_code=code, qmt_code=f"{code}.SZ",
            frame=pd.DataFrame(fetched[code].rows), fact_cutoff_at=CUTOFF,
            window_start=START, source=CNINFO_EVENT_SOURCE,
        )
        bindings = []
        for event in events:
            revision = append_event_revision(
                engine, event, known_at=KNOWN, received_at=KNOWN,
                source=CNINFO_EVENT_SOURCE, batch_id="isolated-exposure-monitor",
            )
            bindings.append({
                "revision_id": revision.revision_id,
                "content_hash": revision.content_hash,
            })
        append_source_coverage(
            engine, fact_kind="event", stock_code=code,
            window_start=START, window_end=CUTOFF.date(), known_at=KNOWN,
            received_at=KNOWN, covered_through_at=CUTOFF,
            watermark_kind="QUERY_CUTOFF",
            watermark_evidence=_evidence(
                receipts[code], rows=events, catalog_count=len(codes),
            ),
            source_rows=events, fact_bindings=bindings,
            source=CNINFO_EVENT_SOURCE, batch_id="isolated-exposure-monitor",
        )
    with engine.connect() as connection:
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE}"
        )).scalar_one() == 27
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE} WHERE result_count=0"
        )).scalar_one() == 20

    batch = _read(engine, codes=codes)
    assert {code: batch.reason_for(code) for code in empty_codes} == {
        code: "PIT_EVENT_AUTHORITATIVE_EMPTY" for code in empty_codes
    }
    assert all(batch.status_for(code) == "AVAILABLE" for code in codes)
    assert all(batch.facts[code] == [] for code in empty_codes)
    assert all(len(batch.facts[code]) == 1 for code in codes[20:])
    assert all(
        batch.facts[code][0]["event_publication_time_verified"] is True
        for code in codes[20:]
    )


def test_date_only_parsed_source_date_survives_real_append_and_pit_readback(engine):
    event = _date_only_event()
    revision = append_event_revision(
        engine, event, known_at=KNOWN, received_at=KNOWN,
        source=CNINFO_EVENT_SOURCE, batch_id="isolated-exposure-monitor",
    )
    with engine.connect() as connection:
        stored = connection.execute(text(
            f"SELECT * FROM {EVENT_REVISION_TABLE}"
        )).mappings().one()
    assert stored["published_at"] is None
    assert stored["publication_time_status"] == TIME_UNVERIFIED
    assert stored["source_published_text"] == "2026-10-09"
    batch = _read(engine)
    assert batch.status_for(CODE) == "AVAILABLE", batch.reason_for(CODE)
    assert batch.facts[CODE][0]["event_revision_id"] == revision.revision_id
    assert batch.facts[CODE][0]["event_publication_time_verified"] is False
    assert batch.facts[CODE][0]["event_publication_precision"] == "DATE_ONLY"


_EVIDENCE_TAMPERING = (
    "provider", "purpose", "primary_provider", "fallback_reason",
    "receipt_status", "receipt_stock", "receipt_qmt_code", "receipt_count",
    "receipt_exhausted", "receipt_pagination", "receipt_directory",
    "receipt_hash", "response_hash", "catalog_hash", "cutoff", "decision",
    "received", "query_end",
)


def _tamper_evidence(evidence, field):
    changed = deepcopy(evidence)
    replacements = {
        "provider": ("provider", "untrusted.announcement"),
        "purpose": ("capture_purpose", "UNAUTHORIZED_MONITOR"),
        "primary_provider": ("primary_provider", "qmt.announcement"),
        "fallback_reason": (
            "fallback_reason", "QMT_ANNOUNCEMENT_API_UNAVAILABLE",
        ),
        "receipt_hash": ("provider_receipt_hash", "0" * 64),
        "response_hash": ("source_response_hash", "0" * 64),
        "catalog_hash": ("catalog_manifest_hash", "not-a-hash"),
        "cutoff": (
            "fact_cutoff_at", (CUTOFF + timedelta(seconds=1)).isoformat(),
        ),
        "decision": (
            "decision_at", (KNOWN - timedelta(seconds=1)).isoformat(),
        ),
        "received": (
            "received_at", (KNOWN + timedelta(seconds=1)).isoformat(),
        ),
        "query_end": (
            "query_end_time", (CUTOFF - timedelta(seconds=1)).strftime("%Y%m%d%H%M%S"),
        ),
    }
    receipt_replacements = {
        "receipt_status": ("status", "PARTIAL"),
        "receipt_stock": ("stock_code", "000002"),
        "receipt_qmt_code": ("qmt_code", "000002.SZ"),
        "receipt_count": ("result_count", 1),
        "receipt_exhausted": ("exhausted", False),
        "receipt_pagination": ("pagination_sha256", "0" * 64),
        "receipt_directory": ("directory_attestation_sha256", "0" * 64),
    }
    if field in receipt_replacements:
        key, value = receipt_replacements[field]
        changed["provider_receipt"][key] = value
        # Preserve the outer receipt digest to prove semantic validation is
        # required, not just rejection of an obviously broken outer hash.
        changed["provider_receipt_hash"] = canonical_hash(
            changed["provider_receipt"]
        )
    else:
        key, value = replacements[field]
        changed[key] = value
    return changed


@pytest.mark.parametrize("field", _EVIDENCE_TAMPERING)
def test_exposure_monitor_writer_rejects_tampered_authority(engine, field):
    _fetched, receipts = _captured({CODE: []})
    evidence = _tamper_evidence(_evidence(receipts[CODE]), field)
    with pytest.raises(ValueError):
        _append_empty(engine, evidence)
    with engine.connect() as connection:
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE}"
        )).scalar_one() == 0


def _rebind_coverage_hashes(row, payload):
    """Bind a corrupt fixture canonically so real semantic checks must run."""
    changed = dict(row)
    changed["payload_json"] = canonical_json(payload)
    changed["watermark_hash"] = canonical_hash(payload["watermark"])
    changed["coverage_fingerprint_hash"] = canonical_hash({
        "schema": "probiga.pit-source-coverage-fingerprint.v1",
        "scope_hash": changed["scope_hash"], "payload": payload,
        "supersedes_coverage_id": changed["supersedes_coverage_id"],
    })
    changed["coverage_id"] = canonical_hash({
        "schema": "probiga.pit-source-coverage-id.v1",
        "scope_hash": changed["scope_hash"],
        "coverage_fingerprint_hash": changed["coverage_fingerprint_hash"],
    })
    return changed


def _insert_isolated_coverage(row):
    # Never bypass append-only by modifying a stored row.  Insert a separately
    # seeded historical-corruption fixture into a fresh in-memory database.
    isolated = create_engine("sqlite+pysqlite:///:memory:", future=True)
    assert ensure_pit_fact_schema(isolated)["valid"]
    columns = list(row)
    with isolated.begin() as connection:
        connection.execute(text(
            f"INSERT INTO {SOURCE_COVERAGE_TABLE} ({', '.join(columns)}) "
            f"VALUES ({', '.join(':' + name for name in columns)})"
        ), dict(row))
    return isolated


@pytest.mark.parametrize("field", _EVIDENCE_TAMPERING)
def test_exposure_monitor_reader_rejects_canonically_bound_tampered_authority(engine, field):
    import json

    _fetched, receipts = _captured({CODE: []})
    _append_empty(engine, _evidence(receipts[CODE]))
    with engine.connect() as connection:
        row = dict(connection.execute(text(
            f"SELECT * FROM {SOURCE_COVERAGE_TABLE}"
        )).mappings().one())
    payload = json.loads(row["payload_json"])
    payload["watermark"]["evidence"] = _tamper_evidence(
        payload["watermark"]["evidence"], field,
    )
    isolated = _insert_isolated_coverage(_rebind_coverage_hashes(row, payload))
    try:
        batch = _read(isolated)
        assert batch.status_for(CODE) == "DATA_BLOCKED"
        assert "BAD_COVERAGE_CHAIN" in batch.reason_for(CODE)
        assert CODE not in batch.coverage_by_code
    finally:
        isolated.dispose()


def test_exposure_monitor_empty_coverage_is_not_available_before_receipt_is_known(engine):
    _fetched, receipts = _captured({CODE: []})
    _append_empty(engine, _evidence(receipts[CODE]))
    before = _read(engine, decision=KNOWN - timedelta(microseconds=1))
    assert before.status_for(CODE) != "AVAILABLE"
    assert before.coverage_by_code == {}
    assert _read(engine).status_for(CODE) == "AVAILABLE"
    beyond = _read(
        engine, decision=KNOWN, cutoff=CUTOFF + timedelta(microseconds=1),
    )
    assert beyond.status_for(CODE) != "AVAILABLE"
    assert beyond.coverage_by_code == {}


def test_date_only_fact_cannot_be_used_before_it_was_observed(engine):
    append_event_revision(
        engine, _date_only_event(), known_at=KNOWN, received_at=KNOWN,
        source=CNINFO_EVENT_SOURCE, batch_id="isolated-exposure-monitor",
    )
    before = _read(engine, decision=KNOWN - timedelta(microseconds=1))
    assert before.status_for(CODE) != "AVAILABLE"
    assert before.facts[CODE] == []
    after = _read(engine)
    assert after.status_for(CODE) == "AVAILABLE", after.reason_for(CODE)
    assert after.facts[CODE][0]["event_observed_available_at"] == KNOWN.isoformat(
        timespec="microseconds"
    )


def test_legacy_blank_date_revision_is_superseded_append_only_at_new_knowledge_time(engine):
    event = _date_only_event()
    legacy_known = CUTOFF - timedelta(minutes=1)
    with engine.begin() as connection:
        # Reproduce the exact old write defect with the real lower-level writer:
        # canonical source date remains in payload, but published_value=None
        # serializes an empty immutable source_published_text column.
        legacy = _append_revision(
            connection, table_name=EVENT_REVISION_TABLE, fact_kind="event",
            identity_payload={
                "source": CNINFO_EVENT_SOURCE, "event_key": event["event_key"],
                "stock_code": CODE,
            },
            row_columns={
                "event_key": event["event_key"], "stock_code": CODE,
                "event_date": date.fromisoformat(event["event_date"]),
            },
            payload=event, published_value=None, known_at=legacy_known,
            received_at=legacy_known, source=CNINFO_EVENT_SOURCE,
            batch_id="legacy-immutable-date-only",
        )
    corrected = append_event_revision(
        engine, event, known_at=KNOWN, received_at=KNOWN,
        source=CNINFO_EVENT_SOURCE, batch_id="isolated-exposure-monitor",
    )
    assert corrected.revision_no == 2
    assert corrected.supersedes_revision_id == legacy.revision_id
    assert corrected.content_hash == legacy.content_hash
    assert corrected.idempotent is False
    with engine.connect() as connection:
        rows = connection.execute(text(
            f"SELECT * FROM {EVENT_REVISION_TABLE} ORDER BY revision_no"
        )).mappings().all()
    assert len(rows) == 2
    assert rows[0]["source_published_text"] == ""
    assert rows[0]["batch_id"] == "legacy-immutable-date-only"
    assert rows[1]["source_published_text"] == "2026-10-09"
    assert all(row["published_at"] is None for row in rows)
    assert all(row["publication_time_status"] == TIME_UNVERIFIED for row in rows)
    before = _read(engine, decision=KNOWN - timedelta(microseconds=1))
    assert before.status_for(CODE) != "AVAILABLE"
    assert "PUBLICATION_DATE_MISSING" in before.reason_for(CODE)
    after = _read(engine)
    assert after.status_for(CODE) == "AVAILABLE", after.reason_for(CODE)
    assert after.facts[CODE][0]["event_revision_id"] == corrected.revision_id
    repeated = append_event_revision(
        engine, event, known_at=KNOWN + timedelta(seconds=1),
        source=CNINFO_EVENT_SOURCE, batch_id="another-monitor-batch",
    )
    assert repeated.idempotent is True
    assert repeated.revision_id == corrected.revision_id


@pytest.mark.parametrize("marker", [
    None, "", "today", "20261009", "2026-02-30",
    "2026-10-09T13:14:15", "2026-10-09 13:14:15", 1791475200000,
])
def test_unknown_or_noncanonical_source_date_cannot_invent_publication_time(engine, marker):
    event = _date_only_event()
    event["source_published_text"] = marker
    revision = append_event_revision(
        engine, event, known_at=KNOWN, received_at=KNOWN,
        source=CNINFO_EVENT_SOURCE, batch_id="isolated-unknown-date",
    )
    with engine.connect() as connection:
        stored = connection.execute(text(
            f"SELECT * FROM {EVENT_REVISION_TABLE}"
        )).mappings().one()
    assert stored["published_at"] is None
    assert stored["publication_time_status"] == TIME_UNVERIFIED
    assert revision.publication_time_status == TIME_UNVERIFIED
    # A malformed YYYY-MM-DD marker may remain raw audit evidence, but is
    # never a valid source date and must not become usable or an exact instant.
    assert stored["source_published_text"] in ("", "2026-02-30")
    batch = _read(engine)
    assert batch.status_for(CODE) != "AVAILABLE"
    assert "PUBLICATION_DATE_MISSING" in batch.reason_for(CODE)


def test_provider_row_without_a_source_date_is_rejected_instead_of_using_today(engine):
    with pytest.raises(ValueError, match="no provider publication date"):
        parse_qmt_announcement_frame(
            stock_code=CODE, qmt_code=f"{CODE}.SZ",
            frame=pd.DataFrame([{"title": "undated source disclosure"}]),
            fact_cutoff_at=CUTOFF, window_start=START,
            source=CNINFO_EVENT_SOURCE,
        )
    with engine.connect() as connection:
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {EVENT_REVISION_TABLE}"
        )).scalar_one() == 0
