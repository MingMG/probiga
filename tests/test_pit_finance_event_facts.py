from __future__ import annotations

from datetime import datetime
import json

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import DatabaseError

from server.common import pit_facts as pit_module
from server.common.pit_facts import (
    EVENT_REVISION_TABLE,
    FINANCE_REVISION_TABLE,
    PIT_AVAILABLE,
    PIT_DATA_BLOCKED,
    PIT_NO_ROWS,
    SOURCE_COVERAGE_TABLE,
    TIME_UNVERIFIED,
    append_event_revision,
    append_finance_atomic_batch_seal,
    append_finance_expected_unavailable,
    append_finance_revision,
    append_source_coverage,
    ensure_pit_fact_schema,
    load_event_facts,
    load_finance_atomic_batch_seal,
    load_finance_expected_unavailable,
    load_finance_facts,
    load_finance_history_facts,
    pit_fact_schema_health,
    resolve_common_fact_cutoff,
)
from server.common.qmt_attestation_contract import canonical_digest
from server.common.qmt_stock_catalog import (
    build_catalog_discovery,
    build_catalog_manifest,
)


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    ensure_pit_fact_schema(engine)
    return engine


def _nonfiling_evidence() -> dict[str, str]:
    return {
        "source": "cninfo.finance.nonfiling",
        "reason_code": "CNINFO_REGULATORY_PERIODIC_REPORT_NOT_FILED",
        "stock_code": "002731",
        "expected_report_date": "2026-03-31",
        "announcement_id": "1225497518",
        "announcement_title": "关于公司未在规定期限内披露定期报告的公告",
        "announcement_published_at": "2026-08-25T18:30:00",
        "announcement_url": (
            "https://static.cninfo.com.cn/finalpage/2026-08-25/"
            "1225497518.PDF"
        ),
        "announcement_document_sha256": "a" * 64,
        "catalog_response_sha256": "b" * 64,
        "valid_from": "2026-08-25",
        "valid_until": "2026-09-01",
        "next_retry_date": "2026-08-31",
    }


def _install_finance_test_catalog(engine) -> None:
    members = [
        {
            "qmt_code": "000001.SZ",
            "stock_code": "000001",
            "list_date": "1991-04-03",
            "expire_date": None,
            "instrument_batch_id": "instrument-test",
            "instrument_type": "STOCK",
        },
        {
            "qmt_code": "002731.SZ",
            "stock_code": "002731",
            "list_date": "2014-11-04",
            "expire_date": None,
            "instrument_batch_id": "instrument-test",
            "instrument_type": "STOCK",
        },
    ]
    discovery = build_catalog_discovery(
        current_sectors=("上证A股", "深证A股", "京市A股"),
        expired_sectors=(),
        sector_members=[
            {"sector_name": "深证A股", "qmt_code": item["qmt_code"]}
            for item in members
        ],
    )
    manifest, normalized = build_catalog_manifest(
        batch_id="catalog-finance-test",
        captured_at="2026-08-30 00:00:00",
        history_complete_from="1990-01-01",
        members=members,
        discovery=discovery,
    )
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_stock_catalog_batch (
                batch_id TEXT PRIMARY KEY, captured_at DATETIME NOT NULL,
                history_complete_from DATE NOT NULL, status TEXT NOT NULL,
                member_count INTEGER NOT NULL, member_set_hash TEXT NOT NULL,
                manifest_json TEXT NOT NULL, manifest_hash TEXT NOT NULL,
                created_at DATETIME NOT NULL
            )
        """))
        connection.execute(text("""
            CREATE TABLE qmt_stock_catalog_member (
                batch_id TEXT NOT NULL, qmt_code TEXT NOT NULL,
                stock_code TEXT NOT NULL, list_date DATE NOT NULL,
                expire_date DATE NULL, instrument_batch_id TEXT NOT NULL,
                instrument_type TEXT NOT NULL, created_at DATETIME NOT NULL,
                PRIMARY KEY (batch_id, qmt_code)
            )
        """))
        connection.execute(text("""
            INSERT INTO qmt_stock_catalog_batch
            (batch_id,captured_at,history_complete_from,status,member_count,
             member_set_hash,manifest_json,manifest_hash,created_at)
            VALUES (:batch_id,:captured_at,:history_complete_from,'COMPLETE',
                    :member_count,:member_set_hash,:manifest_json,
                    :manifest_hash,:captured_at)
        """), {
            "batch_id": manifest["batch_id"],
            "captured_at": manifest["captured_at"],
            "history_complete_from": manifest["history_complete_from"],
            "member_count": manifest["member_count"],
            "member_set_hash": manifest["member_set_hash"],
            "manifest_json": json.dumps(
                manifest, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"),
            ),
            "manifest_hash": canonical_digest(manifest),
        })
        connection.execute(text("""
            INSERT INTO qmt_stock_catalog_member
            (batch_id,qmt_code,stock_code,list_date,expire_date,
             instrument_batch_id,instrument_type,created_at)
            VALUES (:batch_id,:qmt_code,:stock_code,:list_date,:expire_date,
                    :instrument_batch_id,:instrument_type,:created_at)
        """), [
            {
                "batch_id": manifest["batch_id"],
                "created_at": manifest["captured_at"],
                **item,
            }
            for item in normalized
        ])


def test_finance_expected_unavailable_is_audited_non_complete_and_expires():
    engine = _engine()
    receipt = append_finance_expected_unavailable(
        engine,
        stock_code="002731",
        expected_report_date="2026-03-31",
        known_at="2026-08-30 09:00:00",
        official_evidence=_nonfiling_evidence(),
        batch_id="cninfo-nonfiling-test",
    )
    with engine.connect() as connection:
        row = connection.execute(
            text(
                f"SELECT coverage_status, result_count FROM {SOURCE_COVERAGE_TABLE} "
                "WHERE coverage_id=:coverage_id"
            ),
            {"coverage_id": receipt.coverage_id},
        ).mappings().one()
    assert row["coverage_status"] == "EXPECTED_UNAVAILABLE"
    assert row["result_count"] == 0

    available, invalid = load_finance_expected_unavailable(
        engine,
        codes=["002731"],
        decision_at="2026-08-30 10:00:00",
        expected_report_date="2026-03-31",
        known_after="2026-08-30 08:55:00",
    )
    assert invalid == {}
    assert available["002731"]["announcement_id"] == "1225497518"

    expired, invalid = load_finance_expected_unavailable(
        engine,
        codes=["002731"],
        decision_at="2026-09-02 10:00:00",
        expected_report_date="2026-03-31",
    )
    assert expired == {}
    assert invalid == {}


def test_new_h1_nonfiling_receipt_cannot_backfill_an_earlier_decision():
    engine = _engine()
    evidence = {
        **_nonfiling_evidence(),
        "expected_report_date": "2026-06-30",
        "announcement_id": "1225539050",
        "announcement_title": "关于无法在法定期限内披露定期报告的公告",
        "announcement_published_at": "2026-09-01T00:00:00",
        "announcement_url": (
            "https://static.cninfo.com.cn/finalpage/2026-09-01/"
            "1225539050.PDF"
        ),
        "valid_from": "2026-09-01",
        "valid_until": "2026-09-08",
        "next_retry_date": "2026-09-07",
    }
    append_finance_expected_unavailable(
        engine,
        stock_code="002731",
        expected_report_date="2026-06-30",
        known_at="2026-09-06 09:00:00",
        official_evidence=evidence,
        batch_id="cninfo-h1-observed-on-september-6",
    )

    historical, invalid = load_finance_expected_unavailable(
        engine,
        codes=["002731"],
        decision_at="2026-09-04 16:05:00",
        expected_report_date="2026-06-30",
    )
    assert historical == {}
    assert invalid == {}

    current, invalid = load_finance_expected_unavailable(
        engine,
        codes=["002731"],
        decision_at="2026-09-06 09:05:00",
        expected_report_date="2026-06-30",
    )
    assert invalid == {}
    assert current["002731"]["announcement_id"] == "1225539050"
    assert current["002731"]["known_at"] == "2026-09-06T09:00:00.000000"


def test_expected_unavailable_satisfies_finance_common_cutoff_without_fake_fact():
    engine = _engine()
    disposition = append_finance_expected_unavailable(
        engine,
        stock_code="002731",
        expected_report_date="2026-03-31",
        known_at="2026-08-30 09:00:00",
        official_evidence=_nonfiling_evidence(),
        batch_id="cninfo-nonfiling-common-cutoff",
    )
    append_source_coverage(
        engine,
        fact_kind="event",
        stock_code="002731",
        window_start="2026-08-08",
        window_end="2026-08-28",
        known_at="2026-08-30 09:01:00",
        covered_through_at="2026-08-30 09:01:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"source_call": "success"},
        source_rows=[],
        fact_bindings=[],
        source="test.event",
        batch_id="event-002731",
    )

    common = resolve_common_fact_cutoff(
        engine,
        codes=["002731"],
        decision_at="2026-08-30 09:05:00",
        finance_start_date="1900-01-01",
        finance_end_date="2026-08-28",
        event_start_date="2026-08-08",
        event_end_date="2026-08-28",
    )
    assert common["status"] == PIT_AVAILABLE
    finance_receipt = next(
        item for item in common["receipts"]
        if item["fact_kind"] == "finance"
    )
    assert finance_receipt["coverage_id"] == disposition.coverage_id
    assert finance_receipt["coverage_status"] == "EXPECTED_UNAVAILABLE"
    assert finance_receipt["expected_report_date"] == "2026-03-31"

    batch = load_finance_facts(
        engine,
        codes=["002731"],
        decision_at="2026-08-30 09:05:00",
        fact_cutoff_at=common["fact_cutoff_at"],
        as_of_date="2026-08-28",
    )
    assert batch.status_for("002731") == PIT_AVAILABLE
    assert batch.reason_for("002731") == "PIT_FINANCE_EXPECTED_UNAVAILABLE"
    assert batch.facts == {}
    assert batch.coverage_by_code["002731"]["coverage_id"] == (
        disposition.coverage_id
    )


def test_expected_unavailable_supersedes_only_stale_required_period():
    engine = _engine()
    append_finance_revision(
        engine,
        {
            "stock_code": "002731",
            "report_date": "2025-09-30",
            "report_type": "Q3",
            "notice_date": "2025-10-30 18:30:00",
            "roe_wtd": 1.0,
        },
        known_at="2026-08-30 08:55:00",
    )
    append_finance_expected_unavailable(
        engine,
        stock_code="002731",
        expected_report_date="2026-03-31",
        known_at="2026-08-30 09:00:00",
        official_evidence=_nonfiling_evidence(),
        batch_id="cninfo-nonfiling-stale-period",
    )

    batch = load_finance_facts(
        engine,
        codes=["002731"],
        decision_at="2026-08-30 09:05:00",
        fact_cutoff_at="2026-08-30 09:00:00",
        as_of_date="2026-08-28",
    )

    assert batch.status_for("002731") == PIT_AVAILABLE
    assert batch.reason_for("002731") == "PIT_FINANCE_EXPECTED_UNAVAILABLE"
    assert "002731" not in batch.facts

    history = load_finance_history_facts(
        engine,
        codes=["002731"],
        decision_at="2026-08-30 09:05:00",
        fact_cutoff_at="2026-08-30 09:00:00",
        as_of_date="2026-08-28",
    )
    assert history.status_for("002731") == PIT_AVAILABLE
    assert history.reason_for("002731") == "PIT_FINANCE_EXPECTED_UNAVAILABLE"
    assert history.facts["002731"] == []


def test_finance_atomic_seal_binds_full_catalog_and_completion_watermark(
    monkeypatch,
):
    engine = _engine()
    monkeypatch.setattr(
        pit_module,
        "FINANCE_ATOMIC_BATCH_QUERY_CODE_LIMIT",
        1,
    )
    _install_finance_test_catalog(engine)
    finance_row = {
        "stock_code": "000001",
        "report_date": "2026-03-31",
        "report_type": "Q1",
        "notice_date": "2026-04-25",
        "roe_wtd": 12.5,
    }
    revision = append_finance_revision(
        engine,
        finance_row,
        known_at="2026-08-30 00:15:42",
        batch_id="finance-member-000001",
    )
    append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code="000001",
        window_start="1900-01-01",
        window_end="2026-08-30",
        known_at="2026-08-30 00:15:42",
        covered_through_at="2026-08-30 00:15:42",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"source_call": "success"},
        source_rows=[finance_row],
        fact_bindings=[{
            "revision_id": revision.revision_id,
            "content_hash": revision.content_hash,
        }],
        source="adata.finance.core_index",
        batch_id="finance-member-000001",
    )
    append_finance_expected_unavailable(
        engine,
        stock_code="002731",
        expected_report_date="2026-03-31",
        known_at="2026-08-30 01:06:35",
        official_evidence=_nonfiling_evidence(),
        batch_id="finance-member-002731",
    )

    sealed = append_finance_atomic_batch_seal(
        engine,
        as_of_date="2026-08-30",
        completed_known_at="2026-08-30 01:10:00",
    )
    assert sealed["eligible_code_count"] == 2
    assert sealed["expected_unavailable_count"] == 1
    assert sealed["source_cutoff_at"] == "2026-08-30T00:15:42.000000"
    assert sealed["completed_known_at"] == "2026-08-30T01:10:00.000000"

    loaded = load_finance_atomic_batch_seal(
        engine,
        codes=["000001", "002731"],
        decision_at="2026-08-30 01:11:00",
        as_of_date="2026-08-28",
    )
    assert loaded["batch_root_sha256"] == sealed["batch_root_sha256"]
    assert loaded["eligible_code_count"] == 2
    assert loaded["members"]["000001"]["strategy_prefix_binding"][
        "latest_report_date"
    ] == "2026-03-31"
    assert loaded["members"]["002731"]["coverage_status"] == (
        "EXPECTED_UNAVAILABLE"
    )
    assert "payload_json" not in loaded["rows"]["000001"]
    assert loaded["rows"]["002731"]["_expected_report_date"] == (
        "2026-03-31"
    )


def _finance_discovery_evidence(
    *,
    changed: bool,
    changed_code: str = "000001",
) -> dict:
    target = "2026-08-31"
    events = []
    if changed:
        events.append({
            "query_field": "UPDATE_DATE",
            "query_date": target,
            "source_security_code": changed_code,
            "stock_code": changed_code,
            "report_date": "2026-03-31",
            "report_type": "Q1",
            "notice_date": "2026-04-25",
            "update_date": target,
            "row_content_sha256": "9" * 64,
        })
    events.sort(key=pit_module.canonical_json)
    queries = []
    for field in ("NOTICE_DATE", "UPDATE_DATE"):
        query_events = [
            event for event in events if event["query_field"] == field
        ]
        query_hash = pit_module.canonical_hash({
            "schema": "probiga.pit-finance-discovery-query-result.v1",
            "query_field": field,
            "query_date": target,
            "events": query_events,
        })
        queries.append({
            "query_field": field,
            "query_date": target,
            "page_size": 500,
            "page_count": 1,
            "total_count": len(query_events),
            "row_count": len(query_events),
            "page_row_counts": [len(query_events)],
            "page_raw_sha256": [pit_module.canonical_hash({"raw": field})],
            "page_content_sha256": [
                pit_module.canonical_hash({"content": field})
            ],
            "content_sha256": query_hash,
        })
    queries.sort(key=lambda item: (item["query_date"], item["query_field"]))
    sweep_root = pit_module.canonical_hash({
        "schema": "probiga.pit-finance-discovery-sweep.v1",
        "queries": queries,
    })
    changed_codes = [changed_code] if changed else []
    universe = ["000001", "002731"]
    return {
        "schema": pit_module.FINANCE_INCREMENTAL_DISCOVERY_SCHEMA,
        "source": pit_module.FINANCE_INCREMENTAL_DISCOVERY_SOURCE,
        "endpoint": "https://datacenter.eastmoney.com/securities/api/data/get",
        "query_mode": "EXACT_DATE",
        "query_fields": ["NOTICE_DATE", "UPDATE_DATE"],
        "window_start": target,
        "window_end": target,
        "universe_code_count": len(universe),
        "universe_code_set_sha256": pit_module.canonical_hash({
            "schema": "probiga.pit-finance-discovery-universe.v1",
            "codes": universe,
        }),
        "stable_sweep_count": 2,
        "stability_status": "STABLE_DOUBLE_SWEEP",
        "stable_content_sha256": sweep_root,
        "event_count": len(events),
        "event_set_sha256": pit_module.canonical_hash({
            "schema": "probiga.pit-finance-discovery-event-set.v1",
            "events": events,
        }),
        "events": events,
        "changed_codes": changed_codes,
        "changed_code_set_sha256": pit_module.canonical_hash({
            "schema": "probiga.pit-finance-discovery-changed-code-set.v1",
            "codes": changed_codes,
        }),
        "sweeps": [
            {
                "sweep_no": ordinal,
                "started_at": f"2026-08-31 01:0{ordinal}:00",
                "completed_at": f"2026-08-31 01:0{ordinal}:30",
                "query_count": 2,
                "page_count": 2,
                "row_count": len(events),
                "content_sha256": sweep_root,
                "queries": queries,
            }
            for ordinal in (1, 2)
        ],
    }


def _install_prior_finance_coverage(engine) -> None:
    finance_row = {
        "stock_code": "000001",
        "report_date": "2026-03-31",
        "report_type": "Q1",
        "notice_date": "2026-04-25",
        "roe_wtd": 12.5,
    }
    revision = append_finance_revision(
        engine,
        finance_row,
        known_at="2026-08-30 00:15:42",
        batch_id="finance-prior-000001",
    )
    append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code="000001",
        window_start="1900-01-01",
        window_end="2026-08-30",
        known_at="2026-08-30 00:15:42",
        covered_through_at="2026-08-30 00:15:42",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"source_call": "success"},
        source_rows=[finance_row],
        fact_bindings=[{
            "revision_id": revision.revision_id,
            "content_hash": revision.content_hash,
        }],
        source="adata.finance.core_index",
        batch_id="finance-prior-000001",
    )
    append_finance_expected_unavailable(
        engine,
        stock_code="002731",
        expected_report_date="2026-03-31",
        known_at="2026-08-30 01:06:35",
        official_evidence=_nonfiling_evidence(),
        batch_id="finance-prior-002731",
    )


def test_finance_atomic_seal_reuses_prior_member_only_with_stable_discovery():
    engine = _engine()
    _install_finance_test_catalog(engine)
    _install_prior_finance_coverage(engine)
    evidence = _finance_discovery_evidence(changed=False)
    discovery = append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code=pit_module.FINANCE_INCREMENTAL_DISCOVERY_CODE,
        window_start="2026-08-31",
        window_end="2026-08-31",
        known_at="2026-08-31 01:04:00",
        covered_through_at="2026-08-31 01:04:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence=evidence,
        source_rows=[],
        fact_bindings=[],
        source=pit_module.FINANCE_INCREMENTAL_DISCOVERY_SOURCE,
        batch_id="finance-discovery-stable",
    )

    sealed = append_finance_atomic_batch_seal(
        engine,
        as_of_date="2026-08-31",
        completed_known_at="2026-08-31 01:10:00",
        incremental_discovery_coverage_id=discovery.coverage_id,
    )
    loaded = load_finance_atomic_batch_seal(
        engine,
        codes=["000001", "002731"],
        decision_at="2026-08-31 01:11:00",
        as_of_date="2026-08-31",
    )

    assert sealed["incremental_discovery_binding"]["coverage_id"] == (
        discovery.coverage_id
    )
    assert loaded["members"]["000001"][
        "incremental_discovery_binding"
    ]["coverage_id"] == discovery.coverage_id


def test_finance_atomic_seal_extends_only_unchanged_member_source_cutoff():
    engine = _engine()
    _install_finance_test_catalog(engine)
    prior_row = {
        "stock_code": "000001",
        "report_date": "2026-03-31",
        "report_type": "Q1",
        "notice_date": "2026-04-25",
        "roe_wtd": 12.5,
    }
    prior_revision = append_finance_revision(
        engine,
        prior_row,
        known_at="2026-08-30 00:15:42",
        batch_id="finance-cutoff-prior-000001",
    )
    append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code="000001",
        window_start="1900-01-01",
        window_end="2026-08-30",
        known_at="2026-08-30 00:15:42",
        covered_through_at="2026-08-30 00:15:42",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"source_call": "success"},
        source_rows=[prior_row],
        fact_bindings=[{
            "revision_id": prior_revision.revision_id,
            "content_hash": prior_revision.content_hash,
        }],
        source="adata.finance.core_index",
        batch_id="finance-cutoff-prior-000001",
    )
    changed_row = {
        "stock_code": "002731",
        "report_date": "2026-03-31",
        "report_type": "Q1",
        "notice_date": "2026-08-31",
        "published_at": "2026-08-31 00:30:00",
        "publication_source": "qmt.announcement",
        "publication_event_key": "AN-20260831-002731",
        "publication_received_at": "2026-08-31 00:30:30",
        "publication_content_sha256": "7" * 64,
        "roe_wtd": 8.0,
    }
    changed_revision = append_finance_revision(
        engine,
        changed_row,
        known_at="2026-08-31 01:03:00",
        batch_id="finance-cutoff-change-002731",
    )
    append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code="002731",
        window_start="1900-01-01",
        window_end="2026-08-31",
        known_at="2026-08-31 01:03:00",
        covered_through_at="2026-08-31 01:03:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"source_call": "success"},
        source_rows=[changed_row],
        fact_bindings=[{
            "revision_id": changed_revision.revision_id,
            "content_hash": changed_revision.content_hash,
        }],
        source="adata.finance.core_index",
        batch_id="finance-cutoff-change-002731",
    )
    evidence = _finance_discovery_evidence(
        changed=True,
        changed_code="002731",
    )
    discovery = append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code=pit_module.FINANCE_INCREMENTAL_DISCOVERY_CODE,
        window_start="2026-08-31",
        window_end="2026-08-31",
        known_at="2026-08-31 01:04:00",
        covered_through_at="2026-08-31 01:04:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence=evidence,
        source_rows=[],
        fact_bindings=[],
        source=pit_module.FINANCE_INCREMENTAL_DISCOVERY_SOURCE,
        batch_id="finance-cutoff-discovery",
    )

    sealed = append_finance_atomic_batch_seal(
        engine,
        as_of_date="2026-08-31",
        completed_known_at="2026-08-31 01:10:00",
        incremental_discovery_coverage_id=discovery.coverage_id,
    )
    loaded = load_finance_atomic_batch_seal(
        engine,
        codes=["000001", "002731"],
        decision_at="2026-08-31 01:11:00",
        as_of_date="2026-08-31",
    )

    assert sealed["source_cutoff_at"] == "2026-08-31T01:03:00.000000"
    assert loaded["source_cutoff_at"] == sealed["source_cutoff_at"]
    assert loaded["batch_root_sha256"] == sealed["batch_root_sha256"]
    assert loaded["members"]["000001"]["known_at"] == (
        "2026-08-30T00:15:42.000000"
    )
    assert loaded["members"]["000001"][
        "incremental_discovery_binding"
    ]["coverage_id"] == discovery.coverage_id


def test_finance_atomic_seal_never_reuses_discovery_changed_member():
    engine = _engine()
    _install_finance_test_catalog(engine)
    _install_prior_finance_coverage(engine)
    evidence = _finance_discovery_evidence(changed=True)
    discovery = append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code=pit_module.FINANCE_INCREMENTAL_DISCOVERY_CODE,
        window_start="2026-08-31",
        window_end="2026-08-31",
        known_at="2026-08-31 01:04:00",
        covered_through_at="2026-08-31 01:04:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence=evidence,
        source_rows=[],
        fact_bindings=[],
        source=pit_module.FINANCE_INCREMENTAL_DISCOVERY_SOURCE,
        batch_id="finance-discovery-changed",
    )

    with pytest.raises(ValueError, match="no valid disposition for 000001"):
        append_finance_atomic_batch_seal(
            engine,
            as_of_date="2026-08-31",
            completed_known_at="2026-08-31 01:10:00",
            incremental_discovery_coverage_id=discovery.coverage_id,
        )


def test_finance_incremental_seal_reuses_parent_and_revalidates_only_delta(
    monkeypatch,
):
    engine = _engine()
    _install_finance_test_catalog(engine)
    finance_row = {
        "stock_code": "000001",
        "report_date": "2026-03-31",
        "report_type": "Q1",
        "notice_date": "2026-04-25",
        "roe_wtd": 12.5,
    }
    revision = append_finance_revision(
        engine,
        finance_row,
        known_at="2026-08-30 00:15:42",
        batch_id="finance-delta-base",
    )
    append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code="000001",
        window_start="1900-01-01",
        window_end="2026-08-30",
        known_at="2026-08-30 00:15:42",
        covered_through_at="2026-08-30 00:15:42",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"source_call": "success"},
        source_rows=[finance_row],
        fact_bindings=[{
            "revision_id": revision.revision_id,
            "content_hash": revision.content_hash,
        }],
        source="adata.finance.core_index",
        batch_id="finance-delta-base",
    )
    append_finance_expected_unavailable(
        engine,
        stock_code="002731",
        expected_report_date="2026-03-31",
        known_at="2026-08-30 01:06:35",
        official_evidence=_nonfiling_evidence(),
        batch_id="finance-delta-unavailable",
    )
    parent = append_finance_atomic_batch_seal(
        engine,
        as_of_date="2026-08-30",
        completed_known_at="2026-08-30 01:10:00",
    )

    revised_row = {**finance_row, "roe_wtd": 13.0}
    revised = append_finance_revision(
        engine,
        revised_row,
        known_at="2026-08-30 01:20:00",
        batch_id="finance-delta-change",
    )
    append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code="000001",
        window_start="1900-01-01",
        window_end="2026-08-30",
        known_at="2026-08-30 01:20:00",
        covered_through_at="2026-08-30 01:20:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"source_call": "success"},
        source_rows=[revised_row],
        fact_bindings=[{
            "revision_id": revised.revision_id,
            "content_hash": revised.content_hash,
        }],
        source="adata.finance.core_index",
        batch_id="finance-delta-change",
    )
    calls: list[list[str]] = []
    original = pit_module._select_finance_atomic_batch_members

    def record(*args, **kwargs):
        calls.append(list(kwargs["codes"]))
        return original(*args, **kwargs)

    monkeypatch.setattr(
        pit_module,
        "_select_finance_atomic_batch_members",
        record,
    )
    delta = append_finance_atomic_batch_seal(
        engine,
        as_of_date="2026-08-30",
        completed_known_at="2026-08-30 01:30:00",
        changed_codes=["000001"],
    )
    loaded = load_finance_atomic_batch_seal(
        engine,
        codes=["000001", "002731"],
        decision_at="2026-08-30 01:31:00",
        as_of_date="2026-08-30",
    )

    assert calls == [["000001"], ["000001"]]
    assert delta["parent_batch_root_sha256"] == parent["batch_root_sha256"]
    assert delta["changed_code_count"] == 1
    assert loaded["changed_code_count"] == 1
    assert loaded["members"]["002731"]["coverage_status"] == (
        "EXPECTED_UNAVAILABLE"
    )


def test_schema_health_and_database_triggers_enforce_append_only():
    engine = _engine()
    receipt = append_finance_revision(
        engine,
        {
            "stock_code": "000001",
            "report_date": "2026-06-30",
            "report_type": "Q2",
            "notice_date": "2026-08-20 18:30:00",
            "roe_wtd": 12.5,
        },
        known_at="2026-08-20 18:31:00",
    )
    event_receipt = append_event_revision(
        engine,
        {
            "stock_code": "000001",
            "art_code": "AN-IMMUTABLE",
            "notice_date": "2026-08-20",
            "display_time": "2026-08-20 18:30:00",
            "title": "不可变公告",
        },
        known_at="2026-08-20 18:31:00",
    )
    coverage_receipt = append_source_coverage(
        engine,
        fact_kind="event",
        stock_code="000001",
        window_start="2026-08-01",
        window_end="2026-08-20",
        known_at="2026-08-20 18:31:00",
        covered_through_at="2026-08-20 18:31:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"source_call": "success"},
        source_rows=[],
        fact_bindings=[],
        source="eastmoney.notice",
        batch_id="coverage-immutable",
    )
    health = pit_fact_schema_health(engine)
    assert health["valid"] is True
    assert health["table_count"] == 3
    assert health["trigger_count"] == 6

    with pytest.raises(DatabaseError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"UPDATE {FINANCE_REVISION_TABLE} "
                    "SET batch_id='tampered' WHERE revision_id=:revision_id"
                ),
                {"revision_id": receipt.revision_id},
            )
    with pytest.raises(DatabaseError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"UPDATE {SOURCE_COVERAGE_TABLE} SET batch_id='tampered' "
                    "WHERE coverage_id=:coverage_id"
                ),
                {"coverage_id": coverage_receipt.coverage_id},
            )
    with pytest.raises(DatabaseError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"DELETE FROM {SOURCE_COVERAGE_TABLE} "
                    "WHERE coverage_id=:coverage_id"
                ),
                {"coverage_id": coverage_receipt.coverage_id},
            )
    with pytest.raises(DatabaseError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"UPDATE {EVENT_REVISION_TABLE} "
                    "SET batch_id='tampered' WHERE revision_id=:revision_id"
                ),
                {"revision_id": event_receipt.revision_id},
            )
    with pytest.raises(DatabaseError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"DELETE FROM {EVENT_REVISION_TABLE} "
                    "WHERE revision_id=:revision_id"
                ),
                {"revision_id": event_receipt.revision_id},
            )
    with pytest.raises(DatabaseError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    f"DELETE FROM {FINANCE_REVISION_TABLE} "
                    "WHERE revision_id=:revision_id"
                ),
                {"revision_id": receipt.revision_id},
            )


def test_finance_late_arrival_and_revision_do_not_rewrite_historical_prefix():
    engine = _engine()
    first = append_finance_revision(
        engine,
        {
            "stock_code": "000001",
            "report_date": "2026-06-30",
            "report_type": "Q2",
            "notice_date": "2026-08-05 19:00:00",
            "net_profit_yoy_gr": 10.0,
            "roe_wtd": 8.0,
        },
        known_at="2026-08-05 19:02:00",
        batch_id="finance-first",
    )
    july = load_finance_facts(
        engine,
        codes=["000001"],
        decision_at="2026-07-31 15:00:00",
        as_of_date="2026-07-31",
    )
    assert july.status_for("000001") == PIT_NO_ROWS
    assert "000001" not in july.facts

    before_revision = load_finance_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-06 15:00:00",
        as_of_date="2026-08-06",
    )
    assert before_revision.status_for("000001") == PIT_AVAILABLE
    assert before_revision.facts["000001"]["finance_revision_id"] == first.revision_id
    assert before_revision.facts["000001"]["net_profit_yoy_gr"] == 10.0
    assert before_revision.facts["000001"]["finance_notice_date"].startswith(
        "2026-08-05T19:00:00"
    )
    assert before_revision.facts["000001"]["finance_knowledge_at"].startswith(
        "2026-08-05T19:02:00"
    )

    second = append_finance_revision(
        engine,
        {
            "stock_code": "000001",
            "report_date": "2026-06-30",
            "report_type": "Q2",
            "notice_date": "2026-08-05 19:00:00",
            "net_profit_yoy_gr": 18.0,
            "roe_wtd": 9.0,
        },
        known_at="2026-08-10 09:05:00",
        batch_id="finance-correction",
    )
    assert second.revision_no == 2
    assert second.supersedes_revision_id == first.revision_id

    replayed_prefix = load_finance_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-06 15:00:00",
        as_of_date="2026-08-06",
    )
    assert replayed_prefix.facts["000001"]["finance_revision_id"] == first.revision_id
    assert replayed_prefix.manifest_hash == before_revision.manifest_hash

    after_revision = load_finance_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-11 15:00:00",
        as_of_date="2026-08-11",
    )
    assert after_revision.facts["000001"]["finance_revision_id"] == second.revision_id
    assert after_revision.facts["000001"]["net_profit_yoy_gr"] == 18.0


def test_same_source_content_is_idempotent_but_changed_content_supersedes():
    engine = _engine()
    source = {
        "stock_code": "600000",
        "report_date": "2026-06-30",
        "notice_date": "2026-08-18 20:00:00",
        "basic_eps": 0.8,
        "etl_sync_at": datetime(2026, 8, 18, 20, 1),
    }
    first = append_finance_revision(
        engine,
        source,
        known_at="2026-08-18 20:01:00",
    )
    repeated = append_finance_revision(
        engine,
        {**source, "etl_sync_at": datetime(2026, 8, 19, 8, 0)},
        known_at="2026-08-19 08:00:00",
    )
    changed = append_finance_revision(
        engine,
        {**source, "basic_eps": 0.9},
        known_at="2026-08-19 09:00:00",
    )
    reverted = append_finance_revision(
        engine,
        source,
        known_at="2026-08-20 09:00:00",
    )
    assert repeated.idempotent is True
    assert repeated.revision_id == first.revision_id
    assert changed.revision_no == 2
    assert changed.supersedes_revision_id == first.revision_id
    assert reverted.revision_no == 3
    assert reverted.supersedes_revision_id == changed.revision_id
    assert reverted.revision_id != first.revision_id
    with engine.connect() as connection:
        assert connection.execute(
            text(f"SELECT COUNT(*) FROM {FINANCE_REVISION_TABLE}")
        ).scalar_one() == 3


def test_finance_date_only_publication_is_usable_only_after_local_receipt():
    engine = _engine()
    append_finance_revision(
        engine,
        {
            "stock_code": "000001",
            "report_date": "2026-06-30",
            "notice_date": "2026-08-05",
            "roe_wtd": 8.0,
        },
        known_at="2026-08-05 21:00:00",
    )
    before_receipt = load_finance_history_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-05 20:59:00",
        as_of_date="2026-08-05",
    )
    assert before_receipt.status_for("000001") == PIT_NO_ROWS
    after_receipt = load_finance_history_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-05 22:35:00",
        as_of_date="2026-08-05",
    )
    assert after_receipt.status_for("000001") == PIT_AVAILABLE
    fact = after_receipt.facts["000001"][0]
    assert fact["finance_publication_precision"] == "DATE_ONLY"
    assert fact["finance_publication_time_verified"] is False
    assert fact["finance_observed_available_at"].startswith(
        "2026-08-05T21:00:00"
    )


def test_finance_exact_publication_allows_a_same_day_atomic_seal():
    engine = _engine()
    _install_finance_test_catalog(engine)
    for index, code in enumerate(("000001", "002731"), start=1):
        source = {
            "stock_code": code,
            "report_date": "2026-06-30",
            "report_type": "H1",
            "notice_date": "2026-08-30",
            "published_at": f"2026-08-30 08:4{index}:15",
            "publication_source": "qmt.announcement",
            "publication_event_key": f"AN-20260805-{index}",
            "publication_received_at": f"2026-08-30 08:4{index}:31",
            "publication_content_sha256": str(index) * 64,
            "roe_wtd": 8.0,
        }
        revision = append_finance_revision(
            engine,
            source,
            known_at="2026-08-30 09:00:00",
        )
        append_source_coverage(
            engine,
            fact_kind="finance",
            stock_code=code,
            window_start="1900-01-01",
            window_end="2026-08-30",
            known_at="2026-08-30 09:00:00",
            covered_through_at="2026-08-30 09:00:00",
            watermark_kind="CAPTURED_AT",
            watermark_evidence={"source_call": "success"},
            source_rows=[source],
            fact_bindings=[{
                "revision_id": revision.revision_id,
                "content_hash": revision.content_hash,
            }],
            source="adata.finance.core_index",
            batch_id=f"finance-same-day-exact-{code}",
        )

    sealed = append_finance_atomic_batch_seal(
        engine,
        as_of_date="2026-08-30",
        completed_known_at="2026-08-30 09:05:00",
    )

    assert sealed["eligible_code_count"] == 2
    loaded = load_finance_atomic_batch_seal(
        engine,
        codes=["000001"],
        decision_at="2026-08-30 09:06:00",
        as_of_date="2026-08-30",
    )
    assert loaded["members"]["000001"]["coverage_status"] == "COMPLETE"


def test_midnight_sentinel_is_not_treated_as_an_exact_publication_time():
    engine = _engine()
    receipt = append_event_revision(
        engine,
        {
            "stock_code": "000001",
            "art_code": "AN-MIDNIGHT-SENTINEL",
            "notice_date": "2026-08-05",
            "display_time": "2026-08-05 00:00:00",
            "title": "源端仅有日期",
        },
        known_at="2026-08-05 08:00:00",
    )
    assert receipt.publication_time_status == TIME_UNVERIFIED
    before = load_event_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-05 07:59:00",
        end_date="2026-08-05",
    )
    assert before.status_for("000001") == PIT_NO_ROWS
    batch = load_event_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-05 15:00:00",
        end_date="2026-08-05",
    )
    assert batch.status_for("000001") == PIT_AVAILABLE
    fact = batch.facts["000001"][0]
    assert fact["event_publication_precision"] == "DATE_ONLY"
    assert fact["event_publication_time_verified"] is False


def test_missing_event_date_cannot_hide_beside_a_valid_event():
    engine = _engine()
    append_event_revision(
        engine,
        {
            "stock_code": "000001",
            "art_code": "AN-VALID",
            "notice_date": "2026-08-05",
            "display_time": "2026-08-05 09:00:00",
            "title": "有效公告",
        },
        known_at="2026-08-05 09:01:00",
    )
    append_event_revision(
        engine,
        {
            "stock_code": "000001",
            "art_code": "AN-MISSING-DATE",
            "title": "缺失发布时间公告",
        },
        known_at="2026-08-05 09:02:00",
    )
    batch = load_event_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-05 15:00:00",
        start_date="2026-08-01",
        end_date="2026-08-05",
    )
    assert batch.status_for("000001") == PIT_DATA_BLOCKED
    assert batch.reason_for("000001") == "PIT_EVENT_PUBLICATION_DATE_MISSING"
    assert not batch.facts.get("000001")


def test_zero_event_coverage_is_valid_only_at_its_bound_watermark():
    engine = _engine()
    first = append_source_coverage(
        engine,
        fact_kind="event",
        stock_code="000001",
        window_start="2026-08-01",
        window_end="2026-08-05",
        known_at="2026-08-05 21:00:00",
        covered_through_at="2026-08-05 21:00:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"http_result": "success", "page_count": 1},
        source_rows=[],
        fact_bindings=[],
        source="eastmoney.notice",
        batch_id="event-empty-1",
    )
    repeated = append_source_coverage(
        engine,
        fact_kind="event",
        stock_code="000001",
        window_start="2026-08-01",
        window_end="2026-08-05",
        known_at="2026-08-05 21:00:00",
        covered_through_at="2026-08-05 21:00:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"http_result": "success", "page_count": 1},
        source_rows=[],
        fact_bindings=[],
        source="eastmoney.notice",
        batch_id="event-empty-1",
    )
    assert repeated.idempotent is True
    assert repeated.coverage_id == first.coverage_id
    exact_cutoff = load_event_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-05 21:00:00",
        start_date="2026-08-01",
        end_date="2026-08-05",
    )
    assert exact_cutoff.status_for("000001") == PIT_AVAILABLE
    assert exact_cutoff.facts["000001"] == []
    assert exact_cutoff.reason_for("000001") == "PIT_EVENT_AUTHORITATIVE_EMPTY"
    assert exact_cutoff.coverage_by_code["000001"]["coverage_id"] == first.coverage_id

    stale_gap = load_event_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-05 21:00:01",
        start_date="2026-08-01",
        end_date="2026-08-05",
    )
    assert stale_gap.status_for("000001") == PIT_NO_ROWS

    event = append_event_revision(
        engine,
        {
            "stock_code": "000001",
            "art_code": "AN-AFTER-COVERAGE",
            "notice_date": "2026-08-05",
            "display_time": "2026-08-05 21:00:30",
            "title": "覆盖凭证之后的新公告",
        },
        known_at="2026-08-05 21:00:30",
    )
    after_event = load_event_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-05 21:00:45",
        start_date="2026-08-01",
        end_date="2026-08-05",
    )
    assert after_event.status_for("000001") == PIT_AVAILABLE
    assert after_event.facts["000001"][0]["event_revision_id"] == event.revision_id


def test_zero_finance_coverage_and_watermark_cannot_be_forged_forward():
    engine = _engine()
    receipt = append_source_coverage(
        engine,
        fact_kind="finance",
        stock_code="688001",
        window_start="1900-01-01",
        window_end="2026-08-05",
        known_at="2026-08-05 21:00:00",
        covered_through_at="2026-08-05 21:00:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"provider_return": "successful_empty"},
        source_rows=[],
        fact_bindings=[],
        source="adata.finance.core_index",
        batch_id="finance-empty",
    )
    batch = load_finance_facts(
        engine,
        codes=["688001"],
        decision_at="2026-08-05 21:00:00",
        as_of_date="2026-08-05",
    )
    assert batch.status_for("688001") == PIT_AVAILABLE
    assert batch.facts.get("688001") is None
    assert batch.coverage_by_code["688001"]["coverage_id"] == receipt.coverage_id
    with pytest.raises(ValueError, match="later than local knowledge"):
        append_source_coverage(
            engine,
            fact_kind="finance",
            stock_code="688001",
            window_start="1900-01-01",
            window_end="2026-08-05",
            known_at="2026-08-05 21:00:00",
            covered_through_at="2026-08-05 22:35:00",
            watermark_kind="SOURCE_SERVER_TIME",
            watermark_evidence={"server_time": "2026-08-05 22:35:00"},
            source_rows=[],
            fact_bindings=[],
            source="adata.finance.core_index",
            batch_id="forged-forward",
        )


def test_common_fact_cutoff_accepts_staggered_receipts_before_decision():
    engine = _engine()
    receipts = {}
    for code, captured_at in (
        ("000001", "2026-08-05 21:05:00"),
        ("000002", "2026-08-05 21:20:00"),
    ):
        receipts[code] = append_source_coverage(
            engine,
            fact_kind="event",
            stock_code=code,
            window_start="2026-08-01",
            window_end="2026-08-05",
            known_at=captured_at,
            covered_through_at=captured_at,
            watermark_kind="CAPTURED_AT",
            watermark_evidence={"provider": "test", "response": "empty"},
            source_rows=[],
            fact_bindings=[],
            source="test.notice",
            batch_id=f"common-t-{code}",
        )
    batch = load_event_facts(
        engine,
        codes=["000001", "000002"],
        fact_cutoff_at="2026-08-05 21:00:00",
        decision_at="2026-08-05 21:25:00",
        start_date="2026-08-01",
        end_date="2026-08-05",
    )
    assert batch.fact_cutoff_at.startswith("2026-08-05T21:00:00")
    assert batch.decision_at.startswith("2026-08-05T21:25:00")
    for code in receipts:
        assert batch.status_for(code) == PIT_AVAILABLE
        assert batch.coverage_by_code[code]["coverage_id"] == receipts[code].coverage_id
        assert batch.coverage_by_code[code]["fact_cutoff_at"].startswith(
            "2026-08-05T21:00:00"
        )


def test_fact_cutoff_excludes_later_event_and_enforces_receipt_decision_bound():
    engine = _engine()
    append_source_coverage(
        engine,
        fact_kind="event",
        stock_code="000001",
        window_start="2026-08-01",
        window_end="2026-08-05",
        known_at="2026-08-05 21:20:00",
        covered_through_at="2026-08-05 21:20:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"provider": "test", "response": "empty"},
        source_rows=[],
        fact_bindings=[],
        source="test.notice",
        batch_id="receipt-after-e",
    )
    before_receipt = load_event_facts(
        engine,
        codes=["000001"],
        fact_cutoff_at="2026-08-05 21:00:00",
        decision_at="2026-08-05 21:10:00",
        start_date="2026-08-01",
        end_date="2026-08-05",
    )
    assert before_receipt.status_for("000001") == PIT_NO_ROWS

    future = append_event_revision(
        engine,
        {
            "stock_code": "000001",
            "art_code": "AFTER-T",
            "notice_date": "2026-08-05",
            "display_time": "2026-08-05 21:12:00",
            "title": "T之后公告",
        },
        known_at="2026-08-05 21:13:00",
    )
    excluded = load_event_facts(
        engine,
        codes=["000001"],
        fact_cutoff_at="2026-08-05 21:00:00",
        decision_at="2026-08-05 21:25:00",
        start_date="2026-08-01",
        end_date="2026-08-05",
    )
    assert future.revision_id not in {
        row["event_revision_id"]
        for row in excluded.facts.get("000001", [])
    }


def test_post_cutoff_date_only_fact_cannot_backfill_months_later():
    engine = _engine()
    append_finance_revision(
        engine,
        {
            "stock_code": "000001",
            "report_date": "2026-06-30",
            "notice_date": "2026-07-01",
            "roe_wtd": 9.0,
        },
        known_at="2026-08-01 09:00:00",
    )
    backfill = load_finance_facts(
        engine,
        codes=["000001"],
        fact_cutoff_at="2026-07-01 15:00:00",
        decision_at="2026-08-01 09:01:00",
        as_of_date="2026-07-01",
    )
    assert backfill.status_for("000001") == PIT_DATA_BLOCKED
    assert "LIVE_CAPTURE_WINDOW_EXCEEDED" in backfill.reason_for("000001")

    live = load_finance_facts(
        engine,
        codes=["000001"],
        fact_cutoff_at="2026-08-01 08:55:00",
        decision_at="2026-08-01 09:05:00",
        as_of_date="2026-08-01",
    )
    assert live.status_for("000001") == PIT_AVAILABLE


def test_event_exact_publication_and_knowledge_times_prevent_lookahead():
    engine = _engine()
    future = append_event_revision(
        engine,
        {
            "stock_code": "000001",
            "art_code": "AN-1",
            "notice_date": "2026-08-20",
            "display_time": "2026-08-20 20:00:00",
            "title": "重大合同",
        },
        known_at="2026-08-20 14:00:00",
    )
    before_publish = load_event_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-20 15:00:00",
        start_date="2026-08-01",
        end_date="2026-08-20",
    )
    assert before_publish.status_for("000001") == PIT_DATA_BLOCKED
    assert before_publish.reason_for("000001") == "PIT_EVENT_PUBLISHED_AFTER_DECISION"
    assert not before_publish.facts.get("000001")

    after_publish = load_event_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-20 20:01:00",
        start_date="2026-08-01",
        end_date="2026-08-20",
    )
    assert after_publish.status_for("000001") == PIT_AVAILABLE
    assert after_publish.facts["000001"][0]["event_revision_id"] == future.revision_id

    late = append_event_revision(
        engine,
        {
            "stock_code": "000001",
            "art_code": "AN-2",
            "notice_date": "2026-08-20",
            "display_time": "2026-08-20 10:00:00",
            "title": "风险提示",
        },
        known_at="2026-08-21 09:00:00",
    )
    historical = load_event_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-20 20:30:00",
        start_date="2026-08-01",
        end_date="2026-08-20",
    )
    assert late.revision_id not in {
        row["event_revision_id"] for row in historical.facts["000001"]
    }


def test_event_revision_prefix_idempotency_and_unverified_time_blocking():
    engine = _engine()
    first = append_event_revision(
        engine,
        {
            "stock_code": "600000",
            "art_code": "AN-REV",
            "notice_date": "2026-08-20",
            "display_time": "2026-08-20 09:30:00",
            "title": "业绩预增",
            "etl_sync_at": "2026-08-20 09:31:00",
        },
        known_at="2026-08-20 09:31:00",
    )
    repeated = append_event_revision(
        engine,
        {
            "stock_code": "600000",
            "art_code": "AN-REV",
            "notice_date": "2026-08-20",
            "display_time": "2026-08-20 09:30:00",
            "title": "业绩预增",
            "etl_sync_at": "2026-08-20 10:00:00",
        },
        known_at="2026-08-20 10:00:00",
    )
    corrected = append_event_revision(
        engine,
        {
            "stock_code": "600000",
            "art_code": "AN-REV",
            "notice_date": "2026-08-20",
            "display_time": "2026-08-20 09:30:00",
            "title": "业绩预减",
        },
        known_at="2026-08-21 09:00:00",
    )
    assert repeated.revision_id == first.revision_id
    assert repeated.idempotent is True
    assert corrected.supersedes_revision_id == first.revision_id
    historical = load_event_facts(
        engine,
        codes=["600000"],
        decision_at="2026-08-20 15:00:00",
        end_date="2026-08-20",
    )
    assert historical.facts["600000"][0]["title"] == "业绩预增"

    append_event_revision(
        engine,
        {
            "stock_code": "600000",
            "art_code": "AN-DATE-ONLY",
            "notice_date": "2026-08-21",
            "display_time": "2026-08-21",
            "title": "日期公告",
        },
        known_at="2026-08-21 10:00:00",
    )
    observed = load_event_facts(
        engine,
        codes=["600000"],
        decision_at="2026-08-21 15:00:00",
        end_date="2026-08-21",
    )
    assert observed.status_for("600000") == PIT_AVAILABLE
    date_only = next(
        row for row in observed.facts["600000"]
        if row.get("art_code") == "AN-DATE-ONLY"
    )
    assert date_only["event_publication_precision"] == "DATE_ONLY"
    with engine.connect() as connection:
        assert connection.execute(
            text(f"SELECT COUNT(*) FROM {EVENT_REVISION_TABLE}")
        ).scalar_one() == 3


def test_missing_revision_schema_fails_closed_without_legacy_fallback():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    batch = load_finance_facts(
        engine,
        codes=["000001"],
        decision_at="2026-08-20 15:00:00",
        as_of_date="2026-08-20",
    )
    assert batch.status_for("000001") != PIT_AVAILABLE
    assert "SCHEMA_UNAVAILABLE" in batch.reason_for("000001")


def test_common_fact_cutoff_requires_complete_fresh_receipts_for_every_code():
    engine = _engine()
    for code, observed in (
        ("000001", "2026-08-05 22:30:00"),
        ("000002", "2026-08-05 22:31:00"),
    ):
        for kind, start in (
            ("finance", "1900-01-01"),
            ("event", "2026-07-22"),
        ):
            append_source_coverage(
                engine,
                fact_kind=kind,
                stock_code=code,
                window_start=start,
                window_end="2026-08-05",
                known_at=observed,
                covered_through_at=observed,
                watermark_kind="CAPTURED_AT",
                watermark_evidence={"source_call": "success"},
                source_rows=[],
                fact_bindings=[],
                source=f"test.{kind}",
                batch_id=f"batch-{kind}-{code}",
            )
    resolved = resolve_common_fact_cutoff(
        engine,
        codes=["000001", "000002"],
        decision_at="2026-08-05 22:35:00",
        finance_start_date="1900-01-01",
        finance_end_date="2026-08-05",
        event_start_date="2026-07-22",
        event_end_date="2026-08-05",
    )
    assert resolved["status"] == PIT_AVAILABLE
    assert resolved["fact_cutoff_at"].replace("T", " ") == (
        "2026-08-05 22:30:00.000000"
    )
    assert len(resolved["receipts"]) == 4
    assert len(resolved["receipt_root_hash"]) == 64

    missing = resolve_common_fact_cutoff(
        engine,
        codes=["000001", "000002", "000003"],
        decision_at="2026-08-05 22:35:00",
        finance_start_date="1900-01-01",
        finance_end_date="2026-08-05",
        event_start_date="2026-07-22",
        event_end_date="2026-08-05",
    )
    assert missing["status"] == PIT_DATA_BLOCKED
    assert "000003" in missing["reason"]

    stale = resolve_common_fact_cutoff(
        engine,
        codes=["000001", "000002"],
        decision_at="2026-08-05 23:01:01",
        finance_start_date="1900-01-01",
        finance_end_date="2026-08-05",
        event_start_date="2026-07-22",
        event_end_date="2026-08-05",
    )
    assert stale["status"] == PIT_DATA_BLOCKED
    assert stale["reason"] == "PIT_COMMON_CUTOFF_STALE_OR_BACKFILL"


def _sealed_common_cutoff_case():
    engine = _engine()
    _install_finance_test_catalog(engine)
    _install_prior_finance_coverage(engine)
    append_finance_atomic_batch_seal(
        engine,
        as_of_date="2026-08-30",
        completed_known_at="2026-08-30 01:10:00",
    )
    for code in ("000001", "002731"):
        for known in ("2026-08-30 01:09:00", "2026-08-30 01:10:00"):
            append_source_coverage(
                engine,
                fact_kind="event",
                stock_code=code,
                window_start="2026-08-10",
                window_end="2026-08-30",
                known_at=known,
                covered_through_at=known,
                watermark_kind="CAPTURED_AT",
                watermark_evidence={"source_call": "success"},
                source_rows=[],
                fact_bindings=[],
                source="test.event",
                batch_id=f"event-{code}-{known}",
            )
    return engine, {
        "codes": ["000001", "002731"],
        "decision_at": "2026-08-30 01:11:00",
        "finance_start_date": "1900-01-01",
        "finance_end_date": "2026-08-30",
        "event_start_date": "2026-08-10",
        "event_end_date": "2026-08-30",
    }


def test_sealed_common_cutoff_avoids_finance_rescan_with_identical_receipts():
    engine, kwargs = _sealed_common_cutoff_case()
    statements = []

    def record(_connection, _cursor, statement, _parameters, _context, _many):
        statements.append(statement)

    event.listen(engine, "before_cursor_execute", record)
    resolved = resolve_common_fact_cutoff(engine, **kwargs)
    event.remove(engine, "before_cursor_execute", record)
    assert resolved["status"] == PIT_AVAILABLE
    final_scans = [
        sql for sql in statements
        if "ORDER BY fact_kind, stock_code, scope_hash, revision_no" in sql
    ]
    assert len(final_scans) == 1
    assert "WHERE fact_kind='event' AND stock_code IN" in final_scans[0]
    # Finance member queries remain part of the real seal validation.
    assert any("WHERE fact_kind='finance' AND stock_code IN" in sql for sql in statements)
    assert {row["fact_kind"] for row in resolved["receipts"]} == {"finance", "event"}

    def restore_legacy_scan(_connection, _cursor, statement, parameters, _context, _many):
        if "ORDER BY fact_kind, stock_code, scope_hash, revision_no" in statement:
            statement = statement.replace(
                "WHERE fact_kind='event' AND",
                "WHERE fact_kind IN ('finance','event') AND",
            )
        return statement, parameters

    event.listen(engine, "before_cursor_execute", restore_legacy_scan, retval=True)
    legacy = resolve_common_fact_cutoff(engine, **kwargs)
    event.remove(engine, "before_cursor_execute", restore_legacy_scan)
    assert resolved == legacy
    engine.dispose()


@pytest.mark.parametrize("corrupted_kind", ("finance", "event"))
def test_sealed_common_cutoff_still_rejects_corrupted_evidence(corrupted_kind):
    engine, kwargs = _sealed_common_cutoff_case()
    # Simulate damaged persisted evidence in this isolated SQLite fixture.
    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER trg_pit_source_coverage_immutable_bu"))
        connection.execute(
            text(
                f"UPDATE {SOURCE_COVERAGE_TABLE} SET result_count=result_count+1 "
                "WHERE fact_kind=:kind AND stock_code='000001' AND revision_no=1"
            ),
            {"kind": corrupted_kind},
        )
    resolved = resolve_common_fact_cutoff(engine, **kwargs)
    assert resolved["status"] == PIT_DATA_BLOCKED
    if corrupted_kind == "finance":
        assert resolved["reason"] == "PIT_FINANCE_ATOMIC_BATCH_INVALID:ValueError"
    else:
        assert resolved["reason"] == "PIT_COMMON_CUTOFF_INCOMPLETE:event:000001:BAD_CHAIN"
    assert resolved["fact_cutoff_at"] == ""
    engine.dispose()
