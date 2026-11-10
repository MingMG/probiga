from __future__ import annotations

from datetime import date, datetime, timedelta
import json
import hashlib
import zlib
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.dialects import mysql
from sqlalchemy.exc import DatabaseError, OperationalError
from sqlalchemy.pool import QueuePool

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
    append_event_revisions,
    append_finance_atomic_batch_seal,
    append_finance_expected_unavailable,
    append_finance_revision,
    append_finance_revisions,
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


def _finance_data_exclusion(code="002731", observed="2026-08-30 01:20:00"):
    return pit_module.build_finance_data_exclusion(
        stock_code=code, target_date="2026-08-30", observed_at=observed,
        source="eastmoney.finance.mainfinadata.direct",
        reason_code="FINANCE_FETCH_FAILED", error_detail="Timeout: source unavailable",
    )


def test_finance_exclusion_preserves_catalog_and_blocks_old_facts():
    engine = _engine()
    _install_finance_test_catalog(engine)
    _install_prior_finance_coverage(engine)
    before = engine.connect()
    count_before = before.execute(text(f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE}")).scalar()
    before.close()
    sealed = append_finance_atomic_batch_seal(
        engine, as_of_date="2026-08-30", completed_known_at="2026-08-30 01:30:00",
        data_exclusions=[_finance_data_exclusion()],
    )
    assert sealed["eligible_code_count"] == 2
    assert sealed["available_code_count"] == 1
    assert sealed["data_excluded_codes"] == ["002731"]
    with engine.connect() as connection:
        assert connection.execute(text(f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE}")).scalar() == count_before + 1
        assert connection.execute(text(f"SELECT COUNT(*) FROM {SOURCE_COVERAGE_TABLE} WHERE coverage_status='DATA_EXCLUDED'")).scalar() == 0
    loaded = load_finance_atomic_batch_seal(
        engine, codes=["000001", "002731"], decision_at="2026-08-30 01:31:00",
        as_of_date="2026-08-30",
    )
    assert set(loaded["members"]) == {"000001", "002731"}
    assert loaded["members"]["002731"]["coverage_status"] == "DATA_EXCLUDED"
    for reader in (load_finance_facts, load_finance_history_facts):
        result = reader(
            engine, codes=["000001", "002731"], decision_at="2026-08-30 01:31:00",
            as_of_date="2026-08-30",
        )
        assert result.status_for("000001") == PIT_AVAILABLE
        assert result.status_for("002731") == PIT_DATA_BLOCKED
        assert result.reason_for("002731") == "PIT_FINANCE_DATA_EXCLUDED:FINANCE_FETCH_FAILED"
        assert not result.facts.get("002731")
        assert result.coverage_by_code["002731"]["batch_root_sha256"] == sealed["batch_root_sha256"]
        earlier = reader(
            engine, codes=["002731"], decision_at="2026-08-30 01:15:00",
            as_of_date="2026-08-30",
        )
        assert "DATA_EXCLUDED" not in earlier.reason_for("002731")


@pytest.mark.parametrize("case", ["outside", "duplicate", "future", "tampered"])
def test_finance_exclusion_rejects_unbound_dispositions(case):
    engine = _engine()
    _install_finance_test_catalog(engine)
    _install_prior_finance_coverage(engine)
    exclusions = [_finance_data_exclusion()]
    if case == "outside":
        exclusions = [_finance_data_exclusion("999999")]
    elif case == "duplicate":
        exclusions *= 2
    elif case == "future":
        exclusions = [_finance_data_exclusion(observed="2026-08-30 02:00:00")]
    else:
        exclusions[0]["reason_code"] = "FINANCE_RESPONSE_INVALID"
    with pytest.raises(ValueError, match="exclusion"):
        append_finance_atomic_batch_seal(
            engine, as_of_date="2026-08-30", completed_known_at="2026-08-30 01:30:00",
            data_exclusions=exclusions,
        )


def test_finance_missing_member_still_requires_explicit_disposition():
    engine = _engine()
    _install_finance_test_catalog(engine)
    with pytest.raises(ValueError, match="no valid disposition"):
        append_finance_atomic_batch_seal(
            engine, as_of_date="2026-08-30", completed_known_at="2026-08-30 01:30:00",
            data_exclusions=[_finance_data_exclusion()],
        )


def test_finance_all_excluded_seal_keeps_zero_available_explicit():
    engine = _engine()
    _install_finance_test_catalog(engine)
    append_finance_atomic_batch_seal(
        engine, as_of_date="2026-08-30", completed_known_at="2026-08-30 01:30:00",
        data_exclusions=[_finance_data_exclusion(code) for code in ("000001", "002731")],
    )
    loaded = load_finance_atomic_batch_seal(
        engine, codes=["000001", "002731"], decision_at="2026-08-30 01:31:00",
        as_of_date="2026-08-30",
    )
    assert loaded["eligible_code_count"] == loaded["data_excluded_count"] == 2
    assert loaded["available_code_count"] == 0


def test_finance_exact_seal_binding_survives_a_later_batch_and_propagates_exclusions():
    engine, kwargs = _sealed_common_cutoff_case()
    first = pit_module.load_latest_finance_atomic_batch_baseline(
        engine, decision_at="2026-08-30 01:15:00",
    )
    latest = append_finance_atomic_batch_seal(
        engine, as_of_date="2026-08-30", completed_known_at="2026-08-30 01:30:00",
        changed_codes=[], data_exclusions=[_finance_data_exclusion()],
    )
    original = load_finance_atomic_batch_seal(
        engine, codes=["000001", "002731"], decision_at="2026-08-30 01:31:00",
        as_of_date="2026-08-30", seal_coverage_id=first["seal_coverage_id"],
    )
    assert original["seal_coverage_id"] == first["seal_coverage_id"]
    assert original["data_excluded_count"] == 0
    resolved = resolve_common_fact_cutoff(
        engine, **{**kwargs, "decision_at": "2026-08-30 01:31:00"},
    )
    assert resolved["status"] == PIT_AVAILABLE
    assert resolved["data_excluded_codes"] == ["002731"]
    assert resolved["finance_atomic_batch"]["seal_coverage_id"] == latest["seal_coverage_id"]
    excluded_receipt = next(item for item in resolved["receipts"]
                            if item["stock_code"] == "002731" and item["fact_kind"] == "finance")
    assert excluded_receipt["coverage_status"] == "DATA_EXCLUDED"
    assert excluded_receipt["reason_code"] == "FINANCE_FETCH_FAILED"


@pytest.mark.parametrize("changed_codes", [None, []])
def test_finance_excluded_member_requires_fresh_recovery(changed_codes):
    engine = _engine()
    _install_finance_test_catalog(engine)
    _install_prior_finance_coverage(engine)
    append_finance_atomic_batch_seal(
        engine, as_of_date="2026-08-30", completed_known_at="2026-08-30 01:30:00",
        data_exclusions=[_finance_data_exclusion()],
    )
    with pytest.raises(ValueError, match="fresh source observation"):
        append_finance_atomic_batch_seal(
            engine, as_of_date="2026-08-30", completed_known_at="2026-08-30 01:40:00",
            changed_codes=changed_codes,
        )
    append_finance_expected_unavailable(
        engine, stock_code="002731", expected_report_date="2026-03-31",
        known_at="2026-08-30 01:41:00", official_evidence=_nonfiling_evidence(),
        batch_id="fresh-after-failed-capture",
    )
    restored = append_finance_atomic_batch_seal(
        engine, as_of_date="2026-08-30", completed_known_at="2026-08-30 01:42:00",
        changed_codes=changed_codes,
    )
    assert restored["data_excluded_count"] == 0
    assert restored["available_code_count"] == 2


def test_finance_delta_reuses_validated_exclusions_without_rereading_parent_history():
    engine = _engine()
    _install_finance_test_catalog(engine)
    _install_prior_finance_coverage(engine)
    append_finance_atomic_batch_seal(
        engine, as_of_date="2026-08-30", completed_known_at="2026-08-30 01:30:00",
        data_exclusions=[_finance_data_exclusion()],
    )
    baseline = pit_module.load_latest_finance_atomic_batch_baseline(
        engine, decision_at="2026-08-30 01:40:00")
    for target in ("2026-08-30", "2026-08-31"):
        original = pit_module.load_finance_data_exclusions(engine,
            codes=["000001", "002731"], decision_at="2026-08-30 01:40:00",
            as_of_date=target)
        assert pit_module._finance_data_exclusions_from_baseline(baseline,
            codes=["000001", "002731"], target=date.fromisoformat(target)) == original
        assert bool(original) == (target == "2026-08-30")

    history_reads = []

    def record(_conn, _cursor, statement, _parameters, _context, _many):
        if f"SELECT * FROM {SOURCE_COVERAGE_TABLE} WHERE coverage_id=" in statement:
            history_reads.append(statement)

    event.listen(engine, "before_cursor_execute", record)
    try:
        with pytest.raises(ValueError, match="fresh source observation"):
            append_finance_atomic_batch_seal(engine, as_of_date="2026-08-30",
                completed_known_at="2026-08-30 01:40:00", changed_codes=[])
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert len(history_reads) == 1
    engine.dispose()


def _nonfiling_evidence(report_date="2026-03-31", *, stock_code="002731") -> dict:
    from server.common.finance_nonfiling_evidence import EVIDENCE_SCHEMA, nonfiling_statement
    period = "第一季度" if report_date == "2026-03-31" else "半年度"
    body = f"证券代码：{stock_code}。因公司未在法定期限内披露2026年{period}报告，股票继续停牌。"
    return {
        "evidence_schema": EVIDENCE_SCHEMA,
        "announcement_document_text": body,
        "announcement_document_text_sha256": hashlib.sha256(body.encode()).hexdigest(),
        "nonfiling_statement": nonfiling_statement(body, stock_code, report_date),
        "catalog_identity": {
            "schema": "probiga.cninfo-nonfiling-catalogue.v1",
            "stock_code": stock_code, "org_id": "9900022974",
            "window_start": "2026-08-01", "window_end": "2026-09-06",
            "page_count": 1, "row_count": 1, "complete": True,
        },
        "source": "cninfo.finance.nonfiling",
        "reason_code": "CNINFO_REGULATORY_PERIODIC_REPORT_NOT_FILED",
        "stock_code": stock_code,
        "expected_report_date": report_date,
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


def test_existing_nonfiling_v1_keeps_immutable_hash_and_original_audit_interval():
    fixture = json.loads((Path(__file__).parent / "fixtures/finance_nonfiling_legacy_coverage.json").read_text(encoding="utf-8"))
    row = fixture["row"]
    original = json.dumps(row, sort_keys=True)
    pit_module._validate_coverage_chain([row])
    assert json.dumps(row, sort_keys=True) == original
    engine = _engine()
    with engine.begin() as connection:
        connection.execute(text(
            f"INSERT INTO {SOURCE_COVERAGE_TABLE} ({','.join(row)}) VALUES "
            f"({','.join(':'+key for key in row)})"
        ), row)
    available, invalid = load_finance_expected_unavailable(
        engine, codes=["002731"], expected_report_date="2026-03-31",
        decision_at="2026-08-30 10:00:00",
    )
    assert invalid == {}
    assert available["002731"]["coverage_id"] == row["coverage_id"]
    expired, invalid = load_finance_expected_unavailable(
        engine, codes=["002731"], expected_report_date="2026-03-31",
        decision_at="2026-09-12 10:00:00",
    )
    assert expired == invalid == {}
    old_evidence = json.loads(row["payload_json"])["official_evidence"]
    with pytest.raises(ValueError, match="body evidence version"):
        append_finance_expected_unavailable(
            engine, stock_code="002731", expected_report_date="2026-03-31",
            known_at="2026-08-30 11:00:00", official_evidence=old_evidence,
            batch_id="new-append-cannot-reuse-v1",
        )


@pytest.mark.parametrize("mutation", ["body", "period", "catalogue", "publication", "version"])
def test_new_nonfiling_append_rejects_evidence_drift(mutation):
    evidence = _nonfiling_evidence()
    if mutation == "body":
        evidence["announcement_document_text"] += "篡改"
    elif mutation == "period":
        evidence["expected_report_date"] = "2026-06-30"
    elif mutation == "catalogue":
        evidence["catalog_identity"]["complete"] = False
    elif mutation == "publication":
        evidence["announcement_url"] = evidence["announcement_url"].replace("2026-08-25", "2026-08-24")
    else:
        evidence["evidence_schema"] = "unknown"
    with pytest.raises(ValueError):
        append_finance_expected_unavailable(
            _engine(), stock_code="002731", expected_report_date="2026-03-31",
            known_at="2026-08-30 09:00:00", official_evidence=evidence,
            batch_id="invalid-body-evidence",
        )


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
        **_nonfiling_evidence("2026-06-30"),
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
    row_content_hash: str = "9" * 64,
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
            "row_content_sha256": row_content_hash,
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


@pytest.mark.parametrize("problem", [None, "sequence", "parent", "clock", "selected_payload"])
def test_finance_discovery_reads_current_payload_without_reloading_historical_responses(problem):
    engine = _engine()
    receipts = []
    for minute in (4, 5):
        receipts.append(append_source_coverage(
            engine, fact_kind="finance", stock_code=pit_module.FINANCE_INCREMENTAL_DISCOVERY_CODE,
            window_start="2026-08-31", window_end="2026-08-31",
            known_at=f"2026-08-31 01:0{minute}:00",
            covered_through_at=f"2026-08-31 01:0{minute}:00",
            watermark_kind="CAPTURED_AT", watermark_evidence=_finance_discovery_evidence(changed=False),
            source_rows=[], fact_bindings=[], source=pit_module.FINANCE_INCREMENTAL_DISCOVERY_SOURCE,
            batch_id=f"finance-discovery-{minute}",
        ))
    selected_id = receipts[-1].coverage_id
    with engine.begin() as c:
        # Make old payload reads actually fail at the database boundary. A
        # SELECT of link columns remains valid; only this receipt may read JSON.
        physical = SOURCE_COVERAGE_TABLE + "_physical"
        columns = [r[1] for r in c.exec_driver_sql(f"PRAGMA table_info({SOURCE_COVERAGE_TABLE})")]
        c.exec_driver_sql(f"ALTER TABLE {SOURCE_COVERAGE_TABLE} RENAME TO {physical}")
        def current_payload_only(coverage_id, payload):
            if coverage_id != selected_id:
                raise ValueError("historical source response must not be reloaded")
            return "{}" if problem == "selected_payload" else payload
        c.connection.driver_connection.create_function("current_payload_only", 2, current_payload_only)
        overrides = {"payload_json": "current_payload_only(coverage_id,payload_json)"}
        if problem == "sequence":
            overrides["revision_no"] = f"CASE WHEN coverage_id='{selected_id}' THEN 3 ELSE revision_no END"
        elif problem == "parent":
            overrides["supersedes_coverage_id"] = f"CASE WHEN coverage_id='{selected_id}' THEN '{'0' * 64}' ELSE supersedes_coverage_id END"
        elif problem == "clock":
            overrides["received_at"] = f"CASE WHEN coverage_id='{receipts[0].coverage_id}' THEN '2026-08-31 01:09:00' ELSE received_at END"
        projected = [overrides[name] + " AS " + name if name in overrides else name for name in columns]
        c.exec_driver_sql(f"CREATE VIEW {SOURCE_COVERAGE_TABLE} AS SELECT {','.join(projected)} FROM {physical}")
    kwargs = dict(coverage_id=selected_id, codes=["000001", "002731"],
                  decision_at="2026-08-31 01:10:00", as_of_date="2026-08-31")
    if problem:
        with pytest.raises(ValueError):
            pit_module.load_finance_incremental_discovery(engine, **kwargs)
    else:
        result = pit_module.load_finance_incremental_discovery(engine, **kwargs)
        assert result["coverage_id"] == selected_id
        assert result["changed_codes"] == []
    engine.dispose()


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


@pytest.mark.parametrize("source_content_changed", [False, True])
def test_finance_same_day_reobservation_reuses_already_incorporated_source_contents(source_content_changed):
    engine = _engine()
    _install_finance_test_catalog(engine)
    _install_prior_finance_coverage(engine)
    with engine.connect() as c:
        original = json.loads(c.execute(text(f"SELECT payload_json FROM {SOURCE_COVERAGE_TABLE} "
                                            "WHERE stock_code='000001'")).scalar_one())
    append_source_coverage(
        engine, fact_kind="finance", stock_code="000001", source="adata.finance.core_index",
        window_start="1900-01-01", window_end="2026-08-31", known_at="2026-08-31 01:05:00",
        covered_through_at="2026-08-31 01:05:00", watermark_kind="CAPTURED_AT",
        watermark_evidence={"source_call": "success"}, source_rows=original["source_rows"],
        fact_bindings=original["fact_bindings"], batch_id="finance-current-day-primary",
    )
    receipts = []
    for index, known in enumerate(("2026-08-31 01:06:00", "2026-08-31 01:12:00")):
        discovery = append_source_coverage(
            engine, fact_kind="finance", stock_code=pit_module.FINANCE_INCREMENTAL_DISCOVERY_CODE,
            source=pit_module.FINANCE_INCREMENTAL_DISCOVERY_SOURCE,
            window_start="2026-08-31", window_end="2026-08-31", known_at=known,
            covered_through_at=known, watermark_kind="CAPTURED_AT",
            watermark_evidence=_finance_discovery_evidence(
                changed=True, row_content_hash="8" * 64 if index and source_content_changed else "9" * 64),
            source_rows=[], fact_bindings=[], batch_id=f"finance-discovery-reobservation-{index}",
        )
        receipts.append(append_finance_atomic_batch_seal(
            engine, as_of_date="2026-08-31", completed_known_at=f"2026-08-31 01:{10 + index * 10:02d}:00",
            incremental_discovery_coverage_id=discovery.coverage_id,
            changed_codes=[] if index else None,
        ))
    # The unavailable issuer is still due for its scheduled retry; only the
    # already-incorporated available stock can avoid a duplicate recheck.
    assert receipts[1]["changed_code_count"] == 1 + int(source_content_changed)
    assert receipts[1]["parent_seal_coverage_id"] == receipts[0]["seal_coverage_id"]
    loaded = load_finance_atomic_batch_seal(engine, codes=["000001", "002731"],
        decision_at="2026-08-31 01:21:00", as_of_date="2026-08-31")
    assert loaded["eligible_code_count"] == 2
    assert loaded["members"]["000001"]["coverage_status"] == "COMPLETE"
    engine.dispose()


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
    receipt = load_finance_atomic_batch_seal(
        engine,
        codes=["000001", "002731"],
        decision_at="2026-08-30 01:31:00",
        as_of_date="2026-08-30",
        seal_coverage_id=delta["seal_coverage_id"],
        verify_source_rows=False,
    )
    assert calls == [["000001"], ["000001"]]
    assert receipt["members"] == loaded["members"]
    assert receipt["batch_root_sha256"] == loaded["batch_root_sha256"]
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


def test_bulk_finance_matches_single_writer_and_preserves_duplicate_identity_chain():
    source = {
        "stock_code": "600000", "report_date": "2026-06-30",
        "notice_date": "2026-08-18 20:00:00", "basic_eps": 0.8,
    }
    other_report = {**source, "report_date": "2026-03-31"}
    rows = [
        source, other_report, dict(source),
        {**source, "basic_eps": 0.9}, source, dict(source),
        {**source, "published_at": "2026-08-18 20:30:00"},
    ]
    single_engine = _engine()
    bulk_engine = _engine()
    arguments = {
        "known_at": "2026-08-19 08:00:00",
        "received_at": "2026-08-19 07:59:59",
        "source": "eastmoney.finance.mainfinadata.direct",
        "batch_id": "finance-first-batch",
    }
    expected = [
        append_finance_revision(single_engine, row, **arguments) for row in rows
    ]
    actual = append_finance_revisions(bulk_engine, iter(rows), **arguments)
    assert actual == expected
    assert [receipt.revision_no for receipt in actual] == [1, 1, 1, 2, 3, 3, 4]
    assert [receipt.idempotent for receipt in actual] == [
        False, False, True, False, False, True, False,
    ]
    for engine in (single_engine, bulk_engine):
        with engine.connect() as connection:
            stored = connection.execute(text(
                f"SELECT * FROM {FINANCE_REVISION_TABLE} "
                "ORDER BY identity_hash, revision_no"
            )).mappings().all()
        if engine is single_engine:
            expected_rows = [dict(row) for row in stored]
        else:
            assert [dict(row) for row in stored] == expected_rows


def test_bulk_finance_reuses_immutable_rows_across_capture_times_and_batches():
    engine = _engine()
    source = {
        "stock_code": "600000", "report_date": "2026-06-30",
        "notice_date": "2026-08-18 20:00:00", "basic_eps": 0.8,
    }
    first = append_finance_revisions(
        engine, [source], known_at="2026-08-19 08:00:00", batch_id="first",
    )[0]
    repeated = append_finance_revisions(
        engine, [{**source, "etl_sync_at": "2026-08-20 09:00:00"}],
        known_at="2026-08-20 09:00:00", batch_id="second",
    )[0]
    assert repeated.idempotent is True
    assert repeated.revision_id == first.revision_id
    with engine.connect() as connection:
        stored = connection.execute(text(
            f"SELECT batch_id, known_at FROM {FINANCE_REVISION_TABLE}"
        )).mappings().all()
    assert len(stored) == 1
    assert stored[0]["batch_id"] == "first"
    assert str(stored[0]["known_at"]).startswith("2026-08-19 08:00:00")


def test_bulk_finance_reads_metadata_in_bounded_batches_instead_of_per_report():
    engine = _engine()
    rows = [{
        "stock_code": "600000",
        "report_date": (date(2020, 1, 1) + timedelta(days=index)).isoformat(),
        "notice_date": "2026-08-18 20:00:00", "basic_eps": index / 10,
    } for index in range(207)]
    original = append_finance_revisions(
        engine, rows, known_at="2026-08-19 08:00:00", batch_id="first",
    )
    statements = []

    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append((statement, parameters))

    event.listen(engine, "before_cursor_execute", record)
    try:
        repeated = append_finance_revisions(
            engine, rows, known_at="2026-08-20 09:00:00", batch_id="second",
        )
    finally:
        event.remove(engine, "before_cursor_execute", record)
    assert all(receipt.idempotent for receipt in repeated)
    assert [item.revision_id for item in repeated] == [
        item.revision_id for item in original
    ]
    assert len(statements) == 3 < len(rows)
    assert [len(parameters) for _, parameters in statements] == [100, 100, 7]
    for statement, _ in statements:
        assert "WHERE identity_hash IN" in statement
        assert "ORDER BY identity_hash, revision_no DESC" in statement
        assert "payload_json" not in statement
        assert "FOR UPDATE" not in statement


def test_bulk_finance_write_failure_rolls_back_entire_owned_transaction():
    engine = _engine()
    source = {
        "stock_code": "600000", "report_date": "2026-06-30",
        "notice_date": "2026-08-18 20:00:00", "basic_eps": 0.8,
    }
    original = append_finance_revision(
        engine, source, known_at="2026-08-19 08:00:00",
    )
    writes = []

    def fail_second_insert(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith(f"INSERT INTO {FINANCE_REVISION_TABLE}"):
            writes.append(statement)
            if len(writes) == 2:
                raise OperationalError(statement, parameters, RuntimeError("write failed"))

    event.listen(engine, "before_cursor_execute", fail_second_insert)
    try:
        with pytest.raises(OperationalError, match="write failed"):
            append_finance_revisions(
                engine,
                [{**source, "basic_eps": 0.9}, {**source, "report_date": "2026-03-31"}],
                known_at="2026-08-20 09:00:00",
            )
    finally:
        event.remove(engine, "before_cursor_execute", fail_second_insert)
    assert len(writes) == 2
    with engine.connect() as connection:
        assert connection.execute(text(
            f"SELECT revision_id FROM {FINANCE_REVISION_TABLE}"
        )).scalars().all() == [original.revision_id]


def test_bulk_finance_keeps_caller_transaction_and_rolls_back_with_it():
    engine = _engine()
    source = {
        "stock_code": "600000", "report_date": "2026-06-30",
        "notice_date": "2026-08-18 20:00:00", "basic_eps": 0.8,
    }
    with pytest.raises(RuntimeError, match="caller failed"):
        with engine.begin() as connection:
            receipts = append_finance_revisions(
                connection, [source], known_at="2026-08-19 08:00:00",
            )
            assert len(receipts) == 1
            assert connection.in_transaction()
            raise RuntimeError("caller failed")
    with engine.connect() as connection:
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {FINANCE_REVISION_TABLE}"
        )).scalar_one() == 0


def test_bulk_finance_duplicate_insert_receipt_keeps_following_revision_chain():
    engine = _engine()
    source = {
        "stock_code": "600000", "report_date": "2026-06-30",
        "notice_date": "2026-08-18 20:00:00", "basic_eps": 0.8,
    }
    inserted = []

    def insert_before_original(conn, cursor, statement, parameters, context, executemany):
        if not inserted and statement.startswith(f"INSERT INTO {FINANCE_REVISION_TABLE}"):
            # Materialize the same revision before the original INSERT reaches
            # SQLite, exercising the actual unique violation and receipt lookup.
            inserted.append(statement)
            cursor.execute(statement, parameters)

    event.listen(engine, "before_cursor_execute", insert_before_original)
    try:
        receipts = append_finance_revisions(
            engine, [source, source, {**source, "basic_eps": 0.9}],
            known_at="2026-08-19 08:00:00",
        )
    finally:
        event.remove(engine, "before_cursor_execute", insert_before_original)
    assert len(inserted) == 1
    assert [item.idempotent for item in receipts] == [True, True, False]
    assert receipts[0].revision_id == receipts[1].revision_id
    assert receipts[2].revision_no == 2
    assert receipts[2].supersedes_revision_id == receipts[0].revision_id
    with engine.connect() as connection:
        assert connection.execute(text(
            f"SELECT COUNT(*) FROM {FINANCE_REVISION_TABLE}"
        )).scalar_one() == 2


@pytest.mark.parametrize("receipt_stage", ["fingerprint_lookup", "insert_conflict"])
def test_bulk_finance_older_fingerprint_receipt_does_not_replace_locked_head(
    monkeypatch, receipt_stage,
):
    source = {
        "stock_code": "600000", "report_date": "2026-06-30",
        "notice_date": "2026-08-18 20:00:00", "basic_eps": 0.8,
    }
    original_lookup = pit_module._existing_fingerprint
    results = []
    for bulk in (False, True):
        engine = _engine()
        first = append_finance_revision(
            engine, source, known_at="2026-08-19 08:00:00",
        )
        head = append_finance_revision(
            engine, {**source, "basic_eps": 0.9},
            known_at="2026-08-19 09:00:00",
        )
        with engine.connect() as connection:
            older = dict(connection.execute(text(
                f"SELECT * FROM {FINANCE_REVISION_TABLE} "
                "WHERE revision_id=:revision_id"
            ), {"revision_id": first.revision_id}).mappings().one())
        lookups = []

        def older_receipt(connection, table_name, identity_hash, fingerprint):
            lookups.append(fingerprint)
            if receipt_stage == "fingerprint_lookup" and len(lookups) == 1:
                return older
            if receipt_stage == "insert_conflict":
                if len(lookups) == 1:
                    return None
                if len(lookups) == 2:
                    return older
            return original_lookup(connection, table_name, identity_hash, fingerprint)

        conflicts = []

        def duplicate_existing_row(conn, cursor, statement, parameters, context, executemany):
            if (
                receipt_stage == "insert_conflict"
                and not conflicts
                and statement.startswith(f"INSERT INTO {FINANCE_REVISION_TABLE}")
            ):
                conflicts.append(statement)
                # Trigger a real unique-key failure without adding another row.
                return (
                    f"INSERT INTO {FINANCE_REVISION_TABLE} "
                    f"SELECT * FROM {FINANCE_REVISION_TABLE} "
                    f"WHERE revision_id='{head.revision_id}'", (),
                )
            return statement, parameters

        arguments = {"known_at": "2026-08-20 09:00:00", "batch_id": "later"}
        rows = [source, {**source, "basic_eps": 1.0}]
        event.listen(engine, "before_cursor_execute", duplicate_existing_row, retval=True)
        try:
            with monkeypatch.context() as scoped:
                scoped.setattr(pit_module, "_existing_fingerprint", older_receipt)
                receipts = (
                    append_finance_revisions(engine, rows, **arguments)
                    if bulk else [
                        append_finance_revision(engine, row, **arguments) for row in rows
                    ]
                )
        finally:
            event.remove(engine, "before_cursor_execute", duplicate_existing_row)
        assert receipts[0].revision_id == first.revision_id
        assert receipts[0].idempotent is True
        assert receipts[1].revision_no == 3
        assert receipts[1].supersedes_revision_id == head.revision_id
        assert len(conflicts) == (receipt_stage == "insert_conflict")
        with engine.connect() as connection:
            assert connection.execute(text(
                f"SELECT COUNT(*) FROM {FINANCE_REVISION_TABLE}"
            )).scalar_one() == 3
        results.append(receipts)
    assert results[0] == results[1]


def test_bulk_finance_mysql_read_locks_identity_ranges_including_absent_keys():
    existing_identity = "a" * 64
    absent_identity = "b" * 64
    statements = []

    def execute(statement, parameters):
        compiled = statement.params(**parameters).compile(
            dialect=mysql.dialect(), compile_kwargs={"render_postcompile": True},
        )
        statements.append((str(compiled), parameters))
        # The SQL order returns the newest metadata first for each identity.
        return SimpleNamespace(mappings=lambda: iter([
            {"identity_hash": existing_identity, "revision_id": "2" * 64},
            {"identity_hash": existing_identity, "revision_id": "1" * 64},
        ]))

    connection = SimpleNamespace(dialect=mysql.dialect(), execute=execute)
    latest = pit_module._latest_finance_revisions(
        connection, [absent_identity, existing_identity, existing_identity],
    )
    assert latest[existing_identity]["revision_id"] == "2" * 64
    assert latest[absent_identity] is None
    statement, parameters = statements[0]
    assert parameters["identity_hashes"] == [existing_identity, absent_identity]
    assert "FORCE INDEX (uk_pit_finance_identity_revision)" in statement
    assert "WHERE identity_hash IN (%s, %s)" in statement
    assert statement.endswith("ORDER BY identity_hash, revision_no DESC FOR UPDATE")
    assert not any(token in statement for token in (
        "payload_json", "GROUP BY", "JOIN", "LIMIT", "MAX(",
    ))


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


@pytest.mark.parametrize(
    ("known_at", "decision_at", "expected_status"),
    [
        ("2026-08-25 18:25:00", "2026-08-25 22:20:00", PIT_AVAILABLE),
        ("2026-08-25 18:25:00", "2026-08-25 18:24:00", PIT_NO_ROWS),
        ("2026-08-25 18:51:00", "2026-08-25 22:20:00", PIT_DATA_BLOCKED),
    ],
)
def test_date_only_announcement_uses_capture_delay_and_actual_reader_knowledge(
    known_at, decision_at, expected_status,
):
    engine = _engine()
    append_event_revision(
        engine,
        {
            "stock_code": "000001",
            "art_code": "EVENING-ANNOUNCEMENT",
            "notice_date": "2026-08-25",
            "display_time": "2026-08-25",
            "title": "收盘公告",
        },
        known_at=known_at,
    )
    batch = load_event_facts(
        engine,
        codes=["000001"],
        fact_cutoff_at="2026-08-25 18:20:00",
        decision_at=decision_at,
        start_date="2026-08-11",
        end_date="2026-08-25",
    )
    assert batch.status_for("000001") == expected_status
    if expected_status == PIT_AVAILABLE:
        assert batch.facts["000001"][0]["event_publication_precision"] == "DATE_ONLY"
    elif expected_status == PIT_DATA_BLOCKED:
        assert "LIVE_CAPTURE_WINDOW_EXCEEDED" in batch.reason_for("000001")


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


def test_event_bulk_writer_matches_single_rows_and_bounds_round_trips():
    engine = _engine()
    single_engine = _engine()
    reads = []
    inserts = []

    @event.listens_for(engine, "before_cursor_execute")
    def observe(connection, cursor, statement, parameters, context, executemany):
        if EVENT_REVISION_TABLE in statement:
            if statement.startswith("SELECT"):
                reads.append(statement)
            if statement.startswith("INSERT"):
                inserts.append(len(parameters) if executemany else 1)

    rows = [
        {
            "stock_code": str(600000 + index // 5),
            "event_key": f"CNINFO-{index % 5}",
            "published_at": None if index % 5 == 0 else "2026-08-20 09:30:00",
            "source_published_text": "2026-08-20",
            "title": f"公司公告 {index}",
        }
        for index in range(204, -1, -1)
    ]
    source = {"source": "cninfo.announcement", "batch_id": "source-batch"}
    first_at = "2026-08-21 09:00:00"
    later_at = "2026-08-22 09:00:00"
    with engine.begin() as connection:
        first = append_event_revisions(connection, rows, known_at=first_at, **source)
    with single_engine.begin() as connection:
        expected = [
            append_event_revision(connection, row, known_at=first_at, **source)
            for row in rows
        ]
    assert first == expected
    assert first[4].publication_time_status == TIME_UNVERIFIED
    assert len(reads) == 6
    assert inserts == [100, 100, 5]

    reads.clear()
    inserts.clear()
    with engine.begin() as connection:
        repeated = append_event_revisions(connection, rows, known_at=later_at, **source)
    assert [receipt.revision_id for receipt in repeated] == [
        receipt.revision_id for receipt in first
    ]
    assert all(receipt.idempotent for receipt in repeated)
    assert len(reads) == 3
    assert inserts == []

    amendments = [{**rows[4], "published_at": "2026-08-20 10:05:00"}, rows[0]]
    with engine.begin() as connection:
        updated = append_event_revisions(connection, amendments, known_at=later_at, **source)
    with single_engine.begin() as connection:
        expected = [
            append_event_revision(connection, row, known_at=later_at, **source)
            for row in amendments
        ]
    assert updated == expected
    assert updated[0].revision_no == 2
    assert updated[0].supersedes_revision_id == first[4].revision_id
    assert updated[1].idempotent
    query = text(f"SELECT * FROM {EVENT_REVISION_TABLE} ORDER BY revision_id")
    with engine.connect() as connection, single_engine.connect() as single:
        assert connection.execute(query).all() == single.execute(query).all()


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


def test_common_fact_cutoff_requires_complete_observed_receipts_for_every_code():
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

    later_read = resolve_common_fact_cutoff(
        engine,
        codes=["000001", "000002"],
        decision_at="2026-08-05 23:01:01",
        finance_start_date="1900-01-01",
        finance_end_date="2026-08-05",
        event_start_date="2026-07-22",
        event_end_date="2026-08-05",
    )
    assert later_read["status"] == PIT_AVAILABLE
    assert later_read["fact_cutoff_at"] == resolved["fact_cutoff_at"]
    assert later_read["receipts"] == resolved["receipts"]


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


def _multi_seal_common_cutoff_case():
    engine, kwargs = _sealed_common_cutoff_case()
    for minute in (11, 12):
        append_finance_atomic_batch_seal(
            engine,
            as_of_date="2026-08-30",
            completed_known_at=f"2026-08-30 01:{minute}:00",
            changed_codes=[],
        )
    kwargs["decision_at"] = "2026-08-30 01:13:00"
    return engine, kwargs


def test_finance_seal_pages_preserve_full_reader_result(monkeypatch):
    engine, kwargs = _multi_seal_common_cutoff_case()
    paged = resolve_common_fact_cutoff(engine, **kwargs)
    assert paged["status"] == PIT_AVAILABLE

    def legacy_full_reader(engine, *, decision_at):
        with engine.connect() as connection:
            rows = [dict(row) for row in connection.execute(text(
                f"SELECT * FROM {SOURCE_COVERAGE_TABLE} "
                "WHERE fact_kind='finance' AND stock_code='000000' "
                "AND source='probiga.finance.atomic_batch' "
                "AND known_at<=:known AND received_at<=:known "
                "ORDER BY scope_hash, revision_no"
            ), {"known": pit_module.normalize_decision_at(decision_at)}).mappings()]
        pit_module._validate_coverage_chain(rows)
        row = rows[-1]
        evidence = pit_module._parse_payload(row)["watermark"]["evidence"]
        return row, evidence, {
            member["stock_code"]: dict(member) for member in evidence["members"]
        }, rows

    monkeypatch.setattr(pit_module, "_load_latest_finance_atomic_seal_evidence", legacy_full_reader)
    assert resolve_common_fact_cutoff(engine, **kwargs) == paged
    engine.dispose()


def test_finance_current_seal_does_not_read_unused_historical_catalog_payloads():
    engine, kwargs = _multi_seal_common_cutoff_case()
    expected = resolve_common_fact_cutoff(engine, **kwargs)
    with engine.begin() as c:
        old_id = c.execute(text(
            f"SELECT coverage_id FROM {SOURCE_COVERAGE_TABLE} "
            "WHERE source='probiga.finance.atomic_batch' AND revision_no=1"
        )).scalar_one()
        physical = SOURCE_COVERAGE_TABLE + "_physical"
        columns = [r[1] for r in c.exec_driver_sql(f"PRAGMA table_info({SOURCE_COVERAGE_TABLE})")]
        c.exec_driver_sql(f"ALTER TABLE {SOURCE_COVERAGE_TABLE} RENAME TO {physical}")

        def needed_payload_only(coverage_id, payload):
            if coverage_id == old_id:
                raise ValueError("unused historical catalog payload must not be read")
            return payload

        c.connection.driver_connection.create_function("needed_payload_only", 2, needed_payload_only)
        projected = ["needed_payload_only(coverage_id,payload_json) AS payload_json"
                     if name == "payload_json" else name for name in columns]
        c.exec_driver_sql(f"CREATE VIEW {SOURCE_COVERAGE_TABLE} AS SELECT {','.join(projected)} FROM {physical}")
    assert resolve_common_fact_cutoff(engine, **kwargs) == expected
    engine.dispose()


@pytest.mark.parametrize("revision", [2, 3])
def test_finance_current_seal_still_rejects_current_or_needed_parent_payload_damage(revision):
    engine, kwargs = _multi_seal_common_cutoff_case()
    with engine.begin() as c:
        c.execute(text("DROP TRIGGER trg_pit_source_coverage_immutable_bu"))
        c.execute(text(f"UPDATE {SOURCE_COVERAGE_TABLE} SET payload_json='{{}}' "
                       "WHERE source='probiga.finance.atomic_batch' AND revision_no=:revision"),
                  {"revision": revision})
    result = resolve_common_fact_cutoff(engine, **kwargs)
    assert result["status"] == PIT_DATA_BLOCKED
    assert result["reason"] == "PIT_FINANCE_ATOMIC_BATCH_INVALID:ValueError"
    engine.dispose()


@pytest.mark.parametrize("damage", ("result_count=result_count+1", "revision_no=7", "supersedes_coverage_id='broken'"))
def test_finance_seal_pages_validate_oldest_payload_and_all_links(damage):
    engine, kwargs = _multi_seal_common_cutoff_case()
    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER trg_pit_source_coverage_immutable_bu"))
        connection.execute(text(
            f"UPDATE {SOURCE_COVERAGE_TABLE} SET {damage} "
            "WHERE fact_kind='finance' AND stock_code='000000' "
            "AND source='probiga.finance.atomic_batch' AND revision_no=1"
        ))
    result = resolve_common_fact_cutoff(engine, **kwargs)
    assert result["reason"] == "PIT_FINANCE_ATOMIC_BATCH_INVALID:ValueError"
    assert result["status"] == PIT_DATA_BLOCKED
    assert result["fact_cutoff_at"] == ""
    engine.dispose()


def _mysql_compress_frame(raw):
    encoded = zlib.compress(raw)
    return len(raw).to_bytes(4, "little") + encoded + (b"." if encoded[-1] == 0x20 else b"")


def _mysql_seal_transport(engine, *, corrupt=None):
    """Execute the real MySQL payload projection over the same SQLite facts."""
    with engine.connect() as connection:
        dbapi = connection.connection.driver_connection
        dbapi.create_function("OCTET_LENGTH", 1, lambda value: len(value.encode("utf-8")))
        dbapi.create_function("COMPRESS", 1, lambda value:
            (corrupt or (lambda packed: packed))(_mysql_compress_frame(value.encode("utf-8"))))

    class MysqlConnection:
        dialect = SimpleNamespace(name="mysql")

        def __init__(self):
            self.connection = engine.connect()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return self.connection.__exit__(*args)

        def get_isolation_level(self):
            return "REPEATABLE READ"

        def execution_options(self, *, isolation_level):
            assert isolation_level == "REPEATABLE READ"
            return self

        def begin(self):
            return self.connection.begin()

        def execute(self, statement, params):
            sql = str(statement).replace(" FORCE INDEX (idx_pit_coverage_decision)", "")
            return self.connection.execute(statement if sql == str(statement) else text(sql), params)

    return SimpleNamespace(connect=MysqlConnection)


def test_finance_nonfiling_uses_bounded_compressed_reads_without_changing_evidence(monkeypatch):
    engine = _engine()
    codes = [f"{code:06d}" for code in range(1, 81)] + ["002731", "002732", "002733"]
    receipts = {}
    for code, days in (("000001", (30,)), ("000042", (30, 31)), ("002731", (30, 31))):
        for day in days:
            receipts[code, day] = append_finance_expected_unavailable(
                engine,
                stock_code=code,
                expected_report_date="2026-03-31",
                known_at=f"2026-08-{day} 09:00:00",
                official_evidence=_nonfiling_evidence(stock_code=code),
                batch_id=f"nonfiling-{code}-{day}",
            )
    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER trg_pit_source_coverage_immutable_bu"))
        connection.execute(text(
            f"UPDATE {SOURCE_COVERAGE_TABLE} SET watermark_hash='broken' "
            "WHERE coverage_id=:coverage_id"
        ), {"coverage_id": receipts["000042", 30].coverage_id})

    mysql_reader = _mysql_seal_transport(engine)
    queries = []

    def record(connection, _cursor, sql, _parameters, context, _many):
        if f"FROM {SOURCE_COVERAGE_TABLE}" in sql:
            bound_codes = [
                value for key, value in context.compiled_parameters[0].items()
                if key.startswith("codes_")
            ]
            queries.append((
                connection, connection.get_transaction(), sql, bound_codes,
                context.execution_options.get("stream_results"),
            ))

    try:
        for day in (30, 31):
            kwargs = {
                "codes": list(reversed(codes)) + ["002731", ""],
                "decision_at": f"2026-08-{day} 10:00:00",
                "expected_report_date": "2026-03-31",
                "known_after": "2026-08-30 08:55:00",
            }
            expected = load_finance_expected_unavailable(engine, **kwargs)
            available, invalid = expected
            assert set(available) == {"000001", "002731"}
            assert available["002731"]["coverage_id"] == receipts["002731", day].coverage_id
            assert set(invalid) == {"000042"}
            assert invalid["000042"].startswith("PIT_FINANCE_BAD_UNAVAILABLE_CHAIN:")
            event.listen(engine, "before_cursor_execute", record)
            try:
                assert load_finance_expected_unavailable(mysql_reader, **kwargs) == expected
            finally:
                event.remove(engine, "before_cursor_execute", record)
            assert [bound_codes for _, _, _, bound_codes, _ in queries] == [
                codes[:40], codes[40:80], codes[80:],
            ]
            connection, transaction, *_ = queries[0]
            assert transaction is not None
            assert all(
                current is connection and active is transaction
                and "COMPRESS(payload_json)" in sql and "OCTET_LENGTH(payload_json)" in sql
                and "MAX_EXECUTION_TIME(60000)" in sql
                and streaming is True
                for current, active, sql, _, streaming in queries
            )
            queries.clear()
        times = iter((0, 0, 1801))
        monkeypatch.setattr(pit_module, "monotonic", lambda: next(times))
        with pytest.raises(TimeoutError, match="finance_nonfiling: read deadline after 40 codes"):
            load_finance_expected_unavailable(mysql_reader, **kwargs)
    finally:
        engine.dispose()


def test_finance_compressed_history_is_byte_exact_and_preserves_all_seal_results():
    engine, kwargs = _multi_seal_common_cutoff_case()
    plain = pit_module._load_latest_finance_atomic_seal_evidence(
        engine, decision_at=kwargs["decision_at"])
    mysql_reader = _mysql_seal_transport(engine)
    packed = pit_module._load_latest_finance_atomic_seal_evidence(
        mysql_reader, decision_at=kwargs["decision_at"])
    assert packed == plain  # Original payload bytes, IDs, roots, members and every parent.
    assert len(packed[3]) == 3
    engine.dispose()


@pytest.mark.parametrize("damage", [None, "old_payload", "current_payload", "parent_revision"])
def test_finance_compressed_member_reads_preserve_current_revision_and_parent_anchor(monkeypatch, damage):
    engine = _engine()
    _install_prior_finance_coverage(engine)
    with engine.connect() as connection:
        original = json.loads(connection.execute(text(
            f"SELECT payload_json FROM {SOURCE_COVERAGE_TABLE} WHERE stock_code='000001'"
        )).scalar_one())
    append_source_coverage(
        engine, fact_kind="finance", stock_code="000001",
        window_start="1900-01-01", window_end="2026-08-30",
        known_at="2026-08-30 01:12:00", covered_through_at="2026-08-30 01:12:00",
        watermark_kind="CAPTURED_AT", watermark_evidence={"source_call": "success"},
        source_rows=original["source_rows"], fact_bindings=original["fact_bindings"],
        source="adata.finance.core_index", batch_id="finance-second-coverage",
    )
    with engine.connect() as connection:
        expected_bytes = [value.encode("utf-8") for value in connection.execute(text(
            f"SELECT payload_json FROM {SOURCE_COVERAGE_TABLE} AS selected "
            "WHERE stock_code IN ('000001','002731') AND NOT EXISTS ("
            f"SELECT 1 FROM {SOURCE_COVERAGE_TABLE} AS newer WHERE newer.scope_hash=selected.scope_hash "
            "AND newer.revision_no>selected.revision_no) ORDER BY stock_code,scope_hash"
        )).scalars()]
    assert len(expected_bytes) == 2
    kwargs = dict(codes=["000001", "002731"], listing_dates={},
                  completed_known_at=datetime(2026, 8, 30, 1, 13), as_of_date=date(2026, 8, 30))
    plain = pit_module._select_finance_atomic_batch_members(engine, **kwargs)
    mysql_reader = _mysql_seal_transport(engine)
    if damage:
        with engine.begin() as connection:
            connection.execute(text("DROP TRIGGER trg_pit_source_coverage_immutable_bu"))
            revision = 2 if damage == "current_payload" else 1
            assignment = "revision_no=9" if damage == "parent_revision" else "payload_json='{}'"
            connection.execute(text(f"UPDATE {SOURCE_COVERAGE_TABLE} SET {assignment} "
                                    "WHERE stock_code='000001' AND revision_no=:revision"), {"revision": revision})
        if damage == "old_payload":
            # An unused historical body cannot block a current full snapshot.
            # Explicit history auditing still detects the corruption.
            assert pit_module._select_finance_atomic_batch_members(mysql_reader, **kwargs) == plain
            with engine.connect() as c:
                history = [dict(row) for row in c.execute(text(
                    f"SELECT * FROM {SOURCE_COVERAGE_TABLE} WHERE stock_code='000001' ORDER BY revision_no"
                )).mappings()]
            with pytest.raises(ValueError, match="coverage payload schema differs"):
                pit_module._validate_coverage_chain(history)
        else:
            with pytest.raises(ValueError):
                pit_module._select_finance_atomic_batch_members(mysql_reader, **kwargs)
    else:
        decoded = []
        original_decode = pit_module._decode_coverage_payload

        def observe(frame, *, expected_bytes, stage):
            payload = original_decode(frame, expected_bytes=expected_bytes, stage=stage)
            decoded.append(payload.encode("utf-8"))
            return payload

        monkeypatch.setattr(pit_module, "_decode_coverage_payload", observe)
        assert pit_module._select_finance_atomic_batch_members(mysql_reader, **kwargs) == plain
        assert decoded == expected_bytes
    engine.dispose()


@pytest.mark.parametrize("damage", ["result_count=result_count+1", "payload_json='{}'",
                                    "revision_no=7", "supersedes_coverage_id='broken'"])
def test_finance_compressed_history_still_rejects_oldest_payload_or_chain_damage(damage):
    engine, kwargs = _multi_seal_common_cutoff_case()
    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER trg_pit_source_coverage_immutable_bu"))
        connection.execute(text(f"UPDATE {SOURCE_COVERAGE_TABLE} SET {damage} "
            "WHERE fact_kind='finance' AND stock_code='000000' "
            "AND source='probiga.finance.atomic_batch' AND revision_no=1"))
    if damage == "payload_json='{}'":
        # An unused immutable body belongs to explicit historical auditing;
        # it does not prevent using the intact current snapshot and parent.
        assert pit_module._load_latest_finance_atomic_seal_evidence(
            _mysql_seal_transport(engine), decision_at=kwargs["decision_at"])[0]
        with engine.connect() as c:
            history = [dict(row) for row in c.execute(text(
                f"SELECT * FROM {SOURCE_COVERAGE_TABLE} WHERE source='probiga.finance.atomic_batch' ORDER BY revision_no"
            )).mappings()]
        with pytest.raises(ValueError):
            pit_module._validate_coverage_chain(history)
    else:
        with pytest.raises(ValueError):
            pit_module._load_latest_finance_atomic_seal_evidence(
                _mysql_seal_transport(engine), decision_at=kwargs["decision_at"])
    engine.dispose()


@pytest.mark.parametrize("corrupt", [lambda frame: frame[:-1],
    lambda frame: (int.from_bytes(frame[:4], "little") + 1).to_bytes(4, "little") + frame[4:],
    lambda frame: frame + b"unexpected", lambda frame: None])
def test_finance_history_rejects_broken_compressed_transport(corrupt):
    engine, kwargs = _multi_seal_common_cutoff_case()
    with pytest.raises(ValueError, match="finance_seal_history"):
        pit_module._load_latest_finance_atomic_seal_evidence(
            _mysql_seal_transport(engine, corrupt=corrupt), decision_at=kwargs["decision_at"])
    engine.dispose()


def test_finance_payload_bound_uses_stored_size_and_preserves_large_unicode_json():
    raw = json.dumps({"title": "原始公告\u0000", "members": "A" * (8 * 1024 * 1024)},
                     ensure_ascii=False).encode("utf-8")
    assert pit_module._decode_coverage_payload(_mysql_compress_frame(raw),
        expected_bytes=len(raw)).encode("utf-8") == raw
    # The trusted SQL byte length, not the frame's claim, bounds inflation.
    forged = len(raw).to_bytes(4, "little") + zlib.compress(raw + b"X")
    with pytest.raises(ValueError, match="inflation/boundary"):
        pit_module._decode_coverage_payload(forged, expected_bytes=len(raw))
    with pytest.raises(ValueError, match="UTF-8"):
        pit_module._decode_coverage_payload(_mysql_compress_frame(b"\xff"), expected_bytes=1)


def test_finance_mysql_space_marker_is_accepted_only_at_the_real_stream_boundary():
    raw = b"\x1f"  # Adler checksum ends in 0x20; MySQL appends the marker.
    frame = _mysql_compress_frame(raw)
    assert frame.endswith(b" .")
    assert pit_module._decode_coverage_payload(frame, expected_bytes=len(raw)) == raw.decode()
    with pytest.raises(ValueError, match="trailing bytes"):
        pit_module._decode_coverage_payload(frame[:-1], expected_bytes=len(raw))


def test_finance_seal_read_error_reports_only_stage_and_numeric_errno():
    engine, kwargs = _sealed_common_cutoff_case()

    def fail(_connection, _cursor, statement, parameters, _context, _many):
        if "SELECT coverage_id, scope_hash, revision_no" in statement:
            raise OperationalError(statement, parameters, Exception(2013, "secret connection details"))

    event.listen(engine, "before_cursor_execute", fail)
    result = resolve_common_fact_cutoff(engine, **kwargs)
    assert result["reason"] == (
        "PIT_FINANCE_ATOMIC_BATCH_INVALID:OperationalError:errno=2013:"
        "stage=finance_seal_history"
    )
    assert "secret" not in str(result)
    assert result["status"] == PIT_DATA_BLOCKED
    engine.dispose()


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


def test_common_cutoff_code_chunks_preserve_receipts_and_bound_queries(monkeypatch):
    engine, kwargs = _sealed_common_cutoff_case()
    expected = resolve_common_fact_cutoff(engine, **kwargs)
    monkeypatch.setattr(pit_module, "COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT", 1)
    scopes = []

    def record(_connection, _cursor, sql, _parameters, context, _many):
        if "ORDER BY fact_kind, stock_code, scope_hash, revision_no" in sql:
            scopes.append([
                value for key, value in context.compiled_parameters[0].items()
                if key.startswith("codes_")
            ])

    event.listen(engine, "before_cursor_execute", record)
    assert resolve_common_fact_cutoff(engine, **kwargs) == expected
    assert scopes == [["000001"], ["002731"]]
    engine.dispose()


def test_common_cutoff_later_chunk_failure_cannot_return_partial_receipts(monkeypatch):
    engine, kwargs = _sealed_common_cutoff_case()
    monkeypatch.setattr(pit_module, "COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT", 1)
    reads = []

    def fail(_connection, _cursor, sql, parameters, _context, _many):
        if "ORDER BY fact_kind, stock_code, scope_hash, revision_no" in sql:
            reads.append(sql)
            if len(reads) == 2:
                raise OperationalError(sql, parameters, Exception(2013, "lost connection"))

    event.listen(engine, "before_cursor_execute", fail)
    result = resolve_common_fact_cutoff(engine, **kwargs)
    assert result["status"] == PIT_DATA_BLOCKED
    assert result["reason"] == "PIT_COMMON_CUTOFF_SCHEMA_UNAVAILABLE:OperationalError"
    assert result["receipts"] == []
    assert result["receipt_root_hash"] == ""
    engine.dispose()


@pytest.mark.parametrize("stage, exposed", [("batch-payload:440", True), ("untrusted endpoint text", False)])
def test_common_cutoff_reports_only_fixed_announcement_reply_stages(monkeypatch, stage, exposed):
    from server.common.qmt_announcement_pit import LIVE_ANNOUNCEMENT_BATCH_REPLY_DEADLINE, QMTAnnouncementBlocked

    engine, kwargs = _sealed_common_cutoff_case()

    def timeout(*_args, **_kwargs):
        raise QMTAnnouncementBlocked(LIVE_ANNOUNCEMENT_BATCH_REPLY_DEADLINE, stage)

    monkeypatch.setattr("server.common.qmt_announcement_pit.validate_complete_announcement_batch", timeout)
    result = resolve_common_fact_cutoff(engine, **kwargs, require_qmt_event_batch=True)
    assert result["status"] == PIT_DATA_BLOCKED
    assert result["reason"] == LIVE_ANNOUNCEMENT_BATCH_REPLY_DEADLINE + (f":{stage}" if exposed else "")
    assert result["receipts"] == []
    engine.dispose()


@pytest.mark.parametrize("fault", [None, "predecessor", "later-timeout", "missing-payload", "checkpoint-resume"])
def test_live_common_cutoff_preserves_full_event_chains_with_bounded_mysql_transport(
    monkeypatch, tmp_path, fault,
):
    from server.common.announcement_provider import CninfoMarketAnnouncementProvider, ProviderBackedAnnouncementAdapter
    from server.common.qmt_announcement_pit import CNINFO_ANNOUNCEMENT_SOURCE, synchronize_qmt_announcements
    from tests.test_announcement_provider_fallback import _Client, _master, _row_at
    from tests.test_qmt_announcement_pit import _catalog, _install_catalog

    engine = _engine()
    codes = [f"{index:06d}" for index in range(1, 46)]
    cutoff = datetime(2026, 8, 30, 18, 20)
    received = cutoff + timedelta(minutes=5)
    start = cutoff.date() - timedelta(days=30)
    catalog = _install_catalog(
        engine, _catalog(*((code, f"{code}.SZ") for code in codes)),
        captured_at="2026-08-30 00:00:00",
    )
    for code in codes:
        finance_row = {
            "stock_code": code, "report_date": "2026-03-31", "report_type": "Q1",
            "notice_date": "2026-04-25", "roe_wtd": 12.5,
        }
        revision = append_finance_revision(engine, finance_row, known_at="2026-08-30 18:01:00")
        append_source_coverage(
            engine, fact_kind="finance", stock_code=code, window_start="1900-01-01",
            window_end=cutoff.date(), known_at="2026-08-30 18:01:00",
            covered_through_at="2026-08-30 18:01:00", watermark_kind="CAPTURED_AT",
            watermark_evidence={"source_call": "success"}, source_rows=[finance_row],
            fact_bindings=[{"revision_id": revision.revision_id, "content_hash": revision.content_hash}],
            source="adata.finance.core_index", batch_id="finance-coverage-prior",
        )
        append_source_coverage(
            engine, fact_kind="event", stock_code=code, window_start=start,
            window_end=cutoff.date(), known_at="2026-08-30 18:05:00",
            covered_through_at="2026-08-30 18:05:00", watermark_kind="CAPTURED_AT",
            watermark_evidence={"source_call": "success"}, source_rows=[], fact_bindings=[],
            source=CNINFO_ANNOUNCEMENT_SOURCE, batch_id="event-coverage-prior",
        )
    append_finance_atomic_batch_seal(engine, as_of_date=cutoff.date(), completed_known_at="2026-08-30 18:22:00")
    monkeypatch.setattr("server.common.qmt_announcement_pit._load_catalog", lambda *_: catalog)
    master = _master(*codes)
    provider = CninfoMarketAnnouncementProvider(
        client=_Client(masters=[master, master], rows_by_code={codes[0]: [_row_at(codes[0], 0, cutoff)]}),
        minimum_request_interval=0, now_fn=lambda: received,
    )
    publication = synchronize_qmt_announcements(
        engine, xtdata=ProviderBackedAnnouncementAdapter(provider), checkpoint_root=tmp_path,
        source=CNINFO_ANNOUNCEMENT_SOURCE, fallback_reason="QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE",
        capture_fact_cutoff_at=cutoff, now_fn=lambda: received,
    )
    assert publication["status"] == "COMPLETE" and publication["coverage_count"] == len(codes)
    append_source_coverage(
        engine, fact_kind="event", stock_code=codes[-1], window_start=cutoff.date(),
        window_end=cutoff.date(), known_at="2026-08-30 18:29:00",
        covered_through_at="2026-08-30 18:29:00", watermark_kind="CAPTURED_AT",
        watermark_evidence={"source_call": "success"}, source_rows=[], fact_bindings=[],
        source=CNINFO_ANNOUNCEMENT_SOURCE, batch_id="event-coverage-successor",
    )
    kwargs = dict(
        codes=codes, decision_at="2026-08-30 18:31:00", require_qmt_event_batch=True,
        finance_start_date="1900-01-01", finance_end_date=cutoff.date(),
        event_start_date=start, event_end_date=cutoff.date(),
    )
    if fault == "predecessor":
        with engine.begin() as connection:
            connection.execute(text("DROP TRIGGER trg_pit_source_coverage_immutable_bu"))
            connection.execute(text(
                f"UPDATE {SOURCE_COVERAGE_TABLE} SET watermark_hash='broken' "
                "WHERE batch_id='event-coverage-prior' AND stock_code=:code"
            ), {"code": codes[-1]})
    expected = resolve_common_fact_cutoff(engine, **kwargs)
    if fault == "predecessor":
        assert expected["status"] == PIT_DATA_BLOCKED
        assert expected["reason"] == f"PIT_COMMON_CUTOFF_INCOMPLETE:event:{codes[-1]}:BAD_CHAIN"
    else:
        assert expected["status"] == PIT_AVAILABLE
        assert expected["qmt_event_batch"]["batch_root_hash"] == publication["batch_root_hash"]
        assert next(row for row in expected["receipts"] if row["stock_code"] == codes[-1]
                    and row["fact_kind"] == "event")["batch_id"] == publication["batch_id"]

    empty_kwargs = dict(
        fact_kind="event", codes=codes, decision_at=datetime(2026, 8, 30, 18, 31),
        fact_cutoff_at=cutoff, start_date=start, end_date=cutoff.date(),
        source=CNINFO_ANNOUNCEMENT_SOURCE, batch_id=publication["batch_id"],
    )
    if fault is None:
        expected_empty, expected_errors = pit_module._authoritative_empty_coverage(engine, **empty_kwargs)
        assert codes[1] in expected_empty and codes[0] not in expected_empty
        assert codes[-1] not in expected_empty and expected_errors == {}
        event_kwargs = dict(
            codes=codes, decision_at=kwargs["decision_at"], fact_cutoff_at=expected["fact_cutoff_at"],
            start_date=start, end_date=cutoff.date(), require_qmt_complete_batch=True,
        )
        expected_events = load_event_facts(engine, **event_kwargs)
        assert expected_events.status_for(codes[0]) == PIT_AVAILABLE
        assert expected_events.status_for(codes[1]) == PIT_AVAILABLE
        assert expected_events.status_for(codes[-1]) == PIT_NO_ROWS

    payload_budget = 7000
    monkeypatch.setattr("server.common.qmt_announcement_pit.VALIDATION_READ_PAYLOAD_BYTES", payload_budget)
    with engine.connect() as connection:
        sizes = dict(connection.execute(text(
            f"SELECT coverage_id, LENGTH(CAST(payload_json AS BLOB)) FROM {SOURCE_COVERAGE_TABLE}"
        )).tuples().all())
    mysql_reader = _mysql_seal_transport(engine)
    inventories, size_reads, payloads = [], [], []
    checkpoint_interrupted = False

    def record(connection, _cursor, sql, parameters, context, _many):
        nonlocal checkpoint_interrupted
        if "FORCE INDEX (uk_pit_coverage_scope_revision)" in sql:
            bindings = context.compiled_parameters[0]
            scopes = [value for key, value in bindings.items() if key.startswith("scope_hashes_")]
            inventories.append((connection.get_transaction(), scopes, sql, context.execution_options))
        if "FORCE INDEX (PRIMARY)" in sql and "OCTET_LENGTH(payload_json)" in sql and (
            "COMPRESS(payload_json)" not in sql and "AND batch_id=" not in sql
        ):
            bindings = context.compiled_parameters[0]
            ids = [value for key, value in bindings.items() if key.startswith("coverage_ids_")]
            size_reads.append((connection.get_transaction(), ids))
        if "FORCE INDEX (PRIMARY) WHERE coverage_id IN" in sql:
            bindings = context.compiled_parameters[0]
            ids = [value for key, value in bindings.items() if key.startswith("coverage_ids_")]
            payloads.append((connection.get_transaction(), ids, sql, context.execution_options))
            if fault == "later-timeout" and len(inventories) == 2:
                raise OperationalError(sql, parameters, Exception(3024, "execution deadline"))
            if fault == "checkpoint-resume" and len(inventories) == 2 and not checkpoint_interrupted:
                checkpoint_interrupted = True
                raise OperationalError(sql, parameters, Exception(3024, "execution deadline"))
            if fault == "missing-payload" and len(inventories) == 2:
                sql += f" AND coverage_id<>'{ids[0]}'"
        return sql.replace(" FORCE INDEX (PRIMARY)", "").replace(
            " FORCE INDEX (uk_pit_coverage_scope_revision)", "",
        ), parameters

    def assert_bounded_reads():
        assert [scopes for _, scopes, _, _ in inventories] == [
            sorted(pit_module._source_coverage_scope_hash("event", code, CNINFO_ANNOUNCEMENT_SOURCE)
                   for code in group)
            for group in (codes[:40], codes[40:])
        ]
        transaction = inventories[0][0]
        assert transaction is not None and all(row[0] is transaction for row in inventories + size_reads + payloads)
        assert all(
            "payload_json" not in sql and "known_at<=" in sql and "received_at<=" in sql and "batch_id=" not in sql
            and options["stream_results"]
            for _, _, sql, options in inventories
        )
        assert size_reads and all(0 < len(ids) <= 40 for _, ids in size_reads)
        assert payloads and all(
            len(ids) <= 40 and (len(ids) == 1 or sum(sizes[key] for key in ids) <= payload_budget)
            and "COMPRESS(payload_json)" in sql and "MAX_EXECUTION_TIME(60000)" in sql
            and options["stream_results"]
            for _, ids, sql, options in payloads
        )

    event.listen(engine, "before_cursor_execute", record, retval=True)
    selections = []
    try:
        if fault == "checkpoint-resume":
            monkeypatch.setattr(
                "server.engine.strategy_governance.validate_privileged_trigger_migration_seal",
                lambda _connection: {
                    "trigger_inventory_server_uuid": "isolated-database",
                    "trigger_inventory_seal_database": "isolated-schema",
                    "pit_fact_schema_contract_hash": "contract",
                    "trigger_inventory_table_comment": "attested-schema",
                },
            )
            checkpoint_args = {"event_checkpoint_path": tmp_path / "analysis-state" / "event.sqlite"}
            progress = []
            interrupted = resolve_common_fact_cutoff(
                mysql_reader, **kwargs, **checkpoint_args,
                event_progress=lambda done, total: progress.append((done, total)),
            )
            assert interrupted["status"] == PIT_DATA_BLOCKED
            assert interrupted["receipt_root_hash"] == "" and interrupted["receipts"] == []
            assert progress == [(0, 45), (40, 45)]
            first_chunk_ids = {key for _, keys, *_ in payloads[:-1] for key in keys}
            assert first_chunk_ids
            inventories.clear()
            size_reads.clear()
            payloads.clear()

            resumed = resolve_common_fact_cutoff(
                mysql_reader, **kwargs, **checkpoint_args, capture_historical_selection=selections.append,
            )
            assert resumed == expected
            assert len(inventories) == 2
            assert payloads and not any(first_chunk_ids.intersection(keys) for _, keys, *_ in payloads)

            damaged_checkpoint = tmp_path / "analysis-state" / "damaged.sqlite"
            damaged_checkpoint.write_text("invalid local state", encoding="utf-8")
            damaged_checkpoint.chmod(0o600)
            assert resolve_common_fact_cutoff(
                mysql_reader, **kwargs, event_checkpoint_path=damaged_checkpoint,
            ) == expected

            late = append_source_coverage(
                engine, fact_kind="event", stock_code=codes[0], window_start=start,
                window_end=cutoff.date(), known_at="2026-08-30 18:32:00",
                covered_through_at="2026-08-30 18:32:00", watermark_kind="CAPTURED_AT",
                watermark_evidence={"source_call": "success"}, source_rows=[], fact_bindings=[],
                source=CNINFO_ANNOUNCEMENT_SOURCE, batch_id="event-coverage-late",
            )
            inventories.clear()
            size_reads.clear()
            payloads.clear()
            assert resolve_common_fact_cutoff(mysql_reader, **kwargs, **checkpoint_args) == expected
            assert not payloads and not size_reads

            later = {**kwargs, "decision_at": "2026-08-30 18:33:00"}
            assert resolve_common_fact_cutoff(mysql_reader, **later, **checkpoint_args) == (
                resolve_common_fact_cutoff(engine, **later)
            )
            assert [key for _, keys, *_ in payloads for key in keys] == [late.coverage_id]

            with engine.begin() as connection:
                connection.execute(text("DROP TRIGGER trg_pit_source_coverage_immutable_bu"))
                connection.execute(text(
                    f"UPDATE {SOURCE_COVERAGE_TABLE} SET watermark_hash='broken' "
                    "WHERE batch_id='event-coverage-prior' AND stock_code=:code"
                ), {"code": codes[-1]})
            corrupted = resolve_common_fact_cutoff(mysql_reader, **later, **checkpoint_args)
            assert corrupted["status"] == PIT_DATA_BLOCKED
            assert corrupted["reason"] == f"PIT_COMMON_CUTOFF_INCOMPLETE:event:{codes[-1]}:BAD_CHAIN"
            engine.dispose()
            return
        actual = resolve_common_fact_cutoff(mysql_reader, **kwargs, capture_historical_selection=selections.append)
        assert_bounded_reads()
        if fault is None:
            inventories.clear()
            size_reads.clear()
            payloads.clear()
            assert pit_module._authoritative_empty_coverage(mysql_reader, **empty_kwargs) == (
                expected_empty, expected_errors,
            )
            assert_bounded_reads()
    finally:
        event.remove(engine, "before_cursor_execute", record)
    if fault in {"later-timeout", "missing-payload"}:
        assert actual["status"] == PIT_DATA_BLOCKED
        assert actual["reason"] == "PIT_COMMON_CUTOFF_SCHEMA_UNAVAILABLE:" + (
            "QMT_ANNOUNCEMENT_LIVE_BATCH_REPLY_DEADLINE:common-cutoff-coverage:40:payload:0"
            if fault == "later-timeout" else "ValueError"
        )
        assert actual["receipts"] == [] and actual["receipt_root_hash"] == ""
    else:
        assert actual == expected
    if fault is None:
        assert len(selections) == 1
        proof = selections[0]
        assert proof.selection.batch["batch_root_hash"] == publication["batch_root_hash"]
        assert proof.selection.members[codes[-1]][1] is False
        assert "payload_json" not in proof.selection.members[codes[0]][0]

        def unexpected_read(*_args, **_kwargs):
            pytest.fail("a sealed live sidecar must avoid another announcement or coverage proof read")

        monkeypatch.setattr(pit_module, "_authoritative_empty_coverage", unexpected_read)
        monkeypatch.setattr(
            "server.common.qmt_announcement_pit.validate_complete_announcement_batch", unexpected_read,
        )
        assert load_event_facts(mysql_reader, **event_kwargs, historical_selection=proof) == expected_events
        for mismatch in (
            {"codes": codes[:-1]}, {"decision_at": "2026-08-30 18:31:01"},
            {"fact_cutoff_at": cutoff - timedelta(seconds=1)}, {"start_date": start - timedelta(days=1)},
        ):
            rejected = load_event_facts(mysql_reader, **{**event_kwargs, **mismatch}, historical_selection=proof)
            assert rejected.reason_for(codes[0]) == "PIT_EVENT_VALIDATED_LIVE_SCOPE_MISMATCH"
        wrong_engine = load_event_facts(engine, **event_kwargs, historical_selection=proof)
        assert wrong_engine.reason_for(codes[0]) == "PIT_EVENT_VALIDATED_LIVE_SCOPE_MISMATCH"
    else:
        assert selections == []
    engine.dispose()


def test_common_cutoff_compact_unsealed_candidate_preserves_nonfiling_metadata(monkeypatch):
    engine = _engine()
    append_finance_expected_unavailable(
        engine,
        stock_code="002731",
        expected_report_date="2026-03-31",
        known_at="2026-08-30 01:06:35",
        official_evidence=_nonfiling_evidence(),
        batch_id="compact-nonfiling",
    )
    append_source_coverage(
        engine,
        fact_kind="event",
        stock_code="002731",
        window_start="2026-08-10",
        window_end="2026-08-30",
        known_at="2026-08-30 01:07:00",
        covered_through_at="2026-08-30 01:07:00",
        watermark_kind="CAPTURED_AT",
        watermark_evidence={"source_call": "success"},
        source_rows=[],
        fact_bindings=[],
        source="test.event",
        batch_id="compact-event",
    )
    monkeypatch.setattr(pit_module, "COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT", 1)
    result = resolve_common_fact_cutoff(
        engine,
        codes=["002731"],
        decision_at="2026-08-30 01:08:00",
        finance_start_date="1900-01-01",
        finance_end_date="2026-08-30",
        event_start_date="2026-08-10",
        event_end_date="2026-08-30",
    )
    assert result["status"] == PIT_AVAILABLE
    finance = next(row for row in result["receipts"] if row["fact_kind"] == "finance")
    assert finance["expected_report_date"] == "2026-03-31"
    assert finance["reason_code"] == "CNINFO_REGULATORY_PERIODIC_REPORT_NOT_FILED"
    engine.dispose()


@pytest.mark.parametrize("kind", ("finance", "event"))
def test_empty_coverage_chunks_keep_complete_evidence_and_invalid_metadata(monkeypatch, kind):
    engine = _engine()
    for code in ("000001", "002731"):
        for minute in (9, 10):
            known = f"2026-08-30 01:{minute:02d}:00"
            append_source_coverage(
                engine,
                fact_kind=kind,
                stock_code=code,
                window_start="1900-01-01",
                window_end="2026-08-30",
                known_at=known,
                covered_through_at=known,
                watermark_kind="CAPTURED_AT",
                watermark_evidence={"source_call": "success"},
                source_rows=[],
                fact_bindings=[],
                source=f"test.{kind}",
                batch_id=f"empty-{code}-{minute}",
            )
    kwargs = {
        "fact_kind": kind,
        "codes": ["000001", "002731"],
        "fact_cutoff_at": datetime(2026, 8, 30, 1, 10),
        "decision_at": datetime(2026, 8, 30, 1, 11),
        "start_date": date(1900, 1, 1),
        "end_date": date(2026, 8, 30),
    }
    expected = pit_module._authoritative_empty_coverage(engine, **kwargs)
    assert set(expected[0]) == {"000001", "002731"}
    assert expected[1] == {}
    mysql_reader = _mysql_seal_transport(engine)
    queries = []

    def record(_connection, _cursor, sql, _parameters, context, _many):
        if "ORDER BY stock_code, scope_hash, revision_no" in sql:
            codes = [
                value for key, value in context.compiled_parameters[0].items()
                if key.startswith("codes_")
            ]
            queries.append((sql, codes, context.execution_options.get("stream_results")))

    event.listen(engine, "before_cursor_execute", record)
    try:
        with monkeypatch.context() as smaller_chunks:
            smaller_chunks.setattr(pit_module, "COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT", 1)
            assert pit_module._authoritative_empty_coverage(engine, **kwargs) == expected
            assert [codes for _, codes, _ in queries] == [["000001"], ["002731"]]
            queries.clear()
            assert pit_module._authoritative_empty_coverage(mysql_reader, **kwargs) == expected
            assert [codes for _, codes, _ in queries] == [["000001"], ["002731"]]
        assert all(
            "COMPRESS(payload_json)" in sql and "OCTET_LENGTH(payload_json)" in sql
            and streaming is True
            for sql, _, streaming in queries
        )
        queries.clear()
        wider_codes = kwargs["codes"] + [f"300{index:03d}" for index in range(39)]
        wider_scope = {**kwargs, "codes": wider_codes}
        assert pit_module._authoritative_empty_coverage(mysql_reader, **wider_scope) == expected
        assert [codes for _, codes, _ in queries] == [wider_codes[:40], wider_codes[40:]]
    finally:
        event.remove(engine, "before_cursor_execute", record)

    with engine.connect() as connection:
        predecessor_payload = connection.execute(text(
            f"SELECT payload_json FROM {SOURCE_COVERAGE_TABLE} "
            "WHERE stock_code='000001' AND revision_no=1"
        )).scalar_one()
    predecessor_frame = _mysql_compress_frame(predecessor_payload.encode("utf-8"))
    damaged_reader = _mysql_seal_transport(
        engine, corrupt=lambda frame: frame[:-1] if frame == predecessor_frame else frame,
    )
    available, invalid = pit_module._authoritative_empty_coverage(damaged_reader, **kwargs)
    assert available == {"002731": expected[0]["002731"]}
    assert set(invalid) == {"000001"}
    assert invalid["000001"].startswith(f"PIT_{kind.upper()}_BAD_COVERAGE_CHAIN:")

    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER trg_pit_source_coverage_immutable_bu"))
        connection.execute(text(
            f"UPDATE {SOURCE_COVERAGE_TABLE} SET result_count=result_count+1 "
            "WHERE stock_code='000001' AND revision_no=1"
        ))
    available, invalid = pit_module._authoritative_empty_coverage(engine, **kwargs)
    assert set(available) == {"002731"}
    assert set(invalid) == {"000001"}
    assert invalid["000001"].startswith(f"PIT_{kind.upper()}_BAD_COVERAGE_CHAIN:")
    mysql_reader = _mysql_seal_transport(engine)
    assert pit_module._authoritative_empty_coverage(mysql_reader, **kwargs) == (available, invalid)
    engine.dispose()


def _finance_revision_chunk_case():
    engine = _engine()
    for code in ("000001", "000002"):
        for report, report_type in (("2026-03-31", "Q1"), ("2026-06-30", "Q2")):
            append_finance_revision(
                engine,
                {
                    "stock_code": code, "report_date": report,
                    "report_type": report_type,
                    "notice_date": "2026-08-20 18:30:00", "roe_wtd": 12.5,
                },
                known_at="2026-08-20 18:31:00",
                batch_id=f"finance-chunk-{code}-{report_type}",
            )
        append_finance_revision(
            engine,
            {
                "stock_code": code, "report_date": "2026-03-31",
                "report_type": "Q1", "notice_date": "2026-08-20 18:32:00",
                "roe_wtd": 13.0,
            },
            known_at="2026-08-20 18:33:00",
            batch_id=f"finance-chunk-{code}-amendment",
        )
    return engine, {
        "codes": ["000002", "000001"],
        "decision_at": "2026-08-20 18:35:00",
        "fact_cutoff_at": "2026-08-20 18:34:00",
        "as_of_date": "2026-08-20",
    }


def test_finance_revision_chunks_keep_exact_full_manifest_and_one_connection(monkeypatch):
    engine, kwargs = _finance_revision_chunk_case()
    expected = load_finance_facts(engine, **kwargs)
    assert set(expected.facts) == {"000001", "000002"}
    monkeypatch.setattr(pit_module, "COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT", 1)
    reads = []
    original = pit_module._query_revisions

    def record(connection, **parameters):
        rows = original(connection, **parameters)
        reads.append((id(connection), parameters["codes"], len(rows)))
        return rows

    monkeypatch.setattr(pit_module, "_query_revisions", record)
    actual = load_finance_facts(engine, **kwargs)
    assert actual == expected
    assert [(codes, count) for _, codes, count in reads] == [
        (["000001"], 3), (["000002"], 3),
    ]
    assert len({connection for connection, _, _ in reads}) == 1
    assert actual.facts["000001"]["finance_report_date"] == "2026-06-30"
    engine.dispose()


def test_finance_revision_later_chunk_error_blocks_the_whole_batch(monkeypatch):
    engine, kwargs = _finance_revision_chunk_case()
    monkeypatch.setattr(pit_module, "COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT", 1)
    original = pit_module._query_revisions

    def fail(connection, **parameters):
        if parameters["codes"] == ["000002"]:
            raise OperationalError("finance revision read", {}, Exception(2013, "lost connection"))
        return original(connection, **parameters)

    monkeypatch.setattr(pit_module, "_query_revisions", fail)
    result = load_finance_facts(engine, **kwargs)
    assert result.facts == {}
    assert set(result.status_by_code.values()) == {pit_module.PIT_SCHEMA_UNAVAILABLE}
    assert set(result.reason_by_code.values()) == {"PIT_FINANCE_SCHEMA_UNAVAILABLE:OperationalError"}
    engine.dispose()


def test_finance_revision_chunks_validate_old_report_chains(monkeypatch):
    engine, kwargs = _finance_revision_chunk_case()
    monkeypatch.setattr(pit_module, "COMMON_FACT_CUTOFF_QUERY_CODE_LIMIT", 1)
    with engine.begin() as connection:
        connection.execute(text("DROP TRIGGER trg_pit_finance_revision_immutable_bu"))
        connection.execute(text(
            f"UPDATE {FINANCE_REVISION_TABLE} SET content_hash='broken' "
            "WHERE stock_code='000002' AND report_date='2026-03-31' AND revision_no=1"
        ))
    result = load_finance_facts(engine, **kwargs)
    assert result.status_by_code["000001"] == PIT_AVAILABLE
    assert result.status_by_code["000002"] == PIT_DATA_BLOCKED
    assert result.reason_by_code["000002"].startswith("PIT_FINANCE_BAD_CHAIN:")
    engine.dispose()


def test_finance_revision_chunks_preserve_nonfiling_with_single_connection_pool(tmp_path):
    engine = create_engine(
        f"sqlite+pysqlite:///{(tmp_path / 'single-pool.db').as_posix()}",
        poolclass=QueuePool, pool_size=1, max_overflow=0, pool_timeout=0.05,
    )
    ensure_pit_fact_schema(engine)
    append_finance_expected_unavailable(
        engine,
        stock_code="002731",
        expected_report_date="2026-03-31",
        known_at="2026-08-30 01:06:35",
        official_evidence=_nonfiling_evidence(),
        batch_id="single-connection-nonfiling",
    )
    result = load_finance_facts(
        engine, codes=["002731"], decision_at="2026-08-30 01:08:00",
        fact_cutoff_at="2026-08-30 01:07:00", as_of_date="2026-08-30",
    )
    assert result.status_for("002731") == PIT_AVAILABLE
    assert result.reason_for("002731") == "PIT_FINANCE_EXPECTED_UNAVAILABLE"
    assert result.coverage_by_code["002731"]["coverage_id"]
    assert engine.pool.checkedout() == 0
    engine.dispose()

def test_complete_late_cninfo_batch_is_visible_only_after_actual_receipt(tmp_path, monkeypatch):
    from server.common.announcement_provider import CninfoMarketAnnouncementProvider, ProviderBackedAnnouncementAdapter
    from server.common.qmt_announcement_pit import CNINFO_ANNOUNCEMENT_SOURCE, synchronize_qmt_announcements
    from tests.test_announcement_provider_fallback import _Client, _master, _row_at
    from tests.test_qmt_announcement_pit import _catalog, _install_catalog

    engine = _engine()
    codes = ["000001", "000002"]
    cutoff = datetime(2026, 8, 30, 18, 20)
    received = cutoff + timedelta(hours=6)
    target = cutoff.date()
    start = target - timedelta(days=30)
    catalog = _install_catalog(
        engine, _catalog(*((code, f"{code}.SZ") for code in codes)),
        captured_at="2026-08-30 00:00:00",
    )
    for code in codes:
        finance = {
            "stock_code": code, "report_date": "2026-03-31", "report_type": "Q1",
            "notice_date": "2026-04-25", "roe_wtd": 12.5,
        }
        revision = append_finance_revision(engine, finance, known_at="2026-08-30 18:01:00")
        append_source_coverage(
            engine, fact_kind="finance", stock_code=code, window_start="1900-01-01",
            window_end=target, known_at="2026-08-30 18:01:00",
            covered_through_at="2026-08-30 18:01:00", watermark_kind="CAPTURED_AT",
            watermark_evidence={"source_call": "success"}, source_rows=[finance],
            fact_bindings=[{"revision_id": revision.revision_id, "content_hash": revision.content_hash}],
            source="adata.finance.core_index", batch_id="finance-first",
        )
    append_finance_atomic_batch_seal(
        engine, as_of_date=target, completed_known_at="2026-08-30 18:22:00",
    )
    monkeypatch.setattr("server.common.qmt_announcement_pit._load_catalog", lambda *_: catalog)
    master = _master(*codes)
    provider = CninfoMarketAnnouncementProvider(
        client=_Client(masters=[master, master], rows_by_code={codes[0]: [_row_at(codes[0], 0, cutoff)]}),
        minimum_request_interval=0, now_fn=lambda: received,
    )
    publication = synchronize_qmt_announcements(
        engine, xtdata=ProviderBackedAnnouncementAdapter(provider), checkpoint_root=tmp_path,
        source=CNINFO_ANNOUNCEMENT_SOURCE, fallback_reason="QMT_ANNOUNCEMENT_TERMINAL_DEPENDENCY_UNAVAILABLE",
        capture_fact_cutoff_at=cutoff, now_fn=lambda: received,
    )
    assert publication["status"] == "COMPLETE"
    common_inputs = dict(
        codes=codes, finance_start_date="1900-01-01", finance_end_date=target,
        event_start_date=start, event_end_date=target, require_qmt_event_batch=True,
    )
    assert resolve_common_fact_cutoff(
        engine, **common_inputs, decision_at=received - timedelta(seconds=1),
    )["status"] == PIT_DATA_BLOCKED

    # The source batch already exists in this fixture. A later evaluation
    # still cannot use it before the scheduler verified the actual commit.
    accepted_at = received + timedelta(minutes=1)
    assert resolve_common_fact_cutoff(
        engine, **common_inputs, decision_at=received + timedelta(seconds=1),
    )["status"] == PIT_DATA_BLOCKED

    from server.common import daily_delivery_control as control
    from tests.test_daily_delivery_control import _stage_evidence

    control.privileged_migrate_daily_delivery_schema(engine)
    stage, uid = "qmt_announcement_pit", "1" * 32
    monkeypatch.setattr(control, "_control_now", lambda: received)
    control.start_daily_stage_attempt(
        engine, scheduler_run_uid=uid, stage_name=stage, trade_date=target,
        release_id="a" * 40, strategy_release_id="b" * 64,
        lease_owner="test-windows",
    )
    evidence = _stage_evidence(uid, stage=stage, target=target.isoformat())
    evidence.pop("evidence_sha256")
    output = json.dumps(publication, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(output.encode()).hexdigest()
    evidence.update(replay_output=output, replay_output_sha256=digest, input_receipt_root_sha256=digest)
    evidence["evidence_sha256"] = control.canonical_sha256(evidence)
    with engine.begin() as connection:
        control.finish_daily_stage_attempt(
            connection, scheduler_run_uid=uid, status="success", now=accepted_at,
            input_root_sha256=digest, checkpoint=evidence,
        )
    assert resolve_common_fact_cutoff(
        engine, **common_inputs, decision_at=accepted_at - timedelta(seconds=1),
    )["status"] == PIT_DATA_BLOCKED
    selected = resolve_common_fact_cutoff(
        engine, **common_inputs, decision_at=accepted_at + timedelta(seconds=1),
    )
    assert selected["status"] == PIT_AVAILABLE, selected["reason"]
    assert selected["qmt_event_batch"]["batch_root_hash"] == publication["batch_root_hash"]
    assert selected["qmt_event_batch"]["received_at"] == publication["received_at"]
    result = load_event_facts(
        engine, codes=codes, start_date=start, end_date=target,
        decision_at=accepted_at + timedelta(seconds=1), fact_cutoff_at=cutoff,
        require_qmt_complete_batch=True,
    )
    assert result.status_for(codes[0]) == PIT_AVAILABLE
    assert result.status_for(codes[1]) == PIT_AVAILABLE
    assert result.facts[codes[0]]
    assert result.facts[codes[1]] == []
