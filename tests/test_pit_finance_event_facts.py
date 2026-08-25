from __future__ import annotations

from datetime import datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DatabaseError

from server.common.pit_facts import (
    EVENT_REVISION_TABLE,
    FINANCE_REVISION_TABLE,
    PIT_AVAILABLE,
    PIT_DATA_BLOCKED,
    PIT_NO_ROWS,
    SOURCE_COVERAGE_TABLE,
    TIME_UNVERIFIED,
    append_event_revision,
    append_finance_revision,
    append_source_coverage,
    ensure_pit_fact_schema,
    load_event_facts,
    load_finance_facts,
    load_finance_history_facts,
    pit_fact_schema_health,
    resolve_common_fact_cutoff,
)


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    ensure_pit_fact_schema(engine)
    return engine


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
