from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.exc import OperationalError

from server.common.qmt_announcement_pit import QMTAnnouncementBlocked
from server.common.pit_source_coverage_index import (
    PIT_SOURCE_COVERAGE_DATE_INDEX_COLUMNS,
    PIT_SOURCE_COVERAGE_DATE_INDEX_NAME,
)
from tools.sync_qmt_announcement_pit import _existing_live_batch_inventory


def test_dated_inventory_retains_the_legacy_batch_window_without_scanning_history():
    engine = create_engine("sqlite://")
    source = "cninfo.announcement"
    legacy = "cninfo-ann-legacy-without-timestamp"
    newer = "cninfo-ann-z-newer"
    target = date(2026, 11, 12)
    decision_at = datetime(2026, 11, 13, 0, 14, 37)
    capture_deadline = datetime(2026, 11, 12, 23)

    def row(
        batch, known, code="000001", count=0, *,
        source=source, status="COMPLETE", watermark="QUERY_CUTOFF",
    ):
        return (
            "event", source, watermark, known, known, "2026-11-12 15:00:00",
            "2026-10-01", "2026-11-12", status, count, batch, code,
        )

    original_query = False

    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def original_date_predicate(connection, cursor, sql, parameters, context, executemany):
        if original_query and "GROUP BY batch_id" in sql:
            assert "AND known_at>=? AND known_at<=?" in sql
            sql = sql.replace(
                "AND known_at>=? AND known_at<=?",
                "AND DATE(known_at)>=DATE(?) AND known_at<=? AND DATE(known_at)<=DATE(?)",
            )
            parameters = (*parameters[:4], parameters[3], *parameters[4:])
        return sql, parameters

    with engine.begin() as connection:
        connection.exec_driver_sql("""
            CREATE TABLE st_pit_source_coverage (
                fact_kind TEXT, source TEXT, watermark_kind TEXT, known_at DATETIME,
                received_at DATETIME, covered_through_at DATETIME, window_start DATE,
                window_end DATE, coverage_status TEXT, result_count INTEGER,
                batch_id TEXT, stock_code TEXT
            )
        """)
        connection.exec_driver_sql(
            f"CREATE INDEX {PIT_SOURCE_COVERAGE_DATE_INDEX_NAME} "
            f"ON st_pit_source_coverage ({','.join(PIT_SOURCE_COVERAGE_DATE_INDEX_COLUMNS)})"
        )
        connection.exec_driver_sql(
            "INSERT INTO st_pit_source_coverage VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            [
                row(f"cninfo-ann-old-{index:05d}", "2026-11-11 23:59:59.999999")
                for index in range(6000)
            ] + [
                row(legacy, "2026-11-12 00:00:00.000000"),
                row(legacy, "2026-11-12 00:00:00.000000", "000002", 2),
                row(newer, "2026-11-12 22:00:00.000000", count=3),
                row("cninfo-ann-invalid", "2026-11-12 22:30:00", status="PARTIAL"),
                row("cninfo-ann-wrong-watermark", "2026-11-12 22:30:00", watermark="HISTORICAL"),
                row("cninfo-ann-foreign", "2026-11-12 22:30:00", source="eastmoney.notice"),
                row("unrelated-provider-batch", "2026-11-12 22:30:00"),
                row("cninfo-ann-at-deadline", "2026-11-12 23:00:00"),
                row("cninfo-ann-midnight-readback", "2026-11-13 00:14:37"),
                row("cninfo-ann-future", "2026-11-13 00:14:37.000001"),
            ],
        )

        def inventory(deadline):
            return _existing_live_batch_inventory(
                connection, source=source, target=target, decision_at=decision_at,
                capture_deadline=deadline,
            )

        original_query = True
        expected = inventory(capture_deadline)
        assert [batch["batch_id"] for batch in expected] == [newer, legacy]
        assert expected[1]["coverage_row_count"] == expected[1]["distinct_stock_count"] == 2
        assert expected[1]["event_count"] == 2
        assert expected[1]["empty_stock_count"] == 1
        dbapi = connection.connection.driver_connection
        try:
            dbapi.set_progress_handler(lambda: 1, 4000)
            with pytest.raises(QMTAnnouncementBlocked) as failure:
                inventory(capture_deadline)
            assert failure.value.reason_code == "QMT_ANNOUNCEMENT_EXISTING_BATCH_INVENTORY_FAILED"
            assert failure.value.detail == "cninfo.announcement:OperationalError"
            original_query = False
            assert inventory(capture_deadline) == expected
            assert [batch["batch_id"] for batch in inventory(None)] == [
                "cninfo-ann-midnight-readback", "cninfo-ann-at-deadline",
            ]
        finally:
            dbapi.set_progress_handler(None, 0)


def test_inventory_deadline_is_a_specific_failure_with_safe_driver_metadata():
    engine = create_engine("sqlite://")

    @event.listens_for(engine, "before_cursor_execute")
    def simulate_server_deadline(connection, cursor, sql, parameters, context, executemany):
        raise OperationalError(sql, parameters, RuntimeError(3024, "private driver text"))

    with engine.connect() as connection, pytest.raises(QMTAnnouncementBlocked) as failure:
        _existing_live_batch_inventory(
            connection, source="cninfo.announcement", target=date(2026, 11, 12),
            decision_at=datetime(2026, 11, 13, 0, 14, 37), capture_deadline=None,
        )
    assert str(failure.value) == (
        "QMT_ANNOUNCEMENT_EXISTING_BATCH_INVENTORY_FAILED:cninfo.announcement:OperationalError:errno=3024"
    )
