"""Regression for the completed 2026-11-02 collector's MySQL readback timeout."""
from datetime import date, datetime, timedelta
import json

import pytest
from sqlalchemy import create_engine, event, text

from biz.notice import sync_notice_em as notice
from server.common import scheduler_validation as validation


def _fixture():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    captured = datetime(2026, 11, 2, 21, 0)
    codes = [f"{index:06d}" for index in range(1, 206)]
    rows = [{"stock_code": code, "art_code": "AN-" + code,
        "notice_date": date(2026, 11, 2), "title": "真实公告", "column_name": "公司公告",
        "display_time": "2026-11-02 20:00:00", "detail_url": "https://data.eastmoney.com/" + code,
        "association_validated": 1, "etl_sync_at": captured,
        "qmt_code": code + ".SZ", "data_source": notice.NOTICE_PROVIDER_ID,
        "source_time": captured, "received_at": captured, "batch_id": "c" * 64,
        "data_version": notice.NOTICE_DATA_VERSION, "quality_status": notice.NOTICE_QUALITY_STATUS,
        "permission_status": notice.NOTICE_PERMISSION_STATUS} for code in codes]
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE si_all_code (stock_code TEXT PRIMARY KEY)"))
        conn.execute(text("INSERT INTO si_all_code VALUES (:code)"), [{"code": code} for code in codes])
        conn.execute(text("""CREATE TABLE si_notice_eastmoney (
            stock_code TEXT, art_code TEXT, notice_date DATE, title TEXT, column_name TEXT,
            display_time TEXT, detail_url TEXT, association_validated INTEGER, etl_sync_at DATETIME,
            qmt_code TEXT, data_source TEXT, source_time DATETIME, received_at DATETIME,
            batch_id TEXT, data_version TEXT, quality_status TEXT, permission_status TEXT,
            PRIMARY KEY(stock_code,art_code))"""))
        conn.execute(text("CREATE INDEX idx_notice_stock_date ON si_notice_eastmoney(stock_code,notice_date)"))
        columns = list(rows[0])
        insert = text("INSERT INTO si_notice_eastmoney (" + ",".join(columns) + ") VALUES (" +
                      ",".join(":" + column for column in columns) + ")")
        conn.execute(insert, rows)
        # The original plan scanned each stock's entire old history and then
        # filtered the 45-day date range. Keep substantial out-of-window data.
        conn.execute(insert, [{**row, "art_code": f"OLD-{i}", "notice_date": date(2020, 1, 1) + timedelta(days=i)}
                              for row in rows for i in range(80)])
        conn.execute(insert, [{**rows[0], "stock_code": "999999", "art_code": "OUTSIDE-CATALOG"}])
    receipt = notice._notice_sync_result(started_at=datetime(2026, 11, 2, 18), finished_at=captured,
        codes=codes, succeeded_codes=codes, nonempty_codes=codes, failed_codes=[], failure_sample=[],
        written_rows=len(rows), minimum_coverage=1.0, minimum_row_coverage=0.0,
        request_window_start=date(2026, 9, 18), request_window_end=date(2026, 11, 3),
        target_trade_date=date(2026, 11, 2), batch_id="c" * 64,
        persisted_manifest=[{"stock_code": row["stock_code"], "row_count": 1,
                             "row_hash": notice._notice_row_hash([row])} for row in rows])
    return engine, codes, receipt


def test_completed_notice_batch_revalidates_with_real_stock_date_index_and_exact_hashes():
    engine, codes, receipt = _fixture()
    plans, batches = [], []
    @event.listens_for(engine, "before_cursor_execute")
    def inspect(conn, _cursor, sql, params, _context, _many):
        if "FROM si_notice_eastmoney AS notice" in sql and not sql.lstrip().startswith("EXPLAIN"):
            plans.extend(row[-1] for row in conn.exec_driver_sql("EXPLAIN QUERY PLAN " + sql, params))
            batches.append(len(params) - 2)
    ok, message = validation._validate_notice_eastmoney_receipt(engine,
        task={"task_type": "notice_eastmoney"}, output=json.dumps(receipt, default=str),
        started_at=datetime(2026, 11, 2, 18), now=datetime(2026, 11, 2, 21, 1))
    assert ok, message
    assert batches == [100, 100, 5]
    assert len(codes) == 205
    searches = [plan for plan in plans if "SEARCH notice" in plan]
    assert len(searches) == 3
    assert all("idx_notice_stock_date" in plan and "stock_code=?" in plan
               and "notice_date>?" in plan and "notice_date<?" in plan for plan in searches)
    # A wrong batch in any later code batch is still read, never hidden by
    # filtering to the expected batch_id, and prevents a false PASS.
    with engine.begin() as conn:
        conn.execute(text("UPDATE si_notice_eastmoney SET batch_id=:bad WHERE stock_code=:code AND notice_date=:day"),
                     {"bad": "d" * 64, "code": codes[-1], "day": date(2026, 11, 2)})
    ok, message = validation._validate_notice_eastmoney_receipt(engine,
        task={"task_type": "notice_eastmoney"}, output=json.dumps(receipt, default=str),
        started_at=datetime(2026, 11, 2, 18), now=datetime(2026, 11, 2, 21, 1))
    assert not ok and "invalid association or provenance" in message
    engine.dispose()


def test_notice_readback_stops_at_total_deadline_after_one_real_batch(monkeypatch):
    engine, codes, _receipt = _fixture()
    clock = iter([0.0, 1.0, 181.0])
    monkeypatch.setattr(validation, "monotonic", lambda: next(clock))
    with pytest.raises(RuntimeError, match="notice_readback: total deadline exceeded"):
        validation._read_notice_validation_window(engine, codes=codes,
            window_start=date(2026, 9, 18), window_end=date(2026, 11, 3))
    engine.dispose()
