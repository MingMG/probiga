"""Regression for the completed 2026-11-02 collector's MySQL readback timeout."""
from datetime import date, datetime, timedelta
import json
import zlib

import pytest
from sqlalchemy import create_engine, event, text

from biz.notice import sync_notice_em as notice
from server.common import scheduler_validation as validation


@pytest.fixture(autouse=True)
def _isolated_source_shards(monkeypatch, tmp_path):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))


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
    shards = notice._notice_incremental_shards(window_start=date(2026, 9, 18),
        window_end=date(2026, 11, 3), target_trade_date=date(2026, 11, 2))
    source_manifest = []
    for row in rows:
        code = row["stock_code"]
        saved = {"rows": [{"art_code": row["art_code"], "title": row["title"],
            "notice_date": str(row["notice_date"]), "display_time": row["display_time"],
            "columns": [{"column_name": row["column_name"]}], "codes": [{"stock_code": code}]}],
            "captured_at": captured.isoformat(), "total_hits": 1, "page_count": 1}
        shards.save(code, saved)
        _fetch, parsed, entry = notice._notice_source_snapshot(code, saved,
            window_start=date(2026, 9, 18), window_end=date(2026, 11, 3),
            batch_id="c" * 64, finished_at=captured)
        row.update({field: parsed[0][field] for field in row if field in parsed[0]})
        source_manifest.append(entry)
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
        source_manifest=source_manifest,
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
    assert batches == [50, 50, 50, 50, 5]
    assert len(codes) == 205
    searches = [plan for plan in plans if "SEARCH notice" in plan]
    assert len(searches) == 5
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
    clock = iter([0.0, 1.0, 901.0])
    monkeypatch.setattr(validation, "monotonic", lambda: next(clock))
    with pytest.raises(RuntimeError, match="notice_readback: total deadline exceeded"):
        validation._read_notice_validation_window(engine, codes=codes,
            window_start=date(2026, 9, 18), window_end=date(2026, 11, 3))
    engine.dispose()


def _compressed_group(code, rows, *, padding=b""):
    raw = json.dumps(rows, ensure_ascii=False, default=str).encode("utf8") + padding
    compressed = zlib.compress(raw)
    trailer = b"." if compressed[-1] == 0x20 else b""
    return {"stock_code": code, "row_count": len(rows),
            "packed_rows": len(raw).to_bytes(4, "little") + compressed + trailer}


def test_mysql_group_decode_preserves_all_fields_and_existing_value_hash():
    engine, codes, _receipt = _fixture()
    rows = validation._read_notice_validation_window(engine, codes=codes[:1],
        window_start=date(2026, 9, 18), window_end=date(2026, 11, 3))
    mysql_rows = [dict(row) for row in rows]
    for row in mysql_rows:
        for field in ("etl_sync_at", "source_time", "received_at"):
            row[field] += ".000000"
    decoded = validation._decode_notice_readback_group(_compressed_group(codes[0], mysql_rows),
                                                       expected_codes={codes[0]})
    assert set(decoded[0]) == set(validation._NOTICE_READBACK_FIELDS)
    assert decoded[0]["display_time"] == rows[0]["display_time"]
    assert notice._notice_row_hash(decoded) == notice._notice_row_hash(rows)
    assert validation._notice_persisted_manifest_hash(decoded, requested_codes=codes[:1]) == (
        validation._notice_persisted_manifest_hash(rows, requested_codes=codes[:1]))
    engine.dispose()


def test_mysql_native_space_protection_trailer_is_verified():
    row = {field: None for field in validation._NOTICE_READBACK_FIELDS}
    row.update(stock_code="000001", art_code="AN-1", association_validated=1)
    # MySQL adds '.' when the *compressed stream*, not input, ends in 0x20.
    group = next(candidate for size in range(512)
                 if (candidate := _compressed_group("000001", [row], padding=b"\t" * size))[
                     "packed_rows"].endswith(b" ."))
    assert validation._decode_notice_readback_group(group, expected_codes={"000001"}) == [row]
    missing_trailer = {**group, "packed_rows": group["packed_rows"][:-1]}
    with pytest.raises(RuntimeError, match="trailing bytes"):
        validation._decode_notice_readback_group(missing_trailer, expected_codes={"000001"})
