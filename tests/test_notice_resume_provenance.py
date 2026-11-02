"""Reproduce a resumed source snapshot's mismatch with attempt timestamps."""
from datetime import date, datetime
from dataclasses import replace
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text

from biz.notice import sync_notice_em as notice
from server.common import scheduler_validation as validation


def test_verified_resume_preserves_verified_publication_without_writes(
    monkeypatch, tmp_path, capsys,
):
    """Actual shards + parse + transactional replacement + final DB readback."""
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE si_all_code (stock_code TEXT PRIMARY KEY)"))
        conn.execute(text("INSERT INTO si_all_code VALUES ('000001'),('000002')"))
        conn.execute(text("""CREATE TABLE si_notice_eastmoney (
            stock_code TEXT, art_code TEXT, notice_date DATE, title TEXT, column_name TEXT,
            display_time TEXT, detail_url TEXT, association_validated INTEGER, etl_sync_at DATETIME,
            qmt_code TEXT, data_source TEXT, source_time DATETIME, received_at DATETIME,
            batch_id TEXT, data_version TEXT, quality_status TEXT, permission_status TEXT,
            PRIMARY KEY(stock_code,art_code))"""))
        conn.execute(text("CREATE INDEX idx_notice_stock_date ON si_notice_eastmoney(stock_code,notice_date)"))
    # Replacement first deletes this exact scope, so a plain SQLite INSERT
    # exercises the same transaction while avoiding MySQL-only UPSERT syntax.
    monkeypatch.setattr(notice, "UPSERT_SQL", text(str(notice.UPSERT_SQL).split("ON DUPLICATE KEY UPDATE")[0]))
    captured = datetime(2026, 11, 2, 18, 5)
    clock = [datetime(2026, 11, 2, 18, 6)]
    monkeypatch.setattr(notice, "_shanghai_now", lambda: clock[0])
    fetched = []

    def fetch(_client, code, **kwargs):
        fetched.append(code)
        return notice.NoticeFetchResult(
            rows=[{"art_code": "AN-1", "title": "真实来源公告",
                   "notice_date": "2026-11-02", "display_time": "2026-11-02 18:01:00",
                   "codes": [{"stock_code": code}]}] if code == "000001" else [],
            captured_at=captured, window_start=kwargs["begin_date"],
            window_end=kwargs["end_date"], exhausted=True, bounded=True,
            page_count=1, total_hits=1 if code == "000001" else 0,
        )

    monkeypatch.setattr(notice, "fetch_pages", fetch)

    def run(started):
        assert notice._run_incremental(
            engine=engine, codes=["000001", "000002"], started_at=started,
            window_start=date(2026, 9, 18), window_end=date(2026, 11, 3),
            target_trade_date=date(2026, 11, 2), page_size=100, max_pages=100,
            sleep_seconds=0, minimum_coverage=1, minimum_row_coverage=0,
        ) == 0
        output = capsys.readouterr().out.strip().splitlines()[-1]
        receipt = json.loads(output)
        assert receipt["status"] == "PASS"
        ok, message = validation._validate_notice_eastmoney_receipt(
            engine, task={"task_type": "notice_eastmoney"}, output=output,
            started_at=started, now=clock[0],
        )
        return receipt, ok, message

    first, ok, message = run(datetime(2026, 11, 2, 18))
    assert ok, message
    assert fetched == ["000001", "000002"]
    fetched.clear()
    statements = []
    event.listen(engine, "before_cursor_execute",
        lambda _conn, _cursor, statement, _params, _context, _many: statements.append(statement))
    clock[0] = datetime(2026, 11, 2, 22, 1)
    resumed, ok, message = run(datetime(2026, 11, 2, 22))
    assert fetched == []  # Both nonempty and authoritative-empty shards resume.
    assert resumed["written_notice_count"] == 1
    assert resumed["succeeded_code_count"] == 2
    assert resumed["reused_code_count"] == 2
    assert resumed["reused_notice_count"] == 1
    assert resumed["newly_written_notice_count"] == 0
    assert not any(sql.lstrip().upper().startswith(("DELETE", "INSERT", "UPDATE"))
                   for sql in statements)
    assert resumed["batch_id"] != first["batch_id"]
    with engine.connect() as conn:
        row = dict(conn.execute(text("SELECT * FROM si_notice_eastmoney")).mappings().one())
    assert row["batch_id"] == first["batch_id"]
    assert validation._coerce_datetime(row["source_time"]) == datetime(2026, 11, 2, 18, 1)
    assert validation._coerce_datetime(row["received_at"]) == captured
    assert validation._coerce_datetime(row["etl_sync_at"]) == datetime(2026, 11, 2, 18, 6)
    assert resumed["persisted_manifest_sha256"] == validation._notice_persisted_manifest_hash(
        [row], requested_codes=["000001", "000002"],
    )
    assert ok, message
    engine.dispose()


@pytest.fixture
def notice_partitions(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_all_code (stock_code TEXT PRIMARY KEY)"))
        connection.execute(text("INSERT INTO si_all_code VALUES ('000001'),('000002')"))
        connection.execute(text("""CREATE TABLE si_notice_eastmoney (
            stock_code TEXT, art_code TEXT, notice_date DATE, title TEXT, column_name TEXT,
            display_time TEXT, detail_url TEXT, association_validated INTEGER, etl_sync_at DATETIME,
            qmt_code TEXT, data_source TEXT, source_time DATETIME, received_at DATETIME,
            batch_id TEXT, data_version TEXT, quality_status TEXT, permission_status TEXT,
            PRIMARY KEY(stock_code,art_code))"""))
        connection.execute(text("CREATE INDEX idx_notice_stock_date ON "
                                "si_notice_eastmoney(stock_code,notice_date)"))
    monkeypatch.setattr(notice, "UPSERT_SQL", text(
        str(notice.UPSERT_SQL).split("ON DUPLICATE KEY UPDATE")[0]))
    clock = [datetime(2026, 11, 2, 18, 6)]
    monkeypatch.setattr(notice, "_shanghai_now", lambda: clock[0])
    fetched = []
    statements = []
    event.listen(engine, "before_cursor_execute",
        lambda _conn, _cursor, statement, params, _context, _many:
            statements.append((statement, params)))

    def fetch(_client, code, **kwargs):
        fetched.append(code)
        return notice.NoticeFetchResult(
            rows=[{"art_code": "AN-" + code, "title": "原始" + code,
                   "notice_date": "2026-11-02", "display_time": "2026-11-02 18:01:00",
                   "codes": [{"stock_code": code}]}],
            captured_at=datetime(2026, 11, 2, 18, 5), window_start=kwargs["begin_date"],
            window_end=kwargs["end_date"], exhausted=True, bounded=True,
            page_count=1, total_hits=1)

    monkeypatch.setattr(notice, "fetch_pages", fetch)

    def run(started):
        status = notice._run_incremental(engine=engine, codes=["000001", "000002"],
            started_at=started, window_start=date(2026, 9, 18), window_end=date(2026, 11, 3),
            target_trade_date=date(2026, 11, 2), page_size=100, max_pages=100,
            sleep_seconds=0, minimum_coverage=1, minimum_row_coverage=0)
        receipt = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
        return status, receipt

    def rows():
        with engine.connect() as connection:
            return [dict(row) for row in connection.execute(text(
                "SELECT * FROM si_notice_eastmoney ORDER BY stock_code")).mappings()]

    yield SimpleNamespace(engine=engine, clock=clock, fetched=fetched,
        statements=statements, run=run, rows=rows)
    engine.dispose()


def test_partial_publication_restart_reuses_commit_and_finishes_missing_partition(
    notice_partitions, monkeypatch,
):
    case = notice_partitions
    original = notice.reconcile_rows

    def interrupted(engine, rows, **kwargs):
        if kwargs["stock_code"] == "000002":
            raise KeyboardInterrupt("process exited after first durable partition")
        return original(engine, rows, **kwargs)

    monkeypatch.setattr(notice, "reconcile_rows", interrupted)
    with pytest.raises(KeyboardInterrupt):
        case.run(datetime(2026, 11, 2, 18))
    original_row, = case.rows()
    assert case.fetched == ["000001", "000002"]
    monkeypatch.setattr(notice, "reconcile_rows", original)
    case.fetched.clear()
    case.statements.clear()
    case.clock[0] = datetime(2026, 11, 3, 5)
    started = datetime(2026, 11, 3, 4, 59)
    status, receipt = case.run(started)
    assert status == 0 and case.fetched == []
    assert case.rows()[0] == original_row
    assert case.rows()[1]["batch_id"] == receipt["batch_id"] != original_row["batch_id"]
    assert receipt["written_notice_count"] == 2
    assert receipt["reused_code_count"] == receipt["reused_notice_count"] == 1
    assert receipt["newly_written_notice_count"] == 1
    writes = [sql for sql, _params in case.statements
              if sql.lstrip().upper().startswith(("INSERT", "DELETE"))]
    assert len(writes) == 2  # Only the missing partition's atomic DELETE/INSERT.
    assert receipt["persisted_manifest_sha256"] == validation._notice_persisted_manifest_hash(
        case.rows(), requested_codes=["000001", "000002"])
    ok, message = validation._validate_notice_eastmoney_receipt(case.engine,
        task={"task_type": "notice_eastmoney"}, output=json.dumps(receipt),
        started_at=started, now=case.clock[0])
    assert ok, message


@pytest.mark.parametrize("change", ["database_content", "source_content", "data_version"])
def test_resume_rewrites_only_a_changed_database_or_source_partition(
    notice_partitions, change,
):
    case = notice_partitions
    assert case.run(datetime(2026, 11, 2, 18))[0] == 0
    original_rows = case.rows()
    if change == "source_content":
        shards = notice._notice_incremental_shards(window_start=date(2026, 9, 18),
            window_end=date(2026, 11, 3), target_trade_date=date(2026, 11, 2))
        source = shards.load("000001")
        source["rows"][0]["title"] = "源端已修订的实际内容"
        source["captured_at"] = datetime(2026, 11, 2, 21, 5).isoformat()
        shards.save("000001", source)
    else:
        with case.engine.begin() as connection:
            update = ("title='被改坏'" if change == "database_content"
                      else "data_version='obsolete-version'")
            connection.execute(text("UPDATE si_notice_eastmoney SET " + update
                                    + " WHERE stock_code='000001'"))
    case.clock[0] = datetime(2026, 11, 2, 22, 1)
    case.fetched.clear()
    status, receipt = case.run(datetime(2026, 11, 2, 22))
    assert status == 0 and case.fetched == []
    repaired, untouched = case.rows()
    assert untouched == original_rows[1]
    assert repaired["title"] == ("源端已修订的实际内容" if change == "source_content" else "原始000001")
    assert repaired["data_version"] == notice.NOTICE_DATA_VERSION
    assert repaired["batch_id"] == receipt["batch_id"] != original_rows[0]["batch_id"]
    assert receipt["newly_written_notice_count"] == receipt["reused_notice_count"] == 1
    ok, message = validation._validate_notice_eastmoney_receipt(case.engine,
        task={"task_type": "notice_eastmoney"}, output=json.dumps(receipt),
        started_at=datetime(2026, 11, 2, 22), now=case.clock[0])
    assert ok, message


@pytest.mark.parametrize("source_publication_known", [False, True])
@pytest.mark.parametrize("checkpoint_retained", [False, True])
def test_identical_reobserved_source_keeps_first_database_capture_and_publication(
    notice_partitions, source_publication_known, checkpoint_retained, monkeypatch,
):
    case = notice_partitions
    if not source_publication_known:
        fetch_with_publication = notice.fetch_pages

        def without_publication(*args, **kwargs):
            response = fetch_with_publication(*args, **kwargs)
            response.rows[0]["display_time"] = ""
            return response

        monkeypatch.setattr(notice, "fetch_pages", without_publication)
    assert case.run(datetime(2026, 11, 2, 18))[0] == 0
    original_rows = case.rows()
    shards = notice._notice_incremental_shards(window_start=date(2026, 9, 18),
        window_end=date(2026, 11, 3), target_trade_date=date(2026, 11, 2))
    if checkpoint_retained:
        for code in ("000001", "000002"):
            source = shards.load(code)
            source["captured_at"] = datetime(2026, 11, 2, 21, 5).isoformat()
            shards.save(code, source)
    else:
        for code in ("000001", "000002"):
            shards._path(code).unlink()
        fetch_again = notice.fetch_pages

        def reobserved(*args, **kwargs):
            return replace(fetch_again(*args, **kwargs),
                           captured_at=datetime(2026, 11, 2, 21, 5))

        monkeypatch.setattr(notice, "fetch_pages", reobserved)
    case.fetched.clear()
    case.statements.clear()
    case.clock[0] = datetime(2026, 11, 3, 5)
    started = datetime(2026, 11, 3, 4, 59)
    status, receipt = case.run(started)
    assert status == 0
    assert case.fetched == ([] if checkpoint_retained else ["000001", "000002"])
    assert case.rows() == original_rows
    assert receipt["newly_written_notice_count"] == 0
    assert receipt["reused_notice_count"] == receipt["reused_code_count"] == 2
    assert not any(sql.lstrip().upper().startswith(("INSERT", "DELETE", "UPDATE"))
                   for sql, _params in case.statements)
    ok, message = validation._validate_notice_eastmoney_receipt(case.engine,
        task={"task_type": "notice_eastmoney"}, output=json.dumps(receipt),
        started_at=started, now=case.clock[0])
    assert ok, message
