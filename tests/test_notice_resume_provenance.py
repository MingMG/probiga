"""Reproduce a resumed source snapshot's mismatch with attempt timestamps."""
from datetime import date, datetime
import json

from sqlalchemy import create_engine, text

from biz.notice import sync_notice_em as notice
from server.common import scheduler_validation as validation


def test_verified_resume_preserves_capture_and_validates_actual_new_publication(
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
    clock[0] = datetime(2026, 11, 2, 22, 1)
    resumed, ok, message = run(datetime(2026, 11, 2, 22))
    assert fetched == []  # Both nonempty and authoritative-empty shards resume.
    assert resumed["written_notice_count"] == 1
    assert resumed["succeeded_code_count"] == 2
    assert resumed["batch_id"] != first["batch_id"]
    with engine.connect() as conn:
        row = dict(conn.execute(text("SELECT * FROM si_notice_eastmoney")).mappings().one())
    assert row["batch_id"] == resumed["batch_id"]
    assert validation._coerce_datetime(row["source_time"]) == datetime(2026, 11, 2, 18, 1)
    assert validation._coerce_datetime(row["received_at"]) == captured
    assert validation._coerce_datetime(row["etl_sync_at"]) == clock[0]
    assert resumed["persisted_manifest_sha256"] == validation._notice_persisted_manifest_hash(
        [row], requested_codes=["000001", "000002"],
    )
    assert ok, message
    engine.dispose()
