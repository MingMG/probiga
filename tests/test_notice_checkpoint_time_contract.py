"""Cross-attempt source reuse must retain independently replayable provenance."""
from datetime import date, datetime, timedelta
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from biz.notice import sync_notice_em as notice
from server.common import scheduler_validation as validation


@pytest.fixture
def resumed_notice(monkeypatch, tmp_path, capsys):
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
        connection.execute(text("CREATE INDEX idx_notice_stock_date "
                                "ON si_notice_eastmoney(stock_code,notice_date)"))
    monkeypatch.setattr(notice, "UPSERT_SQL", text(
        str(notice.UPSERT_SQL).split("ON DUPLICATE KEY UPDATE")[0]))
    captured = datetime(2026, 11, 2, 18, 5, 17)
    clock = [datetime(2026, 11, 2, 18, 6)]
    monkeypatch.setattr(notice, "_shanghai_now", lambda: clock[0])
    fetched = []

    def fetch(_client, code, **kwargs):
        fetched.append(code)
        rows = [{"art_code": "AN-1", "title": "独立原始来源内容",
                 "notice_date": "2026-11-02", "display_time": "2026-11-02 18:01:00",
                 "codes": [{"stock_code": code}]}] if code == "000001" else []
        return notice.NoticeFetchResult(rows=rows, captured_at=captured,
            window_start=kwargs["begin_date"], window_end=kwargs["end_date"],
            exhausted=True, bounded=True, page_count=1, total_hits=len(rows))

    monkeypatch.setattr(notice, "fetch_pages", fetch)

    def run(started):
        assert notice._run_incremental(engine=engine, codes=["000001", "000002"],
            started_at=started, window_start=date(2026, 9, 18), window_end=date(2026, 11, 3),
            target_trade_date=date(2026, 11, 2), page_size=100, max_pages=100,
            sleep_seconds=0, minimum_coverage=1, minimum_row_coverage=0) == 0
        return json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    first = run(datetime(2026, 11, 2, 18))
    assert fetched == ["000001", "000002"]
    fetched.clear()
    clock[0] = datetime(2026, 11, 2, 22, 1)
    started = datetime(2026, 11, 2, 22)
    receipt = run(started)
    assert fetched == [], "verified nonempty and authoritative-empty shards must both resume"
    case = SimpleNamespace(engine=engine, root=tmp_path, first=first, receipt=receipt,
        captured=captured, started=started, finished=clock[0], run=run, clock=clock,
        fetched=fetched)
    yield case
    engine.dispose()


def _db_rows(case):
    with case.engine.connect() as connection:
        return [dict(row) for row in connection.execute(text(
            "SELECT * FROM si_notice_eastmoney ORDER BY stock_code,art_code")).mappings()]


def _validate(case, receipt=None):
    return validation._validate_notice_eastmoney_receipt(case.engine,
        task={"task_type": "notice_eastmoney"}, output=json.dumps(receipt or case.receipt),
        started_at=case.started, now=case.finished)


def _resign_with_current_database_hash(case):
    receipt = dict(case.receipt)
    receipt["persisted_manifest_sha256"] = validation._notice_persisted_manifest_hash(
        _db_rows(case), requested_codes=["000001", "000002"])
    receipt["result_sha256"] = notice._sha256({key: value for key, value in receipt.items()
                                               if key != "result_sha256"})
    return receipt


def test_cross_attempt_resume_preserves_source_and_publication_before_attempt(resumed_notice):
    case = resumed_notice
    row, = _db_rows(case)
    assert validation._coerce_datetime(row["source_time"]) == datetime(2026, 11, 2, 18, 1)
    assert validation._coerce_datetime(row["received_at"]) == case.captured < case.started
    assert case.captured <= validation._coerce_datetime(row["etl_sync_at"]) < case.started
    assert row["batch_id"] == case.first["batch_id"] != case.receipt["batch_id"]
    assert case.receipt["written_notice_count"] == 1
    assert case.receipt["authoritative_empty_code_count"] == 1
    ok, message = _validate(case)
    assert ok, message


@pytest.mark.parametrize("delta_seconds", [-60, 60])
def test_forged_old_capture_fails_even_with_matching_database_and_receipt_content_hash(
    resumed_notice, delta_seconds,
):
    case = resumed_notice
    with case.engine.begin() as connection:
        connection.execute(text("UPDATE si_notice_eastmoney SET received_at=:value"),
                           {"value": case.captured + timedelta(seconds=delta_seconds)})
    ok, _message = _validate(case, _resign_with_current_database_hash(case))
    assert not ok, "an independently replayed raw shard must bind the original capture"


@pytest.mark.parametrize("value", [datetime(2026, 11, 2, 18, 5, 16),
                                    datetime(2026, 11, 2, 22, 2)])
def test_publication_cannot_precede_capture_or_exceed_receipt_finish(resumed_notice, value):
    case = resumed_notice
    with case.engine.begin() as connection:
        connection.execute(text("UPDATE si_notice_eastmoney SET etl_sync_at=:value"),
                           {"value": value})
    ok, _message = _validate(case)
    assert not ok


def test_source_publication_is_replayed_instead_of_trusted_from_rehashed_database(resumed_notice):
    case = resumed_notice
    with case.engine.begin() as connection:
        connection.execute(text("UPDATE si_notice_eastmoney SET source_time='2026-11-02 18:02:00'"))
    ok, _message = _validate(case, _resign_with_current_database_hash(case))
    assert not ok


@pytest.mark.parametrize("code", ["000001", "000002"])
def test_missing_nonempty_or_authoritative_empty_source_shard_rejects_prior_pass(
    resumed_notice, code,
):
    from server.common.acquisition_shards import AcquisitionShards

    case = resumed_notice
    shards = AcquisitionShards("eastmoney-notice-incremental", {
        "window_start": "2026-09-18", "window_end": "2026-11-03",
        "target": "2026-11-02", "data_version": notice.NOTICE_DATA_VERSION,
    }, create=False)
    assert shards.load(code) is not None
    shards._path(code).unlink()
    ok, _message = _validate(case)
    assert not ok, "empty responses require retained source proof too"


@pytest.mark.parametrize("mutation", ["capture", "raw_content", "pagination"])
def test_resealed_but_changed_shard_cannot_satisfy_the_original_source_root(
    resumed_notice, mutation,
):
    from server.common.acquisition_shards import AcquisitionShards

    case = resumed_notice
    shards = AcquisitionShards("eastmoney-notice-incremental", {
        "window_start": "2026-09-18", "window_end": "2026-11-03",
        "target": "2026-11-02", "data_version": notice.NOTICE_DATA_VERSION,
    }, create=False)
    saved = shards.load("000001")
    assert saved is not None
    if mutation == "capture":
        saved["captured_at"] = (case.captured + timedelta(seconds=60)).isoformat()
    elif mutation == "raw_content":
        saved["rows"][0]["title"] = "changed durable source content"
    else:
        saved["total_hits"] += 1
    shards.save("000001", saved)
    ok, _message = _validate(case)
    assert not ok


@pytest.mark.parametrize("mutation", ["capture", "raw_only_value"])
def test_authoritative_empty_or_unprojected_raw_values_are_bound_by_the_source_root(
    resumed_notice, mutation,
):
    from server.common.acquisition_shards import AcquisitionShards

    case = resumed_notice
    shards = AcquisitionShards("eastmoney-notice-incremental", {
        "window_start": "2026-09-18", "window_end": "2026-11-03",
        "target": "2026-11-02", "data_version": notice.NOTICE_DATA_VERSION,
    }, create=False)
    code = "000002" if mutation == "capture" else "000001"
    saved = shards.load(code)
    assert saved is not None
    if mutation == "capture":
        assert saved["rows"] == []
        saved["captured_at"] = (case.captured + timedelta(seconds=60)).isoformat()
    else:
        # The retained raw source, including information not projected into
        # the business table, has to be the exact source that was published.
        saved["rows"][0]["source_only_value"] = "new value after publication"
    shards.save(code, saved)
    ok, _message = _validate(case)
    assert not ok


def test_old_parsed_only_source_root_cannot_claim_the_final_capture_contract(resumed_notice):
    case = resumed_notice
    shards = notice._notice_incremental_shards(window_start=date(2026, 9, 18),
        window_end=date(2026, 11, 3), target_trade_date=date(2026, 11, 2), create=False)
    old_entries = []
    for code in ("000001", "000002"):
        _fetch, _rows, entry = notice._notice_source_snapshot(code, shards.load(code),
            window_start=date(2026, 9, 18), window_end=date(2026, 11, 3),
            batch_id=case.receipt["batch_id"], finished_at=case.finished)
        old_entries.append({key: value for key, value in entry.items()
                            if key not in {"captured_at", "raw_sha256"}})
    receipt = dict(case.receipt)
    receipt["source_manifest_sha256"] = notice._sha256(old_entries)
    receipt["result_sha256"] = notice._sha256({key: value for key, value in receipt.items()
                                               if key != "result_sha256"})
    ok, message = _validate(case, receipt)
    assert not ok and "source checkpoint" in message


@pytest.mark.parametrize("start_us,finish_us", [(100000, 200000), (500000, 600000)])
def test_prior_second_precision_publication_survives_fractional_attempt_clock(
    resumed_notice, start_us, finish_us,
):
    case = resumed_notice
    # Existing MySQL DATETIME(0) values remain valid even when a new attempt's
    # local clock has fractions; resuming must not rewrite those old values.
    case.started = datetime(2026, 11, 2, 22, 2, 0, start_us)
    case.finished = datetime(2026, 11, 2, 22, 2, 0, finish_us)
    case.clock[0] = case.finished
    case.receipt = case.run(case.started)
    assert case.fetched == []
    row, = _db_rows(case)
    assert validation._coerce_datetime(row["etl_sync_at"]) == datetime(2026, 11, 2, 18, 6)
    ok, message = _validate(case)
    assert ok, message
