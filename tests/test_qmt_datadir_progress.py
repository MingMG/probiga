"""Minute progress reflects committed staging and streamed readback work."""
import datetime as dt
import json

import pytest
from sqlalchemy import create_engine

from integrations.qmt import datadir_import as importer
from server.common import qmt_datadir_minute_coverage as coverage
from server.common.qmt_datadir_progress import MinuteImportProgress


RUN_ID = "qdd_20261102220000_1234abcd"


def _events(tmp_path):
    path = tmp_path / "qmt-datadir-progress" / (RUN_ID + ".jsonl")
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_progress_is_immediately_durable_without_console_or_terminal_receipt_growth(
    monkeypatch, tmp_path, capsys,
):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    progress = MinuteImportProgress(run_id=RUN_ID, total_units=1342611,
        operation="harvest", days=["2026-11-02"])
    first = progress.emit("source_read")
    advanced = progress.emit("stage_commit", completed_units=50000, current_unit="000001")
    waiting = progress.emit("stage_commit", current_unit="000002")
    events = _events(tmp_path)
    assert events == [first, advanced, waiting]
    assert waiting["completed_units"] == 50000
    assert waiting["last_progress_at"] == advanced["last_progress_at"]
    assert waiting["checkpoint"] == {"table": "qmt_datadir_minute_stage", "run_id": RUN_ID}
    assert waiting["total_units"] == 1342611
    assert capsys.readouterr().out == ""


def test_harvest_progress_only_counts_rows_after_actual_sqlite_stage_commit(monkeypatch, tmp_path):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    engine = create_engine("sqlite+pysqlite:///" + str(tmp_path / "stage.sqlite"))
    with engine.begin() as connection:
        connection.exec_driver_sql("""CREATE TABLE qmt_datadir_minute_stage (
            run_id TEXT,stock_code TEXT,trade_time TEXT,trade_date TEXT,price NUMERIC,
            change NUMERIC,change_pct NUMERIC,volume NUMERIC,amount NUMERIC,qmt_code TEXT)""")
    day = dt.date(2026, 11, 2)
    native = [(day, ("000001", "2026-11-02 09:30:00", "2026-11-02", 10, 0, 0, 1, 10,
                     "000001.SZ")),
              (day, ("000001", "2026-11-02 09:31:00", "2026-11-02", 10, 0, 0, 1, 10,
                     "000001.SZ"))]
    monkeypatch.setattr(importer, "load_dat", lambda *_args: object())
    # A tiny sequence supplies the decoder's immutable source digest.
    class Selected(list):
        def tobytes(self):
            return b"native bytes"
    monkeypatch.setattr(importer, "select_records", lambda *_args: Selected([1, 2]))
    monkeypatch.setattr(importer, "iter_minute_rows", lambda *_args: iter(native))
    original_emit = MinuteImportProgress.emit
    observed_commits = []

    def checked_emit(self, phase, **kwargs):
        if phase == "stage_commit" and kwargs.get("completed_units"):
            with engine.connect() as observer:
                observed_commits.append(observer.exec_driver_sql(
                    "SELECT COUNT(*) FROM qmt_datadir_minute_stage").scalar_one())
        return original_emit(self, phase, **kwargs)

    monkeypatch.setattr(MinuteImportProgress, "emit", checked_emit)
    class BoundConnection:
        def __init__(self, connection):
            self.connection = connection
        def exec_driver_sql(self, sql, params):
            return self.connection.exec_driver_sql(sql.replace("%s", "?"), params)
        def commit(self):
            self.connection.commit()
    with engine.connect() as connection:
        actual = importer.harvest(BoundConnection(connection), root=tmp_path,
            universe=[importer.UniverseMember("000001.SZ", "SZ", "000001")],
            sessions=[day], run_id=RUN_ID, chunk_rows=1,
            codes_by_session={day: {"000001"}})
    assert observed_commits == [2]
    assert actual[day]["rows"] == 2
    completed = _events(tmp_path)[-1]
    assert completed["phase"] == "harvest_complete" and completed["status"] == "COMPLETE"
    assert completed["completed_units"] == 2
    assert completed["source_read_seconds"] >= 0 and completed["stage_commit_seconds"] >= 0
    engine.dispose()


def test_matrix_reports_full_progress_and_keeps_the_existing_canonical_hash(monkeypatch, tmp_path):
    from server.common.qmt_datadir_result import row_hash

    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    day = "2026-11-02"
    codes = tuple(f"{code:06d}" for code in range(1, 209))
    scope = {"codes": codes, "daily_close_by_code": dict.fromkeys(codes, "10"),
             "full_market_manifest_hash": "a" * 64,
             "expected_traded_code_set_sha256": "b" * 64}
    rows = [dict(stock_code=code, trade_time=day + " " + moment, trade_date=day,
        price=10, change=0, change_pct=0, volume=1, amount=10, qmt_code=code + ".SZ",
        data_source="qmt_datadir", source_time=day + " " + moment, batch_id="same",
        data_version="qmt_datadir_v1", quality_status="VERIFIED", permission_status="SUPPORTED")
        for code in codes for moment in coverage.CORE_GRID]
    progress = MinuteImportProgress(run_id=RUN_ID, total_units=len(rows),
        operation="publication_readback", days=[day])
    actual = coverage.validate_matrix(iter(rows), day=day, scope=scope, progress=progress)
    assert actual["row_hash"] == row_hash(rows)
    events = _events(tmp_path)
    assert any(event["phase"] == "consume" and event["completed_units"] == 50000
               and event["budget_seconds"] == 180 for event in events)
    assert events[-1]["completed_units"] == len(rows) == 50128
    assert events[-1]["status"] == "COMPLETE"


def test_failed_database_execute_names_the_stage_in_its_durable_progress(monkeypatch, tmp_path):
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE sm_stock_minute (trade_date TEXT,data_source TEXT)")
    # The provider probe succeeds, then the actual matrix SELECT fails at
    # execute before it can yield even one row.
    with pytest.raises(Exception):
        coverage.persisted_matrix(engine, "2026-11-02", run_id=RUN_ID,
            scope={"codes": ["000001"]})
    event = _events(tmp_path)[-1]
    assert event["phase"] == "execute_firstrow" and event["status"] == "FAILED"
    assert event["completed_units"] == 0 and event["error_type"] == "OperationalError"
    engine.dispose()


def test_unavailable_console_cannot_override_a_business_error(monkeypatch, tmp_path):
    import builtins

    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    progress = MinuteImportProgress(run_id=RUN_ID, total_units=241,
        operation="harvest", days=["2026-11-02"])
    def unavailable(*_args, **_kwargs):
        raise BrokenPipeError("original scheduler pipe exited")
    monkeypatch.setattr(builtins, "print", unavailable)
    event = progress.emit("stage_commit", status="FAILED", console=True,
                          error_type="OriginalDatabaseFailure")
    assert _events(tmp_path)[-1] == event
    assert event["error_type"] == "OriginalDatabaseFailure"
