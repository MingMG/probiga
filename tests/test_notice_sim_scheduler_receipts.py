from __future__ import annotations

import json
from datetime import datetime

import pytest
from sqlalchemy import create_engine, text

from biz.analysis import sync_sim_trade
from biz.notice import sync_notice_em
from server.common import scheduler_validation
from server.engine.sim_trade_engine import STRATEGY_CONFIG


def _engine():
    return create_engine("sqlite+pysqlite:///:memory:")


def test_notice_scheduler_requires_exact_full_universe_receipt_and_readback():
    engine = _engine()
    captured = datetime(2026, 8, 26, 20, 15, 30)
    persisted_row = {
        "stock_code": "000001",
        "art_code": "AN-1",
        "notice_date": datetime(2026, 8, 26).date(),
        "title": "公告",
        "column_name": "公司公告",
        "display_time": "2026-08-26 20:10:00",
        "detail_url": "https://data.eastmoney.com/notices/detail/000001/AN-1.html",
        "association_validated": 1,
        "qmt_code": "000001.SZ",
        "data_source": sync_notice_em.NOTICE_PROVIDER_ID,
        "source_time": captured,
        "received_at": captured,
        "batch_id": "c" * 64,
        "data_version": sync_notice_em.NOTICE_DATA_VERSION,
        "quality_status": sync_notice_em.NOTICE_QUALITY_STATUS,
        "permission_status": "PUBLIC",
    }
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_all_code (stock_code TEXT)"))
        connection.execute(
            text(
                """
                CREATE TABLE si_notice_eastmoney (
                    stock_code TEXT, art_code TEXT, notice_date DATE,
                    title TEXT, column_name TEXT, display_time TEXT,
                    detail_url TEXT, etl_sync_at DATETIME,
                    association_validated INTEGER, qmt_code TEXT,
                    data_source TEXT, source_time DATETIME,
                    received_at DATETIME, batch_id TEXT, data_version TEXT,
                    quality_status TEXT, permission_status TEXT
                )
                """
            )
        )
        connection.execute(
            text("INSERT INTO si_all_code(stock_code) VALUES ('000001'),('000002')")
        )
        connection.execute(
            text(
                """
                INSERT INTO si_notice_eastmoney
                    (stock_code,art_code,notice_date,title,column_name,
                     display_time,detail_url,etl_sync_at,
                     association_validated,qmt_code,data_source,source_time,
                     received_at,batch_id,data_version,quality_status,
                     permission_status)
                VALUES (:stock_code,:art_code,:notice_date,:title,:column_name,
                        :display_time,:detail_url,:captured,
                        :association_validated,:qmt_code,:data_source,
                        :source_time,:received_at,:batch_id,:data_version,
                        :quality_status,:permission_status)
                """
            ),
            {
                "captured": captured,
                **persisted_row,
            },
        )
    receipt = sync_notice_em._notice_sync_result(
        started_at=datetime(2026, 8, 26, 20, 15),
        finished_at=datetime(2026, 8, 26, 20, 16),
        codes=["000001", "000002"],
        succeeded_codes=["000001", "000002"],
        nonempty_codes=["000001"],
        failed_codes=[],
        failure_sample=[],
        written_rows=1,
        minimum_coverage=1.0,
        minimum_row_coverage=0.0,
        request_window_start=datetime(2026, 7, 12).date(),
        request_window_end=datetime(2026, 8, 27).date(),
        batch_id="c" * 64,
        persisted_manifest=[
            {
                "stock_code": "000001",
                "row_count": 1,
                "row_hash": sync_notice_em._notice_row_hash(
                    [persisted_row]
                ),
            },
            {
                "stock_code": "000002",
                "row_count": 0,
                "row_hash": sync_notice_em._notice_row_hash([]),
            },
        ],
    )
    output = json.dumps(receipt, ensure_ascii=False, sort_keys=True)
    task = {"task_type": "notice_eastmoney"}

    assert (
        scheduler_validation.scheduler_output_status(
            task,
            output,
            return_code=0,
        )
        == "success"
    )
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        output=output,
        started_at=datetime(2026, 8, 26, 20, 15),
        now=datetime(2026, 8, 26, 20, 16, 5),
    )
    assert result.checked is True
    assert result.ok is True
    assert "codes=2" in result.message


def test_notice_historical_recovery_uses_bound_target_not_execution_day():
    """Reproduce the 2026-09-01 recovery that finished after midnight."""

    engine = _engine()
    captured = datetime(2026, 9, 3, 1, 12, 30)
    persisted_row = {
        "stock_code": "000001",
        "art_code": "AN-0901",
        "notice_date": datetime(2026, 9, 1).date(),
        "title": "历史恢复公告",
        "column_name": "公司公告",
        "display_time": "2026-09-01 20:10:00",
        "detail_url": (
            "https://data.eastmoney.com/notices/detail/000001/AN-0901.html"
        ),
        "association_validated": 1,
        "qmt_code": "000001.SZ",
        "data_source": sync_notice_em.NOTICE_PROVIDER_ID,
        "source_time": captured,
        "received_at": captured,
        "batch_id": "e" * 64,
        "data_version": sync_notice_em.NOTICE_DATA_VERSION,
        "quality_status": sync_notice_em.NOTICE_QUALITY_STATUS,
        "permission_status": "PUBLIC",
    }
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_all_code (stock_code TEXT)"))
        connection.execute(text("INSERT INTO si_all_code VALUES ('000001')"))
        connection.execute(text("""
            CREATE TABLE si_notice_eastmoney (
                stock_code TEXT, art_code TEXT, notice_date DATE,
                title TEXT, column_name TEXT, display_time TEXT,
                detail_url TEXT, etl_sync_at DATETIME,
                association_validated INTEGER, qmt_code TEXT,
                data_source TEXT, source_time DATETIME,
                received_at DATETIME, batch_id TEXT, data_version TEXT,
                quality_status TEXT, permission_status TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO si_notice_eastmoney
                (stock_code, art_code, notice_date, title, column_name,
                 display_time, detail_url, etl_sync_at,
                 association_validated, qmt_code, data_source, source_time,
                 received_at, batch_id, data_version, quality_status,
                 permission_status)
            VALUES
                (:stock_code, :art_code, :notice_date, :title, :column_name,
                 :display_time, :detail_url, :received_at,
                 :association_validated, :qmt_code, :data_source, :source_time,
                 :received_at, :batch_id, :data_version, :quality_status,
                 :permission_status)
        """), persisted_row)
    receipt = sync_notice_em._notice_sync_result(
        started_at=datetime(2026, 9, 3, 0, 57, 27),
        finished_at=datetime(2026, 9, 3, 1, 30, 15),
        codes=["000001"],
        succeeded_codes=["000001"],
        nonempty_codes=["000001"],
        failed_codes=[],
        failure_sample=[],
        written_rows=1,
        minimum_coverage=1.0,
        minimum_row_coverage=0.0,
        request_window_start=datetime(2026, 7, 18).date(),
        request_window_end=datetime(2026, 9, 2).date(),
        target_trade_date=datetime(2026, 9, 1).date(),
        batch_id="e" * 64,
        persisted_manifest=[{
            "stock_code": "000001",
            "row_count": 1,
            "row_hash": sync_notice_em._notice_row_hash([persisted_row]),
        }],
    )
    # The production b507 collector predates the explicit receipt field.  Its
    # scheduler-private target still has to make the immutable receipt usable.
    receipt.pop("target_trade_date")
    receipt["result_sha256"] = sync_notice_em._sha256({
        key: value
        for key, value in receipt.items()
        if key != "result_sha256"
    })
    output = json.dumps(receipt, ensure_ascii=False, sort_keys=True)
    task = {
        "task_type": "notice_eastmoney",
        "_scheduler_target_trade_date": "2026-09-01",
    }

    assert scheduler_validation.scheduler_output_status(
        task,
        output,
        return_code=0,
    ) == "success"
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        output=output,
        started_at=datetime(2026, 9, 3, 0, 57, 27),
        now=datetime(2026, 9, 3, 1, 30, 16),
    )
    assert result.checked is True
    assert result.ok is True
    assert "rows=1" in result.message

    wrong_target = {
        **task,
        "_scheduler_target_trade_date": "2026-09-02",
    }
    assert scheduler_validation.scheduler_output_status(
        wrong_target,
        output,
        return_code=0,
    ) == "failed"


@pytest.mark.parametrize(
    "field,value,message",
    (
        ("association_validated", 0, "invalid association"),
        ("data_source", "legacy_unverified", "invalid association"),
        ("batch_id", "d" * 64, "invalid association"),
        ("title", "被篡改公告", "content hash differs"),
    ),
)
def test_notice_scheduler_rejects_unvalidated_provenance_or_content(
    monkeypatch, field, value, message
):
    captured = datetime(2026, 8, 26, 20, 15, 30)
    persisted_row = {
        "stock_code": "000001",
        "art_code": "AN-1",
        "notice_date": datetime(2026, 8, 26).date(),
        "title": "公告",
        "column_name": "公司公告",
        "display_time": "2026-08-26 20:10:00",
        "detail_url": "https://data.eastmoney.com/notices/detail/000001/AN-1.html",
        "association_validated": 1,
        "etl_sync_at": captured,
        "qmt_code": "000001.SZ",
        "data_source": sync_notice_em.NOTICE_PROVIDER_ID,
        "source_time": captured,
        "received_at": captured,
        "batch_id": "c" * 64,
        "data_version": sync_notice_em.NOTICE_DATA_VERSION,
        "quality_status": sync_notice_em.NOTICE_QUALITY_STATUS,
        "permission_status": "PUBLIC",
    }
    receipt = sync_notice_em._notice_sync_result(
        started_at=datetime(2026, 8, 26, 20, 15),
        finished_at=datetime(2026, 8, 26, 20, 16),
        codes=["000001"],
        succeeded_codes=["000001"],
        nonempty_codes=["000001"],
        failed_codes=[],
        failure_sample=[],
        written_rows=1,
        minimum_coverage=1.0,
        minimum_row_coverage=0.0,
        request_window_start=datetime(2026, 7, 12).date(),
        request_window_end=datetime(2026, 8, 27).date(),
        batch_id="c" * 64,
        persisted_manifest=[
            {
                "stock_code": "000001",
                "row_count": 1,
                "row_hash": sync_notice_em._notice_row_hash(
                    [persisted_row]
                ),
            }
        ],
    )
    altered_row = {**persisted_row, field: value}

    def fake_read_all(_engine, sql, params=None):
        if "FROM si_all_code" in sql:
            return [{"stock_code": "000001"}]
        if "FROM si_notice_eastmoney" in sql:
            return [altered_row]
        raise AssertionError(sql)

    monkeypatch.setattr(scheduler_validation, "_read_all", fake_read_all)
    ok, actual = scheduler_validation._validate_notice_eastmoney_receipt(
        object(),
        output=json.dumps(receipt, ensure_ascii=False, sort_keys=True),
        started_at=datetime(2026, 8, 26, 20, 15),
        now=datetime(2026, 8, 26, 20, 16, 5),
    )

    assert ok is False
    assert message in actual


def test_notice_scheduler_rejects_tampered_receipt_even_with_zero_exit():
    receipt = sync_notice_em._notice_sync_result(
        started_at=datetime(2026, 8, 26, 20, 15),
        finished_at=datetime(2026, 8, 26, 20, 16),
        codes=["000001"],
        succeeded_codes=["000001"],
        nonempty_codes=["000001"],
        failed_codes=[],
        failure_sample=[],
        written_rows=1,
        minimum_coverage=1.0,
        minimum_row_coverage=0.0,
        request_window_start=datetime(2026, 7, 12).date(),
        request_window_end=datetime(2026, 8, 27).date(),
        batch_id="c" * 64,
    )
    receipt["written_notice_count"] = 2

    assert (
        scheduler_validation.scheduler_output_status(
            {"task_type": "notice_eastmoney"},
            json.dumps(receipt),
            return_code=0,
        )
        == "failed"
    )


def _history_scheduler_fixture(tmp_path, monkeypatch):
    monkeypatch.delenv("PROBIGA_DEPLOYMENT_MODE", raising=False)
    monkeypatch.delenv("PROBIGA_JOB_LOG_ROOT", raising=False)
    engine = _engine()
    requested_codes = ["000001", "000002", "000003", "000004"]
    catalog_codes = ["000001", "000002"]
    created_at = datetime(2026, 8, 25, 19, 0)
    captured_at = datetime(2026, 8, 26, 20, 0)
    ledger_path = tmp_path / "notice-history-ledger.json"
    ledger = sync_notice_em._new_history_ledger(
        requested_codes,
        now=created_at,
    )

    def persisted_row(code, art_code, title):
        return {
            "stock_code": code,
            "art_code": art_code,
            "notice_date": datetime(2026, 8, 26).date(),
            "title": title,
            "column_name": "公司公告",
            "display_time": "2026-08-26 19:58:00",
            "detail_url": (
                "https://data.eastmoney.com/notices/detail/"
                f"{code}/{art_code}.html"
            ),
            "association_validated": 1,
            "qmt_code": sync_notice_em.to_qmt_symbol(code),
            "data_source": sync_notice_em.NOTICE_PROVIDER_ID,
            "source_time": captured_at,
            "received_at": captured_at,
            "batch_id": ledger["batch_id"],
            "data_version": sync_notice_em.NOTICE_DATA_VERSION,
            "quality_status": sync_notice_em.NOTICE_QUALITY_STATUS,
            "permission_status": sync_notice_em.NOTICE_PERMISSION_STATUS,
        }

    rows_by_code = {
        "000001": [persisted_row("000001", "AN-1", "目录股票公告")],
        "000002": [],
        "000003": [persisted_row("000003", "AN-3", "遗留股票公告")],
        "000004": [],
    }
    entries = []
    for code in requested_codes:
        rows = rows_by_code[code]
        row_hash = sync_notice_em._notice_row_hash(rows)
        entries.append(
            {
                "stock_code": code,
                "captured_at": captured_at.isoformat(
                    sep=" ", timespec="microseconds"
                ),
                "total_hits": len(rows),
                "page_count": 1,
                "written_count": len(rows),
                "deleted_count": 1,
                "persisted_count": len(rows),
                "source_row_hash": row_hash,
                "persisted_row_hash": row_hash,
            }
        )
    ledger = sync_notice_em._atomic_write_history_ledger(
        ledger_path,
        {
            **ledger,
            "status": "COMPLETE",
            "updated_at": captured_at.isoformat(
                sep=" ", timespec="microseconds"
            ),
            "completed_at": captured_at.isoformat(
                sep=" ", timespec="microseconds"
            ),
            "next_offset": len(requested_codes),
            "completed_code_count": len(requested_codes),
            "completed_code_set_hash": sync_notice_em._code_set_hash(
                requested_codes
            ),
            "completed_entries": entries,
            "evidence_chain_sha256": sync_notice_em._history_entry_chain(
                entries
            ),
            "last_failure": None,
        },
    )
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_all_code (stock_code TEXT)"))
        connection.execute(text("""
            CREATE TABLE si_notice_eastmoney (
                stock_code TEXT, art_code TEXT, notice_date DATE,
                title TEXT, column_name TEXT, display_time TEXT,
                detail_url TEXT, etl_sync_at DATETIME,
                association_validated INTEGER, qmt_code TEXT,
                data_source TEXT, source_time DATETIME,
                received_at DATETIME, batch_id TEXT, data_version TEXT,
                quality_status TEXT, permission_status TEXT
            )
        """))
        connection.execute(
            text("INSERT INTO si_all_code(stock_code) VALUES (:code)"),
            [{"code": code} for code in catalog_codes],
        )
        all_rows = [row for rows in rows_by_code.values() for row in rows]
        connection.execute(text("""
            INSERT INTO si_notice_eastmoney
                (stock_code, art_code, notice_date, title, column_name,
                 display_time, detail_url, etl_sync_at,
                 association_validated, qmt_code, data_source, source_time,
                 received_at, batch_id, data_version, quality_status,
                 permission_status)
            VALUES
                (:stock_code, :art_code, :notice_date, :title, :column_name,
                 :display_time, :detail_url, :received_at,
                 :association_validated, :qmt_code, :data_source, :source_time,
                 :received_at, :batch_id, :data_version, :quality_status,
                 :permission_status)
        """), all_rows)
    started_at = datetime(2026, 8, 27, 0, 5)
    finished_at = datetime(2026, 8, 27, 0, 5, 1)
    receipt = sync_notice_em._history_repair_result(
        started_at=started_at,
        finished_at=finished_at,
        codes=requested_codes,
        ledger=ledger,
        processed_this_run=0,
    )
    task = {
        "task_type": sync_notice_em.HISTORY_TASK_TYPE,
        "script_path": "biz/notice/sync_notice_em.py",
        "script_args": (
            "--mode historical-repair --from-si-all-code --limit 0 "
            f"--history-state-file {ledger_path}"
        ),
    }
    return engine, task, receipt, ledger_path, started_at, finished_at


def test_notice_history_complete_replay_validates_ledger_full_pool_and_legacy(
    tmp_path, monkeypatch
):
    engine, task, receipt, _path, started_at, finished_at = (
        _history_scheduler_fixture(tmp_path, monkeypatch)
    )
    output = json.dumps(receipt, ensure_ascii=False, sort_keys=True)

    assert (
        scheduler_validation.scheduler_output_status(
            task,
            output,
            return_code=0,
        )
        == "success"
    )
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        output=output,
        started_at=started_at,
        now=finished_at,
    )
    assert result.checked is True
    assert result.ok is True
    assert "codes=4" in result.message
    assert "legacy=2" in result.message


def test_notice_history_validator_selects_latest_complete_current_generation(
    tmp_path, monkeypatch
) -> None:
    engine, task, old_receipt, base_path, old_started, old_finished = (
        _history_scheduler_fixture(tmp_path, monkeypatch)
    )
    parent = sync_notice_em._read_history_ledger(base_path, codes=None)
    frozen_parent = base_path.read_bytes()
    child_codes = [*parent["requested_codes"], "000005"]
    created_at = datetime(2026, 8, 27, 0, 6)
    completed_at = datetime(2026, 8, 27, 0, 7)
    child = sync_notice_em._new_history_generation_ledger(
        parent,
        child_codes,
        now=created_at,
    )
    empty_hash = sync_notice_em._notice_row_hash([])
    new_entry = {
        "stock_code": "000005",
        "captured_at": completed_at.isoformat(
            sep=" ", timespec="microseconds"
        ),
        "total_hits": 0,
        "page_count": 1,
        "written_count": 0,
        "deleted_count": 0,
        "persisted_count": 0,
        "source_row_hash": empty_hash,
        "persisted_row_hash": empty_hash,
    }
    entries = [*child["completed_entries"], new_entry]
    child_path = sync_notice_em._history_generation_path(base_path, child_codes)
    child = sync_notice_em._atomic_write_history_ledger(
        child_path,
        {
            **child,
            "status": "COMPLETE",
            "updated_at": completed_at.isoformat(
                sep=" ", timespec="microseconds"
            ),
            "completed_at": completed_at.isoformat(
                sep=" ", timespec="microseconds"
            ),
            "next_offset": len(child_codes),
            "completed_code_count": len(child_codes),
            "completed_code_set_hash": sync_notice_em._code_set_hash(child_codes),
            "completed_entries": entries,
            "evidence_chain_sha256": sync_notice_em._history_entry_chain(entries),
            "last_failure": None,
        },
    )
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO si_all_code(stock_code) VALUES ('000005')")
        )
    started_at = datetime(2026, 8, 27, 0, 8)
    finished_at = datetime(2026, 8, 27, 0, 8, 1)
    receipt = sync_notice_em._history_repair_result(
        started_at=started_at,
        finished_at=finished_at,
        codes=child_codes,
        ledger=child,
        processed_this_run=0,
    )

    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        output=json.dumps(receipt, ensure_ascii=False, sort_keys=True),
        started_at=started_at,
        now=finished_at,
    )
    assert result.checked is True
    assert result.ok is True
    assert "codes=5" in result.message
    assert base_path.read_bytes() == frozen_parent

    stale = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        output=json.dumps(old_receipt, ensure_ascii=False, sort_keys=True),
        started_at=old_started,
        now=old_finished,
    )
    assert stale.ok is False
    assert "latest generation" in stale.message


def test_notice_history_progress_and_failures_never_report_success(
    tmp_path, monkeypatch
):
    _engine_value, task, passing, _path, _started, _finished = (
        _history_scheduler_fixture(tmp_path, monkeypatch)
    )
    progress = {
        **passing,
        "status": "PROGRESS",
        "retryable": True,
        "completed_code_count": 3,
        "remaining_code_count": 1,
        "processed_code_count_this_run": 1,
        "ledger_status": "PROGRESS",
    }
    progress["result_sha256"] = sync_notice_em._sha256(
        {
            key: value
            for key, value in progress.items()
            if key != "result_sha256"
        }
    )
    transient = sync_notice_em._history_failure_result(
        started_at=datetime(2026, 8, 27, 0, 5),
        finished_at=datetime(2026, 8, 27, 0, 5, 1),
        error=RuntimeError("Eastmoney request failed"),
        codes=["000001"],
    )
    terminal = sync_notice_em._history_failure_result(
        started_at=datetime(2026, 8, 27, 0, 5),
        finished_at=datetime(2026, 8, 27, 0, 5, 1),
        error=ValueError("history ledger checksum differs"),
        codes=["000001"],
    )

    assert scheduler_validation.scheduler_output_status(
        task, json.dumps(progress), return_code=2
    ) == "failed"
    assert scheduler_validation.scheduler_output_status(
        task, json.dumps(transient), return_code=2
    ) == "failed"
    assert scheduler_validation.scheduler_output_status(
        task, json.dumps(terminal), return_code=2
    ) == "blocked"
    assert scheduler_validation.scheduler_output_status(
        task, json.dumps(passing), return_code=2
    ) == "failed"


@pytest.mark.parametrize(
    ("statement", "message"),
    (
        (
            "UPDATE si_notice_eastmoney SET association_validated=0 "
            "WHERE stock_code='000001'",
            "provenance",
        ),
        (
            "UPDATE si_notice_eastmoney SET title='篡改' "
            "WHERE stock_code='000003'",
            "content hash",
        ),
        (
            "UPDATE si_notice_eastmoney "
            "SET stock_code='000004', qmt_code='000004.SZ' "
            "WHERE stock_code='000003'",
            "legacy association cleanup",
        ),
    ),
)
def test_notice_history_rejects_wrong_provenance_content_or_legacy_cleanup(
    tmp_path, monkeypatch, statement, message
):
    engine, task, receipt, _path, started_at, finished_at = (
        _history_scheduler_fixture(tmp_path, monkeypatch)
    )
    with engine.begin() as connection:
        connection.execute(text(statement))
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        output=json.dumps(receipt, ensure_ascii=False, sort_keys=True),
        started_at=started_at,
        now=finished_at,
    )
    assert result.checked is True
    assert result.ok is False
    assert message in result.message


def test_notice_history_rejects_tampered_protected_ledger(
    tmp_path, monkeypatch
):
    engine, task, receipt, ledger_path, started_at, finished_at = (
        _history_scheduler_fixture(tmp_path, monkeypatch)
    )
    ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
    ledger["next_offset"] = 1
    ledger_path.write_text(json.dumps(ledger), encoding="utf-8")

    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        output=json.dumps(receipt, ensure_ascii=False, sort_keys=True),
        started_at=started_at,
        now=finished_at,
    )
    assert result.checked is True
    assert result.ok is False
    assert "protected COMPLETE ledger is invalid" in result.message


def _sim_engine():
    engine = _engine()
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE si_trade_calendar "
                "(trade_date DATE, trade_status INTEGER)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE st_recommended_stocks "
                "(pick_date DATE, stock_code TEXT, recommend_status TEXT)"
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE st_sim_signal (
                    trade_mode TEXT, signal_date DATE, trade_date DATE,
                    stock_code TEXT, strategy_type TEXT, updated_at DATETIME
                )
                """
            )
        )
        connection.execute(
            text(
                "INSERT INTO si_trade_calendar VALUES "
                "('2026-08-25',1),('2026-08-26',1)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_recommended_stocks VALUES "
                "('2026-08-25','000001','ALLOW'),"
                "('2026-08-25','000002',NULL)"
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO st_sim_signal VALUES
                ('live','2026-08-25','2026-08-26','000001',
                 'ultra_short','2026-08-26 09:20:20')
                """
            )
        )
    return engine


def _sim_receipt() -> dict:
    recommendation_codes = ["000001", "000002"]
    identities = ["000001:ultra_short"]
    strategy_count = len(STRATEGY_CONFIG)
    return sync_sim_trade.build_task_receipt(
        {
            "status": "ok",
            "trade_date": "2026-08-26",
            "signal_date": "2026-08-25",
            "total_recommendations": len(recommendation_codes),
            "recommendation_code_count": len(recommendation_codes),
            "recommendation_code_set_hash": (
                scheduler_validation._sim_identity_set_hash(
                    recommendation_codes
                )
            ),
            "strategy_count": strategy_count,
            "allowed_count": len(identities),
            "rejected_count": len(recommendation_codes) * strategy_count
            - len(identities),
            "signal_identity_count": len(identities),
            "signal_identity_hash": (
                scheduler_validation._sim_identity_set_hash(identities)
            ),
            "counts": {"NEW": 1, "total": 1},
            "recommendation_prerequisite": {
                "status": "exists",
                "signal_date": "2026-08-25",
                "count": len(recommendation_codes),
                "read_only": True,
            },
        },
        task_mode="prepare_signals",
        requested_trade_date="2026-08-26",
        requested_signal_date="2026-08-25",
        started_at=datetime(2026, 8, 26, 9, 20),
        finished_at=datetime(2026, 8, 26, 9, 20, 30),
    )


def test_sim_prepare_scheduler_reconciles_recommendations_and_signal_identities():
    engine = _sim_engine()
    receipt = _sim_receipt()
    output = json.dumps(receipt, ensure_ascii=False, sort_keys=True)
    task = {"task_type": "sim_trade_signal_prepare"}

    assert (
        scheduler_validation.scheduler_output_status(
            task,
            output,
            return_code=0,
        )
        == "success"
    )
    result = scheduler_validation.validate_scheduler_task_result(
        task,
        engine=engine,
        output=output,
        started_at=datetime(2026, 8, 26, 9, 20),
        now=datetime(2026, 8, 26, 9, 20, 40),
    )
    assert result.checked is True
    assert result.ok is True
    assert "recommendations=2" in result.message
    assert "signals=1" in result.message


def test_sim_prepare_data_blocked_receipt_remains_retryable_failure():
    receipt = sync_sim_trade.build_task_receipt(
        {
            "status": "error",
            "trade_date": "2026-08-26",
            "signal_date": "2026-08-25",
            "error": "recommendations missing",
            "recommendation_prerequisite": {
                "count": 0,
                "signal_date": "2026-08-25",
            },
        },
        task_mode="prepare_signals",
        requested_trade_date="2026-08-26",
        requested_signal_date="2026-08-25",
        started_at=datetime(2026, 8, 26, 9, 20),
        finished_at=datetime(2026, 8, 26, 9, 20, 1),
    )

    assert receipt["status"] == "DATA_BLOCKED"
    assert (
        scheduler_validation.scheduler_output_status(
            {"task_type": "sim_trade_signal_prepare"},
            json.dumps(receipt),
            return_code=2,
        )
        == "failed"
    )
