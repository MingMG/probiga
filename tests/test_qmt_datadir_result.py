from __future__ import annotations

import json

from sqlalchemy import create_engine, text

from server.common import qmt_datadir_result as result
from server.common.scheduler_validation import scheduler_output_status
from tools import import_qmt_datadir_history as minute_importer


BUILD_SHA = "a" * 40


def _database():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_datadir_import_partition (
              dataset TEXT, trade_date TEXT, source_root TEXT, status TEXT,
              code_count INTEGER, row_count INTEGER, content_sha256 TEXT,
              run_id TEXT, started_at TEXT, finished_at TEXT, detail_json TEXT,
              PRIMARY KEY(dataset, trade_date)
            )
        """))
        connection.execute(text("""
            CREATE TABLE sm_stock_kline (
              stock_code TEXT, trade_time TEXT, trade_date TEXT,
              k_type INTEGER, adjust_type INTEGER, open REAL, close REAL,
              high REAL, low REAL, volume REAL, amount REAL,
              `change` REAL, change_pct REAL, pre_close REAL, qmt_code TEXT,
              data_source TEXT, source_time TEXT, batch_id TEXT,
              data_version TEXT, quality_status TEXT, permission_status TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO sm_stock_kline VALUES (
              '600000','2026-10-27 15:00:00','2026-10-27',1,0,
              10,11,11,10,100,1100,1,10,10,'600000.SH',
              'qmt_datadir','2026-10-27 15:00:00','batch','version',
              'VERIFIED','SUPPORTED'
            )
        """))
    return engine


def test_datadir_result_requires_build_and_complete_readback(monkeypatch):
    engine = _database()
    fingerprint = result.persisted_fingerprint(
        engine,
        dataset="stock_daily",
        trade_date="2026-10-27",
    )
    partition = {
        "trade_date": "2026-10-27",
        "status": "COMPLETE",
        "source_root": r"D:\QMT\datadir",
        "run_id": "run-1",
        "row_count": 1,
        "code_count": 1,
        "content_sha256": "b" * 64,
        "row_hash": fingerprint["row_hash"],
        "eligible_code_count": 1,
        "coverage": 1.0,
        "readback_rows": 1,
        "readback_codes": 1,
        "attestation_run_id": "attest-1",
    }
    payload = result.build_result(
        dataset="stock_daily",
        sessions=["2026-10-27"],
        partitions=[partition],
        build_sha=BUILD_SHA,
    )

    assert result.validate_task_result(
        "qmt_stock_daily_canonical",
        payload,
        return_code=0,
        expected_build_sha=BUILD_SHA,
    ) == "complete"
    assert result.validate_task_result(
        "qmt_stock_daily_canonical",
        payload,
        return_code=0,
        expected_build_sha="c" * 40,
    ) == "failed"
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", BUILD_SHA)
    assert scheduler_output_status(
        {"task_type": "qmt_stock_daily_canonical"},
        json.dumps(payload),
        return_code=0,
    ) == "success"

    with engine.begin() as connection:
        connection.execute(
            text("""
              INSERT INTO qmt_datadir_import_partition VALUES (
                'stock_daily','2026-10-27',:root,'COMPLETE',1,1,:sha,
                'run-1','2026-10-27 15:45:00','2026-10-27 15:46:00',:detail
              )
            """),
            {
                "root": partition["source_root"],
                "sha": partition["content_sha256"],
                "detail": json.dumps({
                    "readback_rows": 1,
                    "readback_codes": 1,
                    "row_hash": fingerprint["row_hash"],
                    "eligible_code_count": 1,
                    "coverage": 1.0,
                    "attestation_run_id": "attest-1",
                }),
            },
        )

    from types import SimpleNamespace
    from server.common import qmt_daily_market_truth

    monkeypatch.setattr(
        qmt_daily_market_truth,
        "load_qmt_daily_market_truth",
        lambda *_args, **_kwargs: SimpleNamespace(run_id="attest-1"),
    )
    proof = result.validate_persisted_result(
        engine,
        task_type="qmt_stock_daily_canonical",
        payload=payload,
        expected_session="2026-10-27",
    )
    assert proof["row_count"] == 1
    engine.dispose()


def test_minute_import_rejects_incomplete_native_grid():
    assert minute_importer._minute_grid_shortfall({
        "code_rows": {"600000": 241, "000001": 12},
    }) == 1
    assert minute_importer._minute_grid_shortfall({
        "code_rows": {"600000": 241},
    }) == 0
