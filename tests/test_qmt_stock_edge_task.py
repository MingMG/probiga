from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from server.common.qmt_attestation_contract import expected_stock_set_contract
from server.common.qmt_history_coverage import minute_time_grid
from server.common.scheduler_validation import (
    scheduler_output_status,
    validate_scheduler_task_result,
)
from tools import sync_qmt_stock_edge as publisher


TRADE_DATE = "2026-08-26"


def _daily_engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE sm_stock_kline (
                stock_code TEXT NOT NULL, trade_date TEXT NOT NULL,
                k_type INTEGER NOT NULL, adjust_type INTEGER NOT NULL,
                `open` REAL, `close` REAL, high REAL, low REAL,
                volume REAL, amount REAL, pre_close REAL,
                data_source TEXT, batch_id TEXT, data_version TEXT,
                quality_status TEXT, permission_status TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO sm_stock_kline VALUES
            ('000001',:day,1,0,10,11,12,9,100,1000,9.5,
             :provider,'batch-1','version-1','QMT_ATTESTED','SUPPORTED'),
            ('600000',:day,1,0,8,8.5,9,7.5,200,1600,8,
             :provider,'batch-1','version-1','QMT_ATTESTED','SUPPORTED')
        """), {"day": TRADE_DATE, "provider": publisher.PROVIDER})
    return engine


def _daily_attestation():
    contract = expected_stock_set_contract(
        TRADE_DATE, ["000001", "600000"]
    )
    return {
        "run_id": "daily-run-1",
        "status": "COMPLETED",
        "apply": True,
        "provider": publisher.PROVIDER,
        "start_date": TRADE_DATE,
        "end_date": TRADE_DATE,
        "target_rows": 2,
        "qmt_rows": 2,
        "matched_rows": 2,
        "missing_qmt_rows": 0,
        "source_only_rows": 0,
        "catalog_missing_target_rows": 0,
        "target_not_catalog_rows": 0,
        "catalog_missing_source_rows": 0,
        "source_not_catalog_rows": 0,
        "mismatched_rows": 0,
        "catalog_manifest_hash": "a" * 64,
        "calendar_manifest_hash": "b" * 64,
        "daily_universe": {TRADE_DATE: contract},
    }


def test_daily_partition_requires_exact_catalog_set_and_attested_rows():
    engine = _daily_engine()
    proof = publisher._validate_daily_partition(
        engine,
        trade_date=TRADE_DATE,
        attestation=_daily_attestation(),
    )
    assert proof["row_count"] == proof["code_count"] == 2
    assert proof["code_set_hash"] == expected_stock_set_contract(
        TRADE_DATE, ["000001", "600000"]
    )["stock_set_hash"]

    with engine.begin() as connection:
        connection.execute(text(
            "DELETE FROM sm_stock_kline WHERE stock_code='600000'"
        ))
    with pytest.raises(publisher.StockDataBlocked, match="database partition"):
        publisher._validate_daily_partition(
            engine,
            trade_date=TRADE_DATE,
            attestation=_daily_attestation(),
        )


def _minute_engine():
    engine = create_engine("sqlite:///:memory:", future=True)
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE sm_stock_minute (
                stock_code TEXT NOT NULL, trade_time TEXT NOT NULL,
                trade_date TEXT NOT NULL, price REAL, avg_price REAL,
                `change` REAL, change_pct REAL, volume REAL, amount REAL
            )
        """))
        rows = [
            {
                "code": code,
                "at": f"{TRADE_DATE} {minute}",
                "day": TRADE_DATE,
            }
            for code in ("000001", "600000")
            for minute in minute_time_grid()
        ]
        connection.execute(text("""
            INSERT INTO sm_stock_minute
                (stock_code,trade_time,trade_date,price,avg_price,
                 `change`,change_pct,volume,amount)
            VALUES (:code,:at,:day,10,10,0,0,100,1000)
        """), rows)
    return engine


def _minute_receipt():
    return {
        "row_count": 482,
        "receipt_id": "minute-receipt-1",
        "manifest": {"bar_count": 482},
        "entities": [
            {"stock_code": "000001", "expected_state": "TRADED"},
            {"stock_code": "600000", "expected_state": "TRADED"},
        ],
        "evidence": {
            "reference_roots": {
                "catalog_manifest_hash": "a" * 64,
                "calendar_manifest_hash": "b" * 64,
            }
        },
    }


def test_minute_partition_requires_every_code_on_native_241_grid():
    engine = _minute_engine()
    proof = publisher._validate_minute_partition(
        engine,
        trade_date=TRADE_DATE,
        receipt=_minute_receipt(),
    )
    assert proof["row_count"] == 482
    assert proof["minute_grid_count"] == 241
    assert proof["traded_code_count"] == 2

    with engine.begin() as connection:
        connection.execute(text("""
            DELETE FROM sm_stock_minute
             WHERE stock_code='600000' AND trade_time=:at
        """), {"at": f"{TRADE_DATE} 15:00:00"})
    with pytest.raises(publisher.StockDataBlocked, match="grid differs"):
        publisher._validate_minute_partition(
            engine,
            trade_date=TRADE_DATE,
            receipt=_minute_receipt(),
        )


def _daily_result():
    payload = {
        "schema": publisher.RESULT_SCHEMA,
        "status": "PASS",
        "dataset": "daily",
        "task_type": publisher.TASK_TYPES["daily"],
        "executor_owner": publisher.EDGE_ROLE,
        "provider": publisher.PROVIDER,
        "build_sha": "1" * 40,
        "sessions": [TRADE_DATE],
        "session_count": 1,
        "session_set_hash": publisher._digest([TRADE_DATE]),
        "calendar": {
            "batch_id": "calendar-1",
            "manifest_hash": "2" * 64,
            "session_set_hash": "3" * 64,
        },
        "source_identity": {
            "strategy_release_protocol": "release-v1",
            "strategy_identity_protocol": "identity-v1",
            "strategy_identity_frozen": True,
            "strategy_build_sha": "1" * 40,
            "strategy_git_blob": "4" * 40,
            "strategy_source_sha256": "5" * 64,
            "strategy_artifact_sha256": "6" * 64,
            "strategy_loaded_identity_sha256": "7" * 64,
        },
        "partitions": [{
            "trade_date": TRADE_DATE,
            "row_count": 2,
            "row_hash": "8" * 64,
            "code_count": 2,
            "code_set_hash": "9" * 64,
            "attestation_run_id": "daily-run-1",
            "catalog_manifest_hash": "a" * 64,
            "calendar_manifest_hash": "b" * 64,
        }],
    }
    payload["partition_manifest_hash"] = publisher._digest(
        payload["partitions"]
    )
    return publisher._signed(payload)


def test_scheduler_output_requires_signed_current_build_receipt(monkeypatch):
    result = _daily_result()
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", "1" * 40)
    rendered = json.dumps(result, sort_keys=True)
    assert publisher.validate_task_result(result, 0) == "complete"
    assert scheduler_output_status(
        {"task_type": publisher.TASK_TYPES["daily"]},
        rendered,
        return_code=0,
    ) == "success"

    tampered = dict(result)
    tampered["partitions"] = [
        {**result["partitions"][0], "row_count": 1}
    ]
    assert publisher.validate_task_result(tampered, 0) == "failed"


def test_scheduler_rejects_receipt_from_the_other_stock_dataset(monkeypatch):
    result = _daily_result()
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", "1" * 40)

    assert scheduler_output_status(
        {"task_type": publisher.TASK_TYPES["minute"]},
        json.dumps(result, sort_keys=True),
        return_code=0,
    ) == "failed"

    validation = validate_scheduler_task_result(
        {"task_type": publisher.TASK_TYPES["minute"]},
        engine=object(),
        output=json.dumps(result, sort_keys=True),
        now=datetime(2026, 8, 26, 16, 0),
    )
    assert validation.checked and not validation.ok
    assert "dataset differs" in validation.message


def test_scheduler_db_gate_rejects_receipt_database_drift(monkeypatch):
    result = _daily_result()
    engine = _daily_engine()
    monkeypatch.setattr(
        publisher,
        "_sessions",
        lambda *_args, **_kwargs: (SimpleNamespace(), [TRADE_DATE]),
    )
    monkeypatch.setattr(publisher, "get_kline_engine", lambda: engine)
    monkeypatch.setattr(
        publisher,
        "load_qmt_daily_market_truth",
        lambda *_args, **_kwargs: SimpleNamespace(
            run_id="daily-run-1",
            catalog_manifest_hash="a" * 64,
            calendar_manifest_hash="b" * 64,
            attested_row_count=2,
        ),
    )
    monkeypatch.setattr(
        publisher,
        "_read_daily_partition",
        lambda *_args, **_kwargs: {
            key: result["partitions"][0][key]
            for key in ("row_count", "row_hash", "code_count", "code_set_hash")
        },
    )
    proof = publisher.validate_persisted_result(
        engine,
        result,
        now=datetime(2026, 8, 26, 16, 0),
    )
    assert proof["row_count"] == 2

    monkeypatch.setattr(
        publisher,
        "_read_daily_partition",
        lambda *_args, **_kwargs: {
            **{
                key: result["partitions"][0][key]
                for key in ("row_count", "row_hash", "code_count", "code_set_hash")
            },
            "row_hash": "f" * 64,
        },
    )
    with pytest.raises(publisher.StockDataBlocked, match="partition differs"):
        publisher.validate_persisted_result(
            engine,
            result,
            now=datetime(2026, 8, 26, 16, 0),
        )


def test_scheduler_validation_calls_exact_persisted_gate(monkeypatch):
    result = _daily_result()
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", "1" * 40)
    calls = []
    monkeypatch.setattr(
        publisher,
        "validate_persisted_result",
        lambda *_args, **kwargs: calls.append(kwargs) or {
            "sessions": [TRADE_DATE],
            "row_count": 2,
        },
    )
    validation = validate_scheduler_task_result(
        {
            "task_type": publisher.TASK_TYPES["daily"],
            "_release_target_date": TRADE_DATE,
            "_trigger_source": "release_catchup",
        },
        engine=object(),
        output=json.dumps(result),
        now=datetime(2026, 8, 26, 16, 0),
    )
    assert validation.checked and validation.ok
    assert calls == [{"now": datetime(2026, 8, 26, 16, 0), "expected_session": TRADE_DATE}]

    mismatch = validate_scheduler_task_result(
        {
            "task_type": publisher.TASK_TYPES["daily"],
            "_release_target_date": "2026-08-27",
            "_trigger_source": "release_catchup",
        },
        engine=object(),
        output=json.dumps(result),
        now=datetime(2026, 8, 27, 18, 0),
    )
    assert mismatch.checked and not mismatch.ok
    assert "receipt session differs" in mismatch.message
    assert len(calls) == 1


class _CalendarReceipt:
    batch_id = "calendar-1"
    manifest_hash = "a" * 64
    session_set_hash = "b" * 64

    def __init__(self, sessions):
        self.sessions = tuple(sessions)

    def sessions_between(self, start_date, end_date):
        return tuple(
            session
            for session in self.sessions
            if start_date <= session <= end_date
        )


@pytest.mark.parametrize("dataset", ("daily", "minute"))
@pytest.mark.parametrize(
    ("now", "expected"),
    (
        (datetime(2026, 8, 27, 0, 0), "2026-08-26"),
        (datetime(2026, 8, 27, 8, 0), "2026-08-26"),
        (datetime(2026, 8, 27, 15, 4), "2026-08-26"),
        (datetime(2026, 8, 27, 15, 5), "2026-08-27"),
        (datetime(2026, 8, 29, 8, 0), "2026-08-28"),
    ),
)
def test_latest_stock_session_uses_dataset_close_cutoff_and_calendar(
    monkeypatch,
    dataset,
    now,
    expected,
):
    receipt = _CalendarReceipt(
        ("2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28")
    )
    monkeypatch.setattr(
        publisher,
        "load_trade_calendar_receipt",
        lambda *_args, **_kwargs: receipt,
    )
    engine = SimpleNamespace(connect=lambda: nullcontext(object()))

    _calendar, sessions = publisher._sessions(
        engine,
        dataset=dataset,
        latest_session=True,
        start_date="",
        end_date="",
        now=now,
    )

    assert sessions == [expected]


def test_stock_persisted_gate_uses_release_session_not_ordinary_latest(
    monkeypatch,
):
    result = _daily_result()
    history_engine = _daily_engine()
    calls = []

    def resolve(*_args, **kwargs):
        calls.append(kwargs)
        session = kwargs["start_date"] if not kwargs["latest_session"] else "2026-08-27"
        return SimpleNamespace(), [session]

    monkeypatch.setattr(publisher, "_sessions", resolve)
    monkeypatch.setattr(publisher, "get_kline_engine", lambda: history_engine)
    monkeypatch.setattr(
        publisher,
        "load_qmt_daily_market_truth",
        lambda *_args, **_kwargs: SimpleNamespace(
            run_id="daily-run-1",
            catalog_manifest_hash="a" * 64,
            calendar_manifest_hash="b" * 64,
            attested_row_count=2,
        ),
    )
    monkeypatch.setattr(
        publisher,
        "_read_daily_partition",
        lambda *_args, **_kwargs: {
            key: result["partitions"][0][key]
            for key in ("row_count", "row_hash", "code_count", "code_set_hash")
        },
    )

    proof = publisher.validate_persisted_result(
        object(),
        result,
        now=datetime(2026, 8, 27, 17, 59),
        expected_session=TRADE_DATE,
    )
    assert proof["sessions"] == [TRADE_DATE]
    assert calls[-1]["latest_session"] is False
    assert calls[-1]["start_date"] == calls[-1]["end_date"] == TRADE_DATE

    with pytest.raises(publisher.StockDataBlocked, match="stale stock receipt"):
        publisher.validate_persisted_result(
            object(),
            result,
            now=datetime(2026, 8, 27, 18, 0),
            expected_session="2026-08-27",
        )
