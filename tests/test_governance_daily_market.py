from datetime import datetime

import pytest
from sqlalchemy import create_engine, text

from server.engine import governance_daily_market as market


DAY = "2026-11-13"
CAPTURED = "2026-11-15T11:30:35"
COMPLETED = "2026-11-15T11:55:28"
DECISION = datetime.fromisoformat("2026-11-16T01:11:14")


def test_read_failure_names_partition_without_exposing_sql_or_credentials():
    import time
    def failed(day):
        raise RuntimeError("private connection details")
    with pytest.raises(ValueError, match=r"2026-11-13 \(RuntimeError, errno=None\)") as error:
        list(market.read_close_batches([DAY], failed, deadline=time.monotonic() + 2))
    assert "private" not in str(error.value)


@pytest.fixture
def close_partition(monkeypatch):
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE sm_stock_kline (
                stock_code TEXT, trade_date TEXT, k_type INT, adjust_type INT,
                open REAL, close REAL, high REAL, low REAL, volume INT, amount REAL, pre_close REAL,
                data_source TEXT, quality_status TEXT, permission_status TEXT,
                source_time TEXT, received_at TEXT, batch_id TEXT, data_version TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO sm_stock_kline VALUES
            ('000001', :day, 1, 0, 10, 11, 11, 10, 100, 1100, 9, 'qmt_datadir',
             'VERIFIED', 'SUPPORTED', :event, :captured, 'writer', 'data-v1')
        """), {"day": DAY, "event": f"{DAY}T15:00:00", "captured": CAPTURED})
        manifest = {
            "manifest_hash": "a" * 64,
            "run_id": "run",
            "captured_at": CAPTURED,
            "entities": [{
                "classification": "TRADED",
                "evidence": market.canonical_bar({
                    "stock_code": "000001", "trade_date": DAY, "open": 10, "close": 11, "high": 11,
                    "low": 10, "pre_close": 9, "volume": 100, "amount": 1100,
                }),
                "row_hash": "b" * 64,
            }],
        }
        receipt = {
            "status": "COMPLETE", "run_id": "run", "finished_at": COMPLETED,
            "row_count": 1, "code_count": 1, "detail": {
                "full_market_manifest_hash": "a" * 64, "coverage": 1.0,
                "readback_rows": 1, "readback_codes": 1,
            },
        }
        monkeypatch.setattr(market, "load_daily_manifest", lambda *args, **kwargs: manifest)
        monkeypatch.setattr(market, "load_daily_native_evidence", lambda *args, **kwargs: None)
        monkeypatch.setattr(market, "load_daily_manifest_completion", lambda *args, **kwargs: receipt)
        yield connection, manifest, receipt


def test_completed_native_partition_binds_exact_readback(close_partition):
    connection, _, _ = close_partition
    verified = market.load_native_datadir_close(connection, DAY, cutoff=DECISION)
    assert verified.completed_at == datetime.fromisoformat(COMPLETED)
    assert verified.rows[0]["source_pre_close_origin"] == "NATIVE_QMT"
    assert verified.rows[0]["attestation_id"] == market.canonical_digest({
        "manifest_hash": "a" * 64, "entity_row_hash": "b" * 64,
    })
    connection.execute(text("UPDATE sm_stock_kline SET pre_close=8"))
    with pytest.raises(ValueError, match="全市场行情与清单不一致"):
        market.load_native_datadir_close(connection, DAY, cutoff=DECISION)


def test_public_or_late_receipt_cannot_prove_native_close(close_partition):
    connection, manifest, receipt = close_partition
    with pytest.raises(ValueError, match="知识截止"):
        market.load_native_datadir_close(connection, DAY, cutoff=datetime.fromisoformat(CAPTURED))
    receipt["detail"]["full_market_manifest_hash"] = "c" * 64
    with pytest.raises(ValueError, match="完成回执与清单不一致"):
        market.load_native_datadir_close(connection, DAY, cutoff=DECISION)
    manifest["historical_public_bars"] = {"000001": {}}
    with pytest.raises(ValueError, match="缺少 QMT 原生收盘证据"):
        market.load_native_datadir_close(connection, DAY, cutoff=DECISION)


def test_archival_native_partition_does_not_read_public_canonical_rows(close_partition, monkeypatch):
    connection, manifest, completion = close_partition
    monkeypatch.setattr(market, "load_daily_native_evidence", lambda *args, **kwargs: (manifest, completion))
    connection.execute(text("DROP TABLE sm_stock_kline"))
    verified = market.load_native_datadir_close(connection, DAY, cutoff=DECISION)
    assert verified.rows[0]["pre_close"] == "9.000000"
    assert verified.rows[0]["received_at"] == COMPLETED
    assert verified.rows[0]["source_time"] == f"{DAY}T15:00:00"
