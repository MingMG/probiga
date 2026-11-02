from copy import deepcopy
import json

import pytest
from sqlalchemy import text

from server.common import kline_data
from tests.test_qmt_datadir_daily_coverage import DAY, database
from tools import ensure_quality_gate as gate


def test_release_gate_replays_real_local_cache_proof_without_native_relabelling(monkeypatch):
    engine = database(monkeypatch)
    monkeypatch.setattr(kline_data, "get_kline_engine", lambda: engine)
    with engine.connect() as connection:
        bundle = gate._load_qmt_coverage_bundle(connection, dataset="stock_daily", trade_date=DAY)
    assert bundle["manifest"]["provider"] == "qmt_datadir"
    assert bundle["manifest"]["schema"] == "probiga.qmt-datadir-daily-coverage.v1"
    assert len(bundle["entities"]) == 8
    gate._validate_qmt_canonical_strategy_partitions({("stock_daily", DAY): bundle})
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_kline SET amount=1101"))
    with pytest.raises(ValueError, match="database bars differ"):
        gate._validate_qmt_canonical_strategy_partitions({("stock_daily", DAY): bundle})
    engine.dispose()


def test_release_gate_rejects_a_forged_provider_schema_instead_of_loosening_native_gate(monkeypatch):
    engine = database(monkeypatch)
    monkeypatch.setattr(kline_data, "get_kline_engine", lambda: engine)
    with engine.connect() as connection:
        bundle = gate._load_qmt_coverage_bundle(connection, dataset="stock_daily", trade_date=DAY)
    changed = deepcopy(bundle)
    changed["manifest"]["schema"] = "probiga.qmt-history-coverage.v1"
    with pytest.raises(RuntimeError, match="release proof differs"):
        gate._validate_qmt_canonical_strategy_partitions({("stock_daily", DAY): changed})
    engine.dispose()


def test_release_gate_replays_exact_minute_matrix_and_rejects_mutation(monkeypatch):
    from server.common import qmt_datadir_minute_coverage as minute
    from tests.test_qmt_datadir_minute_coverage import rows as native_rows

    engine = database(monkeypatch)
    monkeypatch.setattr(kline_data, "get_kline_engine", lambda: engine)
    values = [dict(row, price=11) for row in native_rows() if row["stock_code"] == "600000"]
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE sm_stock_minute (" + ",".join(key + " " + (
            "REAL" if key in {"price", "change", "change_pct", "volume", "amount"} else "TEXT")
            for key in values[0]) + ")"))
        connection.execute(text("INSERT INTO sm_stock_minute VALUES (" + ",".join(":"+key for key in values[0]) + ")"), values)
        connection.execute(text("""CREATE TABLE qmt_datadir_import_partition (
          dataset TEXT,trade_date TEXT,source_root TEXT,status TEXT,code_count INTEGER,
          row_count INTEGER,content_sha256 TEXT,run_id TEXT,started_at TEXT,
          finished_at TEXT,detail_json TEXT)"""))
    scope = minute.trade_scope(engine, DAY)
    actual = minute.persisted_matrix(engine, DAY, scope=scope)
    detail = {**actual, "readback_rows": actual["row_count"], "readback_codes": actual["code_count"]}
    with engine.begin() as connection:
        connection.execute(text("INSERT INTO qmt_datadir_import_partition VALUES ('stock_minute',:day,'QMT','COMPLETE',1,241,:hash,'minute-run',:at,:at,:detail)"),
            dict(day=DAY, hash="b"*64, at=DAY+" 20:00:00", detail=json.dumps(detail)))
    with engine.connect() as connection:
        bundle = gate._load_qmt_coverage_bundle(connection, dataset="stock_minute", trade_date=DAY)
    assert bundle["manifest"]["schema"] == minute.SCHEMA
    assert bundle["manifest"]["row_count"] == 241
    gate._validate_qmt_canonical_strategy_partitions({("stock_minute", DAY): bundle})
    with engine.begin() as connection:
        connection.execute(text("UPDATE sm_stock_minute SET amount=1001 WHERE trade_time=:at"),
            {"at": DAY+" 09:31:00"})
    with pytest.raises(ValueError, match="receipt and persisted matrix"):
        gate._validate_qmt_canonical_strategy_partitions({("stock_minute", DAY): bundle})
    engine.dispose()
