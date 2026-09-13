"""Quote publication regression checks, including an isolated native MySQL.

Native checks use an existing executable named by PROBIGA_QUOTE_TEST_MYSQLD
and create their own data directory under PROBIGA_QUOTE_TEST_ROOT. They never
use project database credentials or connect to the production port.
"""
from __future__ import annotations

import os
import socket
import subprocess
import time
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, event, text

from tools import run_big_qmt_bridge as bridge
from server.common.current_quote_schema import (
    privileged_migrate_current_quote_storage,
    validate_current_quote_storage,
)


def test_daily_flow_repair_native_batch_preserves_verified_rows(native_server):
    from tools import crawl_realtime_batch as flow

    engine = create_engine(f"mysql+pymysql://root@127.0.0.1:{native_server}/quote_regression")
    target = "2026-09-11"
    def record(code, value=10, source="east_push2delay"):
        return {"stock_code": code, "trade_date": target,
                **{field: value for field in flow.CAPITAL_FLOW_FIELDS},
                "data_source": source}
    repairs = [record(str(920000 + i)) for i in range(343)]
    expected = {"600000", *(row["stock_code"] for row in repairs)}
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql("DROP TABLE IF EXISTS sm_stock_capital_flow_daily")
            conn.exec_driver_sql("CREATE TABLE sm_stock_capital_flow_daily ("
                "stock_code VARCHAR(6) NOT NULL, trade_date DATE NOT NULL, "
                + ", ".join(f"{field} DECIMAL(30,6)" for field in flow.CAPITAL_FLOW_FIELDS)
                + ", data_source VARCHAR(64), etl_sync_at DATETIME, "
                "PRIMARY KEY (stock_code, trade_date)) ENGINE=InnoDB")
            pd.DataFrame([record("600000", 99, "push2hist"),
                          record("920000", 0, "invalid")]).to_sql(
                "sm_stock_capital_flow_daily", conn, if_exists="append", index=False)
        stored, written = flow._upsert_flow_partition_delta_exact(
            engine, pd.DataFrame(repairs), trade_date=target, expected_codes=expected)
        assert written == 343
        assert set(stored.stock_code) == expected
        good = stored.set_index("stock_code").loc["600000"]
        assert good.data_source == "push2hist"
        assert good.main_net_inflow == 99
        _, written = flow._upsert_flow_partition_delta_exact(
            engine, pd.DataFrame(repairs), trade_date=target, expected_codes=expected)
        assert written == 0
    finally:
        engine.dispose()


def test_concept_repair_reads_native_history_and_verifies_content(native_server):
    from tools import repair_linux_recent_data_gaps as repair
    from tools import sync_eastmoney_concept_market as concept
    primary = create_engine("sqlite+pysqlite:///:memory:")
    history = create_engine(f"mysql+pymysql://root@127.0.0.1:{native_server}/quote_regression")
    columns = {
        "index_code": "VARCHAR(32)", "trade_time": "DATETIME", "trade_date": "DATE",
        "k_type": "INT", "etl_sync_at": "DATETIME",
    }
    row = dict(index_code="BK0001", trade_time="2026-08-26 15:00:00",
               trade_date="2026-08-26", k_type=1, etl_sync_at="2026-08-27 01:00:00",
               open=10, close=11, high=12, low=9, volume=100, amount=1000,
               change=1, change_pct=10)
    with history.begin() as c:
        c.exec_driver_sql("DROP TABLE IF EXISTS sm_concept_east_kline")
        c.exec_driver_sql("CREATE TABLE sm_concept_east_kline (" + ",".join(
            f"`{name}` {columns.get(name, 'DECIMAL(50,6)')} NOT NULL"
            for name in concept.DAILY_COLUMNS
        ) + ") ENGINE=InnoDB")
        c.execute(text("INSERT INTO sm_concept_east_kline (" + ",".join(
            f"`{name}`" for name in concept.DAILY_COLUMNS
        ) + ") VALUES (" + ",".join(f":{name}" for name in concept.DAILY_COLUMNS) + ")"), row)
    partition = repair.PartitionRef("2026-08-26", "concept_kline")
    receipt = {
        "result_sha256": "a" * 64,
        "directory": {"manifest_sha256": "b" * 64, "code_set_sha256": concept._code_set_hash(["BK0001"])},
        "dataset_results": {"kline": {"row_count": 1, "code_count": 1,
            "code_set_sha256": concept._code_set_hash(["BK0001"]),
            "content_sha256": concept.daily_content_hash([row])}},
    }
    inspector = repair.ProductionPartitionInspector(
        primary, history, decision_time=datetime(2026, 8, 27, 1, 30),
        expected_build_sha="a" * 40, prior_proofs={},
    )
    inspector.record_concept_receipt(partition, receipt)
    try:
        proof = inspector(partition)
        assert proof["row_count"] == 1
        assert proof["authority"]["source_receipt_sha256"] == "a" * 64
        with history.begin() as c:
            c.exec_driver_sql("UPDATE sm_concept_east_kline SET `close`=99")
        with pytest.raises(repair.LinuxGapRepairBlocked, match="differs from exact directory receipt"):
            inspector(partition)
    finally:
        primary.dispose()
        history.dispose()


@pytest.mark.parametrize("value,field,expected", [
    (3.552713678800501e-15, "change", "0.000000"),
    (-3.552713678800501e-15, "change", "0.000000"),
    (10.00000049999, "price", "10.000000"),
    ("10.0000005", "price", "10.000001"),
    ("-1.0000005", "change_pct", "-1.000001"),
    (1234567890123.1235, "amount", "1234567890123.123500"),
    (0, "volume", "0.000000"),
])
def test_quote_decimal_removes_binary_roundoff(value, field, expected):
    actual = bridge._quote_decimal(value, field)
    assert actual == Decimal(expected)
    assert str(actual) == expected


@pytest.mark.parametrize("value,field", [
    (None, "price"), (True, "price"), (float("nan"), "price"),
    (float("inf"), "change"), ("-Infinity", "change"),
    ("not-a-number", "volume"), (-1, "amount"), (-1, "volume"),
    (0, "price"), (-1, "price"), ("0.0000001", "price"),
    ("1e44", "amount"), ("-1e44", "change"),
    ("9" * 44 + ".9999999", "amount"),
])
def test_invalid_quote_decimal_is_rejected_before_sql(value, field):
    with pytest.raises(ValueError):
        bridge._quote_decimal(value, field)


@pytest.fixture(scope="module")
def native_server(tmp_path_factory):
    executable = os.getenv("PROBIGA_QUOTE_TEST_MYSQLD")
    root = os.getenv("PROBIGA_QUOTE_TEST_ROOT")
    if not executable or not root:
        pytest.skip("isolated native MySQL executable/test root not configured")
    executable = Path(executable).resolve(strict=True)
    parent = Path(root).resolve()
    parent.mkdir(parents=True, exist_ok=True)
    test_root = parent / ("quote-" + uuid.uuid4().hex)
    test_root.mkdir()
    data = test_root / "data"
    data.mkdir()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    assert port not in {3306, 13306, 33085}
    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    common = [str(executable), "--no-defaults", f"--datadir={data}",
              "--innodb-buffer-pool-size=64M", "--innodb-redo-log-capacity=64M"]
    initialized = subprocess.run(
        common + ["--initialize-insecure"], capture_output=True,
        timeout=90, creationflags=creationflags,
    )
    assert initialized.returncode == 0, initialized.stderr.decode(errors="replace")[-2000:]
    log = (test_root / "server.log").open("wb")
    process = subprocess.Popen(
        common + [f"--port={port}", "--bind-address=127.0.0.1", "--mysqlx=OFF",
                  "--skip-log-bin", "--performance-schema=OFF", "--max-connections=10"],
        stdout=log, stderr=log, creationflags=creationflags,
    )
    admin = create_engine(
        f"mysql+pymysql://root@127.0.0.1:{port}/mysql",
        connect_args={"connect_timeout": 2},
    )
    try:
        deadline = time.monotonic() + 45
        while True:
            assert process.poll() is None, "isolated MySQL exited; inspect " + str(test_root)
            try:
                with admin.connect() as conn:
                    assert Path(conn.exec_driver_sql("SELECT @@datadir").scalar()).resolve() == data.resolve()
                    assert str(conn.exec_driver_sql("SELECT @@version").scalar()).startswith("8.4.")
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.2)
        with admin.begin() as conn:
            conn.exec_driver_sql("CREATE DATABASE quote_regression")
        yield port
    finally:
        if process.poll() is None:
            try:
                with admin.connect() as conn:
                    if Path(conn.exec_driver_sql("SELECT @@datadir").scalar()).resolve() == data.resolve():
                        conn.exec_driver_sql("SHUTDOWN")
            except Exception:
                pass
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.terminate()
                process.wait(timeout=10)
        admin.dispose()
        log.close()


@pytest.fixture
def native_engine(native_server):
    engine = create_engine(f"mysql+pymysql://root@127.0.0.1:{native_server}/quote_regression")
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP TABLE IF EXISTS sm_stock_current")
        conn.exec_driver_sql("""
            CREATE TABLE sm_stock_current (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                stock_code VARCHAR(16) NOT NULL,
                price DECIMAL(50,6) NOT NULL,
                `change` DECIMAL(50,6) NOT NULL,
                change_pct DECIMAL(50,6) NOT NULL,
                volume DECIMAL(50,6) NOT NULL,
                amount DECIMAL(50,6) NOT NULL,
                snapshot_at DATETIME NOT NULL,
                etl_sync_at DATETIME NOT NULL,
                batch_id VARCHAR(64) NOT NULL,
                UNIQUE KEY uk_qmt_sm_stock_current_code(stock_code)
            ) ENGINE=InnoDB
        """)
    yield engine
    engine.dispose()


def quotes(codes, *, price=10, batch="first"):
    return pd.DataFrame([dict(
        stock_code=code, price=price, change=3.552713678800501e-15,
        change_pct=-3.552713678800501e-15, volume=100.0, amount=1000.123456789,
        snapshot_at=datetime(2026, 9, 11, 15), etl_sync_at=datetime(2026, 9, 11, 15),
        batch_id=batch,
    ) for code in codes])


def stored(engine):
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(text(
            "SELECT * FROM sm_stock_current ORDER BY stock_code"
        )).mappings()]


def test_native_full_and_subset_preserve_ids_precision_and_batch(native_engine):
    # More than one SQL chunk, so this covers every chunk's values/bind names.
    codes = [f"{i:06d}" for i in range(1, 503)]
    warning_counts = []

    def collect_warnings(_conn, cursor, statement, *_args):
        if statement.startswith("INSERT INTO sm_stock_current"):
            warning_counts.append(cursor._result.warning_count)

    event.listen(native_engine, "after_cursor_execute", collect_warnings)
    try:
        assert bridge._replace_full_snapshot(native_engine, quotes(codes)) == 502
        first = stored(native_engine)
        assert bridge._replace_tracked_subset(native_engine, quotes([codes[0]], price=11, batch="tracked")) == 1
        subset = stored(native_engine)
        assert [r["id"] for r in first] == [r["id"] for r in subset]
        assert subset[0]["price"] == Decimal("11.000000")
        assert subset[0]["batch_id"] == "tracked"
        assert subset[1:] == first[1:]
        assert bridge._replace_full_snapshot(native_engine, quotes(codes[:-1], price=12, batch="second")) == 501
        final = stored(native_engine)
        assert [r["id"] for r in final] == [r["id"] for r in first[:-1]]
        assert all(r["batch_id"] == "second" for r in final)
        assert all(r["change"] == r["change_pct"] == Decimal(0) for r in final)
        assert all(r["amount"] == Decimal("1000.123457") for r in final)
        assert warning_counts and not any(warning_counts)
    finally:
        event.remove(native_engine, "after_cursor_execute", collect_warnings)


def test_native_full_snapshot_failure_rolls_back_all_prior_chunks(native_engine):
    bridge._replace_full_snapshot(native_engine, quotes(["000001", "000999"]))
    before = stored(native_engine)
    calls = 0

    def fail_second_chunk(_conn, _cursor, statement, *_args):
        nonlocal calls
        if statement.startswith("INSERT INTO sm_stock_current"):
            calls += 1
            if calls == 2:
                raise RuntimeError("injected second chunk failure")

    event.listen(native_engine, "before_cursor_execute", fail_second_chunk)
    try:
        with pytest.raises(RuntimeError, match="second chunk"):
            bridge._replace_full_snapshot(native_engine, quotes([f"{i:06d}" for i in range(1, 503)], price=20))
    finally:
        event.remove(native_engine, "before_cursor_execute", fail_second_chunk)
    assert calls == 2
    assert stored(native_engine) == before


@pytest.mark.parametrize("corruption", ["unique_key", "precision", "duplicate", "nan"])
def test_native_invalid_contract_or_payload_never_changes_current_rows(native_engine, corruption):
    bridge._replace_full_snapshot(native_engine, quotes(["000001"]))
    frame = quotes(["000001"], price=11)
    with native_engine.begin() as conn:
        if corruption == "unique_key":
            conn.exec_driver_sql("ALTER TABLE sm_stock_current DROP INDEX uk_qmt_sm_stock_current_code")
        elif corruption == "precision":
            conn.exec_driver_sql("ALTER TABLE sm_stock_current MODIFY price DECIMAL(50,4) NOT NULL")
        elif corruption == "duplicate":
            frame = pd.concat([frame, frame], ignore_index=True)
        elif corruption == "nan":
            frame.loc[0, "change"] = float("nan")
    before = stored(native_engine)
    with pytest.raises((ValueError, RuntimeError)):
        bridge._replace_full_snapshot(native_engine, frame)
    assert stored(native_engine) == before


def test_native_release_removes_only_redundant_index_preserving_all_rows(native_engine):
    bridge._replace_full_snapshot(native_engine, quotes(["000001", "000002"]))
    with native_engine.begin() as connection:
        connection.exec_driver_sql("ALTER TABLE sm_stock_current ADD INDEX idx_sc_code(stock_code)")
    before = stored(native_engine)
    with pytest.raises(RuntimeError, match='release migration'):
        validate_current_quote_storage(native_engine)
    result = privileged_migrate_current_quote_storage(native_engine)
    assert result['redundant_index_removed'] is True
    assert result['preserved_rows'] == 2
    assert result['integrity_verified'] is True
    assert stored(native_engine) == before
    assert validate_current_quote_storage(native_engine)['status'] == 'HEALTHY'
    assert privileged_migrate_current_quote_storage(native_engine)['redundant_index_removed'] is False


def test_native_release_refuses_different_index_without_mutation(native_engine):
    bridge._replace_full_snapshot(native_engine, quotes(["000001"]))
    with native_engine.begin() as connection:
        connection.exec_driver_sql("ALTER TABLE sm_stock_current ADD INDEX idx_sc_code(stock_code,price)")
    before = stored(native_engine)
    with pytest.raises(RuntimeError, match='different idx_sc_code'):
        privileged_migrate_current_quote_storage(native_engine)
    assert stored(native_engine) == before
    with native_engine.connect() as connection:
        assert connection.exec_driver_sql("SHOW INDEX FROM sm_stock_current WHERE Key_name='idx_sc_code'").rowcount == 2
