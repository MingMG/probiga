"""Compressed minute transport retains the canonical rows and exact scope."""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
import re
from types import SimpleNamespace
import zlib

import pytest
from sqlalchemy import create_engine, text

from server.common import qmt_datadir_minute_coverage as coverage
from server.common.qmt_datadir_result import row_hash

DAY = "2026-11-02"


def _pack(raw):
    raw = raw.encode("utf-8") if isinstance(raw, str) else raw
    packed = zlib.compress(raw)
    return len(raw).to_bytes(4, "little") + packed + (b"." if packed[-1] == 0x20 else b"")


def _scope(codes):
    return dict(codes=codes, daily_close_by_code=dict.fromkeys(codes, "10"),
        full_market_manifest_hash="a" * 64, expected_traded_code_set_sha256="b" * 64)


def _native(codes):
    return [dict(stock_code=code, trade_time=datetime.fromisoformat(DAY + " " + moment),
        trade_date=date.fromisoformat(DAY), price=Decimal("10.000000"), change=Decimal("1.000000"),
        change_pct=Decimal("11.111111"), volume=Decimal("100.000000"), amount=Decimal("1000.000000"),
        qmt_code=code + ".SH", data_source="qmt_datadir", source_time=datetime.fromisoformat(DAY + " " + moment),
        batch_id="original-source-batch", data_version="qmt_datadir_v1", quality_status="VERIFIED",
        permission_status="SUPPORTED") for code in codes for moment in coverage.CORE_GRID]


def _json_rows(rows):
    return [{key: value.isoformat(sep=" ") if isinstance(value, datetime)
             else value.isoformat() if isinstance(value, date) else str(value) if isinstance(value, Decimal)
             else value for key, value in row.items()} for row in rows]


class _MysqlOverSqlite:
    """Execute the production selection on a real indexed SQLite dataset."""
    dialect = SimpleNamespace(name="mysql")

    def __init__(self, connection):
        self.connection = connection
        self.batch_sizes = []

    def execute(self, statement, params):
        sql = str(statement)
        if "COMPRESS(" in sql:
            self.batch_sizes.append(sum(key.startswith("code_") for key in params))
        sql = re.sub(r"FORCE INDEX \(`([^`]+)`\)", r'INDEXED BY "\1"', sql)
        sql = sql.replace("JSON_ARRAYAGG", "json_group_array").replace(" AS CHAR CHARACTER SET utf8mb4", " AS TEXT")
        return self.connection.execute(text(sql), params)


@pytest.mark.parametrize("staged", [False, True])
def test_actual_indexed_batch_queries_preserve_both_hashes_and_sql_types(staged):
    codes = [str(600000 + i) for i in range(53)]
    native = _native(codes)
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.connection.driver_connection.create_function("COMPRESS", 1, _pack)
        table = "qmt_datadir_minute_stage" if staged else "sm_stock_minute"
        fields = ("run_id", "stock_code", "trade_time", "trade_date", "price", "change", "change_pct", "volume", "amount", "qmt_code") if staged else coverage._MATRIX_FIELDS
        connection.execute(text(f"CREATE TABLE {table} (" + ",".join(f'\"{field}\" TEXT' for field in fields) + ")"))
        index = coverage.QMT_DATADIR_STAGE_INDEX_NAME if staged else "idx_smm_date_source_code_time"
        index_fields = "run_id,trade_date,stock_code,trade_time" if staged else "trade_date,data_source,stock_code,trade_time"
        connection.execute(text(f"CREATE INDEX {index} ON {table}({index_fields})"))
        stored = _json_rows(native)
        for row in stored:
            row["run_id"] = "original-source-batch"
        connection.execute(text(f"INSERT INTO {table} VALUES (" + ",".join(":" + field for field in fields) + ")"), stored)
        if staged:
            # Another run on the same date must not leak into this checkpoint.
            connection.execute(text(f"INSERT INTO {table} SELECT 'unrelated'," + ",".join(f'\"{field}\"' for field in fields[1:]) + f" FROM {table} LIMIT 1"))
        transport = _MysqlOverSqlite(connection)
        decoded = list(coverage._mysql_matrix_rows(transport, DAY, scope=_scope(codes),
            deadline=coverage.time.monotonic() + 60, run_id="original-source-batch" if staged else None))
        assert transport.batch_sizes == [50, 3]
    assert decoded == native
    actual = coverage.validate_matrix(decoded, day=DAY, scope=_scope(codes), include_business_fingerprint=True)
    assert actual["row_hash"] == row_hash(native)
    business = [{key: row[key] for key in coverage._MATRIX_FIELDS[:9]} for row in native]
    assert actual["business_row_hash"] == row_hash(business)
    engine.dispose()


@pytest.mark.parametrize("mutation", ["extra_code", "missing_code", "duplicate_bar"])
def test_directory_rejects_scope_or_count_errors_before_data_batches(mutation):
    codes = ["600000", "600001"]
    native = _json_rows(_native(codes))
    if mutation == "extra_code":
        native.append({**native[0], "stock_code": "600002"})
    elif mutation == "missing_code":
        native = [row for row in native if row["stock_code"] != "600001"]
    else:
        native.append(dict(native[0]))
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE sm_stock_minute(stock_code TEXT,trade_date TEXT,data_source TEXT,trade_time TEXT)"))
        connection.execute(text("CREATE INDEX idx_smm_date_source_code_time ON sm_stock_minute(trade_date,data_source,stock_code,trade_time)"))
        connection.execute(text("INSERT INTO sm_stock_minute VALUES(:stock_code,:trade_date,:data_source,:trade_time)"), native)
        transport = _MysqlOverSqlite(connection)
        with pytest.raises(ValueError, match="directory differs"):
            list(coverage._mysql_matrix_rows(transport, DAY, scope=_scope(codes), deadline=coverage.time.monotonic() + 60))
        assert transport.batch_sizes == []
    engine.dispose()


@pytest.mark.parametrize("mutation", ["count", "truncated", "declared_size", "trailing", "foreign_code", "missing_field", "duplicate_json_key", "numeric_json"])
def test_compressed_groups_reject_corrupt_or_lossy_packages(mutation):
    rows = _json_rows(_native(["600000"]))
    group = dict(stock_code="600000", row_count=241)
    if mutation == "foreign_code":
        rows[0]["stock_code"] = "600001"
    elif mutation == "missing_field":
        del rows[0]["batch_id"]
    elif mutation == "numeric_json":
        rows[0]["change_pct"] = 11.111111
    raw = json.dumps(rows)
    if mutation == "duplicate_json_key":
        raw = raw.replace('"stock_code": "600000"', '"stock_code": "600000", "stock_code": "600000"', 1)
    group["packed_rows"] = _pack(raw)
    if mutation == "count":
        group["row_count"] = 240
    elif mutation == "truncated":
        group["packed_rows"] = group["packed_rows"][:-2]
    elif mutation == "declared_size":
        group["packed_rows"] = (coverage._MAX_GROUP_JSON_BYTES + 1).to_bytes(4, "little") + group["packed_rows"][4:]
    elif mutation == "trailing":
        group["packed_rows"] += b"other"
    with pytest.raises(RuntimeError, match="minute_matrix_readback"):
        coverage._decode_matrix_group(group, expected_codes={"600000"})


def test_cached_normalization_keeps_signed_zero_and_aware_offset_bytes():
    native = _native(["600000"])
    for index, row in enumerate(native):
        row["change"] = Decimal("-0.000000") if index % 2 else Decimal("0.000000")
        row["change_pct"] = -0.0 if index % 2 else 0.0
        offset = timezone(timedelta(hours=8))
        row["trade_time"] = row["trade_time"].replace(tzinfo=offset)
        row["source_time"] = row["source_time"].replace(tzinfo=offset)
    actual = coverage.validate_matrix(native, day=DAY, scope=_scope(["600000"]), include_business_fingerprint=True)
    assert actual["row_hash"] == row_hash(native)
    business = [{key: Decimal(str(value)) if key in coverage._NUMERIC_FIELDS else value
                 for key, value in row.items() if key in coverage._MATRIX_FIELDS[:9]} for row in native]
    assert actual["business_row_hash"] == row_hash(business)


def test_consume_budget_covers_measured_full_market_and_remains_bounded():
    assert coverage.matrix_consume_budget({"codes": ["600000"]}) == 180
    assert coverage.matrix_consume_budget({"codes": range(5571)}) == pytest.approx(731.3055)
    assert coverage.matrix_consume_budget({"codes": range(20000)}) == 900


def test_explicit_consume_deadline_still_stops_progressing_rows(monkeypatch):
    clock = iter([0, 0, 0.5, 1.5])
    monkeypatch.setattr(coverage.time, "monotonic", lambda: next(clock))
    with pytest.raises(TimeoutError, match="total deadline exceeded"):
        coverage.validate_matrix(_native(["600000"]), day=DAY, scope=_scope(["600000"]), deadline_seconds=1)
