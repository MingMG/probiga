from __future__ import annotations

from integrations.qmt.safe_upsert import prepare_qmt_rows, safe_upsert_rows


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _RowsResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, engine):
        self.engine = engine

    def execute(self, statement, params=None):
        sql = str(statement)
        self.engine.sql.append(sql)
        self.engine.params.append(params)
        if "information_schema.COLUMNS" in sql:
            return _RowsResult([(column,) for column in self.engine.columns])
        if "SELECT COUNT(*) FROM `tmp_qmt_" in sql:
            return _ScalarResult(self.engine.temp_inserted)
        if sql.startswith("INSERT INTO `tmp_qmt_") and isinstance(params, list):
            self.engine.temp_inserted += len(params)
        return _ScalarResult(None)


class _Transaction:
    def __init__(self, engine):
        self.connection = _Connection(engine)

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class _Engine:
    def __init__(self, columns):
        self.columns = columns
        self.sql = []
        self.params = []
        self.temp_inserted = 0

    def begin(self):
        return _Transaction(self)


def test_prepare_qmt_rows_adds_provenance_and_dedupes_by_key():
    rows, duplicate_count = prepare_qmt_rows(
        [
            {"stock_code": "000001", "price": 10, "unknown": "ignored"},
            {"stock_code": "000001", "price": 11},
        ],
        table_columns=[
            "stock_code",
            "price",
            "data_source",
            "received_at",
            "batch_id",
            "quality_status",
            "permission_status",
        ],
        key_columns=["stock_code"],
        batch_id="batch-1",
    )

    assert duplicate_count == 1
    assert rows == [
        {
            "stock_code": "000001",
            "price": 11,
            "data_source": "gj_qmt",
            "received_at": rows[0]["received_at"],
            "batch_id": "batch-1",
            "quality_status": "PENDING",
            "permission_status": "SUPPORTED",
        }
    ]


def test_safe_upsert_uses_temp_table_and_no_destructive_sql():
    engine = _Engine(
        [
            "stock_code",
            "trade_date",
            "price",
            "data_source",
            "received_at",
            "batch_id",
            "quality_status",
            "permission_status",
        ]
    )

    result = safe_upsert_rows(
        engine,
        table_name="sm_stock_current",
        rows=[{"stock_code": "000001", "trade_date": "2026-06-26", "price": 10.5}],
        key_columns=["stock_code", "trade_date"],
        batch_id="batch-2",
    )

    all_sql = "\n".join(engine.sql).upper()
    assert result.status == "UPSERTED"
    assert result.accepted_rows == 1
    assert "CREATE TEMPORARY TABLE" in all_sql
    assert "ON DUPLICATE KEY UPDATE" in all_sql
    assert "TRUNCATE" not in all_sql
    assert "DELETE FROM" not in all_sql
    assert engine.params[2][0]["data_source"] == "gj_qmt"
    assert engine.params[2][0]["batch_id"] == "batch-2"


def test_safe_upsert_rejects_unsafe_table_name():
    engine = _Engine(["stock_code", "price"])

    try:
        safe_upsert_rows(
            engine,
            table_name="sm_stock_current;drop",
            rows=[{"stock_code": "000001", "price": 10.5}],
            key_columns=["stock_code"],
        )
    except ValueError as exc:
        assert "Unsafe SQL identifier" in str(exc)
    else:
        raise AssertionError("unsafe table name should be rejected")


def test_prepare_qmt_rows_requires_key_values():
    try:
        prepare_qmt_rows(
            [{"stock_code": "", "price": 10}],
            table_columns=["stock_code", "price"],
            key_columns=["stock_code"],
        )
    except ValueError as exc:
        assert "Missing required key columns" in str(exc)
    else:
        raise AssertionError("missing key should be rejected")
