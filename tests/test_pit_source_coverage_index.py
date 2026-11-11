from contextlib import contextmanager
from copy import deepcopy
import re
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from server.common import pit_facts
from server.common import pit_source_coverage_index as migration


INDEX_NAME = "idx_pit_coverage_batch_stock"
INDEX_COLUMNS = ("fact_kind", "source", "batch_id", "stock_code")
BASELINE_PIT_CONTRACT_HASH = "c374e0ba62eb2e5b9bef802ce2bdd89fae0c63391d918e922ff21781707863ae"


class MetadataRows:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return deepcopy(self.rows)


class MysqlIndexConnection:
    def __init__(self, fixture, connection):
        self.fixture = fixture
        self.connection = connection

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        if "information_schema.statistics" in sql.lower():
            assert parameters == {
                "table_name": "st_pit_source_coverage",
                "index_name": INDEX_NAME,
            }
            rows = self.fixture.reflect_index(self.connection)
            for row in rows:
                row.update(self.fixture.metadata_overrides)
            return MetadataRows(rows)
        if sql.upper().startswith("ALTER TABLE"):
            self.fixture.ddl_calls += 1
            assert "ALGORITHM=INPLACE" in sql and "LOCK=NONE" in sql
            match = re.fullmatch(
                r"ALTER TABLE `st_pit_source_coverage` ADD INDEX `([^\x60]+)` "
                r"\(([^)]+)\), ALGORITHM=INPLACE, LOCK=NONE",
                sql,
            )
            assert match is not None
            assert match.group(1) == INDEX_NAME
            columns = tuple(item.strip().strip("`") for item in match.group(2).split(","))
            if self.fixture.ddl_outcome == "missing":
                return MetadataRows([])
            if self.fixture.ddl_outcome == "wrong":
                columns = columns[:-1]
            self.fixture.create_index(self.connection, columns)
            return MetadataRows([])
        raise AssertionError("Unexpected statement in the index migration")


class MysqlIndexFixture:
    dialect = SimpleNamespace(name="mysql")

    def __init__(self, storage, *, ddl_outcome="correct", metadata_overrides=None):
        self.storage = storage
        self.ddl_calls = 0
        self.ddl_outcome = ddl_outcome
        self.metadata_overrides = metadata_overrides or {}

    @contextmanager
    def begin(self):
        with self.storage.begin() as connection:
            yield MysqlIndexConnection(self, connection)

    @contextmanager
    def connect(self):
        with self.storage.connect() as connection:
            yield MysqlIndexConnection(self, connection)

    def create_index(self, connection, columns=INDEX_COLUMNS, *, unique=False):
        assert set(columns) <= pit_facts._REQUIRED_COLUMNS[pit_facts.SOURCE_COVERAGE_TABLE]
        quoted_columns = ",".join(f'"{column}"' for column in columns)
        unique_sql = "UNIQUE " if unique else ""
        connection.execute(text(
            f'CREATE {unique_sql}INDEX "{INDEX_NAME}" ON "st_pit_source_coverage" ({quoted_columns})'
        ))

    def reflect_index(self, connection):
        indexes = connection.exec_driver_sql('PRAGMA index_list("st_pit_source_coverage")').mappings()
        selected = next((row for row in indexes if row["name"] == INDEX_NAME), None)
        if selected is None:
            return []
        columns = connection.exec_driver_sql(f'PRAGMA index_xinfo("{INDEX_NAME}")').mappings()
        return [
            {
                "index_name": INDEX_NAME,
                "non_unique": 0 if selected["unique"] else 1,
                "seq_in_index": row["seqno"] + 1,
                "column_name": row["name"],
                "sub_part": None,
                "index_type": "BTREE",
                "is_visible": "YES",
                "expression": None,
                "collation": "D" if row["desc"] else "A",
            }
            for row in columns if row["key"]
        ]

    def index_sql(self):
        with self.storage.connect() as connection:
            return connection.execute(text(
                "SELECT sql FROM sqlite_master WHERE type='index' AND name=:name"
            ), {"name": INDEX_NAME}).scalar_one_or_none()


@pytest.fixture
def storage():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    pit_facts.ensure_pit_fact_schema(engine)
    for code in ("000001", "600000"):
        pit_facts.append_source_coverage(
            engine, fact_kind="finance", stock_code=code,
            window_start="1900-01-01", window_end="2026-11-10",
            known_at="2026-11-10 21:00:00", covered_through_at="2026-11-10 21:00:00",
            watermark_kind="CAPTURED_AT", watermark_evidence={"fixture": "原始已封存记录"},
            source_rows=[], fact_bindings=[], source="test.finance", batch_id="original-batch",
        )
    yield engine
    engine.dispose()


def source_rows(storage):
    with storage.connect() as connection:
        return connection.execute(text(
            "SELECT * FROM st_pit_source_coverage ORDER BY coverage_id"
        )).mappings().all()


def test_missing_batch_index_is_created_verified_and_idempotent(storage):
    fixture = MysqlIndexFixture(storage)
    original_rows = source_rows(storage)
    before = pit_facts.pit_fact_schema_health(storage)
    assert before["contract_hash"] == BASELINE_PIT_CONTRACT_HASH
    assert fixture.index_sql() is None

    result = migration.ensure_pit_source_coverage_batch_index(fixture)
    assert result == {
        "table": "st_pit_source_coverage", "index": INDEX_NAME,
        "columns": list(INDEX_COLUMNS), "unique": False, "created": True, "verified": True,
    }
    with storage.connect() as connection:
        observed = fixture.reflect_index(connection)
    assert tuple(row["column_name"] for row in observed) == INDEX_COLUMNS
    assert all(row["non_unique"] == 1 for row in observed)
    assert fixture.ddl_calls == 1

    second = migration.ensure_pit_source_coverage_batch_index(fixture)
    assert second == {**result, "created": False}
    assert fixture.ddl_calls == 1
    assert source_rows(storage) == original_rows
    assert pit_facts.pit_fact_schema_health(storage) == before
    assert INDEX_NAME not in pit_facts.PIT_FACT_TABLE_DDLS[pit_facts.SOURCE_COVERAGE_TABLE]


@pytest.mark.parametrize("columns,unique", [
    (("source", "fact_kind", "batch_id", "stock_code"), False),
    (("fact_kind", "source", "batch_id"), False),
    ((*INDEX_COLUMNS, "known_at"), False),
    (INDEX_COLUMNS, True),
])
def test_wrong_existing_index_is_rejected_and_preserved(storage, columns, unique):
    fixture = MysqlIndexFixture(storage)
    with storage.begin() as connection:
        fixture.create_index(connection, columns, unique=unique)
    original_index = fixture.index_sql()
    original_rows = source_rows(storage)
    with pytest.raises(RuntimeError, match="PIT source coverage batch index"):
        migration.ensure_pit_source_coverage_batch_index(fixture)
    assert fixture.ddl_calls == 0
    assert fixture.index_sql() == original_index
    assert source_rows(storage) == original_rows


@pytest.mark.parametrize("override", [
    {"sub_part": 8},
    {"is_visible": "NO"},
    {"index_type": "HASH"},
    {"expression": "lower(source)"},
    {"collation": "D"},
])
def test_noncanonical_mysql_index_metadata_is_rejected(storage, override):
    fixture = MysqlIndexFixture(storage, metadata_overrides=override)
    with storage.begin() as connection:
        fixture.create_index(connection)
    original_index = fixture.index_sql()
    with pytest.raises(RuntimeError, match="PIT source coverage batch index"):
        migration.ensure_pit_source_coverage_batch_index(fixture)
    assert fixture.ddl_calls == 0
    assert fixture.index_sql() == original_index


@pytest.mark.parametrize("outcome", ["missing", "wrong"])
def test_online_add_requires_a_correct_durable_readback(storage, outcome):
    fixture = MysqlIndexFixture(storage, ddl_outcome=outcome)
    original_rows = source_rows(storage)
    with pytest.raises(RuntimeError, match="PIT source coverage batch index"):
        migration.ensure_pit_source_coverage_batch_index(fixture)
    assert fixture.ddl_calls == 1
    assert source_rows(storage) == original_rows
    if outcome == "missing":
        assert fixture.index_sql() is None


def test_sqlite_call_does_not_apply_mysql_index(storage):
    original_rows = source_rows(storage)
    result = migration.ensure_pit_source_coverage_batch_index(storage)
    assert result["not_applicable"] is True
    assert result["created"] is False
    assert result["verified"] is False
    assert MysqlIndexFixture(storage).index_sql() is None
    assert source_rows(storage) == original_rows


def test_batch_index_contract_is_exact():
    assert migration.PIT_SOURCE_COVERAGE_BATCH_INDEX_NAME == INDEX_NAME
    assert migration.PIT_SOURCE_COVERAGE_BATCH_INDEX_COLUMNS == INDEX_COLUMNS
