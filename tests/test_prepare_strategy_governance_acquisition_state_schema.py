from contextlib import nullcontext
from types import SimpleNamespace

import pytest
from sqlalchemy import (
    Column,
    ForeignKeyConstraint,
    MetaData,
    String,
    Table,
    create_engine,
    inspect,
)
from sqlalchemy.dialects.mysql import CHAR, DATETIME, INTEGER, TIMESTAMP, TINYINT, VARCHAR
from sqlalchemy.schema import CreateIndex, CreateTable

from acquisition.store import STATE
from tools import prepare_strategy_governance_schema as schema


@pytest.fixture(autouse=True)
def controlled_database(monkeypatch):
    monkeypatch.setattr(schema, "_selected_database", lambda _connection: "probiga")


@pytest.fixture
def engine():
    value = create_engine("sqlite+pysqlite:///:memory:")
    try:
        yield value
    finally:
        value.dispose()


def test_absent_state_preflight_is_read_only(engine):
    detail = schema._preflight_direct_acquisition_progress_schema(engine)

    assert detail == {
        "schema": schema.DIRECT_ACQUISITION_PROGRESS_SCHEMA,
        "status": "ABSENT_CREATE_ALLOWED",
        "database": "probiga",
        "table": STATE.name,
        "table_exists": False,
        "physical_schema_verified": False,
        "runtime_ddl_required": False,
        "read_only": True,
    }
    assert inspect(engine).get_table_names() == []


def test_prepare_creates_shared_state_once_and_is_idempotent(engine):
    first = schema._prepare_direct_acquisition_progress_schema(engine)
    second = schema._prepare_direct_acquisition_progress_schema(engine)

    reader = inspect(engine)
    assert reader.get_table_names() == [STATE.name]
    assert {column["name"] for column in reader.get_columns(STATE.name)} == set(
        STATE.c.keys()
    )
    assert first["created_table"] is True
    assert first["physical_schema_verified"] is True
    assert second["created_table"] is False
    assert second["physical_schema_verified"] is True


def test_compatible_nullable_extension_is_not_rejected(engine):
    STATE.create(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f"ALTER TABLE {STATE.name} ADD COLUMN operator_note VARCHAR(32) NULL"
        )

    detail = schema._preflight_direct_acquisition_progress_schema(engine)

    assert detail["status"] == "READY"
    assert detail["column_count"] == len(STATE.c) + 1


def test_unexpected_success_timestamp_default_fails_closed(engine):
    ddl = str(CreateTable(STATE).compile(dialect=engine.dialect))
    assert "last_success_at DATETIME" in ddl
    ddl = ddl.replace(
        "last_success_at DATETIME",
        "last_success_at DATETIME DEFAULT CURRENT_TIMESTAMP",
    )
    with engine.begin() as connection:
        connection.exec_driver_sql(ddl)
        for index in STATE.indexes:
            connection.exec_driver_sql(
                str(CreateIndex(index).compile(dialect=engine.dialect))
            )

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="columns differ",
    ):
        schema._preflight_direct_acquisition_progress_schema(engine)


def test_missing_required_state_columns_fail_closed(engine):
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f"CREATE TABLE {STATE.name} (dataset VARCHAR(32) PRIMARY KEY)"
        )

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="columns are incomplete",
    ):
        schema._preflight_direct_acquisition_progress_schema(engine)


def test_unique_key_that_collapses_partitions_fails_closed(engine):
    STATE.create(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql(
            f"CREATE UNIQUE INDEX uk_state_dataset ON {STATE.name} (dataset)"
        )

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="unique key is too narrow",
    ):
        schema._preflight_direct_acquisition_progress_schema(engine)


def test_missing_due_index_fails_closed(engine):
    STATE.create(engine)
    with engine.begin() as connection:
        connection.exec_driver_sql("DROP INDEX idx_acquisition_due")

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="due index is unavailable",
    ):
        schema._preflight_direct_acquisition_progress_schema(engine)


def test_foreign_key_on_progress_table_fails_closed(engine):
    metadata = MetaData()
    Table("progress_owner", metadata, Column("dataset", String(32), primary_key=True))
    progress = STATE.to_metadata(metadata)
    progress.append_constraint(
        ForeignKeyConstraint(["dataset"], ["progress_owner.dataset"])
    )
    metadata.create_all(engine)

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="foreign keys are unsupported",
    ):
        schema._preflight_direct_acquisition_progress_schema(engine)


def test_mysql_progress_table_requires_innodb(monkeypatch):
    class _Inspector:
        def has_table(self, _table_name):
            return True

        def get_table_options(self, _table_name):
            return {"mysql_engine": "MyISAM"}

    connection = SimpleNamespace(dialect=SimpleNamespace(name="mysql"))
    monkeypatch.setattr(schema, "inspect", lambda _connection: _Inspector())

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="is not transactional",
    ):
        schema._direct_acquisition_progress_schema(
            connection,
            allow_absent=True,
        )


@pytest.mark.parametrize(
    ("expected", "actual", "compatible"),
    (
        (STATE.c.dataset.type, VARCHAR(32), True),
        (STATE.c.dataset.type, CHAR(32), False),
        (STATE.c.written_rows.type, INTEGER(), True),
        (STATE.c.written_rows.type, INTEGER(unsigned=True), False),
        (STATE.c.written_rows.type, TINYINT(), False),
        (STATE.c.last_attempt_at.type, DATETIME(), True),
        (STATE.c.last_attempt_at.type, TIMESTAMP(), False),
    ),
)
def test_mysql_reflected_types_preserve_state_semantics(
    expected,
    actual,
    compatible,
):
    assert schema._compatible_progress_type(expected, actual) is compatible


def test_create_rechecks_database_on_its_own_connection(monkeypatch):
    class _Engine:
        def begin(self):
            return nullcontext(SimpleNamespace(database="another_schema"))

    monkeypatch.setattr(
        schema,
        "_preflight_direct_acquisition_progress_schema",
        lambda _engine: {"status": "ABSENT_CREATE_ALLOWED"},
    )
    monkeypatch.setattr(
        schema,
        "_selected_database",
        lambda connection: connection.database,
    )
    creates = []
    monkeypatch.setattr(
        STATE,
        "create",
        lambda **_kwargs: creates.append(True),
    )

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="supports only probiga",
    ):
        schema._prepare_direct_acquisition_progress_schema(_Engine())

    assert creates == []


def test_non_probiga_database_is_rejected_before_ddl(engine, monkeypatch):
    monkeypatch.setattr(
        schema,
        "_selected_database",
        lambda _connection: "another_schema",
    )

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="supports only probiga",
    ):
        schema._prepare_direct_acquisition_progress_schema(engine)

    assert inspect(engine).get_table_names() == []
