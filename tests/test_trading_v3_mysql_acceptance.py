from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine

import server.db.migrations_v3 as migrations_v3
from server.db.migrations_v3 import (
    MIGRATION_PROGRESS_TABLE_DDL,
    MIGRATION_TABLE_DDL,
    MIGRATIONS,
    V3MigrationAcceptanceFaultHook,
    V3_PROJECTION_OUTBOX_MIGRATION_VERSION,
    run_v3_migrations,
)
from tools import trading_v3_mysql_acceptance as acceptance


def test_frozen_v3_acceptance_contract_matches_source():
    acceptance._assert_frozen_contract()
    assert len(acceptance.FROZEN_EXPECTED_V3_MIGRATIONS) == 21
    assert acceptance.FROZEN_EXPECTED_V3_MIGRATIONS[-1] == (
        V3_PROJECTION_OUTBOX_MIGRATION_VERSION,
        "5f53bf5258705e410b93db2b5034bbf9683ebe03b17f854625f6854fb55f7e78",
        4,
    )
    assert len(acceptance.FROZEN_V3_TABLES) == 23


@pytest.mark.parametrize(
    "url",
    (
        "mysql+pymysql://tester:secret@127.0.0.1:33578/a_v3_test_serial",
        "mysql://tester:secret@localhost/prefix_v3_ci_outbox",
    ),
)
def test_acceptance_url_requires_dedicated_v3_database(url):
    assert acceptance.require_dedicated_test_url(url) == url


@pytest.mark.parametrize(
    "url",
    (
        "mysql+pymysql://tester:secret@127.0.0.1:33578/probiga",
        "mysql+pymysql://tester:secret@127.0.0.1:33578/a_v2_evidence_test",
        "sqlite:///a_v3_test.db",
        "mysql+pymysql://tester:secret@127.0.0.1:33578/a_v3_test?charset=utf8",
        "mysql+pymysql:///a_v3_test",
    ),
)
def test_acceptance_url_rejects_wrong_backend_database_or_query(url):
    with pytest.raises(ValueError):
        acceptance.require_dedicated_test_url(url)


def test_acceptance_resolves_only_explicit_test_or_ci_environment_names():
    url = "mysql+pymysql://tester:secret@127.0.0.1:33578/a_v3_test_serial"
    assert (
        acceptance.resolve_test_url(
            "V3_TEST_SERIAL_MYSQL_URL",
            environ={"V3_TEST_SERIAL_MYSQL_URL": url},
        )
        == url
    )
    with pytest.raises(ValueError, match="forbidden"):
        acceptance.resolve_test_url("MYSQL_URL", environ={"MYSQL_URL": url})
    with pytest.raises(ValueError, match="must match"):
        acceptance.resolve_test_url(
            "V2_EVIDENCE_TEST_MYSQL_URL",
            environ={"V2_EVIDENCE_TEST_MYSQL_URL": url},
        )


def test_server_uuid_requires_independent_canonical_non_nil_value():
    server_uuid = "84190384-8ff1-11f1-ab13-74d4dd7f8500"
    assert acceptance.require_expected_server_uuid(server_uuid) == server_uuid
    assert acceptance.resolve_server_uuid(
        "V3_CI_SERIAL_MYSQL_SERVER_UUID",
        environ={"V3_CI_SERIAL_MYSQL_SERVER_UUID": server_uuid},
    ) == server_uuid
    for invalid in ("", "not-a-uuid", "00000000-0000-0000-0000-000000000000"):
        with pytest.raises(ValueError):
            acceptance.require_expected_server_uuid(invalid)


def test_least_privilege_contract_is_exact_and_schema_scoped():
    database = "a_v3_test_serial"
    grant = (
        "GRANT ALTER, CREATE, DELETE, INDEX, INSERT, REFERENCES, SELECT, "
        "TRIGGER, UPDATE ON `a_v3_test_serial`.* TO 'tester'@'127.0.0.1'"
    )
    acceptance._assert_least_privilege_grants(
        ("GRANT USAGE ON *.* TO 'tester'@'127.0.0.1'", grant),
        expected_database=database,
    )
    with pytest.raises(RuntimeError, match="unnecessary"):
        acceptance._assert_least_privilege_grants(
            (grant.replace("UPDATE", "UPDATE, DROP"),),
            expected_database=database,
        )


@pytest.mark.parametrize("version", ("5.7.38-log", "8.4.11"))
def test_identity_accepts_each_exact_validated_oracle_mysql_version(
    monkeypatch,
    version,
):
    database = "a_v3_test_serial"
    server_uuid = "84190384-8ff1-11f1-ab13-74d4dd7f8500"

    class Result:
        def __init__(self, *, scalar=None, rows=()):
            self._scalar = scalar
            self._rows = rows

        def scalar(self):
            return self._scalar

        def scalars(self):
            return iter(self._rows)

    class Connection:
        class Dialect:
            name = "mysql"

        dialect = Dialect()

        def execute(self, statement):
            sql = str(statement).upper()
            if "SHOW GRANTS" in sql:
                return Result(rows=("GRANT USAGE ON *.* TO 'u'@'h'",))
            if "@@SERVER_UUID" in sql:
                return Result(scalar=server_uuid)
            if "@@VERSION_COMMENT" in sql:
                return Result(scalar="MySQL Community Server (GPL)")
            if "VERSION()" in sql:
                return Result(scalar=version)
            if "DATABASE()" in sql:
                return Result(scalar=database)
            raise AssertionError(sql)

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Engine:
        def connect(self):
            return Connection()

    monkeypatch.setattr(
        acceptance,
        "_assert_least_privilege_grants",
        lambda *_args, **_kwargs: None,
    )

    assert acceptance._identity(
        Engine(),
        expected_database=database,
        expected_server_uuid=server_uuid,
    )[:2] == (database, version)


@pytest.mark.parametrize("version_info", ((5, 7, 38), (8, 4, 11)))
def test_v3_migration_gate_accepts_each_validated_version(version_info):
    class Dialect:
        name = "mysql"
        server_version_info = version_info

    class Engine:
        dialect = Dialect()

    class Connection:
        dialect = Dialect()

    migrations_v3._validate_migration_server(
        Engine(),  # type: ignore[arg-type]
        Connection(),  # type: ignore[arg-type]
    )


def test_v3_migration_gate_rejects_unvalidated_adjacent_patch():
    class Dialect:
        name = "mysql"
        server_version_info = (8, 4, 12)

    class Engine:
        dialect = Dialect()

    class Connection:
        dialect = Dialect()

    with pytest.raises(RuntimeError, match="validated Oracle MySQL"):
        migrations_v3._validate_migration_server(
            Engine(),  # type: ignore[arg-type]
            Connection(),  # type: ignore[arg-type]
        )


def test_controlled_prerequisites_are_explicit_and_never_fabricate_ledgers():
    ddl = "\n".join(acceptance.CONTROLLED_PREREQUISITE_DDL).casefold()
    assert acceptance.CONTROLLED_PREREQUISITE_TABLES == {
        "st_news_flash",
        "st_scheduled_tasks",
    }
    assert ddl.count("create table") == 2
    assert "schema_migration" not in ddl
    assert "insert into" not in ddl


def test_combined_schema_inventory_is_frozen_and_isolated():
    assert len(acceptance._expected_final_tables()) == 79
    assert len(acceptance._final_trigger_contract()) == 61
    assert "st_execution_projection_outbox_v2" in (
        acceptance._expected_final_tables()
    )
    assert "st_trade_account_v2" in acceptance._expected_final_tables()


def test_v3_runner_metadata_records_statement_count_and_progress():
    assert "statement_count INT NOT NULL" in MIGRATION_TABLE_DDL
    assert "completed_statement_count INT NOT NULL" in (
        MIGRATION_PROGRESS_TABLE_DDL
    )
    source = Path("server/db/migrations_v3.py").read_text(encoding="utf-8")
    assert "probiga:trading_v3_schema" in source
    assert "INSERT IGNORE INTO schema_migration_v3" in source
    assert "connection.commit()" in source


def test_v3_runner_dry_run_is_non_mutating_on_non_mysql():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        results = run_v3_migrations(engine, dry_run=True)
    finally:
        engine.dispose()
    assert len(results) == len(MIGRATIONS)
    assert {item.status for item in results} == {"would_apply"}
    assert [item.statement_count for item in results] == [
        len(tuple(item["statements"])) for item in MIGRATIONS
    ]


def test_outbox_acceptance_fault_hook_is_narrow_and_one_shot():
    hook = V3MigrationAcceptanceFaultHook.after_outbox_ddl_commit(2)
    assert hook.version == V3_PROJECTION_OUTBOX_MIGRATION_VERSION
    assert hook.committed_statement_count == 2
    with pytest.raises(ValueError, match="incomplete"):
        V3MigrationAcceptanceFaultHook.after_outbox_ddl_commit(4)
    with pytest.raises(ValueError, match="only the outbox"):
        V3MigrationAcceptanceFaultHook("forged-version", 1).validate()
    reused = V3MigrationAcceptanceFaultHook.after_outbox_ddl_commit(1)
    reused.triggered = True
    with pytest.raises(ValueError, match="fresh"):
        reused.validate()
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with pytest.raises(TypeError, match="acceptance_fault_hook"):
            run_v3_migrations(
                engine,
                acceptance_fault_hook=object(),
            )
    finally:
        engine.dispose()


def test_acceptance_modes_and_outputs_remain_non_production():
    parser = acceptance._parser()
    assert parser.parse_args([]).mode == "serial-replay"
    for mode in ("serial-replay", "concurrent-initial", "partial-recovery"):
        assert parser.parse_args(["--mode", mode]).mode == mode
    fields = acceptance.V3MySQLAcceptanceReport.__dataclass_fields__
    for field_name in (
        "outbox_runtime_enabled",
        "production_activation_allowed",
        "actionable_output_allowed",
        "worker_activation_allowed",
    ):
        assert field_name in fields


def test_acceptance_tool_never_reads_generic_application_database_url():
    source = Path("tools/trading_v3_mysql_acceptance.py").read_text(
        encoding="utf-8"
    )
    assert "create_tool_engine()" not in source
    assert "load_project_env" not in source
    assert "_FORBIDDEN_URL_ENVS" in source
