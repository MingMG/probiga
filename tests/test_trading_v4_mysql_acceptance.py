from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
import re
from threading import Lock
import uuid

import pytest
from sqlalchemy.exc import OperationalError

import server.db.migrations_v4 as migrations
from tools import trading_v4_mysql_acceptance as acceptance


TEST_SERVER_UUID = "12345678-1234-4234-8234-123456789abc"
TEST_DATABASE = "probiga_v4_test"
ORACLE_VERSION = "5.7.38"
ORACLE_COMMENT = "MySQL Community Server (GPL)"
TARGET_GRANTS = (
    "GRANT USAGE ON *.* TO 'acceptor'@'localhost'",
    "GRANT ALTER, SELECT, INSERT, UPDATE, CREATE, REFERENCES, TRIGGER "
    "ON `probiga_v4_test`.* TO 'acceptor'@'localhost'",
)
REAL_PREFLIGHT = acceptance._preflight_empty_schema
_BINARY_REGEXP = re.compile(
    r"\bBINARY\s+[A-Za-z_][A-Za-z0-9_.]*\s+(?:NOT\s+)?REGEXP\b",
    flags=re.IGNORECASE,
)


@pytest.fixture(autouse=True)
def _accepted_runtime_identity(monkeypatch):
    def accepted(_engine, database, expected_uuid):
        if expected_uuid != TEST_SERVER_UUID:
            raise RuntimeError("identity mismatch")
        return (database, ORACLE_VERSION, TEST_SERVER_UUID, ORACLE_COMMENT)

    monkeypatch.setattr(
        acceptance,
        "_preflight_empty_schema",
        accepted,
    )
    monkeypatch.setattr(
        acceptance,
        "_assert_engine_identity",
        accepted,
    )
    monkeypatch.setattr(
        acceptance,
        "_assert_job_lease_schema",
        lambda _engine: None,
    )


@dataclass(frozen=True)
class _Result:
    status: str
    checksum: str = "a" * 64


class _Engine:
    def __init__(self) -> None:
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


@pytest.mark.parametrize(
    "value",
    (
        "",
        "sqlite:///:memory:",
        "mysql+pymysql://u:p@localhost/probiga",
        "mysql+pymysql://u:p@localhost/probiga_v4",
        "mysql+pymysql://u:p@localhost/probiga_test",
        "mysql+pymysql://u:p@localhost/v4_test",
        "mariadb+pymysql://u:p@localhost/probiga_v4_test",
        "mysql+pymysql://u:p@localhost/probiga_v4_test?charset=utf8mb4",
    ),
)
def test_dedicated_test_url_rejects_missing_non_mysql_and_unsafe_names(value):
    with pytest.raises(ValueError):
        acceptance.require_dedicated_test_url(value)


@pytest.mark.parametrize(
    "database",
    ("probiga_v4_test", "probiga_v4_ci", "probiga_v4_test_20260803"),
)
def test_dedicated_test_url_accepts_only_explicit_v4_test_names(database):
    value = f"mysql+pymysql://user:secret@localhost/{database}"
    assert acceptance.require_dedicated_test_url(value) == value


def test_frozen_migration_checksum_and_table_contract_is_independent(
    monkeypatch,
):
    acceptance._assert_frozen_migration_contract()
    monkeypatch.setattr(
        acceptance,
        "_DECLARED_MIGRATIONS",
        (("20260803_001_trading_v4_control_plane", "0" * 64, 7),),
    )
    with pytest.raises(RuntimeError, match="frozen"):
        acceptance._assert_frozen_migration_contract()


def test_frozen_004_trigger_contract_matches_all_six_guarded_tables() -> None:
    fourth = acceptance.MIGRATIONS[3]
    assert acceptance.FROZEN_EXPECTED_MIGRATIONS[3] == (
        str(fourth["version"]),
        acceptance._statement_checksum(tuple(fourth["statements"])),
        16,
    )
    declared = tuple(
        sorted(
            (
                name,
                event,
                table_name,
                "BEFORE",
                1,
                acceptance._normalized_trigger_body_hash(
                    migrations._trigger_body_from_ddl(statement)
                ),
            )
            for name, event, table_name, statement in (
                migrations.CONTROL_GUARD_TRIGGER_SPECS
            )
        )
    )
    assert declared == acceptance.FROZEN_CONTROL_GUARD_TRIGGERS
    assert tuple(sorted(migrations.CONTROL_GUARD_TABLES)) == (
        acceptance.FROZEN_CONTROL_GUARD_TABLES
    )
    assert len({item[0] for item in declared}) == 16


def test_version_aware_trigger_contract_preserves_5738_and_attests_84() -> None:
    class Connection:
        class Dialect:
            name = "mysql"
            server_version_info = (5, 7, 38)

        dialect = Dialect()

    connection = Connection()
    assert acceptance._expected_trigger_contract(
        connection,  # type: ignore[arg-type]
        tuple(acceptance.MIGRATIONS[1]["statements"]),
        include_table=False,
    ) == acceptance.FROZEN_JOB_LEASE_TRIGGERS
    assert acceptance._expected_trigger_contract(
        connection,  # type: ignore[arg-type]
        tuple(acceptance.MIGRATIONS[2]["statements"]),
        include_table=False,
    ) == acceptance.FROZEN_CLAIM_TOKEN_TRIGGERS
    assert acceptance._expected_trigger_contract(
        connection,  # type: ignore[arg-type]
        tuple(acceptance.MIGRATIONS[3]["statements"]),
        include_table=True,
    ) == acceptance.FROZEN_CONTROL_GUARD_TRIGGERS

    connection.dialect.server_version_info = (8, 4, 11)
    mysql84_job_contract = acceptance._expected_trigger_contract(
        connection,  # type: ignore[arg-type]
        tuple(acceptance.MIGRATIONS[1]["statements"]),
        include_table=False,
    )
    assert tuple(item[:-1] for item in mysql84_job_contract) == tuple(
        item[:-1] for item in acceptance.FROZEN_JOB_LEASE_TRIGGERS
    )
    assert tuple(item[-1] for item in mysql84_job_contract) != tuple(
        item[-1] for item in acceptance.FROZEN_JOB_LEASE_TRIGGERS
    )


def test_mysql84_partial_prefix_executes_compatible_frozen_trigger() -> None:
    class Connection:
        class Dialect:
            name = "mysql"
            server_version_info = (8, 4, 11)

        dialect = Dialect()

        def __init__(self) -> None:
            self.sql: list[str] = []

        def execute(self, statement):
            rendered = str(statement)
            if _BINARY_REGEXP.search(rendered):
                raise RuntimeError("ER_CHARACTER_SET_MISMATCH")
            self.sql.append(rendered)

    connection = Connection()

    class Begin:
        def __enter__(self):
            return connection

        def __exit__(self, *_args):
            return False

    class Engine:
        def begin(self):
            return Begin()

    frozen = migrations._JOB_LEASE_INSERT_TRIGGER_DDL
    acceptance._apply_partial_migration_prefix(
        Engine(),  # type: ignore[arg-type]
        (frozen,),
    )

    assert migrations._JOB_LEASE_INSERT_TRIGGER_DDL == frozen
    assert len(connection.sql) == 1
    assert _BINARY_REGEXP.search(connection.sql[0]) is None


def test_control_guard_metadata_uses_binary_trigger_ordering() -> None:
    source = Path(acceptance.__file__).read_text(encoding="utf-8")

    assert source.count("ORDER BY BINARY TRIGGER_NAME") == 3
    assert '"ORDER BY TRIGGER_NAME"' not in source


def test_real_head_cas_acceptance_context_uses_v4_artifact_namespace() -> None:
    context = acceptance._acceptance_context(
        datetime(2026, 8, 4, 1, 2, 3, tzinfo=timezone.utc),
        marker="a",
    )

    assert context.universe_version.startswith("v4:")
    assert all(
        value.startswith("v4:")
        for value in context.factor_spec_versions.values()
    )
    assert all(value.startswith("v4:") for value in context.model_versions.values())


def test_frozen_job_lease_triggers_cover_boundary_posix_whitespace() -> None:
    exact_text_fields = (
        "job_id",
        "job_type",
        "input_context_id",
        "run_uid",
        "status",
        "lease_owner",
        "error_code",
        "error_message",
    )
    boundary_pattern = "'(^[[:space:]])|([[:space:]]$)'"
    insert_trigger, update_trigger = tuple(
        acceptance.MIGRATIONS[1]["statements"]
    )[4:]
    for trigger in (insert_trigger, update_trigger):
        for field in exact_text_fields:
            assert f"BINARY NEW.{field} REGEXP" in trigger
        assert trigger.count(boundary_pattern) == len(exact_text_fields)


def test_frozen_mysql_5738_unsigned_int_display_widths_are_exact() -> None:
    max_attempts = next(
        item
        for item in acceptance.FROZEN_JOB_LEASE_COLUMNS
        if item[0] == "max_attempts"
    )
    attempt_count = next(
        item
        for item in acceptance.FROZEN_CLAIM_TOKEN_COLUMNS
        if item[0] == "attempt_count"
    )
    assert max_attempts[1] == "int(10) unsigned"
    assert attempt_count[1] == "int(10) unsigned"


def test_mysql84_integer_metadata_matches_frozen_mysql5738_contract() -> None:
    assert acceptance._normalized_column_type("int(10) unsigned") == (
        "int unsigned"
    )
    assert acceptance._normalized_column_type("int unsigned") == (
        "int unsigned"
    )
    assert acceptance._normalized_column_contracts(
        acceptance.FROZEN_JOB_LEASE_COLUMNS
    )[1][1] == "int unsigned"


def test_resolve_test_url_never_falls_back_to_normal_mysql_url():
    with pytest.raises(ValueError, match="dedicated"):
        acceptance.resolve_test_url(
            environ={"MYSQL_URL": "mysql+pymysql://u:p@localhost/probiga"}
        )


@pytest.mark.parametrize("env_name", ("MYSQL_URL", "DATABASE_URL"))
def test_resolve_test_url_explicitly_rejects_generic_url_variables(env_name):
    with pytest.raises(ValueError, match="forbidden"):
        acceptance.resolve_test_url(
            env_name,
            environ={env_name: "mysql+pymysql://u:p@localhost/probiga_v4_test"},
        )


@pytest.mark.parametrize(
    "env_name",
    (
        "V4_MYSQL_URL",
        "TEST_V4_MYSQL_URL",
        "V4_PROD_MYSQL_URL",
        "v4_test_mysql_url",
        "V4_TEST_DATABASE_URL",
    ),
)
def test_resolve_test_url_rejects_non_test_ci_environment_names(env_name):
    with pytest.raises(ValueError, match="must match"):
        acceptance.resolve_test_url(
            env_name,
            environ={env_name: "mysql+pymysql://u:p@localhost/probiga_v4_test"},
        )


@pytest.mark.parametrize(
    "env_name",
    (
        "V4_TEST_MYSQL_URL",
        "V4_CI_MYSQL_URL",
        "V4_TEST_ACCEPTANCE_MYSQL_URL",
        "V4_CI_JOB_42_MYSQL_URL",
    ),
)
def test_resolve_test_url_accepts_only_explicit_test_ci_environment_names(env_name):
    value = "mysql+pymysql://u:p@localhost/probiga_v4_test"
    assert acceptance.resolve_test_url(env_name, environ={env_name: value}) == value


def test_acceptance_checks_single_pool_replay_and_concurrent_replay(monkeypatch):
    engines: list[_Engine] = []
    engine_calls: list[dict[str, object]] = []
    call_lock = Lock()
    call_count = 0

    def fake_engine(_url: str, **kwargs):
        engine_calls.append(kwargs)
        engine = _Engine()
        engines.append(engine)
        return engine

    def fake_migrations(_engine):
        nonlocal call_count
        with call_lock:
            call_count += 1
            status = "applied" if call_count == 1 else "exists"
        return [_Result(status=status)]

    def fake_tables(_engine):
        return acceptance.V4_CONTROL_PLANE_TABLES

    monkeypatch.setattr(acceptance, "create_tool_engine", fake_engine)
    monkeypatch.setattr(acceptance, "run_v4_migrations", fake_migrations)
    monkeypatch.setattr(acceptance, "_table_names", fake_tables)

    report = acceptance.run_mysql_acceptance(
        "mysql+pymysql://u:p@localhost/probiga_v4_test",
        expected_server_uuid=TEST_SERVER_UUID,
        concurrency=3,
    )

    assert report.started_empty is True
    assert report.initial_migration == ("applied",)
    assert report.serial_replay == ("exists",)
    assert report.concurrent_replays == (("exists",),) * 3
    assert report.observed_tables == tuple(
        sorted(acceptance.V4_CONTROL_PLANE_TABLES)
    )
    assert engine_calls[0]["pool_size"] == 1
    assert engine_calls[0]["max_overflow"] == 0
    assert engine_calls[1]["pool_size"] == 1
    assert all(engine.disposed for engine in engines)


def test_concurrent_initial_mode_has_one_applied_writer_and_no_cleanup(monkeypatch):
    engine = _Engine()
    call_lock = Lock()
    call_count = 0
    engine_kwargs: dict[str, object] = {}

    def fake_engine(_url: str, **kwargs):
        engine_kwargs.update(kwargs)
        return engine

    def fake_migrations(_engine):
        nonlocal call_count
        with call_lock:
            call_count += 1
            status = "applied" if call_count == 1 else "exists"
        return [_Result(status=status)]

    table_calls = 0

    def fake_tables(_engine):
        nonlocal table_calls
        table_calls += 1
        return acceptance.V4_CONTROL_PLANE_TABLES

    monkeypatch.setattr(acceptance, "create_tool_engine", fake_engine)
    monkeypatch.setattr(acceptance, "run_v4_migrations", fake_migrations)
    monkeypatch.setattr(acceptance, "_table_names", fake_tables)

    report = acceptance.run_mysql_concurrent_initial_acceptance(
        "mysql+pymysql://u:p@localhost/probiga_v4_test_concurrent",
        expected_server_uuid=TEST_SERVER_UUID,
        concurrency=4,
    )

    statuses = [
        status
        for run in report.concurrent_initial_runs
        for status in run
    ]
    assert statuses.count("applied") == 1
    assert statuses.count("exists") == 3
    assert report.started_empty is True
    assert report.observed_tables == tuple(
        sorted(acceptance.V4_CONTROL_PLANE_TABLES)
    )
    assert engine_kwargs["pool_size"] == 1
    assert engine_kwargs["max_overflow"] == 0
    assert table_calls == 1
    assert engine.disposed is True


def test_concurrent_initial_mode_rejects_multiple_applied_writers(monkeypatch):
    engine = _Engine()
    def fake_tables(_engine):
        return acceptance.V4_CONTROL_PLANE_TABLES

    monkeypatch.setattr(
        acceptance,
        "create_tool_engine",
        lambda *_args, **_kwargs: engine,
    )
    monkeypatch.setattr(acceptance, "_table_names", fake_tables)
    monkeypatch.setattr(
        acceptance,
        "run_v4_migrations",
        lambda _engine: [_Result(status="applied")],
    )

    with pytest.raises(RuntimeError, match="exactly one applied"):
        acceptance.run_mysql_concurrent_initial_acceptance(
            "mysql+pymysql://u:p@localhost/probiga_v4_test_concurrent",
            expected_server_uuid=TEST_SERVER_UUID,
            concurrency=2,
        )

    assert engine.disposed is True


def test_partial_recovery_mode_completes_prefix_without_cleaning(monkeypatch):
    engine = _Engine()
    table_states = iter(
        (
            frozenset({"st_decision_context_v4"}),
            acceptance.V4_CONTROL_PLANE_TABLES,
        )
    )
    applied_prefixes: list[tuple[str, ...]] = []
    migration_calls = 0

    def fake_migrations(_engine):
        nonlocal migration_calls
        migration_calls += 1
        status = "applied" if migration_calls == 1 else "exists"
        return [
            _Result(status=status)
            for _migration in acceptance.MIGRATIONS
        ]

    monkeypatch.setattr(
        acceptance,
        "create_tool_engine",
        lambda *_args, **_kwargs: engine,
    )
    monkeypatch.setattr(
        acceptance,
        "_table_names",
        lambda _engine: next(table_states),
    )
    monkeypatch.setattr(acceptance, "run_v4_migrations", fake_migrations)
    monkeypatch.setattr(
        acceptance,
        "_apply_partial_migration_prefix",
        lambda _engine, statements: applied_prefixes.append(statements),
    )

    report = acceptance.run_mysql_partial_recovery_acceptance(
        "mysql+pymysql://u:p@localhost/probiga_v4_test_partial",
        expected_server_uuid=TEST_SERVER_UUID,
        partial_statement_count=1,
    )

    assert len(applied_prefixes) == 1
    assert len(applied_prefixes[0]) == 1
    assert report.partial_statement_count == 1
    assert report.partial_observed_tables == ("st_decision_context_v4",)
    assert report.recovery_migration == ("applied",) * len(
        acceptance.MIGRATIONS
    )
    assert report.recovery_replay == ("exists",) * len(
        acceptance.MIGRATIONS
    )
    assert report.observed_tables == tuple(
        sorted(acceptance.V4_CONTROL_PLANE_TABLES)
    )
    assert engine.disposed is True


def test_partial_recovery_mode_covers_004_trigger_prefix(monkeypatch):
    engine = _Engine()
    completed_prefixes: list[int] = []
    partial_prefixes: list[tuple[str, ...]] = []
    migration_calls = 0

    def fake_migrations(_engine):
        nonlocal migration_calls
        migration_calls += 1
        statuses = (
            (
                "exists",
                "exists",
                "exists",
                "applied",
                "applied",
                "applied",
                "applied",
            )
            if migration_calls == 1
            else ("exists",) * 7
        )
        return [_Result(status=status) for status in statuses]

    monkeypatch.setattr(
        acceptance,
        "create_tool_engine",
        lambda *_args, **_kwargs: engine,
    )
    monkeypatch.setattr(
        acceptance,
        "_table_names",
        lambda _engine: acceptance.V4_CONTROL_PLANE_TABLES,
    )
    monkeypatch.setattr(acceptance, "run_v4_migrations", fake_migrations)
    monkeypatch.setattr(
        acceptance,
        "_apply_completed_migration_prefix",
        lambda _engine, count: completed_prefixes.append(count),
    )
    monkeypatch.setattr(
        acceptance,
        "_apply_partial_migration_prefix",
        lambda _engine, statements: partial_prefixes.append(statements),
    )

    report = acceptance.run_mysql_partial_recovery_acceptance(
        "mysql+pymysql://u:p@localhost/probiga_v4_test_partial",
        expected_server_uuid=TEST_SERVER_UUID,
        partial_migration_index=3,
        partial_statement_count=15,
    )

    assert completed_prefixes == [3]
    assert len(partial_prefixes) == 1
    assert partial_prefixes[0] == tuple(
        acceptance.MIGRATIONS[3]["statements"]
    )[:15]
    assert report.partial_migration_version == (
        "20260804_004_v4_control_plane_guards"
    )
    assert report.recovery_migration == (
        "exists",
        "exists",
        "exists",
        "applied",
        "applied",
        "applied",
        "applied",
    )
    assert report.recovery_replay == ("exists",) * 7
    assert engine.disposed is True


@pytest.mark.parametrize("count", (True, 0, -1, 999, 1.5))
def test_partial_recovery_rejects_invalid_prefix_lengths(count):
    error = TypeError if type(count) is not int else ValueError
    with pytest.raises(error):
        acceptance.run_mysql_partial_recovery_acceptance(
            "mysql+pymysql://u:p@localhost/probiga_v4_test_partial",
            expected_server_uuid=TEST_SERVER_UUID,
            partial_statement_count=count,
        )


@pytest.mark.parametrize("index", (True, -1, 7, 1.5))
def test_partial_recovery_rejects_invalid_migration_indexes(index):
    error = TypeError if type(index) is not int else ValueError
    with pytest.raises(error):
        acceptance.run_mysql_partial_recovery_acceptance(
            "mysql+pymysql://u:p@localhost/probiga_v4_test_partial",
            expected_server_uuid=TEST_SERVER_UUID,
            partial_migration_index=index,
            partial_statement_count=1,
        )


def test_empty_database_must_report_an_applied_initial_migration(monkeypatch):
    engines: list[_Engine] = []

    def fake_engine(_url: str, **_kwargs):
        engine = _Engine()
        engines.append(engine)
        return engine

    def fake_tables(_engine):
        return acceptance.V4_CONTROL_PLANE_TABLES

    monkeypatch.setattr(acceptance, "create_tool_engine", fake_engine)
    monkeypatch.setattr(
        acceptance,
        "run_v4_migrations",
        lambda _engine: [_Result(status="exists")],
    )
    monkeypatch.setattr(acceptance, "_table_names", fake_tables)

    with pytest.raises(RuntimeError, match="initial migration"):
        acceptance.run_mysql_acceptance(
            "mysql+pymysql://u:p@localhost/probiga_v4_test",
            expected_server_uuid=TEST_SERVER_UUID,
        )

    assert all(engine.disposed for engine in engines)


@pytest.mark.parametrize(
    "existing_tables",
    (
        frozenset({"st_order_v2"}),
        frozenset({"schema_migration_v4"}),
        acceptance.V4_CONTROL_PLANE_TABLES,
    ),
)
def test_acceptance_always_refuses_nonempty_database_before_migration(
    monkeypatch,
    existing_tables,
):
    engine = _Engine()
    monkeypatch.setattr(
        acceptance,
        "create_tool_engine",
        lambda *_args, **_kwargs: engine,
    )
    monkeypatch.setattr(
        acceptance,
        "_preflight_empty_schema",
        lambda *_args: (_ for _ in ()).throw(
            RuntimeError(
                "V4 acceptance database must start completely empty: "
                + repr(existing_tables)
            )
        ),
    )
    called = False

    def fake_migrations(_engine):
        nonlocal called
        called = True
        return []

    monkeypatch.setattr(acceptance, "run_v4_migrations", fake_migrations)

    with pytest.raises(RuntimeError, match="must start completely empty"):
        acceptance.run_mysql_acceptance(
            "mysql+pymysql://u:p@localhost/probiga_v4_test",
            expected_server_uuid=TEST_SERVER_UUID,
        )

    assert called is False
    assert engine.disposed is True


def test_parser_has_no_cleanliness_bypass_flag():
    with pytest.raises(SystemExit):
        acceptance._parser().parse_args(["--require-clean"])


@pytest.mark.parametrize("concurrency", (True, 0, 1, 9, 2.5))
def test_acceptance_rejects_invalid_concurrency(concurrency):
    with pytest.raises(ValueError):
        acceptance.run_mysql_acceptance(
            "mysql+pymysql://u:p@localhost/probiga_v4_test",
            expected_server_uuid=TEST_SERVER_UUID,
            concurrency=concurrency,
        )


@pytest.mark.parametrize(
    "env_name",
    (
        "V4_TEST_MYSQL_SERVER_UUID",
        "V4_CI_MYSQL_SERVER_UUID",
        "V4_TEST_ACCEPTANCE_MYSQL_SERVER_UUID",
    ),
)
def test_server_uuid_is_resolved_only_from_independent_test_ci_env(env_name):
    assert acceptance.resolve_server_uuid(
        env_name,
        environ={env_name: TEST_SERVER_UUID.upper()},
    ) == TEST_SERVER_UUID


@pytest.mark.parametrize(
    ("env_name", "value"),
    (
        ("V4_MYSQL_SERVER_UUID", TEST_SERVER_UUID),
        ("MYSQL_SERVER_UUID", TEST_SERVER_UUID),
        ("V4_TEST_MYSQL_SERVER_UUID", ""),
        ("V4_TEST_MYSQL_SERVER_UUID", "00000000-0000-0000-0000-000000000000"),
    ),
)
def test_server_uuid_resolver_fails_closed(env_name, value):
    with pytest.raises(ValueError):
        acceptance.resolve_server_uuid(env_name, environ={env_name: value})


@pytest.mark.parametrize(
    "runner",
    (
        acceptance.run_mysql_acceptance,
        acceptance.run_mysql_concurrent_initial_acceptance,
        acceptance.run_mysql_partial_recovery_acceptance,
        acceptance.run_mysql_head_cas_acceptance,
        acceptance.run_mysql_transaction_recovery_acceptance,
        acceptance.run_mysql_job_lease_behavior_acceptance,
    ),
)
def test_every_acceptance_runner_requires_independent_server_uuid(runner):
    with pytest.raises(TypeError, match="expected_server_uuid"):
        runner("mysql+pymysql://u:p@localhost/probiga_v4_test")


class _QueryResult:
    def __init__(self, *, scalar=None, rows=()):
        self._scalar = scalar
        self._rows = rows

    def scalar(self):
        return self._scalar

    def scalars(self):
        return iter(self._rows)


class _IdentityConnection:
    class _Dialect:
        name = "mysql"

    dialect = _Dialect()

    def __init__(
        self,
        *,
        database=TEST_DATABASE,
        version=ORACLE_VERSION,
        server_uuid=TEST_SERVER_UUID,
        version_comment=ORACLE_COMMENT,
        grants=TARGET_GRANTS,
    ):
        self.database = database
        self.version = version
        self.server_uuid = server_uuid
        self.version_comment = version_comment
        self.grants = grants

    def execute(self, statement, _parameters=None):
        sql = str(statement).upper()
        if "SHOW GRANTS" in sql:
            return _QueryResult(rows=self.grants)
        if "@@SERVER_UUID" in sql:
            return _QueryResult(scalar=self.server_uuid)
        if "@@VERSION_COMMENT" in sql:
            return _QueryResult(scalar=self.version_comment)
        if "VERSION()" in sql:
            return _QueryResult(scalar=self.version)
        if "DATABASE()" in sql:
            return _QueryResult(scalar=self.database)
        raise AssertionError(sql)


class _PreflightConnection(_IdentityConnection):
    def __init__(self, *, objects=None, **kwargs):
        super().__init__(**kwargs)
        self.objects = objects or {}
        self.query_count = 0

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _traceback):
        return False

    def execute(self, statement, parameters=None):
        self.query_count += 1
        sql = str(statement).upper()
        for kind, catalog in (
            ("tables", "INFORMATION_SCHEMA.TABLES"),
            ("routines", "INFORMATION_SCHEMA.ROUTINES"),
            ("events", "INFORMATION_SCHEMA.EVENTS"),
        ):
            if catalog in sql:
                return _QueryResult(rows=self.objects.get(kind, ()))
        return super().execute(statement, parameters)


class _PreflightEngine:
    class _Dialect:
        name = "mysql"

    dialect = _Dialect()

    def __init__(self, connection):
        self.connection = connection

    def connect(self):
        return self.connection


@pytest.mark.parametrize("version", ("5.7.38", "8.4.11"))
def test_runtime_identity_accepts_bound_validated_oracle_mysql(version):
    assert acceptance._server_identity_from_connection(
        _IdentityConnection(version=version),
        TEST_DATABASE,
        TEST_SERVER_UUID,
    ) == (TEST_DATABASE, version, TEST_SERVER_UUID, ORACLE_COMMENT)


def test_preflight_binds_identity_and_all_empty_inventories_on_one_connection():
    connection = _PreflightConnection()
    identity = REAL_PREFLIGHT(
        _PreflightEngine(connection),
        TEST_DATABASE,
        TEST_SERVER_UUID,
    )
    assert identity == (
        TEST_DATABASE,
        ORACLE_VERSION,
        TEST_SERVER_UUID,
        ORACLE_COMMENT,
    )
    assert connection.query_count == 8


@pytest.mark.parametrize("kind", ("tables", "routines", "events"))
def test_preflight_rejects_every_nonempty_schema_object_kind(kind):
    connection = _PreflightConnection(objects={kind: ("unexpected_object",)})
    with pytest.raises(RuntimeError, match="completely empty"):
        REAL_PREFLIGHT(
            _PreflightEngine(connection),
            TEST_DATABASE,
            TEST_SERVER_UUID,
        )


@pytest.mark.parametrize(
    "overrides",
    (
        {"database": "production"},
        {"version": "5.7.39"},
        {"version": "5.7.38-MariaDB"},
        {"version_comment": "Percona Server (GPL)"},
        {"version_comment": "MariaDB Server"},
        {"server_uuid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"},
    ),
)
def test_runtime_identity_rejects_wrong_database_version_vendor_or_uuid(
    overrides,
):
    with pytest.raises(RuntimeError):
        acceptance._server_identity_from_connection(
            _IdentityConnection(**overrides),
            TEST_DATABASE,
            TEST_SERVER_UUID,
        )


@pytest.mark.parametrize(
    "grants",
    (
        ("GRANT ALL PRIVILEGES ON *.* TO 'acceptor'@'localhost'",),
        (
            "GRANT SELECT, INSERT, UPDATE, CREATE, REFERENCES, DROP "
            "ON `probiga_v4_test`.* "
            "TO 'acceptor'@'localhost'",
        ),
        ("GRANT SELECT ON `production`.* TO 'acceptor'@'localhost'",),
        ("GRANT USAGE ON *.* TO 'acceptor'@'localhost'",),
    ),
)
def test_runtime_identity_rejects_broad_foreign_or_incomplete_grants(grants):
    with pytest.raises(RuntimeError):
        acceptance._server_identity_from_connection(
            _IdentityConnection(grants=grants),
            TEST_DATABASE,
            TEST_SERVER_UUID,
        )


def test_head_cas_acceptance_has_one_winner_and_single_connection_pools(
    monkeypatch,
):
    engines: list[_Engine] = []
    pool_sizes: list[int] = []

    def fake_engine(_url, **kwargs):
        pool_sizes.append(kwargs["pool_size"])
        engine = _Engine()
        engines.append(engine)
        return engine

    class _Repository:
        def __init__(self, _engine):
            pass

        def get_head(self, _channel, *, account_id):
            assert account_id == "v4-acceptance-paper"
            return {"head_version": 2, "run_uid": "v4-cas-run-b"}

    monkeypatch.setattr(acceptance, "create_tool_engine", fake_engine)
    monkeypatch.setattr(acceptance, "TradingV4Repository", _Repository)
    monkeypatch.setattr(
        acceptance,
        "run_v4_migrations",
        lambda _engine: [_Result(status="applied")],
    )
    monkeypatch.setattr(
        acceptance,
        "_seed_head_cas_scenario",
        lambda _repository: (
            "v4-cas-run-a",
            ("v4-cas-run-b", "v4-cas-run-c"),
            datetime(2026, 8, 3, tzinfo=timezone.utc),
        ),
    )
    monkeypatch.setattr(
        acceptance,
        "_publish_head_cas_worker",
        lambda _engine, **kwargs: (
            kwargs["run_uid"],
            "published" if kwargs["run_uid"].endswith("b") else "conflict",
        ),
    )

    report = acceptance.run_mysql_head_cas_acceptance(
        "mysql+pymysql://u:p@localhost/probiga_v4_test",
        expected_server_uuid=TEST_SERVER_UUID,
    )

    assert report.successful_run_uid == "v4-cas-run-b"
    assert report.conflicting_run_uid == "v4-cas-run-c"
    assert report.final_head_version == 2
    assert report.production_activation_allowed is False
    assert report.actionable_output_allowed is False
    assert pool_sizes == [1, 1, 1]
    assert all(engine.disposed for engine in engines)


def test_transaction_recovery_entry_is_fail_closed_and_pool_one(monkeypatch):
    engine = _Engine()
    engine_kwargs = {}

    def fake_engine(_url, **kwargs):
        engine_kwargs.update(kwargs)
        return engine

    monkeypatch.setattr(acceptance, "create_tool_engine", fake_engine)
    monkeypatch.setattr(
        acceptance,
        "run_v4_migrations",
        lambda _engine: [_Result(status="applied")],
    )
    monkeypatch.setattr(
        acceptance,
        "_run_transaction_recovery_probes",
        lambda _engine, **_kwargs: (True, True, True),
    )

    report = acceptance.run_mysql_transaction_recovery_acceptance(
        "mysql+pymysql://u:p@localhost/probiga_v4_test",
        expected_server_uuid=TEST_SERVER_UUID,
    )

    assert report.explicit_rollback_absent is True
    assert report.disconnect_rollback_absent is True
    assert report.recovery_write_visible is True
    assert report.production_activation_allowed is False
    assert report.actionable_output_allowed is False
    assert engine_kwargs["pool_size"] == 1
    assert engine_kwargs["max_overflow"] == 0
    assert engine.disposed is True


def test_transaction_recovery_entry_rejects_any_failed_probe(monkeypatch):
    engine = _Engine()
    monkeypatch.setattr(
        acceptance,
        "create_tool_engine",
        lambda *_args, **_kwargs: engine,
    )
    monkeypatch.setattr(
        acceptance,
        "run_v4_migrations",
        lambda _engine: [_Result(status="applied")],
    )
    monkeypatch.setattr(
        acceptance,
        "_run_transaction_recovery_probes",
        lambda _engine, **_kwargs: (True, False, True),
    )
    with pytest.raises(RuntimeError, match="invariant failed"):
        acceptance.run_mysql_transaction_recovery_acceptance(
            "mysql+pymysql://u:p@localhost/probiga_v4_test",
            expected_server_uuid=TEST_SERVER_UUID,
        )
    assert engine.disposed is True


def _valid_job_lease_matrix():
    return acceptance._JobLeaseBehaviorMatrix(
        direct_sql_whitespace_rejections=(
            "insert-leading",
            "update-trailing",
        ),
        same_token_outcomes=("claimed", "replayed"),
        two_worker_claimed_count=1,
        two_worker_empty_count=1,
        lock_timeout_error_codes=(1205,),
        lock_timeout_retry_succeeded=True,
        deadlock_error_codes=(1213,),
        deadlock_attempts=(1, 2),
        max_lease_duration_seconds=900,
        over_limit_rejected=True,
        expired_job_status="FAILED",
        expired_job_error_code=acceptance.EXHAUSTED_LEASE_ERROR_CODE,
        terminal_retry_conflict=True,
        historical_token_reuse_conflict=True,
        transaction_rollback_absent=True,
    )


def test_job_lease_behavior_entry_is_isolated_fail_closed_and_observable(
    monkeypatch,
):
    engine = _Engine()
    engine_kwargs = {}
    observed_call = {}

    def fake_engine(_url, **kwargs):
        engine_kwargs.update(kwargs)
        return engine

    def fake_probes(_engine, **kwargs):
        observed_call.update(kwargs)
        return _valid_job_lease_matrix()

    monkeypatch.setattr(acceptance, "create_tool_engine", fake_engine)
    monkeypatch.setattr(
        acceptance,
        "run_v4_migrations",
        lambda _engine: [_Result(status="applied")],
    )
    monkeypatch.setattr(
        acceptance,
        "_table_names",
        lambda _engine: acceptance.V4_CONTROL_PLANE_TABLES,
    )
    monkeypatch.setattr(
        acceptance,
        "_run_job_lease_behavior_probes",
        fake_probes,
    )

    report = acceptance.run_mysql_job_lease_behavior_acceptance(
        "mysql+pymysql://u:p@localhost/probiga_v4_test_job_lease",
        expected_server_uuid=TEST_SERVER_UUID,
    )

    assert report.mode == "job-lease-behavior"
    assert report.direct_sql_whitespace_rejections == (
        "insert-leading",
        "update-trailing",
    )
    assert report.same_token_outcomes == ("claimed", "replayed")
    assert report.lock_timeout_error_codes == (1205,)
    assert report.deadlock_error_codes == (1213,)
    assert report.max_lease_duration_seconds == 900
    assert report.expired_job_error_code == (
        acceptance.EXHAUSTED_LEASE_ERROR_CODE
    )
    assert report.historical_token_reuse_conflict is True
    assert uuid.UUID(report.acceptance_run_id).version == 4
    assert observed_call["safe_url"].endswith(
        "/probiga_v4_test_job_lease"
    )
    assert observed_call["expected_database"] == (
        "probiga_v4_test_job_lease"
    )
    assert engine_kwargs["pool_size"] == 8
    assert engine_kwargs["max_overflow"] == 0
    assert report.production_activation_allowed is False
    assert report.actionable_output_allowed is False
    assert engine.disposed is True


@pytest.mark.parametrize(
    "matrix",
    (
        replace(
            _valid_job_lease_matrix(),
            direct_sql_whitespace_rejections=("insert-leading",),
        ),
        replace(
            _valid_job_lease_matrix(),
            same_token_outcomes=("claimed", "claimed"),
        ),
        replace(
            _valid_job_lease_matrix(),
            lock_timeout_error_codes=(1205, 1205, 1205, 1205),
        ),
        replace(
            _valid_job_lease_matrix(),
            deadlock_attempts=(1, 5),
        ),
        replace(
            _valid_job_lease_matrix(),
            max_lease_duration_seconds=901,
        ),
        replace(
            _valid_job_lease_matrix(),
            expired_job_status="RUNNING",
        ),
        replace(
            _valid_job_lease_matrix(),
            terminal_retry_conflict=False,
        ),
        replace(
            _valid_job_lease_matrix(),
            historical_token_reuse_conflict=False,
        ),
        replace(
            _valid_job_lease_matrix(),
            transaction_rollback_absent=False,
        ),
    ),
)
def test_job_lease_behavior_matrix_rejects_every_missing_proof(matrix):
    with pytest.raises(RuntimeError, match="failed closed"):
        acceptance._assert_job_lease_behavior_matrix(matrix)


class _SyntheticOperationalOrigin(Exception):
    def __init__(self, code):
        super().__init__(code, "synthetic operational failure")
        self.errno = code


def _synthetic_operational_error(code):
    return OperationalError(
        "synthetic statement",
        {},
        _SyntheticOperationalOrigin(code),
    )


def test_bounded_mysql_transaction_retry_observes_1205_and_1213():
    outcomes = iter((1205, 1213, "committed"))

    def operation():
        outcome = next(outcomes)
        if isinstance(outcome, int):
            raise _synthetic_operational_error(outcome)
        return outcome

    value, attempts, codes = acceptance._run_bounded_mysql_transaction_retry(
        operation
    )
    assert value == "committed"
    assert attempts == 3
    assert codes == (1205, 1213)


def test_bounded_mysql_transaction_retry_exhausts_and_propagates_unrelated():
    attempts = 0

    def deadlocked():
        nonlocal attempts
        attempts += 1
        raise _synthetic_operational_error(1213)

    with pytest.raises(RuntimeError, match="retry exhausted"):
        acceptance._run_bounded_mysql_transaction_retry(deadlocked)
    assert attempts == acceptance.JOB_LEASE_ACCEPTANCE_MAX_TRANSIENT_ATTEMPTS

    with pytest.raises(OperationalError) as raised:
        acceptance._run_bounded_mysql_transaction_retry(
            lambda: (_ for _ in ()).throw(
                _synthetic_operational_error(9999)
            )
        )
    assert raised.value.orig.errno == 9999


def test_parser_exposes_explicit_job_lease_behavior_mode():
    parsed = acceptance._parser().parse_args(
        ["--mode", "job-lease-behavior"]
    )
    assert parsed.mode == "job-lease-behavior"
