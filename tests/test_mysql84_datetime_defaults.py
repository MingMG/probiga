from __future__ import annotations

from dataclasses import replace
from pathlib import Path
import re

import pytest

from server.db.mysql84_datetime_defaults import (
    LEGACY_ZERO_DATETIME_DEFAULT,
    DateTimeColumnMetadata,
    DateTimeDefaultSpec,
    declared_mysql84_datetime_defaults,
    materialize_mysql84_datetime_defaults,
)
from tools import materialize_mysql84_datetime_defaults as cli
from tools import mysql_acceptance_tls as tls


TARGET_UUID = "810354d6-9061-11f1-84ae-74d4dd7f8500"
TARGET_PORT = 33084
TARGET_SCHEMA = "probiga"
TARGET_IDENTITY = {
    "expected_server_uuid": TARGET_UUID,
    "expected_server_port": TARGET_PORT,
}
VALID_URL = "mysql+pymysql://migration:secret@127.0.0.1:33084/probiga"


class _Dialect:
    name = "mysql"


class _Result:
    def __init__(self, rows=()):
        self._rows = tuple(rows)

    def mappings(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class _FakeConnection:
    dialect = _Dialect()

    def __init__(
        self,
        *,
        version: str = "8.4.11",
        defaults_current: bool = False,
    ) -> None:
        self.version = version
        default = (
            "CURRENT_TIMESTAMP"
            if defaults_current
            else LEGACY_ZERO_DATETIME_DEFAULT
        )
        self.columns = {
            (spec.table_name.casefold(), spec.column_name.casefold()): (
                DateTimeColumnMetadata(
                    table_name=spec.table_name,
                    column_name=spec.column_name,
                    data_type="datetime",
                    column_type="datetime",
                    nullable=spec.nullable,
                    column_default=default,
                )
            )
            for spec in declared_mysql84_datetime_defaults()
        }
        self.violations: dict[str, tuple[int, int]] = {}
        self.statements: list[str] = []
        self.commit_count = 0
        self.rollback_count = 0
        self.transaction_open = False

    def in_transaction(self):
        return self.transaction_open

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        if sql.startswith("SELECT @@version AS server_version"):
            return _Result(
                (
                    {
                        "server_version": self.version,
                        "version_comment": "MySQL Community Server - GPL",
                        "server_uuid": TARGET_UUID,
                        "server_port": TARGET_PORT,
                        "current_schema": TARGET_SCHEMA,
                    },
                )
            )
        if "FROM information_schema.COLUMNS" in sql:
            return _Result(
                tuple(
                    {
                        "TABLE_NAME": item.table_name,
                        "COLUMN_NAME": item.column_name,
                        "DATA_TYPE": item.data_type,
                        "COLUMN_TYPE": item.column_type,
                        "IS_NULLABLE": "YES" if item.nullable else "NO",
                        "COLUMN_DEFAULT": item.column_default,
                    }
                    for item in self.columns.values()
                )
            )
        if sql.startswith("SELECT COUNT(*) AS `row_count`"):
            table_match = re.search(r" FROM `([^`]+)`$", sql)
            assert table_match is not None
            table_name = table_match.group(1)
            table_specs = [
                spec
                for spec in declared_mysql84_datetime_defaults()
                if spec.table_name == table_name
            ]
            row = {"row_count": 10}
            for index, spec in enumerate(table_specs):
                all_zero, partial_zero = self.violations.get(spec.key, (0, 0))
                row[f"all_zero_{index:03d}"] = all_zero
                row[f"partial_zero_{index:03d}"] = partial_zero
            return _Result((row,))
        if sql.startswith("ALTER TABLE"):
            match = re.match(r"ALTER TABLE `([^`]+)` (.+)$", sql)
            assert match is not None
            table_name, clauses = match.groups()
            columns = re.findall(
                r"ALTER COLUMN `([^`]+)` SET DEFAULT \(CURRENT_TIMESTAMP\)",
                clauses,
            )
            assert columns
            for column_name in columns:
                key = (table_name.casefold(), column_name.casefold())
                self.columns[key] = replace(
                    self.columns[key],
                    # Oracle MySQL 8.4 canonicalizes the requested
                    # CURRENT_TIMESTAMP expression to now().
                    column_default="now()",
                )
            return _Result()
        raise AssertionError(f"unexpected SQL: {sql}")


def test_frozen_manifest_is_exactly_the_reviewed_eight_columns() -> None:
    specs = declared_mysql84_datetime_defaults()

    assert [(item.key, item.nullable) for item in specs] == [
        ("jq_strategy_meta.created_at", True),
        ("jq_strategy_meta.updated_at", True),
        ("jq_strategy_picks.created_at", True),
        ("st_daily_review.etl_sync_at", False),
        ("st_portfolio_analysis_log.created_at", False),
        ("st_portfolio_trans_log.created_at", False),
        ("st_recommended_stocks.created_at", True),
        ("st_user_portfolio.etl_sync_at", False),
    ]
    assert len({item.key for item in specs}) == 8


def test_apply_requires_explicit_offline_confirmation_before_first_query() -> None:
    connection = _FakeConnection()

    with pytest.raises(RuntimeError, match="offline confirmation"):
        materialize_mysql84_datetime_defaults(
            connection,
            expected_schema=TARGET_SCHEMA,
            **TARGET_IDENTITY,
            apply=True,
        )

    assert connection.statements == []


def test_exact_8411_policy_rejects_other_mysql_before_metadata_or_ddl() -> None:
    connection = _FakeConnection(version="8.4.10")

    with pytest.raises(RuntimeError, match="fail-closed"):
        materialize_mysql84_datetime_defaults(
            connection,
            expected_schema=TARGET_SCHEMA,
            **TARGET_IDENTITY,
        )

    assert not any("information_schema" in sql for sql in connection.statements)
    assert not any(sql.startswith("ALTER TABLE") for sql in connection.statements)


def test_server_uuid_mismatch_stops_before_metadata_audit_or_ddl() -> None:
    connection = _FakeConnection()

    with pytest.raises(RuntimeError, match="UUID mismatch"):
        materialize_mysql84_datetime_defaults(
            connection,
            expected_schema=TARGET_SCHEMA,
            expected_server_uuid="11111111-1111-1111-1111-111111111111",
            expected_server_port=TARGET_PORT,
            apply=True,
            restored_target_offline=True,
        )

    assert not any("information_schema" in sql for sql in connection.statements)
    assert not any(sql.startswith("ALTER TABLE") for sql in connection.statements)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("missing", "missing target DATETIME column"),
        ("type", "column definition drift"),
        ("nullable", "nullability drift"),
        ("default", "default drift"),
    ),
)
def test_metadata_drift_blocks_before_row_audit_and_ddl(
    mutation: str,
    message: str,
) -> None:
    connection = _FakeConnection()
    key = ("jq_strategy_meta", "created_at")
    current = connection.columns[key]
    if mutation == "missing":
        del connection.columns[key]
    elif mutation == "type":
        connection.columns[key] = replace(current, column_type="datetime(6)")
    elif mutation == "nullable":
        connection.columns[key] = replace(current, nullable=False)
    else:
        connection.columns[key] = replace(
            current,
            column_default="1970-01-01 00:00:00",
        )

    with pytest.raises(RuntimeError, match=message):
        materialize_mysql84_datetime_defaults(
            connection,
            expected_schema=TARGET_SCHEMA,
            **TARGET_IDENTITY,
            apply=True,
            restored_target_offline=True,
        )

    assert not any(sql.startswith("SELECT COUNT(*)") for sql in connection.statements)
    assert not any(sql.startswith("ALTER TABLE") for sql in connection.statements)


@pytest.mark.parametrize(
    ("counts", "expected_all", "expected_partial"),
    (((1, 0), 1, 0), ((0, 2), 0, 2)),
)
def test_any_zero_or_partially_zero_value_blocks_whole_batch_without_backfill(
    counts: tuple[int, int],
    expected_all: int,
    expected_partial: int,
) -> None:
    connection = _FakeConnection()
    connection.violations["jq_strategy_meta.created_at"] = counts

    report = materialize_mysql84_datetime_defaults(
        connection,
        expected_schema=TARGET_SCHEMA,
        **TARGET_IDENTITY,
        apply=True,
        restored_target_offline=True,
    )

    violation = report.violation_counts[0]
    assert violation.all_zero_count == expected_all
    assert violation.partial_zero_count == expected_partial
    assert report.ready_to_apply is False
    assert report.complete is False
    assert report.changed_columns == ()
    assert not any(sql.startswith("ALTER TABLE") for sql in connection.statements)
    assert not any(sql.startswith("UPDATE ") for sql in connection.statements)


def test_audit_uses_cast_as_char_for_calendar_components_not_typed_comparison() -> None:
    connection = _FakeConnection()

    report = materialize_mysql84_datetime_defaults(
        connection,
        expected_schema=TARGET_SCHEMA,
        **TARGET_IDENTITY,
    )

    audit_sql = "\n".join(
        sql for sql in connection.statements if sql.startswith("SELECT COUNT(*)")
    )
    assert "CAST(`created_at` AS CHAR)" in audit_sql
    assert "CAST(`etl_sync_at` AS CHAR)" in audit_sql
    assert "`created_at` = '0000-00-00" not in audit_sql
    assert "`etl_sync_at` = '0000-00-00" not in audit_sql
    assert report.ready_to_apply is True
    assert report.complete is False


def test_apply_groups_same_table_columns_preserves_metadata_and_is_idempotent() -> None:
    connection = _FakeConnection()
    before = {
        key: (item.data_type, item.column_type, item.nullable)
        for key, item in connection.columns.items()
    }

    first = materialize_mysql84_datetime_defaults(
        connection,
        expected_schema=TARGET_SCHEMA,
        **TARGET_IDENTITY,
        apply=True,
        restored_target_offline=True,
    )
    first_alters = [
        sql for sql in connection.statements if sql.startswith("ALTER TABLE")
    ]
    second = materialize_mysql84_datetime_defaults(
        connection,
        expected_schema=TARGET_SCHEMA,
        **TARGET_IDENTITY,
        apply=True,
        restored_target_offline=True,
    )
    all_alters = [
        sql for sql in connection.statements if sql.startswith("ALTER TABLE")
    ]

    assert first.complete is True
    assert first.legacy_default_count_before == 8
    assert first.current_timestamp_count_before == 0
    assert len(first.changed_columns) == 8
    assert len(first_alters) == 7
    jq_statement = next(
        sql
        for sql in first_alters
        if sql.startswith("ALTER TABLE `jq_strategy_meta`")
    )
    assert jq_statement.count("ALTER COLUMN") == 2
    assert "SET DEFAULT (CURRENT_TIMESTAMP)" in jq_statement
    assert second.complete is True
    assert second.changed_columns == ()
    assert second.legacy_default_count_before == 0
    assert second.current_timestamp_count_before == 8
    assert all_alters == first_alters
    assert {
        key: (item.data_type, item.column_type, item.nullable)
        for key, item in connection.columns.items()
    } == before


def test_clean_connection_is_required_before_identity_query() -> None:
    connection = _FakeConnection()
    connection.transaction_open = True

    with pytest.raises(RuntimeError, match="clean connection"):
        materialize_mysql84_datetime_defaults(
            connection,
            expected_schema=TARGET_SCHEMA,
            **TARGET_IDENTITY,
        )

    assert connection.statements == []


@pytest.fixture
def ca_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "migration-ca.pem"
    path.write_text("unit-test-ca", encoding="ascii")
    monkeypatch.setattr(
        tls.ssl,
        "create_default_context",
        lambda **_kwargs: object(),
    )
    return path.resolve()


class _ConnectionContext:
    def __init__(self) -> None:
        self.connection = object()

    def __enter__(self) -> object:
        return self.connection

    def __exit__(self, *_args: object) -> None:
        return None


class _Engine:
    def __init__(self) -> None:
        self.context = _ConnectionContext()
        self.disposed = False

    def connect(self) -> _ConnectionContext:
        return self.context

    def dispose(self) -> None:
        self.disposed = True


class _CliReport:
    ready_to_apply = True
    complete = True
    expected_column_count = 8
    schema = TARGET_SCHEMA
    changed_columns = ("jq_strategy_meta.created_at",)
    violation_counts = ()

    def as_dict(self) -> dict[str, object]:
        return {"schema": self.schema}


def test_formal_cli_reuses_verified_tls_engine_and_forwards_safety_gates(
    ca_file: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    engine = _Engine()
    observed: dict[str, object] = {}
    monkeypatch.setattr(cli, "load_project_env", lambda: None)
    monkeypatch.setenv(cli.MIGRATION_URL_ENV, VALID_URL)

    def fake_create_engine(url: str, **kwargs: object) -> _Engine:
        observed["engine"] = (url, kwargs)
        return engine

    def fake_materialize(connection: object, **kwargs: object) -> _CliReport:
        observed["materialize"] = (connection, kwargs)
        return _CliReport()

    monkeypatch.setattr(cli, "create_mysql_acceptance_engine", fake_create_engine)
    monkeypatch.setattr(
        cli,
        "materialize_mysql84_datetime_defaults",
        fake_materialize,
    )
    argv = [
        "--schema",
        TARGET_SCHEMA,
        "--expected-server-uuid",
        TARGET_UUID,
        "--expected-server-port",
        str(TARGET_PORT),
        "--ssl-ca",
        str(ca_file),
        "--apply",
        "--confirm-restored-target-offline",
        "--json",
    ]

    assert cli.main(argv) == 0

    url, engine_kwargs = observed["engine"]
    assert url == VALID_URL
    assert engine_kwargs == {
        "tls_config": tls.MySQLAcceptanceTLSConfig(ssl_ca=str(ca_file)),
        "pool_pre_ping": True,
        "pool_recycle": 900,
        "future": True,
    }
    connection, materialize_kwargs = observed["materialize"]
    assert connection is engine.context.connection
    assert materialize_kwargs == {
        "expected_schema": TARGET_SCHEMA,
        "expected_server_uuid": TARGET_UUID,
        "expected_server_port": TARGET_PORT,
        "apply": True,
        "restored_target_offline": True,
    }
    assert engine.disposed is True
    assert '"status": "ok"' in capsys.readouterr().out


@pytest.mark.parametrize(
    "url",
    (
        VALID_URL + "?ssl_disabled=true",
        "mysql://migration:secret@127.0.0.1:33084/probiga",
        "mysql+pymysql://migration:secret@127.0.0.1:3306/probiga",
    ),
)
def test_formal_cli_rejects_unsafe_url_before_engine_creation(
    url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "load_project_env", lambda: None)
    monkeypatch.setenv(cli.MIGRATION_URL_ENV, url)
    monkeypatch.setattr(
        cli,
        "create_mysql_acceptance_engine",
        lambda *_args, **_kwargs: pytest.fail("engine must not be created"),
    )

    with pytest.raises(SystemExit):
        cli.main(
            [
                "--schema",
                TARGET_SCHEMA,
                "--expected-server-uuid",
                TARGET_UUID,
                "--expected-server-port",
                str(TARGET_PORT),
                "--ssl-ca",
                str(Path(__file__).resolve()),
            ]
        )
