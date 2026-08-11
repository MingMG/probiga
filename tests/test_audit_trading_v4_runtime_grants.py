from __future__ import annotations

import pytest
from sqlalchemy.engine import make_url

from server.integrations.v4_database_roles import (
    ROLE_MANIFEST_HASH,
    V4RuntimeDatabaseRole,
    V4RuntimeRoleAudit,
    role_column_grants,
    role_grants,
)
from tools import audit_trading_v4_runtime_grants as audit_cli


SERVER_UUID = "84190384-8ff1-11f1-ab13-74d4dd7f8500"
DATABASE = "pb_chain_v3_test_serial1"


def _environ(*, database: str = DATABASE):
    values = {}
    for index, role in enumerate(V4RuntimeDatabaseRole, start=1):
        url_env, uuid_env = audit_cli.role_environment_variables("TEST", role)
        values[url_env] = (
            f"mysql+pymysql://role{index}:secret{index}@127.0.0.1:33578/"
            f"{database}"
        )
        values[uuid_env] = SERVER_UUID
    return values


def test_role_environment_variables_are_fixed_and_have_no_generic_fallback():
    assert audit_cli.role_environment_variables(
        "TEST", V4RuntimeDatabaseRole.PREDICTOR
    ) == (
        "V4_TEST_PREDICTOR_MYSQL_URL",
        "V4_TEST_PREDICTOR_MYSQL_SERVER_UUID",
    )
    with pytest.raises(audit_cli.V4RuntimeGrantAuditCliError, match="required"):
        audit_cli.resolve_targets(
            environment="TEST",
            role=V4RuntimeDatabaseRole.PREDICTOR,
            environ={
                "MYSQL_URL": "mysql+pymysql://u:p@127.0.0.1/ignored_test",
                "DATABASE_URL": "mysql+pymysql://u:p@127.0.0.1/ignored_test",
            },
        )


def test_resolve_all_targets_requires_same_test_database_uuid_and_five_users():
    targets = audit_cli.resolve_targets(environment="TEST", environ=_environ())
    assert len(targets) == 5
    assert {target.database for target in targets} == {DATABASE}
    assert {target.expected_server_uuid for target in targets} == {SERVER_UUID}
    assert len({target.expected_username for target in targets}) == 5

    wrong_database = _environ()
    url_env, _ = audit_cli.role_environment_variables(
        "TEST", V4RuntimeDatabaseRole.API_READER
    )
    wrong_database[url_env] = wrong_database[url_env].replace(
        DATABASE, "another_v4_test_roles"
    )
    with pytest.raises(audit_cli.V4RuntimeGrantAuditCliError, match="same database"):
        audit_cli.resolve_targets(environment="TEST", environ=wrong_database)

    duplicate_user = _environ()
    predictor_url_env, _ = audit_cli.role_environment_variables(
        "TEST", V4RuntimeDatabaseRole.PREDICTOR
    )
    reader_url_env, _ = audit_cli.role_environment_variables(
        "TEST", V4RuntimeDatabaseRole.API_READER
    )
    duplicate_user[reader_url_env] = duplicate_user[predictor_url_env]
    with pytest.raises(audit_cli.V4RuntimeGrantAuditCliError, match="distinct"):
        audit_cli.resolve_targets(environment="TEST", environ=duplicate_user)


@pytest.mark.parametrize(
    "url",
    (
        "mysql+pymysql://root:secret@127.0.0.1:33578/pb_v4_test_roles",
        "mysql+pymysql://role:secret@127.0.0.1:33578/production_v4_test",
        "mysql+pymysql://role:secret@127.0.0.1:33578/probiga_v4_test?x=1",
        "postgresql://role:secret@127.0.0.1:5432/probiga_v4_test",
        "mysql+pymysql://role@127.0.0.1:33578/probiga_v4_test",
    ),
)
def test_single_target_rejects_admin_production_query_non_mysql_and_no_password(url):
    url_env, uuid_env = audit_cli.role_environment_variables(
        "TEST", V4RuntimeDatabaseRole.PREDICTOR
    )
    with pytest.raises(audit_cli.V4RuntimeGrantAuditCliError):
        audit_cli.resolve_targets(
            environment="TEST",
            role=V4RuntimeDatabaseRole.PREDICTOR,
            environ={url_env: url, uuid_env: SERVER_UUID},
        )


class _IdentityResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def one(self):
        return self.row


class _Connection:
    class _Dialect:
        name = "mysql"

    dialect = _Dialect()

    def __init__(self, username, *, server_version="5.7.38"):
        self.username = username
        self.server_version = server_version

    def execute(self, statement, parameters=None):
        assert str(statement).startswith("SELECT VERSION()")
        return _IdentityResult(
            {
                "server_version": self.server_version,
                "version_comment": "MySQL Community Server (GPL)",
                "database_name": DATABASE,
                "server_uuid": SERVER_UUID,
                "authenticated_user": f"{self.username}@127.0.0.1",
            }
        )


class _ConnectionContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, _exc_type, _exc, _traceback):
        return False


class _Engine:
    def __init__(self, username):
        self.connection = _Connection(username)
        self.disposed = False

    def connect(self):
        return _ConnectionContext(self.connection)

    def dispose(self):
        self.disposed = True


def test_runtime_grant_identity_accepts_mysql_8411_and_stays_non_production():
    target = audit_cli.resolve_targets(
        environment="TEST",
        role=V4RuntimeDatabaseRole.PREDICTOR,
        environ=_environ(),
    )[0]
    identity = audit_cli._server_identity(
        _Connection(target.expected_username, server_version="8.4.11"),
        target,
    )

    assert identity.server_version == "8.4.11"
    assert audit_cli.PRODUCTION_ACTIVATION_ALLOWED is False


def test_all_role_audit_binds_identity_and_keeps_hard_gates_closed(monkeypatch):
    engines = []

    def engine_factory(url, **kwargs):
        assert kwargs["isolation_level"] == "REPEATABLE READ"
        engine = _Engine(str(make_url(url).username))
        engines.append(engine)
        return engine

    def exact_audit(_connection, *, role, expected_database):
        index = tuple(V4RuntimeDatabaseRole).index(role) + 1
        return V4RuntimeRoleAudit(
            role=role,
            database=expected_database,
            current_user=f"role{index}@127.0.0.1",
            table_grants=role_grants(role),
            column_grants=role_column_grants(role),
            grant_options_checked=True,
            column_privileges_checked=True,
            routine_privileges_checked=True,
            proxy_privileges_checked=True,
            physical_tables_checked=True,
        )

    monkeypatch.setattr(audit_cli, "audit_current_user_role", exact_audit)
    report = audit_cli.audit_runtime_roles(
        environment="TEST",
        environ=_environ(),
        engine_factory=engine_factory,
    )
    assert report["status"] == "PASSED"
    assert report["role_count"] == 5
    assert report["manifest_hash"] == ROLE_MANIFEST_HASH
    assert report["read_only"] is True
    assert report["production_activation_allowed"] is False
    assert report["actionable_output_allowed"] is False
    assert [item["table_count"] for item in report["roles"]] == [17, 10, 37, 24, 10]
    assert [item["table_privilege_count"] for item in report["roles"]] == [
        25,
        16,
        58,
        24,
        10,
    ]
    assert [item["column_privilege_count"] for item in report["roles"]] == [
        0,
        20,
        0,
        0,
        0,
    ]
    assert [item["privilege_count"] for item in report["roles"]] == [
        25,
        36,
        58,
        24,
        10,
    ]
    assert all(engine.disposed for engine in engines)


def test_main_failure_is_sanitized_and_never_prints_credentials(monkeypatch, capsys):
    def fail(**_kwargs):
        raise RuntimeError("mysql+pymysql://role:DO_NOT_PRINT@host/db")

    monkeypatch.setattr(audit_cli, "audit_runtime_roles", fail)
    assert audit_cli.main(["--environment", "TEST", "--role", "v4_predictor"]) == 2
    captured = capsys.readouterr()
    assert "DO_NOT_PRINT" not in captured.err
    payload = __import__("json").loads(captured.err)
    assert payload["status"] == "FAILED_CLOSED"
    assert payload["read_only"] is True
    assert payload["production_activation_allowed"] is False
    assert payload["actionable_output_allowed"] is False
