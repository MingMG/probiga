from __future__ import annotations

import json
import stat
from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from server.db import migrations_v4
from tools import prepare_strategy_governance_schema as schema


ADMIN_GRANTS = (
    "GRANT SYSTEM_VARIABLES_ADMIN, SHOW_ROUTINE ON *.* TO "
    "`probiga_trigger_admin`@`127.0.0.1` REQUIRE SSL",
)
MIGRATOR_GRANTS = (
    "GRANT USAGE ON *.* TO `probiga_migrator`@`127.0.0.1` REQUIRE SSL",
    "GRANT ALL PRIVILEGES ON `probiga`.* TO "
    "`probiga_migrator`@`127.0.0.1`",
)
RUNTIME_GRANTS = (
    "GRANT USAGE ON *.* TO `probiga_runtime`@`127.0.0.1` REQUIRE SSL",
    "GRANT SELECT ON `biga`.* TO "
    "`probiga_runtime`@`127.0.0.1`",
    "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE TEMPORARY TABLES "
    "ON `probiga`.* TO "
    "`probiga_runtime`@`127.0.0.1`",
    "GRANT SELECT ON `probiga_qmt_history`.* TO "
    "`probiga_runtime`@`127.0.0.1`",
)
LEGACY_RUNTIME_GRANTS = (
    RUNTIME_GRANTS[0],
    RUNTIME_GRANTS[1],
    "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX, "
    "REFERENCES, CREATE TEMPORARY TABLES ON `probiga`.* TO "
    "`probiga_runtime`@`127.0.0.1`",
    RUNTIME_GRANTS[3],
)


def _target_state(
    *,
    user: str = schema.EXPECTED_RUNTIME_USER,
    database: str | None = schema.DATABASE_NAME,
    trust: int = 0,
    **changes,
) -> schema.TargetState:
    state = schema.TargetState(
        mysql_version="8.4.11",
        version_comment="MySQL Community Server - GPL",
        database_name=database,
        authenticated_user=user,
        active_roles="NONE",
        server_uuid=schema.EXPECTED_SERVER_UUID,
        server_port=schema.EXPECTED_SERVER_PORT,
        server_hostname=schema.EXPECTED_SERVER_HOSTNAME,
        log_bin=1,
        binlog_format="ROW",
        trust_creators=trust,
        session_sql_mode=schema.EXPECTED_SQL_MODE,
        character_set_client=schema.EXPECTED_CHARACTER_SET_CLIENT,
        collation_connection=schema.EXPECTED_COLLATION_CONNECTION,
        database_collation=schema.EXPECTED_DATABASE_COLLATION,
        tls_cipher="TLS_AES_256_GCM_SHA384",
    )
    return replace(state, **changes)


class _FakeParent:
    def __init__(self, *, owner: int = 0, mode: int = 0o755) -> None:
        self.owner = owner
        self.mode = mode

    def stat(self):
        return SimpleNamespace(
            st_uid=self.owner,
            st_mode=stat.S_IFDIR | self.mode,
        )


class _FakeProtectedPath:
    def __init__(
        self,
        *,
        absolute: bool = True,
        symlink: bool = False,
        owner: int = 0,
        mode: int = 0o600,
        regular: bool = True,
        parent_owner: int = 0,
        parent_mode: int = 0o755,
    ) -> None:
        self.absolute = absolute
        self.symlink = symlink
        self.owner = owner
        self.mode = mode
        self.regular = regular
        self.parent = _FakeParent(owner=parent_owner, mode=parent_mode)

    def is_absolute(self):
        return self.absolute

    def is_symlink(self):
        return self.symlink

    def lstat(self):
        return SimpleNamespace(
            st_uid=self.owner,
            st_mode=(stat.S_IFREG if self.regular else stat.S_IFDIR) | self.mode,
        )

    def resolve(self, *, strict):
        assert strict is True
        return self

    def stat(self):
        return self.lstat()


class _InventoryResult:
    def __init__(self, rows):
        self.rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)


class _InventoryConnection:
    def __init__(self, rows):
        self.rows = list(rows)
        self.statements: list[str] = []

    def execute(self, statement, _params=None):
        self.statements.append(str(statement))
        return _InventoryResult(self.rows)


class _FullInventoryConnection(_InventoryConnection):
    def __init__(self, rows, managed_names):
        super().__init__(rows)
        self.managed_names = set(managed_names)
        self.engine = object()

    def execute(self, statement, _params=None):
        sql = str(statement)
        self.statements.append(sql)
        if "WHERE TRIGGER_SCHEMA=DATABASE() AND (" in sql:
            return _InventoryResult([
                row for row in self.rows
                if str(row.get("trigger_name") or "") in self.managed_names
            ])
        return _InventoryResult(self.rows)


def _contract(
    name: str,
    *,
    table: str = "controlled_ledger",
) -> schema.TriggerContract:
    return schema.TriggerContract(
        name=name,
        timing="BEFORE",
        event="UPDATE",
        table=table,
        body="SET NEW.value = OLD.value",
        normalizer="governance",
        owner="test",
    )


def _trigger_row(
    contract: schema.TriggerContract,
    *,
    definer: str = schema.EXPECTED_MIGRATOR_USER,
    sql_mode: str = schema.EXPECTED_SQL_MODE,
    **changes,
):
    row = {
        "trigger_name": contract.name,
        "definer": definer,
        "action_timing": contract.timing,
        "event_manipulation": contract.event,
        "event_object_table": contract.table,
        "action_orientation": "ROW",
        "action_statement": contract.body,
        "sql_mode": sql_mode,
        "character_set_client": schema.EXPECTED_CHARACTER_SET_CLIENT,
        "collation_connection": schema.EXPECTED_COLLATION_CONNECTION,
        "database_collation": schema.EXPECTED_DATABASE_COLLATION,
    }
    row.update(changes)
    return row


def _full_trigger_rows(
    managed: dict[str, schema.TriggerContract],
) -> list[dict[str, object]]:
    v2_contracts, v2_bodies, v2_orders = schema._v2_release_trigger_contract()
    rows = [
        {
            **_trigger_row(contract),
            "trigger_schema": schema.DATABASE_NAME,
            "event_object_schema": schema.DATABASE_NAME,
            "action_order": 1,
        }
        for contract in managed.values()
    ]
    rows.extend({
        "trigger_name": name,
        "definer": schema.EXPECTED_MIGRATOR_USER,
        "trigger_schema": schema.DATABASE_NAME,
        "event_object_schema": schema.DATABASE_NAME,
        "action_timing": "BEFORE",
        "event_manipulation": event,
        "event_object_table": table_name,
        "action_orientation": "ROW",
        "action_statement": v2_bodies[name],
        "action_order": v2_orders[name],
        "sql_mode": schema.EXPECTED_SQL_MODE,
        "character_set_client": schema.EXPECTED_CHARACTER_SET_CLIENT,
        "collation_connection": schema.EXPECTED_COLLATION_CONNECTION,
        "database_collation": schema.EXPECTED_DATABASE_COLLATION,
    } for name, (event, table_name) in v2_contracts.items())
    return rows


class _RuntimeEngine:
    def __init__(self, *, state: schema.TargetState | None = None) -> None:
        self.state = state or _target_state()
        self.dispose_count = 0

    def dispose(self):
        self.dispose_count += 1

    def connect(self):
        return nullcontext(SimpleNamespace(state=self.state))


def _boundary(*, trust: int = 0) -> schema.DatabaseBoundary:
    return schema.DatabaseBoundary(
        runtime_engine=_RuntimeEngine(state=_target_state(trust=0)),
        migrator_engine=None,
        admin_credential=schema.OptionCredential(
            path=Path("/etc/probiga/mysql-trigger-admin.ini"),
            host=schema.EXPECTED_CLIENT_ENDPOINT_HOST,
            port=schema.EXPECTED_CLIENT_ENDPOINT_PORT,
            user="probiga_trigger_admin",
            password="A" * 64,
        ),
        migrator_credential=None,
        ssl_ca=Path("/etc/probiga/mysql84-ca.pem"),
        runtime_state=_target_state(trust=trust),
        admin_state=_target_state(
            user=schema.EXPECTED_ADMIN_USER,
            database=None,
            trust=trust,
        ),
        migrator_state=None,
    )


def test_fixed_production_option_file_paths_are_not_caller_controlled():
    assert schema.ADMIN_OPTION_FILE == Path(
        "/etc/probiga/mysql-trigger-admin.ini"
    )
    assert schema.MIGRATOR_OPTION_FILE == Path("/etc/probiga/mysql-migrator.ini")


@pytest.mark.parametrize(
    ("os_name", "uid"),
    (("nt", 0), ("posix", 1001)),
)
def test_root_execution_gate_rejects_non_production_broker(
    monkeypatch,
    os_name,
    uid,
):
    monkeypatch.setattr(
        schema,
        "os",
        SimpleNamespace(name=os_name, geteuid=lambda: uid),
    )

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="must run as root",
    ):
        schema._require_root_execution()


def test_root_execution_gate_accepts_posix_root(monkeypatch):
    monkeypatch.setattr(
        schema,
        "os",
        SimpleNamespace(name="posix", geteuid=lambda: 0),
    )
    schema._require_root_execution()


def test_protected_option_file_accepts_only_root_private_regular_file(
    monkeypatch,
):
    candidate = _FakeProtectedPath()
    monkeypatch.setattr(schema.os.path, "lexists", lambda path: path is candidate)

    assert schema._protected_option_file(candidate) is candidate


@pytest.mark.parametrize(
    ("candidate", "exists", "message"),
    (
        (_FakeProtectedPath(absolute=False), True, "missing or not absolute"),
        (_FakeProtectedPath(), False, "missing or not absolute"),
        (_FakeProtectedPath(symlink=True), True, "must not be a symlink"),
        (_FakeProtectedPath(owner=1001), True, "ownership or mode is unsafe"),
        (_FakeProtectedPath(mode=0o640), True, "ownership or mode is unsafe"),
        (
            _FakeProtectedPath(regular=False),
            True,
            "ownership or mode is unsafe",
        ),
        (
            _FakeProtectedPath(parent_owner=1001),
            True,
            "ownership or mode is unsafe",
        ),
        (
            _FakeProtectedPath(parent_mode=0o775),
            True,
            "ownership or mode is unsafe",
        ),
    ),
)
def test_protected_option_file_rejects_path_symlink_owner_and_mode(
    monkeypatch,
    candidate,
    exists,
    message,
):
    monkeypatch.setattr(schema.os.path, "lexists", lambda _path: exists)

    with pytest.raises(schema.PrivilegedSchemaPreparationError, match=message):
        schema._protected_option_file(candidate)


def test_option_file_parser_accepts_only_exact_tcp_credential_shape(
    monkeypatch,
    tmp_path,
):
    password = "A0_-" * 16
    path = tmp_path / "admin.ini"
    path.write_text(
        "[client]\n"
        "protocol=tcp\n"
        "host=127.0.0.1\n"
        f"port={schema.EXPECTED_CLIENT_ENDPOINT_PORT}\n"
        "user=probiga_trigger_admin\n"
        f"password={password}\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(schema, "_protected_option_file", lambda value: value)

    credential = schema._read_option_credential(
        path,
        expected_user="probiga_trigger_admin",
    )

    assert credential == schema.OptionCredential(
        path=path,
        host=schema.EXPECTED_CLIENT_ENDPOINT_HOST,
        port=schema.EXPECTED_CLIENT_ENDPOINT_PORT,
        user="probiga_trigger_admin",
        password=password,
    )
    assert password not in repr(credential)


@pytest.mark.parametrize(
    "body",
    (
        "[mysql]\nprotocol=tcp\nhost=127.0.0.1\nport=13306\n"
        "user=probiga_trigger_admin\npassword=" + "A" * 64,
        "[client]\nprotocol=tcp\nhost=127.0.0.1\nport=13306\n"
        "user=probiga_trigger_admin\npassword=" + "A" * 64 + "\nssl-mode=REQUIRED",
        "[client]\nprotocol=socket\nhost=127.0.0.1\nport=13306\n"
        "user=probiga_trigger_admin\npassword=" + "A" * 64,
        "[client]\nprotocol=tcp\nhost=db.internal\nport=13306\n"
        "user=probiga_trigger_admin\npassword=" + "A" * 64,
        "[client]\nprotocol=tcp\nhost=127.0.0.1\nport=3306\n"
        "user=probiga_trigger_admin\npassword=" + "A" * 64,
        "[client]\nprotocol=tcp\nhost=127.0.0.1\nport=13306\n"
        "user=root\npassword=" + "A" * 64,
        "[client]\nprotocol=tcp\nhost=127.0.0.1\nport=13306\n"
        "user=probiga_trigger_admin\npassword=short",
        "[client]\nprotocol=tcp\nhost=127.0.0.1\nport=13306\n"
        "user=probiga_trigger_admin\npassword=" + "A" * 47 + "!",
    ),
)
def test_option_file_parser_rejects_wrong_shape_target_or_password(
    monkeypatch,
    tmp_path,
    body,
):
    path = tmp_path / "credential.ini"
    path.write_text(body + "\n", encoding="utf-8")
    monkeypatch.setattr(schema, "_protected_option_file", lambda value: value)

    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        schema._read_option_credential(
            path,
            expected_user="probiga_trigger_admin",
        )


def test_option_connection_is_remote_tcp_tls_and_disables_local_infile(
    monkeypatch,
):
    observed = {}
    connection = SimpleNamespace()

    def fake_connect(**kwargs):
        observed.update(kwargs)
        return connection

    monkeypatch.setattr(schema.pymysql, "connect", fake_connect)
    credential = schema.OptionCredential(
        path=Path("/etc/probiga/mysql-trigger-admin.ini"),
        host=schema.EXPECTED_CLIENT_ENDPOINT_HOST,
        port=schema.EXPECTED_CLIENT_ENDPOINT_PORT,
        user="probiga_trigger_admin",
        password="A" * 64,
    )

    assert schema._connect_option(
        credential,
        Path("/etc/probiga/mysql84-ca.pem"),
        database=None,
        configure_trigger_session=False,
        autocommit=True,
    ) is connection
    assert observed["host"] == schema.EXPECTED_CLIENT_ENDPOINT_HOST
    assert observed["port"] == schema.EXPECTED_CLIENT_ENDPOINT_PORT
    assert observed["user"] == "probiga_trigger_admin"
    assert observed["database"] is None
    assert str(observed["ssl_ca"]).replace("\\", "/") == (
        "/etc/probiga/mysql84-ca.pem"
    )
    assert observed["ssl_verify_cert"] is True
    assert observed["local_infile"] is False
    assert observed["cursorclass"] is schema.DictCursor
    assert "unix_socket" not in observed
    assert "read_default_file" not in observed


def test_migrator_sqlalchemy_engine_uses_positional_cursor(monkeypatch):
    observed = {}
    connection = SimpleNamespace()

    def fake_connect_option(*_args, **kwargs):
        observed.update(kwargs)
        return connection

    def fake_create_engine(_url, **kwargs):
        assert kwargs["creator"]() is connection
        return SimpleNamespace()

    monkeypatch.setattr(schema, "_connect_option", fake_connect_option)
    monkeypatch.setattr(schema, "create_engine", fake_create_engine)
    credential = schema.OptionCredential(
        path=Path("/etc/probiga/mysql-migrator.ini"),
        host=schema.EXPECTED_CLIENT_ENDPOINT_HOST,
        port=schema.EXPECTED_CLIENT_ENDPOINT_PORT,
        user="probiga_migrator",
        password="A" * 64,
    )

    schema._create_migrator_engine(
        credential,
        Path("/etc/probiga/mysql84-ca.pem"),
    )

    assert observed["cursorclass"] is schema.Cursor
    assert observed["database"] == schema.DATABASE_NAME
    assert observed["configure_trigger_session"] is True
    assert observed["autocommit"] is False


def test_all_three_database_identity_grant_boundaries_accept_exact_grants():
    schema._validate_admin_grants(ADMIN_GRANTS)
    schema._validate_migrator_grants(MIGRATOR_GRANTS)
    schema._validate_runtime_grants(RUNTIME_GRANTS)
    schema._validate_runtime_grants(LEGACY_RUNTIME_GRANTS)


def test_runtime_grant_summary_is_exact_and_safe_to_publish():
    detail = schema._runtime_grant_summary(RUNTIME_GRANTS)

    assert detail == {
        "observed_contract": "TARGET_LEAST_PRIVILEGE",
        "persistent_ddl_privileges": [],
        "global_privileges": ["USAGE"],
        "schema_privileges": {
            "BIGA.*": ["SELECT"],
            "PROBIGA.*": [
                "CREATE TEMPORARY TABLES",
                "DELETE",
                "INSERT",
                "SELECT",
                "UPDATE",
            ],
            "PROBIGA_QMT_HISTORY.*": ["SELECT"],
        },
        "funding_append_only_tables": [
            "st_strategy_funding_checkpoint",
            "st_strategy_funding_daily_fact",
        ],
        "funding_append_only_verified": True,
        "funding_row_mutation_denied_by_triggers": ["DELETE", "UPDATE"],
        "funding_structural_bypass_privileges": [],
        "truncate_denied_by_absent_drop_privilege": True,
        "trigger_drop_denied_by_absent_trigger_privilege": True,
        "require_ssl": True,
        "roles": [],
        "grant_option": False,
    }


def test_runtime_grant_summary_reports_legacy_ddl_compatibility_truthfully():
    detail = schema._runtime_grant_summary(LEGACY_RUNTIME_GRANTS)

    assert detail["observed_contract"] == "LEGACY_DDL_COMPATIBILITY"
    assert detail["persistent_ddl_privileges"] == [
        "ALTER",
        "CREATE",
        "DROP",
        "INDEX",
        "REFERENCES",
    ]
    assert detail["funding_structural_bypass_privileges"] == detail[
        "persistent_ddl_privileges"
    ]
    assert detail["truncate_denied_by_absent_drop_privilege"] is False
    assert detail["trigger_drop_denied_by_absent_trigger_privilege"] is True


@pytest.mark.parametrize(
    "probiga_privileges",
    (
        # A partial legacy grant is neither the target nor the frozen legacy
        # compatibility contract.
        "SELECT, INSERT, UPDATE, DELETE, CREATE, CREATE TEMPORARY TABLES",
        # No capability beyond either exact contract is tolerated.
        "SELECT, INSERT, UPDATE, DELETE, CREATE TEMPORARY TABLES, TRIGGER",
        "SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX, "
        "REFERENCES, CREATE TEMPORARY TABLES, LOCK TABLES",
    ),
)
def test_runtime_grants_accept_only_target_or_exact_legacy_contract(
    probiga_privileges,
):
    forged = (
        RUNTIME_GRANTS[0],
        RUNTIME_GRANTS[1],
        f"GRANT {probiga_privileges} ON `probiga`.* TO "
        "`probiga_runtime`@`127.0.0.1`",
        RUNTIME_GRANTS[3],
    )

    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        schema._validate_runtime_grants(forged)


@pytest.mark.parametrize(
    "forged_grant",
    (
        "GRANT ALL PRIVILEGES ON `probiga`.* TO "
        "`probiga_runtime`@`127.0.0.1`",
        "GRANT SELECT, TRIGGER ON `probiga`.* TO "
        "`probiga_runtime`@`127.0.0.1`",
        "GRANT SELECT, EVENT ON `probiga`.* TO "
        "`probiga_runtime`@`127.0.0.1`",
        "GRANT SELECT, CREATE ROUTINE ON `probiga`.* TO "
        "`probiga_runtime`@`127.0.0.1`",
        "GRANT SELECT, ALTER ROUTINE ON `probiga`.* TO "
        "`probiga_runtime`@`127.0.0.1`",
        "GRANT SELECT, EXECUTE ON `probiga`.* TO "
        "`probiga_runtime`@`127.0.0.1`",
        "GRANT SELECT, CREATE VIEW ON `probiga`.* TO "
        "`probiga_runtime`@`127.0.0.1`",
        "GRANT SELECT, SHOW VIEW ON `probiga`.* TO "
        "`probiga_runtime`@`127.0.0.1`",
        "GRANT SELECT, LOCK TABLES ON `probiga`.* TO "
        "`probiga_runtime`@`127.0.0.1`",
    ),
)
def test_runtime_grants_reject_all_trust_window_capabilities(forged_grant):
    forged = tuple(
        grant if "ON `probiga`.*" not in grant else forged_grant
        for grant in RUNTIME_GRANTS
    )

    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        schema._validate_runtime_grants(forged)


def test_runtime_grants_deny_every_funding_structural_bypass_privilege():
    detail = schema._runtime_grant_summary(RUNTIME_GRANTS)

    probiga = set(detail["schema_privileges"]["PROBIGA.*"])
    assert {"DROP", "ALTER", "INDEX", "REFERENCES", "TRIGGER"}.isdisjoint(
        probiga
    )
    assert detail["truncate_denied_by_absent_drop_privilege"] is True
    assert detail["trigger_drop_denied_by_absent_trigger_privilege"] is True
    assert detail["persistent_ddl_privileges"] == []


class _RoutineInventoryResult:
    def __init__(self, rows=()):
        self.rows = [dict(row) for row in rows]

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)


class _RoutineInventoryConnection:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.statements: list[str] = []

    def execute(self, statement, _params=None):
        sql = str(statement).strip()
        self.statements.append(sql)
        return _RoutineInventoryResult(self.rows)


def test_runtime_definer_routine_inventory_is_read_only_and_empty():
    connection = _RoutineInventoryConnection()

    detail = schema._validate_no_runtime_definer_routines(connection)

    assert detail == {
        "runtime_definer_routine_count": 0,
        "runtime_definer_routine_inventory_verified": True,
    }
    assert len(connection.statements) == 1
    assert connection.statements[0].upper().startswith("SELECT ")
    assert "SECURITY_TYPE" in connection.statements[0]


def test_runtime_definer_routine_inventory_fails_closed_on_any_row():
    connection = _RoutineInventoryConnection([{
        "routine_schema": "probiga",
        "routine_name": "unsafe_definer",
        "routine_type": "FUNCTION",
    }])

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="SQL SECURITY DEFINER",
    ):
        schema._validate_no_runtime_definer_routines(connection)


class _RoutineAuditCursor:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement):
        self.statements.append(statement)

    def fetchall(self):
        return list(self.rows)


class _RoutineAuditConnection:
    def __init__(self, rows=()):
        self.audit_cursor = _RoutineAuditCursor(rows)

    def cursor(self):
        return self.audit_cursor


def test_complete_routine_inventory_requires_independent_admin_proof():
    connection = _RoutineAuditConnection()

    detail = schema._validate_complete_routine_inventory_dbapi(connection)

    assert detail["runtime_definer_routine_inventory_complete"] is True
    assert detail["runtime_definer_routine_inventory_authority"] == (
        schema.EXPECTED_ADMIN_USER
    )
    assert detail["runtime_definer_routine_inventory_schemas"] == [
        "biga", "probiga", "probiga_qmt_history",
    ]
    sql = connection.audit_cursor.statements[0]
    assert "CURRENT_USER()" not in sql
    assert "('BIGA', 'PROBIGA', 'PROBIGA_QMT_HISTORY')" in sql


def test_runtime_and_migrator_routine_checks_are_self_only():
    for identity in ("runtime", "migrator"):
        connection = _RoutineInventoryConnection()
        assert schema._validate_no_self_definer_routines(
            connection,
            identity=identity,
        ) == {f"{identity}_self_definer_routine_count": 0}
        assert "DEFINER=BINARY CURRENT_USER()" in connection.statements[0]


def test_admin_inventory_authority_rejects_missing_show_routine():
    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        schema._validate_admin_grants((
            "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO "
            "`probiga_trigger_admin`@`127.0.0.1` REQUIRE SSL",
        ))


@pytest.mark.parametrize(
    "grants",
    (
        ADMIN_GRANTS
        + (
            "GRANT SUPER ON *.* TO "
            "`probiga_trigger_admin`@`127.0.0.1`",
        ),
        ADMIN_GRANTS
        + (
            "GRANT SELECT ON `probiga`.* TO "
            "`probiga_trigger_admin`@`127.0.0.1`",
        ),
        ADMIN_GRANTS
        + (
            "GRANT SELECT ON `other_schema`.* TO "
            "`probiga_trigger_admin`@`127.0.0.1`",
        ),
    ),
)
def test_admin_grants_reject_every_extra_global_or_schema_privilege(grants):
    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        schema._validate_admin_grants(grants)


@pytest.mark.parametrize(
    "grants",
    (
        (
            MIGRATOR_GRANTS[0],
            "GRANT SELECT ON `probiga`.* TO "
            "`probiga_migrator`@`127.0.0.1`",
        ),
        MIGRATOR_GRANTS
        + (
            "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO "
            "`probiga_migrator`@`127.0.0.1`",
        ),
        MIGRATOR_GRANTS
        + (
            "GRANT SELECT ON `other_schema`.* TO "
            "`probiga_migrator`@`127.0.0.1`",
        ),
    ),
)
def test_migrator_grants_require_one_exact_probiga_only_boundary(grants):
    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        schema._validate_migrator_grants(grants)


@pytest.mark.parametrize(
    "grants",
    (
        RUNTIME_GRANTS
        + (
            "GRANT SUPER ON *.* TO `probiga_runtime`@`127.0.0.1`",
        ),
        RUNTIME_GRANTS
        + (
            "GRANT SYSTEM_VARIABLES_ADMIN ON *.* TO "
            "`probiga_runtime`@`127.0.0.1`",
        ),
        RUNTIME_GRANTS
        + (
            "GRANT SELECT ON `other_schema`.* TO "
            "`probiga_runtime`@`127.0.0.1`",
        ),
        (RUNTIME_GRANTS[0],),
        RUNTIME_GRANTS[1:],
    ),
)
def test_runtime_grants_reject_admin_cross_schema_or_missing_schema_rights(
    grants,
):
    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        schema._validate_runtime_grants(grants)


@pytest.mark.parametrize(
    "validator",
    (
        schema._validate_admin_grants,
        schema._validate_migrator_grants,
        schema._validate_runtime_grants,
    ),
)
@pytest.mark.parametrize(
    "grants",
    (
        (
            "GRANT `schema_admin`@`%` TO "
            "`database_user`@`127.0.0.1`",
        ),
        (
            "GRANT USAGE ON *.* TO `database_user`@`127.0.0.1` REQUIRE SSL",
            "GRANT SELECT ON `probiga`.* TO "
            "`database_user`@`127.0.0.1` WITH GRANT OPTION",
        ),
    ),
)
def test_every_database_identity_rejects_roles_and_grant_option(
    validator,
    grants,
):
    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        validator(grants)


def test_show_create_user_tls_is_attached_to_usage_grant():
    grants_without_tls = tuple(item.replace(" REQUIRE SSL", "") for item in RUNTIME_GRANTS)

    resolved = schema._with_account_tls_clause(
        grants_without_tls,
        "CREATE USER `probiga_runtime`@`127.0.0.1` "
        "IDENTIFIED WITH 'caching_sha2_password' AS '<redacted>' REQUIRE SSL",
    )

    assert resolved == RUNTIME_GRANTS
    schema._validate_runtime_grants(resolved)


class _GrantMetadataCursor:
    def __init__(self, grants, create_user):
        self.grants = grants
        self.create_user = create_user

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement):
        self.statement = statement

    def fetchall(self):
        assert self.statement == "SHOW GRANTS FOR CURRENT_USER()"
        return [{"grant": item} for item in self.grants]

    def fetchone(self):
        assert self.statement == "SHOW CREATE USER CURRENT_USER()"
        return {"CREATE USER": self.create_user}


class _GrantMetadataDbapiConnection:
    def __init__(self, grants, create_user):
        self.grants = grants
        self.create_user = create_user

    def cursor(self):
        return _GrantMetadataCursor(self.grants, self.create_user)


class _GrantMetadataResult:
    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)

    def one(self):
        assert len(self.rows) == 1
        return self.rows[0]


class _GrantMetadataSaConnection:
    def __init__(self, grants, create_user):
        self.grants = grants
        self.create_user = create_user

    def execute(self, statement):
        sql = str(statement)
        if sql == "SHOW GRANTS FOR CURRENT_USER()":
            return _GrantMetadataResult([(item,) for item in self.grants])
        assert sql == "SHOW CREATE USER CURRENT_USER()"
        return _GrantMetadataResult([(self.create_user,)])


def test_grant_readers_accept_mysql_single_column_show_create_user():
    grants_without_tls = tuple(item.replace(" REQUIRE SSL", "") for item in RUNTIME_GRANTS)
    create_user = (
        "CREATE USER `probiga_runtime`@`127.0.0.1` "
        "IDENTIFIED WITH 'caching_sha2_password' AS '<redacted>' REQUIRE SSL"
    )

    assert schema._dbapi_grants(
        _GrantMetadataDbapiConnection(grants_without_tls, create_user)
    ) == RUNTIME_GRANTS
    assert schema._sa_grants(
        _GrantMetadataSaConnection(grants_without_tls, create_user)
    ) == RUNTIME_GRANTS


def test_show_create_user_without_tls_does_not_forge_tls_requirement():
    grants_without_tls = tuple(item.replace(" REQUIRE SSL", "") for item in RUNTIME_GRANTS)

    resolved = schema._with_account_tls_clause(
        grants_without_tls,
        "CREATE USER `probiga_runtime`@`127.0.0.1` "
        "IDENTIFIED WITH 'caching_sha2_password' AS '<redacted>' REQUIRE NONE",
    )

    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        schema._validate_runtime_grants(resolved)


def test_target_state_is_built_from_fixed_server_and_session_metadata():
    row = {
        "mysql_version": "8.4.11",
        "version_comment_value": "MySQL Community Server - GPL",
        "database_name": schema.DATABASE_NAME,
        "authenticated_user": schema.EXPECTED_RUNTIME_USER,
        "active_roles": "NONE",
        "server_uuid_value": schema.EXPECTED_SERVER_UUID.upper(),
        "server_port": "3306",
        "server_hostname": schema.EXPECTED_SERVER_HOSTNAME,
        "log_bin": "ON",
        "binlog_format": "row",
        "trust_creators": "OFF",
        "session_sql_mode": schema.EXPECTED_SQL_MODE,
        "character_set_client": schema.EXPECTED_CHARACTER_SET_CLIENT,
        "collation_connection": schema.EXPECTED_COLLATION_CONNECTION,
        "database_collation": schema.EXPECTED_DATABASE_COLLATION,
    }

    assert schema._state_from_row(row, "TLS_AES_256_GCM_SHA384") == _target_state()


def test_exact_runtime_and_admin_target_states_are_accepted():
    schema._validate_target_state(
        _target_state(),
        expected_user=schema.EXPECTED_RUNTIME_USER,
        require_database=True,
        expected_trust=0,
        require_trigger_session=True,
    )
    schema._validate_target_state(
        _target_state(
            user=schema.EXPECTED_ADMIN_USER,
            database=None,
        ),
        expected_user=schema.EXPECTED_ADMIN_USER,
        require_database=False,
        expected_trust=0,
        require_trigger_session=False,
    )


@pytest.mark.parametrize(
    "changes",
    (
        {"mysql_version": "8.4.10"},
        {"version_comment": "MariaDB Server"},
        {"server_uuid": "00000000-0000-0000-0000-000000000000"},
        {"server_port": 3307},
        {"server_hostname": "OTHER-HOST"},
        {"authenticated_user": schema.EXPECTED_ADMIN_USER},
        {"active_roles": "`schema_admin`@`%`"},
        {"log_bin": 0},
        {"binlog_format": "STATEMENT"},
        {"trust_creators": 1},
        {"tls_cipher": ""},
    ),
)
def test_target_state_rejects_version_identity_tls_trust_or_server_drift(changes):
    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        schema._validate_target_state(
            replace(_target_state(), **changes),
            expected_user=schema.EXPECTED_RUNTIME_USER,
            require_database=True,
            expected_trust=0,
            require_trigger_session=True,
        )


@pytest.mark.parametrize(
    "changes",
    (
        {"database_name": "other"},
        {"session_sql_mode": "STRICT_TRANS_TABLES"},
        {"character_set_client": "latin1"},
        {"collation_connection": "utf8mb4_0900_ai_ci"},
        {"database_collation": "utf8mb4_0900_ai_ci"},
    ),
)
def test_target_state_rejects_database_or_trigger_session_metadata_drift(changes):
    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        schema._validate_target_state(
            replace(_target_state(), **changes),
            expected_user=schema.EXPECTED_RUNTIME_USER,
            require_database=True,
            expected_trust=0,
            require_trigger_session=True,
        )


def test_cutover_requires_writer_fence_before_environment_or_database_access(
    monkeypatch,
):
    monkeypatch.setattr(
        schema,
        "load_project_env",
        lambda: pytest.fail("writer-fence rejection must precede environment load"),
    )
    monkeypatch.setattr(
        schema,
        "_open_boundary",
        lambda **_kwargs: pytest.fail("writer-fence rejection must precede DB access"),
    )

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="verified writer fence",
    ):
        schema.prepare_schema(phase="cutover", writers_fenced=False)


def test_trigger_inventory_allows_required_and_optional_absence(monkeypatch):
    required = _contract("trg_required")
    optional = _contract("trg_optional")
    connection = _InventoryConnection([_trigger_row(required)])
    monkeypatch.setattr(
        schema,
        "_normalized_trigger_body",
        lambda _contract, value: " ".join(str(value).upper().split()),
    )

    detail = schema.validate_release_trigger_contracts(
        connection,
        required={required.name: required},
        optional={optional.name: optional},
    )

    assert detail == {
        "required_count": 1,
        "optional_count": 1,
        "observed_count": 1,
        "definer": schema.EXPECTED_MIGRATOR_USER,
        "metadata_frozen": True,
        "legacy_rehome_names": [],
    }
    assert all(
        statement.lstrip().startswith("SELECT ")
        for statement in connection.statements
    )


def test_full_database_trigger_inventory_attests_exact_142_contracts():
    managed = {
        **schema._final_v3_trigger_contracts(),
        **schema._frozen_non_v3_release_trigger_contracts(
            schema._non_v3_trigger_contracts()
        ),
    }
    rows = _full_trigger_rows(managed)
    connection = _FullInventoryConnection(rows, managed)

    detail = schema.validate_full_database_trigger_inventory(
        connection,
        managed_contracts=managed,
    )

    assert detail["expected_count"] == 142
    assert detail["observed_count"] == 142
    assert detail["v2_count"] == 41
    assert detail["managed_count"] == 101
    assert detail["optional_v4_count"] == 0
    assert detail["nameset_sha256"] == (
        schema.EXPECTED_FULL_RELEASE_TRIGGER_NAMESET_HASH
    )
    assert detail["v2_source_contract_sha256"] == (
        schema.EXPECTED_V2_RELEASE_TRIGGER_SOURCE_HASH
    )
    assert detail["metadata_frozen"] is True
    assert all(
        statement.lstrip().startswith("SELECT ")
        for statement in connection.statements
    )


def test_full_database_trigger_inventory_attests_complete_applied_v4_group(
    monkeypatch,
):
    managed = {
        **schema._final_v3_trigger_contracts(),
        **schema._frozen_non_v3_release_trigger_contracts(
            schema._non_v3_trigger_contracts()
        ),
    }
    optional_v4_names = frozenset({
        matched.group(1)
        for migration in migrations_v4.MIGRATIONS
        for statement in migration["statements"]
        if (matched := schema._CREATE_TRIGGER_RE.match(str(statement).strip()))
        is not None
    })
    rows = _full_trigger_rows(managed)
    template = rows[0]
    lineage_names = {
        name for name, _event, _table, _statement
        in migrations_v4.PIT_FACTOR_LINEAGE_TRIGGER_SPECS
    }
    rows.extend({
        **template,
        "trigger_name": name,
        "action_order": 2 if name in lineage_names else 1,
    } for name in optional_v4_names)
    connection = _FullInventoryConnection(rows, managed)
    monkeypatch.setattr(
        schema,
        "_validated_applied_v4_trigger_names",
        lambda engine: optional_v4_names
        if engine is connection.engine else pytest.fail("wrong engine"),
    )

    detail = schema.validate_full_database_trigger_inventory(
        connection,
        managed_contracts=managed,
        include_applied_v4=True,
    )

    assert detail["expected_count"] == 174
    assert detail["observed_count"] == 174
    assert detail["optional_v4_count"] == 32


@pytest.mark.parametrize(
    ("statuses", "expected_count"),
    (("exists", 32), ("would_apply", 0)),
)
def test_optional_v4_trigger_group_is_all_or_absent(
    monkeypatch,
    statuses,
    expected_count,
):
    monkeypatch.setattr(
        migrations_v4,
        "run_v4_migrations",
        lambda _engine, *, dry_run: [
            SimpleNamespace(version=item["version"], status=statuses)
            for item in migrations_v4.MIGRATIONS
        ] if dry_run else pytest.fail("V4 attestation must be read-only"),
    )

    names = schema._validated_applied_v4_trigger_names(object())

    assert len(names) == expected_count


def test_optional_v4_trigger_group_rejects_partial_ledger(monkeypatch):
    monkeypatch.setattr(
        migrations_v4,
        "run_v4_migrations",
        lambda _engine, *, dry_run: [
            SimpleNamespace(
                version=item["version"],
                status="exists" if index == 0 else "would_apply",
            )
            for index, item in enumerate(migrations_v4.MIGRATIONS)
        ] if dry_run else pytest.fail("V4 attestation must be read-only"),
    )

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="ledger is partial",
    ):
        schema._validated_applied_v4_trigger_names(object())


@pytest.mark.parametrize("case", ("missing", "unexpected", "v2_body"))
def test_full_database_trigger_inventory_rejects_any_global_drift(case):
    managed = {
        **schema._final_v3_trigger_contracts(),
        **schema._frozen_non_v3_release_trigger_contracts(
            schema._non_v3_trigger_contracts()
        ),
    }
    rows = _full_trigger_rows(managed)
    if case == "missing":
        rows.pop()
    elif case == "unexpected":
        rows.append({
            **rows[0],
            "trigger_name": "trg_unapproved_global_trigger",
        })
    else:
        v2_names = set(schema._v2_release_trigger_contract()[0])
        target = next(
            row for row in rows if row["trigger_name"] in v2_names
        )
        target["action_statement"] = "SIGNAL SQLSTATE '45000'"

    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        schema.validate_full_database_trigger_inventory(
            _FullInventoryConnection(rows, managed),
            managed_contracts=managed,
        )


class _FrozenTriggerConnection:
    def __init__(self, engine):
        self.engine = engine

    def execute(self, statement, _params=None):
        self.engine.statements.append(str(statement))
        return _InventoryResult(self.engine.rows)


class _FrozenTriggerEngine:
    def __init__(self, rows):
        self.rows = list(rows)
        self.statements: list[str] = []

    def connect(self):
        return nullcontext(_FrozenTriggerConnection(self))


def test_frozen_release_trigger_ensure_is_no_delta_without_trust_window():
    contracts = {
        contract.name: contract
        for contract in (
            _contract("trg_exact_a"),
            _contract("trg_exact_b", table="controlled_audit"),
        )
    }
    engine = _FrozenTriggerEngine([
        _trigger_row(contract) for contract in contracts.values()
    ])

    detail = schema._ensure_frozen_release_triggers(
        engine,
        contracts,
        expected_names=contracts,
        expected_source_contract_hash=(
            schema._release_trigger_source_contract_hash(contracts)
        ),
        trigger_ddl_executor=lambda _statement: pytest.fail(
            "no-delta inventory opened a trigger trust window"
        ),
    )

    assert detail["created_names"] == []
    assert detail["created_count"] == 0
    assert detail["observed_count"] == 2
    assert detail["metadata_frozen"] is True


def test_frozen_release_trigger_ensure_creates_only_missing_then_reads_back():
    contracts = {
        contract.name: contract
        for contract in (
            _contract("trg_exact_a"),
            _contract("trg_exact_b", table="controlled_audit"),
        )
    }
    engine = _FrozenTriggerEngine([
        _trigger_row(contracts["trg_exact_a"])
    ])
    created: list[str] = []

    def create(statement):
        contract = schema._parse_create_trigger(
            statement,
            normalizer="governance",
            owner="test",
        )
        created.append(contract.name)
        engine.rows.append(_trigger_row(contracts[contract.name]))

    detail = schema._ensure_frozen_release_triggers(
        engine,
        contracts,
        expected_names=contracts,
        expected_source_contract_hash=(
            schema._release_trigger_source_contract_hash(contracts)
        ),
        trigger_ddl_executor=create,
    )

    assert created == ["trg_exact_b"]
    assert detail["created_names"] == created
    assert detail["created_count"] == 1
    assert detail["observed_count"] == 2
    assert detail["metadata_frozen"] is True


@pytest.mark.parametrize("drift", ("source_hash", "name_set", "metadata"))
def test_frozen_release_trigger_ensure_fails_before_create_on_drift(drift):
    contracts = {"trg_exact_a": _contract("trg_exact_a")}
    rows = [_trigger_row(contracts["trg_exact_a"])]
    expected_names = set(contracts)
    expected_hash = schema._release_trigger_source_contract_hash(contracts)
    if drift == "source_hash":
        expected_hash = "0" * 64
    elif drift == "name_set":
        expected_names.add("trg_unfrozen")
    else:
        rows[0]["definer"] = "runtime@127.0.0.1"
    engine = _FrozenTriggerEngine(rows)

    with pytest.raises(schema.PrivilegedSchemaPreparationError):
        schema._ensure_frozen_release_triggers(
            engine,
            contracts,
            expected_names=expected_names,
            expected_source_contract_hash=expected_hash,
            trigger_ddl_executor=lambda _statement: pytest.fail(
                "drifted inventory attempted privileged CREATE"
            ),
        )


@pytest.mark.parametrize("case", ("missing", "forbidden", "unexpected"))
def test_trigger_inventory_rejects_missing_forbidden_and_unexpected(
    monkeypatch,
    case,
):
    expected = _contract("trg_expected")
    forbidden = _contract("trg_forbidden")
    unexpected = _contract("trg_unexpected")
    rows = {
        "missing": [],
        "forbidden": [_trigger_row(expected), _trigger_row(forbidden)],
        "unexpected": [_trigger_row(expected), _trigger_row(unexpected)],
    }[case]
    monkeypatch.setattr(
        schema,
        "_normalized_trigger_body",
        lambda _contract, value: " ".join(str(value).upper().split()),
    )

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="inventory is incomplete or unexpected",
    ):
        schema.validate_release_trigger_contracts(
            _InventoryConnection(rows),
            required={expected.name: expected},
            optional={},
            forbidden_names=(forbidden.name,),
        )


def test_both_exact_legacy_metadata_pairs_are_the_only_rehome_exceptions(
    monkeypatch,
):
    contracts = {
        name: _contract(name, table="trade_account_v2")
        for name in schema.LEGACY_TRIGGER_REHOME_METADATA
    }
    rows = [
        _trigger_row(
            contract,
            definer=schema.LEGACY_TRIGGER_REHOME_METADATA[name][0],
            sql_mode=schema.LEGACY_TRIGGER_REHOME_METADATA[name][1],
        )
        for name, contract in contracts.items()
    ]
    monkeypatch.setattr(
        schema,
        "_normalized_trigger_body",
        lambda _contract, value: " ".join(str(value).upper().split()),
    )

    detail = schema.validate_release_trigger_contracts(
        _InventoryConnection(rows),
        required=contracts,
        optional={},
        allow_legacy_rehome=True,
    )

    assert detail["legacy_rehome_names"] == sorted(
        schema.LEGACY_TRIGGER_REHOME_METADATA
    )


@pytest.mark.parametrize(
    ("definer", "sql_mode", "allow_legacy"),
    (
        ("root@127.0.0.1", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION", True),
        ("root@localhost", schema.EXPECTED_SQL_MODE, True),
        ("root@localhost", "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION", False),
    ),
)
def test_legacy_rehome_exception_rejects_every_partial_or_unapproved_match(
    monkeypatch,
    definer,
    sql_mode,
    allow_legacy,
):
    name = "trg_trade_account_v2_real_disabled_bi"
    contract = _contract(name, table="trade_account_v2")
    monkeypatch.setattr(schema, "_normalized_trigger_body", lambda *_args: "same")

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="physical metadata differs",
    ):
        schema.validate_release_trigger_contracts(
            _InventoryConnection(
                [_trigger_row(contract, definer=definer, sql_mode=sql_mode)]
            ),
            required={name: contract},
            optional={},
            allow_legacy_rehome=allow_legacy,
        )


@pytest.mark.parametrize(
    "metadata",
    (
        {"definer": "root@localhost"},
        {"sql_mode": "STRICT_TRANS_TABLES"},
    ),
)
def test_nonlegacy_trigger_rejects_wrong_definer_or_sql_mode(
    monkeypatch,
    metadata,
):
    contract = _contract("trg_current")
    monkeypatch.setattr(schema, "_normalized_trigger_body", lambda *_args: "same")

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="physical metadata differs",
    ):
        schema.validate_release_trigger_contracts(
            _InventoryConnection([_trigger_row(contract, **metadata)]),
            required={contract.name: contract},
            optional={},
        )


class _AdminConnection:
    def __init__(self, *, trust: int, fail_read: bool = False) -> None:
        self.trust = trust
        self.fail_read = fail_read
        self.open = True
        self.closed = False

    def close(self):
        self.open = False
        self.closed = True


def _install_restoration_fakes(
    monkeypatch,
    *,
    secondary_fails: bool = False,
):
    fresh: list[_AdminConnection] = []

    def connect_admin(_boundary):
        connection = _AdminConnection(
            trust=0,
            fail_read=secondary_fails and not fresh,
        )
        fresh.append(connection)
        return connection

    def read_admin(connection):
        if connection.fail_read:
            raise RuntimeError("secondary verification unavailable")
        return _target_state(
            user=schema.EXPECTED_ADMIN_USER,
            database=None,
            trust=connection.trust,
        )

    monkeypatch.setattr(schema, "_connect_admin", connect_admin)
    monkeypatch.setattr(schema, "_read_dbapi_state", read_admin)
    monkeypatch.setattr(
        schema,
        "_set_trust",
        lambda connection, *, enabled: setattr(
            connection,
            "trust",
            int(enabled),
        ),
    )
    monkeypatch.setattr(schema, "_dbapi_grants", lambda _connection: ADMIN_GRANTS)
    monkeypatch.setattr(
        schema,
        "_read_sa_state",
        lambda connection: connection.state,
    )
    monkeypatch.setattr(schema, "_sa_grants", lambda _connection: RUNTIME_GRANTS)
    return fresh


def test_restore_forces_off_then_verifies_fresh_admin_and_runtime(
    monkeypatch,
):
    boundary = _boundary(trust=1)
    primary = _AdminConnection(trust=1)
    fresh = _install_restoration_fakes(monkeypatch)

    result = schema._restore_and_double_verify(boundary, primary)

    assert result == {
        "restore_primary_verified": True,
        "restore_secondary_verified": True,
        "runtime_trust_off_verified": True,
    }
    assert primary.trust == 0
    assert len(fresh) == 1
    assert fresh[0].closed is True
    assert boundary.runtime_engine.dispose_count == 1


def test_restore_reports_failure_when_fresh_admin_cannot_verify_off(
    monkeypatch,
):
    boundary = _boundary(trust=1)
    primary = _AdminConnection(trust=1)
    _install_restoration_fakes(monkeypatch, secondary_fails=True)

    result = schema._restore_and_double_verify(boundary, primary)

    assert result == {
        "restore_primary_verified": True,
        "restore_secondary_verified": False,
        "runtime_trust_off_verified": True,
    }


class _TriggerCursor:
    def __init__(self, owner):
        self.owner = owner

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, _params=None):
        self.owner.events.append("execute-create")
        self.owner.statements.append(str(statement))
        if self.owner.fail_create:
            raise RuntimeError("injected trigger DDL failure")


class _TriggerMigratorConnection:
    def __init__(self, admin, events, *, fail_create=False):
        self.admin = admin
        self.events = events
        self.fail_create = fail_create
        self.statements: list[str] = []
        self.open = True

    def cursor(self):
        return _TriggerCursor(self)

    def close(self):
        self.events.append("close-migrator")
        self.open = False


def _install_trigger_executor_fakes(monkeypatch, *, fail_create=False):
    admin = _AdminConnection(trust=0)
    boundary = _boundary(trust=0)
    boundary.migrator_engine = SimpleNamespace()
    events: list[str] = []
    migrator = _TriggerMigratorConnection(
        admin,
        events,
        fail_create=fail_create,
    )

    def read_state(connection):
        if connection is admin:
            events.append(f"read-admin-{admin.trust}")
            return _target_state(
                user=schema.EXPECTED_ADMIN_USER,
                database=None,
                trust=admin.trust,
            )
        events.append(f"read-migrator-{admin.trust}")
        return _target_state(
            user=schema.EXPECTED_MIGRATOR_USER,
            trust=admin.trust,
        )

    monkeypatch.setattr(schema, "_read_dbapi_state", read_state)
    monkeypatch.setattr(
        schema,
        "_owns_window_lock",
        lambda _admin: events.append("owns-lock") or True,
    )

    def connect_migrator(_boundary):
        assert admin.trust == 0
        events.append("connect-migrator-off")
        return migrator

    monkeypatch.setattr(schema, "_connect_migrator", connect_migrator)
    monkeypatch.setattr(
        schema,
        "_dbapi_grants",
        lambda connection: (
            ADMIN_GRANTS if connection is admin else MIGRATOR_GRANTS
        ),
    )
    monkeypatch.setattr(
        schema,
        "_dbapi_trigger_exists",
        lambda _connection, _name: events.append("prove-trigger-absent") or False,
    )

    def set_trust(connection, *, enabled):
        assert connection is admin
        events.append("set-on" if enabled else "set-off")
        admin.trust = int(enabled)

    monkeypatch.setattr(schema, "_set_trust", set_trust)

    def restore(_boundary, primary):
        assert primary is admin
        events.append("restore-off")
        admin.trust = 0
        return {
            "restore_primary_verified": True,
            "restore_secondary_verified": True,
            "runtime_trust_off_verified": True,
        }

    monkeypatch.setattr(schema, "_restore_and_double_verify", restore)
    return boundary, admin, migrator, events


def test_trigger_executor_opens_one_short_window_for_one_frozen_create(
    monkeypatch,
):
    boundary, admin, migrator, events = _install_trigger_executor_fakes(
        monkeypatch
    )
    contract = _contract("trg_window")
    statement = (
        "CREATE TRIGGER `trg_window` BEFORE UPDATE ON `controlled_ledger` "
        "FOR EACH ROW SET NEW.value = OLD.value"
    )
    evidence = {
        "trigger_trust_window_count": 0,
        "trigger_trust_window_names": [],
    }
    execute = schema._build_trigger_ddl_executor(
        boundary,
        admin,
        (contract,),
        evidence,
    )

    execute(statement)

    assert admin.trust == 0
    assert migrator.statements == [statement]
    assert events == [
        "read-admin-0",
        "owns-lock",
        "connect-migrator-off",
        "read-migrator-0",
        "prove-trigger-absent",
        "set-on",
        "execute-create",
        "restore-off",
        "close-migrator",
    ]
    window_start = events.index("set-on")
    assert events[window_start : window_start + 3] == [
        "set-on",
        "execute-create",
        "restore-off",
    ]
    assert evidence["trigger_trust_window_count"] == 1
    assert evidence["trigger_trust_window_names"] == ["trg_window"]
    assert evidence["last_trigger_window_restoration"][
        "trust_restoration_verified"
    ] is True


def test_trigger_executor_rejects_nonfrozen_create_before_database_access(
    monkeypatch,
):
    boundary, admin, _migrator, events = _install_trigger_executor_fakes(
        monkeypatch
    )
    execute = schema._build_trigger_ddl_executor(
        boundary,
        admin,
        (_contract("trg_window"),),
        {},
    )

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="frozen release contract",
    ):
        execute(
            "CREATE TRIGGER `trg_window` BEFORE UPDATE ON `controlled_ledger` "
            "FOR EACH ROW SET NEW.value = 0"
        )

    assert events == []
    assert admin.trust == 0


def test_trigger_executor_restores_off_after_create_failure(monkeypatch):
    boundary, admin, _migrator, events = _install_trigger_executor_fakes(
        monkeypatch,
        fail_create=True,
    )
    execute = schema._build_trigger_ddl_executor(
        boundary,
        admin,
        (_contract("trg_window"),),
        {},
    )

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="frozen trigger creation failed",
    ) as caught:
        execute(
            "CREATE TRIGGER `trg_window` BEFORE UPDATE ON `controlled_ledger` "
            "FOR EACH ROW SET NEW.value = OLD.value"
        )

    assert admin.trust == 0
    assert events[-3:] == ["execute-create", "restore-off", "close-migrator"]
    assert caught.value.safety_evidence == {
        "global_trust_changed": True,
        "trust_restoration_verified": True,
        "trigger_name": "trg_window",
        "restore_primary_verified": True,
        "restore_secondary_verified": True,
        "runtime_trust_off_verified": True,
    }


class _NoDeltaEngine:
    def __init__(self, identity: str):
        self.identity = identity

    def begin(self):
        return nullcontext(SimpleNamespace(engine=self))

    def connect(self):
        return nullcontext(SimpleNamespace(engine=self))


def test_no_delta_cutover_never_enables_trust_and_still_triple_verifies_off(
    monkeypatch,
):
    from server.api.routers import _engine as api_engine_module
    from server.common import pit_facts
    from server.common import production_runtime_schema_bundle
    from server.common import qmt_history_coverage
    from server.common import scheduler_runtime_schema
    from server.common import scheduler_task_history_schema
    from server.common import schema_recovery_evidence
    from server.db import migrations_v3
    from server.engine import dynamic_shadow_ledger_schema
    from server.engine import strategy_governance
    from tools import attest_qmt_daily_kline
    from tools import prepare_strategy_governance_qmt_history as qmt_history
    from tools import sync_guojin_qmt_reference_data as qmt_reference

    boundary = _boundary(trust=0)
    migrator_engine = _NoDeltaEngine("migrator")
    api_engine = _NoDeltaEngine("api")
    boundary.migrator_engine = migrator_engine
    admin = _AdminConnection(trust=0)
    calls: list[str] = []
    validator_engines: dict[str, list[_NoDeltaEngine]] = {
        "pit": [],
        "reference_triggers": [],
        "runtime_bundle": [],
        "qmt_coverage_triggers": [],
        "metric_triggers": [],
        "funding_triggers": [],
        "append_only_triggers": [],
        "governance_table": [],
        "seed_contract": [],
    }

    def record_validator_engine(name, engine_or_connection):
        observed = getattr(engine_or_connection, "engine", engine_or_connection)
        validator_engines[name].append(observed)
    monkeypatch.setattr(schema, "_connect_admin", lambda _boundary: admin)
    monkeypatch.setattr(
        schema,
        "_read_dbapi_state",
        lambda _connection: _target_state(
            user=schema.EXPECTED_ADMIN_USER,
            database=None,
            trust=0,
        ),
    )
    monkeypatch.setattr(schema, "_dbapi_grants", lambda _connection: ADMIN_GRANTS)
    monkeypatch.setattr(schema, "_acquire_lock", lambda _connection: True)
    monkeypatch.setattr(schema, "_release_lock", lambda _connection: True)
    monkeypatch.setattr(
        schema_recovery_evidence,
        "ensure_evidence_table",
        lambda _connection, **_kwargs: calls.append(
            "schema-recovery-evidence-table-off"
        ),
    )
    monkeypatch.setattr(
        schema_recovery_evidence,
        "validate_recovery_evidence_schema",
        lambda _engine: {
            "physical_contract_verified": True,
            "append_only_verified": True,
        },
    )
    monkeypatch.setattr(
        scheduler_runtime_schema,
        "migrate_scheduler_runtime_heartbeat",
        lambda _engine: calls.append("scheduler-runtime-migration-off") or {
            "status": "ok",
            "physical_contract_verified": True,
        },
    )
    monkeypatch.setattr(
        scheduler_runtime_schema,
        "validate_scheduler_runtime_heartbeat_schema",
        lambda _engine: calls.append("scheduler-runtime-validate") or {
            "physical_contract_verified": True,
            "read_only": True,
        },
    )
    monkeypatch.setattr(
        scheduler_task_history_schema,
        "migrate_scheduler_task_history",
        lambda _engine: calls.append("scheduler-history-migration-off") or {
            "status": "ok",
            "physical_contract_verified": True,
        },
    )
    monkeypatch.setattr(
        scheduler_task_history_schema,
        "validate_scheduler_task_history_schema",
        lambda _engine: calls.append("scheduler-history-validate") or {
            "physical_contract_verified": True,
            "runtime_ddl_required": False,
            "read_only": True,
        },
    )
    monkeypatch.setattr(
        production_runtime_schema_bundle,
        "privileged_migrate_runtime_schema_bundle",
        lambda _engine, **kwargs: (
            calls.append("runtime-schema-bundle-off")
            or {
                "migrations": {},
                "seeds": {},
                "runtime_ddl_required": False,
                "trigger_validation_deferred": kwargs.get(
                    "defer_trigger_validation"
                ),
            }
        ),
    )
    monkeypatch.setattr(
        production_runtime_schema_bundle,
        "validate_runtime_schema_bundle",
        lambda engine: (
            record_validator_engine("runtime_bundle", engine)
            or calls.append("runtime-schema-bundle-validate")
            or {
            "contracts": {},
            "runtime_ddl_required": False,
            "read_only": True,
            }
        ),
    )
    monkeypatch.setattr(
        schema,
        "_set_trust",
        lambda *_args, **_kwargs: pytest.fail("no-delta cutover enabled trust"),
    )
    monkeypatch.setattr(schema, "_all_v3_trigger_contracts", lambda: ())
    monkeypatch.setattr(schema, "_final_v3_trigger_contracts", lambda: {})
    monkeypatch.setattr(schema, "_non_v3_trigger_contracts", lambda: {})
    monkeypatch.setattr(
        schema,
        "_frozen_non_v3_release_trigger_contracts",
        lambda _contracts: {},
    )
    monkeypatch.setattr(
        schema,
        "_frozen_governance_release_trigger_contracts",
        lambda _contracts: {},
    )
    monkeypatch.setattr(
        schema,
        "_ensure_frozen_release_triggers",
        lambda *_args, **_kwargs: {
            "required_count": 0,
            "observed_count": 0,
            "metadata_frozen": True,
            "source_contract_hash": (
                schema.EXPECTED_GOVERNANCE_RELEASE_TRIGGER_SOURCE_HASH
            ),
            "expected_names": [],
            "created_names": [],
            "created_count": 0,
        },
    )
    monkeypatch.setattr(schema, "_v3_trigger_states", lambda _plan: ({}, {}))
    monkeypatch.setattr(
        schema,
        "_runtime_least_privilege_evidence",
        lambda _boundary: {
            "runtime_least_privilege_verified": True,
            "runtime_definer_routine_count": 0,
        },
    )
    monkeypatch.setattr(
        schema,
        "_restore_and_double_verify",
        lambda _boundary, _admin: calls.append("triple-off") or {
            "restore_primary_verified": True,
            "restore_secondary_verified": True,
            "runtime_trust_off_verified": True,
        },
    )
    monkeypatch.setattr(
        schema,
        "_rehome_legacy_triggers",
        lambda *_args, **_kwargs: (),
    )
    monkeypatch.setattr(
        schema,
        "validate_release_trigger_contracts",
        lambda *_args, **_kwargs: {"observed_count": 0},
    )
    monkeypatch.setattr(
        schema,
        "validate_full_database_trigger_inventory",
        lambda *_args, **_kwargs: {
            "observed_count": 0,
            "metadata_frozen": True,
        },
    )

    def fake_migrations(_engine, *, dry_run=False, **kwargs):
        if not dry_run:
            assert callable(kwargs.get("trigger_ddl_executor"))
        return []

    monkeypatch.setattr(migrations_v3, "run_v3_migrations", fake_migrations)
    monkeypatch.setattr(
        schema,
        "_prepare_qmt_reference_schema_tables",
        lambda _engine: calls.append("qmt-reference-schema-off") or {
            "contract_hash": qmt_reference.REFERENCE_SCHEMA_CONTRACT_HASH,
            "table_names": list(qmt_reference.REFERENCE_TABLE_NAMES),
            "trigger_names": list(qmt_reference.REFERENCE_TRIGGER_NAMES),
            "table_ddl_count": 5,
            "migration_ddl_count": 9,
            "runtime_ddl_required": False,
        },
    )
    monkeypatch.setattr(
        schema,
        "_prepare_qmt_history_coverage_schema_tables",
        lambda _engine: calls.append("qmt-coverage-schema-off") or {
            "database": "probiga",
            "table_names": list(
                qmt_history_coverage.COVERAGE_TABLE_NAMES
            ),
            "trigger_names": list(
                qmt_history_coverage.COVERAGE_TRIGGER_NAMES
            ),
            "table_ddl_count": 2,
            "trigger_ddl_count": 4,
            "runtime_ddl_required": False,
        },
    )
    monkeypatch.setattr(
        qmt_history_coverage,
        "validate_coverage_schema",
        lambda connection, **kwargs: (
            record_validator_engine(
                "qmt_coverage_triggers",
                connection,
            )
            if kwargs.get("require_triggers") is True
            else None
        )
        or {
            "database": "probiga",
            "table_count": 2,
            "trigger_count": 4,
            "physical_schema_verified": True,
            "physical_seal_verified": True,
        },
    )
    monkeypatch.setattr(
        attest_qmt_daily_kline,
        "privileged_migrate_attestation_tables",
        lambda _engine, **kwargs: (
            calls.append("qmt-schema-off"),
            assert_callable(kwargs.get("trigger_ddl_executor")),
        ),
    )
    monkeypatch.setattr(
        attest_qmt_daily_kline,
        "validate_attestation_schema",
        lambda *_args, **_kwargs: {"errors": []},
    )
    monkeypatch.setattr(
        qmt_history,
        "apply_legacy_completed_run_binding",
        lambda _engine: {"legacy_run_count": 0},
    )
    monkeypatch.setattr(
        pit_facts,
        "ensure_pit_fact_schema",
        lambda _engine, **kwargs: (
            calls.append("pit-schema-off")
            or {
                "status": "READY",
                "trigger_ddl_executor": callable(
                    kwargs.get("trigger_ddl_executor")
                ),
            }
        ),
    )
    monkeypatch.setattr(
        pit_facts,
        "pit_fact_schema_health",
        lambda engine: record_validator_engine("pit", engine) or {"valid": True},
    )
    monkeypatch.setattr(
        qmt_reference,
        "attest_prepared_reference_schema",
        lambda _engine: {
            "contract_key": qmt_reference.REFERENCE_SCHEMA_CONTRACT_KEY,
            "contract_hash": qmt_reference.REFERENCE_SCHEMA_CONTRACT_HASH,
            "table_contract_hash": "a" * 64,
            "trigger_contract_hash": "b" * 64,
        },
    )
    monkeypatch.setattr(
        qmt_reference,
        "validate_reference_tables",
        lambda engine, **kwargs: (
            record_validator_engine("reference_triggers", engine)
            if kwargs.get("verify_triggers") is True
            else None
        ),
    )
    governance_schema_calls = []

    def fake_governance_schema(**kwargs):
        governance_schema_calls.append(dict(kwargs))
        calls.append(
            "governance-schema-base"
            if kwargs.get("base_schema_only") is True
            else "governance-schema-sealed"
        )
        assert kwargs.get("writers_fenced") is True
        assert kwargs.get("trigger_ddl_executor") is None

    monkeypatch.setattr(
        strategy_governance,
        "ensure_strategy_governance_tables",
        fake_governance_schema,
    )
    monkeypatch.setattr(
        dynamic_shadow_ledger_schema,
        "validate_dynamic_shadow_ledger_schema",
        lambda _connection: {
            "table_count": 4,
            "column_count": 57,
            "index_count": 22,
            "foreign_key_count": 15,
            "check_count": 10,
            "contract_hash": "d" * 64,
        },
    )
    monkeypatch.setattr(
        strategy_governance,
        "seed_governance_registry",
        lambda: calls.append("seed"),
    )
    monkeypatch.setattr(
        strategy_governance,
        "validate_metric_input_review_triggers",
        lambda connection: (
            record_validator_engine("metric_triggers", connection)
            or {
            "trigger_count": 2,
            "trigger_names": sorted(
                strategy_governance.EXPECTED_METRIC_INPUT_REVIEW_TRIGGER_NAMES
            ),
            "contract_hash": (
                strategy_governance.METRIC_INPUT_REVIEW_TRIGGER_CONTRACT_HASH
            ),
            }
        ),
    )
    monkeypatch.setattr(
        strategy_governance,
        "validate_governance_table_schema",
        lambda connection: (
            record_validator_engine("governance_table", connection)
            or {
            "table_count": 15,
            "column_count": 1,
            "index_count": 1,
            }
        ),
    )
    monkeypatch.setattr(
        strategy_governance,
        "validate_governance_append_only_triggers",
        lambda engine: (
            record_validator_engine("append_only_triggers", engine)
            or {
            "trigger_count": 38,
            "trigger_names": sorted(
                strategy_governance.EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES
            ),
            "contract_hash": (
                strategy_governance.GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACT_HASH
            ),
            }
        ),
    )
    monkeypatch.setattr(
        schema,
        "validate_strategy_funding_checkpoint_schema",
        lambda connection: (
            record_validator_engine("funding_triggers", connection)
            or {
            "table_count": 2,
            "tables": {
                "st_strategy_funding_daily_fact": {
                    "column_count": 29,
                    "index_count": 9,
                    "foreign_key_count": 3,
                    "check_count": 7,
                },
                "st_strategy_funding_checkpoint": {
                    "column_count": 46,
                    "index_count": 12,
                    "foreign_key_count": 7,
                    "check_count": 13,
                },
            },
            "trigger_count": 4,
                "contract_hash": schema.EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH,
            }
        ),
    )
    monkeypatch.setattr(
        strategy_governance,
        "validate_default_governance_seed_contract",
        lambda engine, **_kwargs: (
            record_validator_engine("seed_contract", engine)
            or {
            "seeded_strategy_count": 12,
            "seeded_combination_count": 6,
            "seed_contract_hash": "a" * 64,
            }
        ),
    )
    monkeypatch.setattr(
        api_engine_module,
        "get_engine",
        lambda: api_engine,
    )
    monkeypatch.setattr(api_engine_module, "dispose_engine", lambda: None)

    detail = schema._cutover_schema(boundary)

    assert detail["trigger_trust_window_count"] == 0
    assert detail["trigger_trust_window_names"] == []
    assert detail["global_trust_changed"] is False
    assert detail["governance_trigger_count"] == 40
    assert detail["governance_append_only_trigger_count"] == 38
    assert detail["governance_metric_review_trigger_count"] == 2
    assert detail["funding_checkpoint_trigger_count"] == 4
    assert detail["trust_restoration_verified"] is True
    assert detail["runtime_least_privilege_verified"] is True
    assert [
        call.get("base_schema_only") for call in governance_schema_calls
    ] == [True, None]
    assert calls.index("governance-schema-base") < calls.index(
        "governance-schema-sealed"
    )
    assert calls.index("governance-schema-sealed") < calls.index("seed")
    assert calls.count("scheduler-runtime-migration-off") == 1
    assert calls.count("scheduler-runtime-validate") == 1
    assert calls.count("triple-off") == 1
    for validator in (
        "pit",
        "reference_triggers",
        "runtime_bundle",
        "qmt_coverage_triggers",
        "metric_triggers",
        "funding_triggers",
        "append_only_triggers",
    ):
        assert validator_engines[validator]
        assert all(
            observed is migrator_engine
            for observed in validator_engines[validator]
        )
    assert validator_engines["governance_table"] == [api_engine]
    assert validator_engines["seed_contract"] == [api_engine]


def assert_callable(value):
    assert callable(value)


class _RepairConnection:
    def __init__(self, engine):
        self.engine = engine

    def execute(self, statement, _params=None):
        sql = str(statement).strip()
        if sql.upper().startswith("SELECT "):
            return _InventoryResult(self.engine.rows)
        match = schema._DROP_TRIGGER_RE.fullmatch(sql)
        if match is not None:
            name = match.group(1)
            self.engine.events.append(f"drop:{name}")
            self.engine.rows = [
                row for row in self.engine.rows
                if row["trigger_name"] != name
            ]
            return _InventoryResult([])
        raise AssertionError(sql)


class _RepairEngine:
    def __init__(self, rows):
        self.rows = list(rows)
        self.events: list[str] = []

    def connect(self):
        return nullcontext(_RepairConnection(self))

    def begin(self):
        return nullcontext(_RepairConnection(self))


def _legacy_repair_contracts():
    return {
        name: _contract(name, table="trade_account_v2")
        for name in schema.LEGACY_TRIGGER_REHOME_METADATA
    }


def test_drop_failure_recover_then_guarded_resume_repairs_only_absent_legacy(
    monkeypatch,
):
    contracts = _legacy_repair_contracts()
    rows = [
        _trigger_row(
            contract,
            definer=schema.LEGACY_TRIGGER_REHOME_METADATA[name][0],
            sql_mode=schema.LEGACY_TRIGGER_REHOME_METADATA[name][1],
        )
        for name, contract in contracts.items()
    ]
    engine = _RepairEngine(rows)
    interrupted_name = sorted(contracts)[0]

    def interrupted_create(_statement):
        engine.events.append(f"interrupted-create:{interrupted_name}")
        raise RuntimeError("injected failure after committed DROP")

    with pytest.raises(RuntimeError, match="committed DROP"):
        schema._rehome_legacy_triggers(
            engine,
            contracts,
            trigger_ddl_executor=interrupted_create,
        )
    assert {row["trigger_name"] for row in engine.rows} == (
        set(contracts) - {interrupted_name}
    )

    admin = _AdminConnection(trust=1)
    boundary = _boundary(trust=1)
    monkeypatch.setattr(schema, "_connect_admin", lambda _boundary: admin)
    monkeypatch.setattr(
        schema,
        "_read_dbapi_state",
        lambda connection: _target_state(
            user=schema.EXPECTED_ADMIN_USER,
            database=None,
            trust=connection.trust,
        ),
    )
    monkeypatch.setattr(schema, "_dbapi_grants", lambda _connection: ADMIN_GRANTS)
    monkeypatch.setattr(schema, "_acquire_lock", lambda _connection: True)
    monkeypatch.setattr(schema, "_release_lock", lambda _connection: True)

    def recover_admin_off(_boundary, primary):
        engine.events.append("recover-admin-off")
        primary.trust = 0
        return {
            "restore_primary_verified": True,
            "restore_secondary_verified": True,
        }

    monkeypatch.setattr(schema, "_restore_and_verify_admin", recover_admin_off)
    monkeypatch.setattr(schema, "load_project_env", lambda: None)
    monkeypatch.setattr(
        schema,
        "create_tool_engine",
        lambda **_kwargs: _RuntimeEngine(),
    )
    monkeypatch.setattr(
        schema,
        "_verify_runtime_trust_off",
        lambda _engine: True,
    )

    recovery = schema._recover_trust(boundary)
    assert recovery["trust_restoration_verified"] is True
    assert admin.trust == 0

    def repair_create(statement):
        contract = schema._frozen_trigger_contract_for_statement(
            statement,
            contracts.values(),
        )
        engine.events.append(f"resume-create:{contract.name}")
        engine.rows.append(_trigger_row(contract))

    repair = schema._repair_interrupted_legacy_rehome(
        engine,
        contracts,
        trigger_ddl_executor=repair_create,
    )

    assert repair == {
        "candidate_names": [interrupted_name],
        "repaired_names": [interrupted_name],
        "post_validation_verified": True,
    }
    assert engine.events == [
        f"drop:{interrupted_name}",
        f"interrupted-create:{interrupted_name}",
        "recover-admin-off",
        f"resume-create:{interrupted_name}",
    ]
    assert {row["trigger_name"] for row in engine.rows} == set(contracts)


def test_guarded_resume_rejects_renamed_trigger_on_legacy_controlled_table():
    contracts = _legacy_repair_contracts()
    existing_name = sorted(contracts)[1]
    unexpected = _contract(
        "trg_trade_account_v2_renamed_unsafe",
        table="trade_account_v2",
    )
    engine = _RepairEngine([
        _trigger_row(contracts[existing_name]),
        _trigger_row(unexpected),
    ])

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="inventory is incomplete or unexpected",
    ):
        schema._repair_interrupted_legacy_rehome(
            engine,
            contracts,
            trigger_ddl_executor=lambda _statement: pytest.fail(
                "drifted resume reached CREATE"
            ),
        )


@pytest.mark.parametrize("all_verified", (True, False))
def test_recover_succeeds_only_after_every_independent_verification(
    monkeypatch,
    all_verified,
):
    boundary = _boundary(trust=1)
    admin = _AdminConnection(trust=1)
    monkeypatch.setattr(schema, "_connect_admin", lambda _boundary: admin)
    monkeypatch.setattr(schema, "_acquire_lock", lambda _connection: True)
    monkeypatch.setattr(schema, "_release_lock", lambda _connection: True)
    monkeypatch.setattr(
        schema,
        "_read_dbapi_state",
        lambda connection: _target_state(
            user=schema.EXPECTED_ADMIN_USER,
            database=None,
            trust=connection.trust,
        ),
    )
    monkeypatch.setattr(schema, "_dbapi_grants", lambda _connection: ADMIN_GRANTS)
    def force_off(_boundary, primary):
        primary.trust = 0
        return {
            "restore_primary_verified": True,
            "restore_secondary_verified": all_verified,
        }

    monkeypatch.setattr(schema, "_restore_and_verify_admin", force_off)
    monkeypatch.setattr(schema, "load_project_env", lambda: None)
    runtime_engine = _RuntimeEngine()
    monkeypatch.setattr(
        schema,
        "create_tool_engine",
        lambda **_kwargs: runtime_engine,
    )
    monkeypatch.setattr(
        schema,
        "_verify_runtime_trust_off",
        lambda _engine: True,
    )

    if all_verified:
        result = schema._recover_trust(boundary)
        assert result["trust_restoration_verified"] is True
        assert result["global_trust_changed"] is True
    else:
        with pytest.raises(
            schema.PrivilegedSchemaPreparationError,
            match="could not recover",
        ) as caught:
            schema._recover_trust(boundary)
        assert caught.value.safety_evidence["trust_restoration_verified"] is False
    assert admin.closed is True


def test_recover_forces_admin_off_before_broken_project_environment(
    monkeypatch,
):
    boundary = _boundary(trust=1)
    admin = _AdminConnection(trust=1)
    order: list[str] = []
    monkeypatch.setattr(schema, "_connect_admin", lambda _boundary: admin)
    def read_state(connection):
        order.append("read-admin-state")
        return _target_state(
            user=schema.EXPECTED_ADMIN_USER,
            database=None,
            trust=connection.trust,
        )

    monkeypatch.setattr(schema, "_read_dbapi_state", read_state)
    monkeypatch.setattr(schema, "_dbapi_grants", lambda _connection: ADMIN_GRANTS)
    monkeypatch.setattr(schema, "_acquire_lock", lambda _connection: True)
    monkeypatch.setattr(schema, "_release_lock", lambda _connection: True)

    def restore_admin(_boundary, primary):
        order.append("admin-off")
        primary.trust = 0
        return {
            "restore_primary_verified": True,
            "restore_secondary_verified": True,
        }

    monkeypatch.setattr(schema, "_restore_and_verify_admin", restore_admin)

    def broken_environment():
        order.append("load-env")
        raise RuntimeError("corrupt env")

    monkeypatch.setattr(schema, "load_project_env", broken_environment)

    with pytest.raises(schema.PrivilegedSchemaPreparationError) as caught:
        schema._recover_trust(boundary)

    assert order == ["admin-off", "read-admin-state", "load-env"]
    assert caught.value.safety_evidence == {
        "global_trust_changed": True,
        "trust_restoration_verified": False,
        "restore_primary_verified": True,
        "restore_secondary_verified": True,
        "runtime_trust_off_verified": False,
    }


class _ReadOnlyResult:
    def mappings(self):
        return self

    def all(self):
        return []

    def __iter__(self):
        return iter(())


class _ReadOnlyConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement, _params=None):
        sql = str(statement).strip()
        self.statements.append(sql)
        if not sql.upper().startswith("SELECT "):
            raise AssertionError(f"preflight attempted mutation: {sql}")
        return _ReadOnlyResult()


class _ReadOnlyEngine:
    def __init__(self) -> None:
        self.connection = _ReadOnlyConnection()

    def connect(self):
        return nullcontext(self.connection)


def test_governance_cutover_recovery_preflight_selects_resume_for_exact_marker_gap(
    monkeypatch,
):
    from server.engine import strategy_governance
    from server.engine.strategy_funding_checkpoint import (
        FUNDING_CHECKPOINT_MIGRATION_HASH,
        FUNDING_CHECKPOINT_MIGRATION_KEY,
    )

    class Result:
        def mappings(self):
            return self

        def all(self):
            return [{
                "migration_key": FUNDING_CHECKPOINT_MIGRATION_KEY,
                "migration_hash": FUNDING_CHECKPOINT_MIGRATION_HASH,
            }]

    class Connection:
        def __init__(self):
            self.statements = []

        def execute(self, statement, params):
            sql = str(statement).strip()
            self.statements.append(sql)
            assert sql.upper().startswith("SELECT ")
            assert params == {
                "migration_key": FUNDING_CHECKPOINT_MIGRATION_KEY,
            }
            return Result()

    monkeypatch.setattr(
        strategy_governance,
        "validate_deferred_governance_trigger_inventory",
        lambda _connection: {
            "expected_trigger_count": 40,
            "installed_trigger_count": 0,
            "missing_trigger_count": 40,
        },
    )
    connection = Connection()

    detail = schema._preflight_governance_cutover_recovery(
        connection,
        governance_tables_present=True,
    )

    assert detail == {
        "schema": "probiga.strategy-governance-cutover-recovery.v1",
        "status": "RESUME_REQUIRED",
        "read_only": True,
        "full_migration_marker_present": True,
        "full_migration_marker_hash_verified": True,
        "expected_trigger_count": 40,
        "installed_trigger_count": 0,
        "missing_trigger_count": 40,
        "resume_required": True,
    }
    assert len(connection.statements) == 1


def test_governance_cutover_recovery_preflight_rejects_drifted_full_marker(
    monkeypatch,
):
    from server.engine import strategy_governance
    from server.engine.strategy_funding_checkpoint import (
        FUNDING_CHECKPOINT_MIGRATION_KEY,
    )

    class Result:
        def mappings(self):
            return self

        def all(self):
            return [{
                "migration_key": FUNDING_CHECKPOINT_MIGRATION_KEY,
                "migration_hash": "0" * 64,
            }]

    class Connection:
        def execute(self, _statement, _params):
            return Result()

    monkeypatch.setattr(
        strategy_governance,
        "validate_deferred_governance_trigger_inventory",
        lambda _connection: {
            "expected_trigger_count": 40,
            "installed_trigger_count": 0,
            "missing_trigger_count": 40,
        },
    )

    with pytest.raises(
        schema.PrivilegedSchemaPreparationError,
        match="full governance migration marker differs",
    ):
        schema._preflight_governance_cutover_recovery(
            Connection(),
            governance_tables_present=True,
        )


def test_preflight_is_read_only_and_v3_is_always_dry_run(monkeypatch):
    from server.common import pit_facts
    from server.db import migrations_v3
    from server.engine import dynamic_shadow_ledger_schema
    from server.engine import strategy_governance
    from tools import attest_qmt_daily_kline
    from tools import sync_guojin_qmt_reference_data as qmt_reference

    engine = _ReadOnlyEngine()
    dry_run_calls: list[bool] = []

    def dry_run_only(observed_engine, *, dry_run=False, **_kwargs):
        assert observed_engine is engine
        dry_run_calls.append(dry_run)
        return []

    monkeypatch.setattr(migrations_v3, "run_v3_migrations", dry_run_only)
    monkeypatch.setattr(schema, "_v3_trigger_states", lambda _plan: ({}, {}))
    monkeypatch.setattr(schema, "_non_v3_trigger_contracts", lambda: {})
    monkeypatch.setattr(
        schema,
        "_frozen_non_v3_release_trigger_contracts",
        lambda _contracts: {},
    )
    monkeypatch.setattr(
        schema,
        "_frozen_governance_release_trigger_contracts",
        lambda _contracts: {},
    )
    monkeypatch.setattr(
        schema,
        "_runtime_least_privilege_evidence",
        lambda _boundary: {"runtime_least_privilege_verified": True},
    )
    monkeypatch.setattr(
        attest_qmt_daily_kline,
        "ensure_attestation_tables",
        lambda *_args, **_kwargs: pytest.fail("preflight called QMT DDL"),
    )
    monkeypatch.setattr(
        strategy_governance,
        "ensure_strategy_governance_tables",
        lambda *_args, **_kwargs: pytest.fail("preflight called governance DDL"),
    )
    monkeypatch.setattr(
        dynamic_shadow_ledger_schema,
        "preflight_dynamic_shadow_ledger_schema_upgrade",
        lambda _connection: {
            "status": "ABSENT_CREATE_ALLOWED",
            "expected_table_count": 4,
            "real_order_authority": False,
        },
    )
    monkeypatch.setattr(
        pit_facts,
        "preflight_pit_fact_schema",
        lambda _connection: {"status": "ABSENT_CREATE_ALLOWED"},
    )
    monkeypatch.setattr(
        qmt_reference,
        "preflight_reference_tables",
        lambda observed_engine: {
            "status": "EMPTY",
            "read_only": True,
            "contract_hash": qmt_reference.REFERENCE_SCHEMA_CONTRACT_HASH,
        }
        if observed_engine is engine
        else pytest.fail("wrong QMT reference preflight engine"),
    )
    boundary = SimpleNamespace(migrator_engine=engine)

    detail = schema._preflight_schema(boundary)

    assert dry_run_calls == [True]
    assert detail["qmt_table_count"] == 0
    assert detail["governance_table_count"] == 0
    assert detail["governance_cutover_recovery"] == {
        "schema": "probiga.strategy-governance-cutover-recovery.v1",
        "status": "CUTOVER_READY",
        "read_only": True,
        "full_migration_marker_present": False,
        "full_migration_marker_hash_verified": False,
        "expected_trigger_count": 0,
        "installed_trigger_count": 0,
        "missing_trigger_count": 0,
        "resume_required": False,
    }
    assert detail["dynamic_shadow_schema"]["status"] == (
        "ABSENT_CREATE_ALLOWED"
    )
    assert detail["qmt_reference_schema"]["status"] == "EMPTY"
    assert engine.connection.statements
    assert all(
        statement.upper().startswith(("SELECT ", "SHOW "))
        for statement in engine.connection.statements
    )


def test_non_v3_release_contract_freezes_all_truth_guards():
    from server.common.auxiliary_runtime_schema import (
        QMT_MEMBERSHIP_IMMUTABILITY_TRIGGER_NAMES,
    )
    from server.common.turnover_snapshot_schema import (
        _expected_trigger_contracts as field_capture_trigger_contracts,
    )
    from tools.sync_guojin_qmt_reference_data import REFERENCE_TRIGGER_NAMES

    contracts = schema._frozen_non_v3_release_trigger_contracts(
        schema._non_v3_trigger_contracts()
    )

    assert len(contracts) == schema.EXPECTED_NON_V3_RELEASE_TRIGGER_COUNT
    assert schema._release_trigger_source_contract_hash(contracts) == (
        schema.EXPECTED_NON_V3_RELEASE_TRIGGER_SOURCE_HASH
    )
    assert set(REFERENCE_TRIGGER_NAMES) <= set(contracts)
    assert set(QMT_MEMBERSHIP_IMMUTABILITY_TRIGGER_NAMES) <= set(contracts)
    assert set(field_capture_trigger_contracts()) <= set(contracts)
    assert schema._release_trigger_owner_counts(contracts) == {
        "market_field_capture": 5,
        "pit_facts": 6,
        "qmt_attestation": 6,
        "qmt_history_coverage": 4,
        "qmt_membership": 6,
        "qmt_reference": 10,
        "scheduler_task_history": 2,
        "schema_recovery_evidence": 2,
        "strategy_governance": 40,
    }
    assert {
        "trg_qmt_kline_attestation_run_completed_bu",
        "trg_qmt_kline_attestation_run_completed_bd",
        "trg_pit_source_coverage_immutable_bu",
        "trg_pit_source_coverage_immutable_bd",
        "trg_privileged_schema_recovery_evidence_immutable_bu",
        "trg_privileged_schema_recovery_evidence_immutable_bd",
    } <= set(contracts)
    evidence_contracts = {
        name: contract for name, contract in contracts.items()
        if contract.owner == "schema_recovery_evidence"
    }
    assert len(evidence_contracts) == 2
    assert schema._release_trigger_source_contract_hash(evidence_contracts) == (
        schema.EXPECTED_SCHEMA_RECOVERY_EVIDENCE_TRIGGER_SOURCE_HASH
    )


def test_qmt_reference_table_preparation_never_creates_triggers():
    from tools.sync_guojin_qmt_reference_data import (
        REFERENCE_SCHEMA_CONTRACT_HASH,
        REFERENCE_TABLE_NAMES,
        REFERENCE_TRIGGER_NAMES,
        reference_migration_ddl_contracts,
        reference_table_ddl_contracts,
    )

    statements: list[str] = []

    class _Connection:
        def execute(self, statement, _params=None):
            statements.append(str(statement).strip())
            return _ReadOnlyResult()

    class _Engine:
        def begin(self):
            return nullcontext(_Connection())

    detail = schema._prepare_qmt_reference_schema_tables(_Engine())

    assert detail["contract_hash"] == REFERENCE_SCHEMA_CONTRACT_HASH
    assert detail["table_names"] == list(REFERENCE_TABLE_NAMES)
    assert detail["trigger_names"] == list(REFERENCE_TRIGGER_NAMES)
    mutations = [
        statement for statement in statements
        if not statement.upper().startswith("SELECT ")
    ]
    assert len(mutations) == (
        len(reference_table_ddl_contracts())
        + len(reference_migration_ddl_contracts())
    )
    assert not any("ADD COLUMN IF NOT EXISTS" in statement.upper()
                   for statement in mutations)
    assert not any("CREATE TRIGGER" in statement.upper()
                   for statement in mutations)
    assert detail["runtime_ddl_required"] is False


@pytest.mark.parametrize(
    "error",
    (
        RuntimeError(
            "mysql+pymysql://probiga_migrator:do-not-print@127.0.0.1/probiga"
        ),
        schema.PrivilegedSchemaPreparationError(
            "password=hunter2 token=super-secret",
            safety_evidence={
                "global_trust_changed": True,
                "trust_restoration_verified": False,
            },
        ),
    ),
)
def test_cli_failure_is_generic_and_never_prints_credentials(
    monkeypatch,
    capsys,
    error,
):
    monkeypatch.setattr(
        schema,
        "prepare_schema",
        lambda **_kwargs: (_ for _ in ()).throw(error),
    )

    assert schema.main(["--phase", "preflight"]) == 2
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["status"] == "blocked"
    assert payload["reason"].endswith("database schema preparation failed closed")
    assert payload["runtime_privileges_changed"] is False
    assert payload["automatic_real_order_submission"] is False
    for secret in (
        "do-not-print",
        "hunter2",
        "super-secret",
        "probiga_migrator:",
    ):
        assert secret not in output
