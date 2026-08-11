from __future__ import annotations

import base64
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path
import subprocess
import sys
from threading import Lock
from types import SimpleNamespace

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from sqlalchemy import text

from server.integrations.v2_execution_evidence_audit import (
    V2EvidenceHashAuditReport,
)
from server.integrations.v2_execution_evidence_writer import (
    writer as evidence_writer,
)

from tools import trading_v2_evidence_mysql_acceptance as acceptance
from tools import trading_v2_evidence_mysql_behavioral_acceptance as behavioral_facade


TEST_SERVER_UUID = "123e4567-e89b-12d3-a456-426614174000"
OTHER_SERVER_UUID = "123e4567-e89b-12d3-a456-426614174001"
TEST_DATABASE = "probiga_v2_evidence_test"
ORACLE_COMMUNITY_COMMENT = "MySQL Community Server (GPL)"
TARGET_SCHEMA_GRANTS = (
    "GRANT USAGE ON *.* TO 'acceptor'@'localhost'",
    "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, "
    "REFERENCES, TRIGGER ON `probiga_v2_evidence_test`.* "
    "TO 'acceptor'@'localhost'",
)


def test_dedicated_behavioral_acceptance_facade_uses_unified_fail_closed_runner():
    assert (
        behavioral_facade.run_mysql_behavioral_acceptance
        is acceptance.run_mysql_behavioral_acceptance
    )
    assert (
        behavioral_facade.CanonicalHashAuditAcceptanceOutcome
        is acceptance.CanonicalHashAuditAcceptanceOutcome
    )
    assert (
        behavioral_facade.ExtendedBehavioralProbeOutcome
        is acceptance.ExtendedBehavioralProbeOutcome
    )

# This is deliberately independent of server.db.migrations_v2.MIGRATIONS and
# the acceptance module's checksum calculation.  Updating a migration must not
# silently update the acceptance oracle in the same process.
FROZEN_EXPECTED_MIGRATIONS = (
    (
        "20260725_001_trading_v2_core",
        "c21ed007b17ff18604d6f022945db330cc2d8e4aef270104570e5eb60ccd6a40",
    ),
    (
        "20260725_002_trading_v2_jobs_and_lifecycle",
        "e54264bcb2392b186a1ee3b9b8c478ced6d80bf7d120710ae3a5c1d36663676d",
    ),
    (
        "20260725_003_trading_v2_execution_research_ops",
        "7be2c5567b0c52c97bdcd80fc17a4eb3d28c303b95acff799e5c5c35705b2a6c",
    ),
    (
        "20260725_004_trading_v2_etf_truth_and_forward",
        "451b18146a10140b3d7256018fbf885823112ec62239eae1e00e6a6ea5894435",
    ),
    (
        "20260725_005_trading_v2_theme_risk_chain",
        "2356709db6d2cb906cf4c839873e3b1ab668d9fb74e0be04e4dcd2eb227e3c8b",
    ),
    (
        "20260726_006_real_trading_hard_guard",
        "16f75c5f0e9e329ebb632cc8cd895c96a626ce76b0c364134e9c54f1b31f9016",
    ),
    (
        "20260726_007_market_regime_transition_state",
        "e47ac4757eb6990a4c741cd80d74f638f3429a89cf3103ab4f17497602b8b0f1",
    ),
    (
        "20260727_008_intraday_dynamic_activation",
        "7aa2c2a51f1a69afbbfeb2408520172d8103aa8f8cfa46cb2ae5558ac9e26d63",
    ),
    (
        "20260730_009_public_quote_failover",
        "d53d1315dd695bb570e1b9058156a3f6a77a86d68fe71d939aec523a4827fb61",
    ),
    (
        "20260730_010_qmt_end_to_end_health",
        "d4a17a3f04c8d5fb0a51ea99c7cfea271abd6576a2ec829d8e57743d55f4d2b8",
    ),
    (
        "20260803_011_v2_execution_evidence_bindings",
        "234a2b7a82573b5551b1485dd68598156e26d050d3b2d9b6a6ea76d3c34072d1",
    ),
    (
        "20260803_012_v2_execution_evidence_guards",
        "cf596bc5157ea5f6d835c07089556164cde9c0fcaf0c3ace10f10b15ba4b6fd1",
    ),
    (
        "20260803_013_v2_execution_evidence_natural_keys",
        "51addc459d4caae896ee656e901123646deb6a46584ac274092aa65026917eb8",
    ),
    (
        "20260803_014_v2_execution_authority_attestations",
        "984e2ea7c637c728745b9b21c3b508980cc046c1c434d9851619984918a3823d",
    ),
    (
        "20260803_015_v2_accounting_outcome_evidence",
        "8e06e57c38f7365fa471a7bde09f5cd4a3ea5aef5fee03c6195fd2930b725a2c",
    ),
)


@dataclass(frozen=True)
class _MigrationResult:
    version: str
    status: str


class _Engine:
    def __init__(self) -> None:
        self.disposed = False
        self.connection = SimpleNamespace(
            dialect=SimpleNamespace(name="mysql"),
            rollback=lambda: None,
        )

    def connect(self):
        return _ConnectionContext(self.connection)

    def dispose(self) -> None:
        self.disposed = True


def _migration_run(status: str) -> tuple[_MigrationResult, ...]:
    return tuple(
        _MigrationResult(version=version, status=status)
        for version, _checksum in FROZEN_EXPECTED_MIGRATIONS
    )


def _migration_run_statuses(statuses: tuple[str, ...]) -> tuple[_MigrationResult, ...]:
    assert len(statuses) == len(FROZEN_EXPECTED_MIGRATIONS)
    return tuple(
        _MigrationResult(version=version, status=status)
        for (version, _checksum), status in zip(
            FROZEN_EXPECTED_MIGRATIONS,
            statuses,
            strict=True,
        )
    )


def _snapshot() -> acceptance.EvidenceAcceptanceSnapshot:
    return acceptance.EvidenceAcceptanceSnapshot(
        migration_versions=tuple(
            version for version, _checksum in FROZEN_EXPECTED_MIGRATIONS
        ),
        checksums=tuple(
            checksum for _version, checksum in FROZEN_EXPECTED_MIGRATIONS
        ),
        observed_tables=tuple(sorted(acceptance.EXPECTED_TABLES)),
        observed_triggers=tuple(sorted(acceptance.EXPECTED_TRIGGER_NAMES)),
        evidence_tables=tuple(sorted(acceptance.EVIDENCE_TABLES)),
        evidence_triggers=tuple(sorted(acceptance.EVIDENCE_TRIGGER_NAMES)),
        migration_ledger_rows=15,
        maintenance_fence_rows=1,
        maintenance_fence_state="INACTIVE",
        business_tables_empty=True,
        stored_routines_empty=True,
        scheduled_events_empty=True,
        metadata_preflight_passed=True,
        production_activation_allowed=False,
        actionable_output_allowed=False,
    )


def _accepted_identity(
    database: str,
    version: str = "5.7.38",
) -> tuple[str, str, str, str]:
    return database, version, TEST_SERVER_UUID, ORACLE_COMMUNITY_COMMENT


def test_acceptance_uses_an_independent_frozen_migration_checksum_contract():
    assert acceptance.FROZEN_EXPECTED_MIGRATIONS == FROZEN_EXPECTED_MIGRATIONS
    assert acceptance.EXPECTED_MIGRATIONS == FROZEN_EXPECTED_MIGRATIONS
    acceptance._assert_frozen_migration_contract()
    assert acceptance.EXPECTED_MIGRATION_COUNT == 15
    assert acceptance.EXPECTED_MIGRATION_STATEMENT_COUNT == 150
    assert len(acceptance.EXPECTED_MIGRATION_OWNED_TABLES) == 51
    assert acceptance.EXPECTED_RUNNER_BOOTSTRAP_TABLES == frozenset(
        {"schema_migration_v2_maintenance_fence"}
    )
    assert len(acceptance.EXPECTED_TABLES) == 52


def test_frozen_migration_contract_rejects_source_derived_checksum_drift(monkeypatch):
    drifted = list(FROZEN_EXPECTED_MIGRATIONS)
    drifted[-1] = (drifted[-1][0], "0" * 64)
    monkeypatch.setattr(acceptance, "_DECLARED_MIGRATIONS", tuple(drifted))

    with pytest.raises(RuntimeError, match="independently frozen"):
        acceptance._assert_frozen_migration_contract()


def test_frozen_migration_contract_rejects_runner_bootstrap_table_drift(
    monkeypatch,
):
    monkeypatch.setattr(
        acceptance,
        "_DECLARED_RUNNER_BOOTSTRAP_TABLES",
        frozenset(),
    )

    with pytest.raises(RuntimeError, match="bootstrap maintenance-fence"):
        acceptance._assert_frozen_migration_contract()


@pytest.mark.parametrize(
    "value",
    (
        "",
        "not a url",
        "sqlite:///:memory:",
        "mariadb+pymysql://u:p@localhost/probiga_v2_evidence_test",
        "mysql+pymysql://u:p@localhost/probiga",
        "mysql+pymysql://u:p@localhost/probiga_v2_test",
        "mysql+pymysql://u:p@localhost/v2_evidence_test",
    ),
)
def test_dedicated_url_rejects_missing_non_mysql_and_unsafe_names(value):
    with pytest.raises(ValueError):
        acceptance.require_dedicated_test_url(value)


@pytest.mark.parametrize(
    "database",
    (
        "probiga_v2_evidence_test",
        "probiga_v2_evidence_ci",
        "probiga_v2_evidence_test_20260803",
        "team_ci_v2_evidence_ci_job_42",
    ),
)
def test_dedicated_url_accepts_explicit_v2_evidence_test_ci_names(database):
    value = f"mysql+pymysql://user:secret@localhost/{database}"
    assert acceptance.require_dedicated_test_url(value) == value


@pytest.mark.parametrize(
    "query",
    (
        "database=production",
        "init_command=SET%20sql_mode%3D%27%27",
        "charset=utf8mb4",
        "ssl_disabled=true",
        "database=production&init_command=SET%20sql_mode%3D%27%27",
    ),
)
def test_dedicated_url_rejects_every_query_parameter_before_connect(query):
    value = (
        "mysql+pymysql://user:secret@localhost/"
        f"probiga_v2_evidence_test?{query}"
    )
    with pytest.raises(ValueError, match="query"):
        acceptance.require_dedicated_test_url(value)


def test_resolver_never_falls_back_to_application_url_variables():
    with pytest.raises(ValueError, match="dedicated"):
        acceptance.resolve_test_url(
            environ={
                "MYSQL_URL": "mysql+pymysql://u:p@localhost/probiga",
                "DATABASE_URL": "mysql+pymysql://u:p@localhost/probiga",
            }
        )


@pytest.mark.parametrize("env_name", ("MYSQL_URL", "DATABASE_URL"))
def test_resolver_explicitly_forbids_generic_url_variables(env_name):
    with pytest.raises(ValueError, match="forbidden"):
        acceptance.resolve_test_url(
            env_name,
            environ={
                env_name: (
                    "mysql+pymysql://u:p@localhost/"
                    "probiga_v2_evidence_test"
                )
            },
        )


@pytest.mark.parametrize(
    "env_name",
    (
        "V2_EVIDENCE_MYSQL_URL",
        "V2_TEST_MYSQL_URL",
        "V2_EVIDENCE_PROD_MYSQL_URL",
        "v2_evidence_test_mysql_url",
        "V2_EVIDENCE_TEST_DATABASE_URL",
    ),
)
def test_resolver_rejects_non_test_ci_environment_names(env_name):
    with pytest.raises(ValueError, match="must match"):
        acceptance.resolve_test_url(
            env_name,
            environ={
                env_name: (
                    "mysql+pymysql://u:p@localhost/"
                    "probiga_v2_evidence_test"
                )
            },
        )


@pytest.mark.parametrize(
    "env_name",
    (
        "V2_EVIDENCE_TEST_MYSQL_URL",
        "V2_EVIDENCE_CI_MYSQL_URL",
        "V2_EVIDENCE_TEST_ACCEPTANCE_MYSQL_URL",
        "V2_EVIDENCE_CI_JOB_42_MYSQL_URL",
    ),
)
def test_resolver_accepts_only_v2_evidence_test_ci_environment_names(env_name):
    value = "mysql+pymysql://u:p@localhost/probiga_v2_evidence_test"
    assert acceptance.resolve_test_url(env_name, environ={env_name: value}) == value


@pytest.mark.parametrize(
    "env_name",
    (
        "V2_EVIDENCE_TEST_MYSQL_SERVER_UUID",
        "V2_EVIDENCE_CI_MYSQL_SERVER_UUID",
        "V2_EVIDENCE_TEST_ACCEPTANCE_MYSQL_SERVER_UUID",
    ),
)
def test_server_uuid_resolver_accepts_only_dedicated_test_ci_names(env_name):
    assert acceptance.resolve_server_uuid(
        env_name,
        environ={env_name: TEST_SERVER_UUID},
    ) == TEST_SERVER_UUID


@pytest.mark.parametrize(
    ("env_name", "value"),
    (
        ("MYSQL_SERVER_UUID", TEST_SERVER_UUID),
        ("V2_EVIDENCE_PROD_MYSQL_SERVER_UUID", TEST_SERVER_UUID),
        ("V2_EVIDENCE_TEST_MYSQL_SERVER_UUID", ""),
        ("V2_EVIDENCE_TEST_MYSQL_SERVER_UUID", "not-a-uuid"),
        (
            "V2_EVIDENCE_TEST_MYSQL_SERVER_UUID",
            "00000000-0000-0000-0000-000000000000",
        ),
    ),
)
def test_server_uuid_resolver_fails_closed_for_unsafe_name_or_value(env_name, value):
    with pytest.raises(ValueError):
        acceptance.resolve_server_uuid(env_name, environ={env_name: value})


@pytest.mark.parametrize(
    "runner",
    (
        acceptance.run_mysql_serial_replay_acceptance,
        acceptance.run_mysql_concurrent_initial_acceptance,
        acceptance.run_mysql_behavioral_acceptance,
    ),
)
def test_every_acceptance_runner_requires_expected_server_uuid(runner):
    with pytest.raises(TypeError, match="expected_server_uuid"):
        runner("mysql+pymysql://u:p@localhost/probiga_v2_evidence_test")


def test_serial_replay_is_empty_opt_in_and_checks_full_snapshot(monkeypatch):
    engine = _Engine()
    migration_calls: list[bool] = []
    table_calls = 0

    monkeypatch.setattr(
        acceptance,
        "create_tool_engine",
        lambda *_args, **_kwargs: engine,
    )
    monkeypatch.setattr(
        acceptance,
        "_server_identity_from_connection",
        lambda _engine, database, expected_server_uuid: _accepted_identity(
            database,
            "5.7.38-log",
        )
        if expected_server_uuid == TEST_SERVER_UUID
        else pytest.fail("unexpected server UUID"),
    )

    def fake_inventory(_engine):
        nonlocal table_calls
        table_calls += 1
        return {"tables": frozenset(), "routines": frozenset(), "events": frozenset()}

    monkeypatch.setattr(
        acceptance,
        "_schema_object_inventory_from_connection",
        fake_inventory,
    )

    def fake_migrations(
        _engine,
        *,
        allow_execution_evidence=False,
        connection=None,
    ):
        assert connection is engine.connection
        migration_calls.append(allow_execution_evidence)
        status = "applied" if len(migration_calls) == 1 else "exists"
        return _migration_run(status)

    monkeypatch.setattr(acceptance, "run_v2_migrations", fake_migrations)
    monkeypatch.setattr(
        acceptance,
        "_post_migration_snapshot",
        lambda _engine, *, connection=None: _snapshot(),
    )

    report = acceptance.run_mysql_serial_replay_acceptance(
        "mysql+pymysql://u:p@localhost/probiga_v2_evidence_test",
        expected_server_uuid=TEST_SERVER_UUID,
    )

    assert report.initial_migration == ("applied",) * 15
    assert report.serial_replay == ("exists",) * 15
    assert report.snapshot.metadata_preflight_passed is True
    assert report.snapshot.production_activation_allowed is False
    assert report.snapshot.actionable_output_allowed is False
    assert migration_calls == [True, True]
    assert table_calls == 1
    assert engine.disposed is True


def test_serial_replay_refuses_nonempty_database_before_migration(monkeypatch):
    engine = _Engine()
    migration_called = False
    monkeypatch.setattr(
        acceptance,
        "create_tool_engine",
        lambda *_args, **_kwargs: engine,
    )
    monkeypatch.setattr(
        acceptance,
        "_server_identity_from_connection",
        lambda _engine, database, expected_server_uuid: _accepted_identity(database)
        if expected_server_uuid == TEST_SERVER_UUID
        else pytest.fail("unexpected server UUID"),
    )
    monkeypatch.setattr(
        acceptance,
        "_schema_object_inventory_from_connection",
        lambda _engine: {
            "tables": frozenset({"schema_migration_v2"}),
            "routines": frozenset(),
            "events": frozenset(),
        },
    )

    def fake_migrations(*_args, **_kwargs):
        nonlocal migration_called
        migration_called = True
        return _migration_run("applied")

    monkeypatch.setattr(acceptance, "run_v2_migrations", fake_migrations)

    with pytest.raises(RuntimeError, match="must start empty"):
        acceptance.run_mysql_serial_replay_acceptance(
            "mysql+pymysql://u:p@localhost/probiga_v2_evidence_test",
            expected_server_uuid=TEST_SERVER_UUID,
        )

    assert migration_called is False
    assert engine.disposed is True


def test_concurrent_initial_has_one_applied_writer_per_migration(monkeypatch):
    engine = _Engine()
    call_lock = Lock()
    call_count = 0
    opt_in_values: list[bool] = []
    engine_options: dict[str, object] = {}

    def fake_engine(_url, **kwargs):
        engine_options.update(kwargs)
        return engine

    def fake_migrations(
        _engine,
        *,
        allow_execution_evidence=False,
        connection=None,
    ):
        assert connection is engine.connection
        nonlocal call_count
        with call_lock:
            call_count += 1
            opt_in_values.append(allow_execution_evidence)
            status = "applied" if call_count == 1 else "exists"
        return _migration_run(status)

    monkeypatch.setattr(acceptance, "create_tool_engine", fake_engine)
    monkeypatch.setattr(
        acceptance,
        "_server_identity_from_connection",
        lambda _engine, database, expected_server_uuid: _accepted_identity(
            database,
            "5.7.38-commercial",
        )
        if expected_server_uuid == TEST_SERVER_UUID
        else pytest.fail("unexpected server UUID"),
    )
    monkeypatch.setattr(
        acceptance,
        "_schema_object_inventory_from_connection",
        lambda _engine: {
            "tables": frozenset(),
            "routines": frozenset(),
            "events": frozenset(),
        },
    )
    monkeypatch.setattr(acceptance, "run_v2_migrations", fake_migrations)
    monkeypatch.setattr(
        acceptance,
        "_post_migration_snapshot",
        lambda _engine, *, connection=None: _snapshot(),
    )

    report = acceptance.run_mysql_concurrent_initial_acceptance(
        "mysql+pymysql://u:p@localhost/probiga_v2_evidence_ci_concurrent",
        expected_server_uuid=TEST_SERVER_UUID,
        concurrency=4,
    )

    assert len(report.concurrent_initial_runs) == 4
    assert report.concurrent_initial_runs.count(("applied",) * 15) == 1
    assert report.concurrent_initial_runs.count(("exists",) * 15) == 3
    assert opt_in_values == [True] * 4
    assert engine_options["pool_size"] == 4
    assert engine_options["max_overflow"] == 0
    assert engine.disposed is True


def test_concurrent_initial_rejects_interleaved_per_migration_winners(monkeypatch):
    engine = _Engine()
    call_lock = Lock()
    call_count = 0
    alternating = tuple(
        "applied" if index % 2 == 0 else "exists" for index in range(15)
    )
    inverse = tuple(
        "exists" if status == "applied" else "applied" for status in alternating
    )

    def fake_migrations(
        _engine,
        *,
        allow_execution_evidence=False,
        connection=None,
    ):
        assert connection is engine.connection
        nonlocal call_count
        assert allow_execution_evidence is True
        with call_lock:
            statuses = alternating if call_count == 0 else inverse
            call_count += 1
        return _migration_run_statuses(statuses)

    monkeypatch.setattr(
        acceptance,
        "create_tool_engine",
        lambda *_args, **_kwargs: engine,
    )
    monkeypatch.setattr(
        acceptance,
        "_server_identity_from_connection",
        lambda _engine, database, _expected_server_uuid: _accepted_identity(database),
    )
    monkeypatch.setattr(
        acceptance,
        "_schema_object_inventory_from_connection",
        lambda _engine: {
            "tables": frozenset(),
            "routines": frozenset(),
            "events": frozenset(),
        },
    )
    monkeypatch.setattr(acceptance, "run_v2_migrations", fake_migrations)
    monkeypatch.setattr(
        acceptance,
        "_post_migration_snapshot",
        lambda _engine, *, connection=None: _snapshot(),
    )

    with pytest.raises(RuntimeError, match="(complete applied|whole|single applied)"):
        acceptance.run_mysql_concurrent_initial_acceptance(
            "mysql+pymysql://u:p@localhost/probiga_v2_evidence_ci_concurrent",
            expected_server_uuid=TEST_SERVER_UUID,
            concurrency=2,
        )

    assert engine.disposed is True


def test_concurrent_initial_rejects_multiple_applied_writers(monkeypatch):
    engine = _Engine()
    monkeypatch.setattr(
        acceptance,
        "create_tool_engine",
        lambda *_args, **_kwargs: engine,
    )
    monkeypatch.setattr(
        acceptance,
        "_server_identity_from_connection",
        lambda _engine, database, expected_server_uuid: _accepted_identity(database)
        if expected_server_uuid == TEST_SERVER_UUID
        else pytest.fail("unexpected server UUID"),
    )
    monkeypatch.setattr(
        acceptance,
        "_schema_object_inventory_from_connection",
        lambda _engine: {
            "tables": frozenset(),
            "routines": frozenset(),
            "events": frozenset(),
        },
    )
    monkeypatch.setattr(
        acceptance,
        "run_v2_migrations",
        lambda _engine, **_kwargs: _migration_run("applied"),
    )

    with pytest.raises(RuntimeError, match="exactly one.*applied"):
        acceptance.run_mysql_concurrent_initial_acceptance(
            "mysql+pymysql://u:p@localhost/probiga_v2_evidence_ci_concurrent",
            expected_server_uuid=TEST_SERVER_UUID,
            concurrency=2,
        )

    assert engine.disposed is True


@pytest.mark.parametrize("concurrency", (True, 0, 1, 9, 2.5, "2"))
def test_concurrent_initial_rejects_invalid_concurrency(concurrency):
    with pytest.raises(ValueError):
        acceptance.run_mysql_concurrent_initial_acceptance(
            "mysql+pymysql://u:p@localhost/probiga_v2_evidence_ci_concurrent",
            expected_server_uuid=TEST_SERVER_UUID,
            concurrency=concurrency,
        )


@pytest.mark.parametrize(
    ("populated_kind", "populated_helper", "object_name"),
    (
        ("tables", "_all_table_names", "stale_table"),
        ("routines", "_all_routine_names", "stale_routine"),
        ("events", "_all_event_names", "stale_event"),
    ),
)
def test_empty_preflight_rejects_tables_routines_and_events(
    monkeypatch,
    populated_kind,
    populated_helper,
    object_name,
):
    engine = object()
    calls: list[str] = []

    def inventory_part(kind, names):
        def read(observed_engine):
            assert observed_engine is engine
            calls.append(kind)
            return names

        return read

    for kind, helper in (
        ("tables", "_all_table_names"),
        ("routines", "_all_routine_names"),
        ("events", "_all_event_names"),
    ):
        names = frozenset({object_name}) if helper == populated_helper else frozenset()
        monkeypatch.setattr(acceptance, helper, inventory_part(kind, names))

    inventory = acceptance._schema_object_inventory(engine)
    assert inventory[populated_kind] == frozenset({object_name})
    assert calls == ["tables", "routines", "events"]

    with pytest.raises(RuntimeError, match=f"{populated_kind}.*{object_name}"):
        acceptance._assert_empty(engine, mode="unit")


def test_empty_preflight_requires_all_three_schema_inventories_empty(monkeypatch):
    engine = object()
    calls: list[str] = []

    for kind, helper in (
        ("tables", "_all_table_names"),
        ("routines", "_all_routine_names"),
        ("events", "_all_event_names"),
    ):
        monkeypatch.setattr(
            acceptance,
            helper,
            lambda observed_engine, kind=kind: (
                calls.append(kind),
                frozenset(),
            )[1]
            if observed_engine is engine
            else pytest.fail("unexpected engine"),
        )

    acceptance._assert_empty(engine, mode="unit")
    assert calls == ["tables", "routines", "events"]


class _QueryResult:
    def __init__(self, *, scalar=None, scalars=(), mappings=(), rows=()) -> None:
        self._scalar = scalar
        self._scalars = tuple(scalars)
        self._mappings = tuple(mappings)
        self._rows = tuple(rows) if rows else tuple((item,) for item in self._scalars)

    def scalar(self):
        return self._scalar

    def scalars(self):
        return iter(self._scalars)

    def mappings(self):
        return iter(self._mappings)

    def all(self):
        return self._rows

    def fetchall(self):
        return self._rows

    def __iter__(self):
        return iter(self._rows)


class _ConnectionContext:
    def __init__(self, connection) -> None:
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, *_args):
        return False


class _ServerConnection:
    dialect = SimpleNamespace(name="mysql")

    def __init__(
        self,
        version: str,
        database: str,
        *,
        server_uuid: str = TEST_SERVER_UUID,
        version_comment: str = ORACLE_COMMUNITY_COMMENT,
        grants: tuple[str, ...] = TARGET_SCHEMA_GRANTS,
    ) -> None:
        self.version = version
        self.database = database
        self.server_uuid = server_uuid
        self.version_comment = version_comment
        self.grants = grants

    def execute(self, statement):
        sql = " ".join(str(statement).split())
        if "SHOW GRANTS" in sql.upper():
            return _QueryResult(scalars=self.grants)
        if "@@server_uuid" in sql.lower():
            return _QueryResult(scalar=self.server_uuid)
        if "@@version_comment" in sql.lower():
            return _QueryResult(scalar=self.version_comment)
        if "VERSION()" in sql:
            return _QueryResult(scalar=self.version)
        if "DATABASE()" in sql:
            return _QueryResult(scalar=self.database)
        raise AssertionError(sql)


class _ConnectableEngine:
    def __init__(self, connection, *, backend: str = "mysql") -> None:
        self.connection = connection
        self.dialect = SimpleNamespace(name=backend)

    def connect(self):
        return _ConnectionContext(self.connection)


@pytest.mark.parametrize(
    ("backend", "version", "database", "message"),
    (
        ("mariadb", "5.7.38", "probiga_v2_evidence_test", "backend"),
        ("mysql", "5.7.37", "probiga_v2_evidence_test", "5.7.38"),
        ("mysql", "8.0.39", "probiga_v2_evidence_test", "5.7.38"),
        ("mysql", "5.7.38garbage", "probiga_v2_evidence_test", "5.7.38"),
        ("mysql", "5.7.38.foo", "probiga_v2_evidence_test", "5.7.38"),
        ("mysql", "5.7.38-MariaDB", "probiga_v2_evidence_test", "Oracle"),
        ("mysql", "5.7.38", "another_v2_evidence_test", "does not match"),
    ),
)
def test_runtime_identity_is_fail_closed(backend, version, database, message):
    engine = _ConnectableEngine(
        _ServerConnection(version, database),
        backend=backend,
    )
    with pytest.raises(RuntimeError, match=message):
        acceptance._server_identity(
            engine,
            "probiga_v2_evidence_test",
            TEST_SERVER_UUID,
        )


@pytest.mark.parametrize("version", ("5.7.38-log", "8.4.11"))
def test_runtime_identity_accepts_exact_validated_mysql_patch(version):
    engine = _ConnectableEngine(
        _ServerConnection(version, "probiga_v2_evidence_test")
    )
    identity = acceptance._server_identity(
        engine,
        "probiga_v2_evidence_test",
        TEST_SERVER_UUID,
    )
    assert identity[:2] == ("probiga_v2_evidence_test", version)


def test_runtime_identity_rejects_server_uuid_mismatch():
    engine = _ConnectableEngine(
        _ServerConnection(
            "5.7.38-log",
            TEST_DATABASE,
            server_uuid=OTHER_SERVER_UUID,
        )
    )
    with pytest.raises(RuntimeError, match="UUID"):
        acceptance._server_identity(engine, TEST_DATABASE, TEST_SERVER_UUID)


@pytest.mark.parametrize(
    "version_comment",
    (
        "Percona Server (GPL), Release 84.0, Revision 1",
        "MariaDB Server",
        "Unknown MySQL-compatible distribution",
        "",
    ),
)
def test_runtime_identity_rejects_non_oracle_version_comment(version_comment):
    engine = _ConnectableEngine(
        _ServerConnection(
            "5.7.38-log",
            TEST_DATABASE,
            version_comment=version_comment,
        )
    )
    with pytest.raises(RuntimeError, match="(Oracle|version comment|distribution)"):
        acceptance._server_identity(engine, TEST_DATABASE, TEST_SERVER_UUID)


@pytest.mark.parametrize(
    "version_comment",
    (
        "MySQL Community Server (GPL)",
        "MySQL Enterprise Server - Commercial",
    ),
)
def test_runtime_identity_accepts_only_oracle_mysql_comments(version_comment):
    engine = _ConnectableEngine(
        _ServerConnection(
            "5.7.38",
            TEST_DATABASE,
            version_comment=version_comment,
        )
    )
    identity = acceptance._server_identity(
        engine,
        TEST_DATABASE,
        TEST_SERVER_UUID,
    )
    assert identity[:2] == (TEST_DATABASE, "5.7.38")


@pytest.mark.parametrize(
    "grants",
    (
        ("GRANT ALL PRIVILEGES ON *.* TO 'acceptor'@'localhost'",),
        (
            "GRANT ALL PRIVILEGES ON `probiga_v2_evidence_test`.* "
            "TO 'acceptor'@'localhost'",
        ),
        (
            "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, "
            "REFERENCES, TRIGGER, EVENT ON `probiga_v2_evidence_test`.* "
            "TO 'acceptor'@'localhost'",
        ),
        (
            "GRANT SELECT, INSERT ON `probiga_v2_evidence_test`.* "
            "TO 'acceptor'@'localhost'",
        ),
        (
            "GRANT USAGE ON *.* TO 'acceptor'@'localhost'",
            "GRANT SELECT ON `production`.* TO 'acceptor'@'localhost'",
        ),
        ("GRANT USAGE ON *.* TO 'acceptor'@'localhost'",),
        (),
    ),
)
def test_runtime_identity_rejects_global_other_schema_or_missing_target_grants(grants):
    engine = _ConnectableEngine(
        _ServerConnection(
            "5.7.38",
            TEST_DATABASE,
            grants=grants,
        )
    )
    with pytest.raises(RuntimeError, match="grant"):
        acceptance._server_identity(engine, TEST_DATABASE, TEST_SERVER_UUID)


def test_runtime_identity_accepts_usage_plus_exact_target_schema_grants():
    engine = _ConnectableEngine(
        _ServerConnection("5.7.38", TEST_DATABASE)
    )
    identity = acceptance._server_identity(
        engine,
        TEST_DATABASE,
        TEST_SERVER_UUID,
    )
    assert identity[:2] == (TEST_DATABASE, "5.7.38")


class _CheckoutTransaction:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class _CheckoutConnection:
    dialect = SimpleNamespace(name="mysql")

    def __init__(self) -> None:
        self.rollback_count = 0
        self.begin_count = 0

    def rollback(self) -> None:
        self.rollback_count += 1

    def begin(self):
        self.begin_count += 1
        return _CheckoutTransaction()


class _CheckoutEngine:
    def __init__(self) -> None:
        self.connections: list[_CheckoutConnection] = []

    def connect(self):
        connection = _CheckoutConnection()
        self.connections.append(connection)
        return _ConnectionContext(connection)


def test_behavioral_proxy_verifies_every_physical_checkout(monkeypatch):
    engine = _CheckoutEngine()
    verified: list[_CheckoutConnection] = []

    def fake_identity(connection, database, server_uuid):
        assert database == TEST_DATABASE
        assert server_uuid == TEST_SERVER_UUID
        verified.append(connection)
        return _accepted_identity(database)

    monkeypatch.setattr(
        acceptance,
        "_server_identity_from_connection",
        fake_identity,
    )
    proxy = acceptance._IdentityBoundEngine(
        engine,
        expected_database=TEST_DATABASE,
        expected_server_uuid=TEST_SERVER_UUID,
    )

    with proxy.connect() as first:
        assert first is engine.connections[0]
    with proxy.begin() as second:
        assert second is engine.connections[1]

    assert verified == engine.connections
    assert [item.rollback_count for item in engine.connections] == [1, 1]
    assert [item.begin_count for item in engine.connections] == [0, 1]


class _SnapshotConnection:
    dialect = SimpleNamespace(name="mysql")

    def __init__(
        self,
        *,
        checksum_override: str | None = None,
        triggers: frozenset[str] | None = None,
        routines: tuple[str, ...] = (),
        events: tuple[str, ...] = (),
        fence_rows: tuple[dict[str, object], ...] | None = None,
    ) -> None:
        self.checksum_override = checksum_override
        self.triggers = triggers or acceptance.EXPECTED_TRIGGER_NAMES
        self.routines = routines
        self.events = events
        self.fence_rows = fence_rows

    def execute(self, statement):
        sql = " ".join(str(statement).split())
        if sql.startswith("SELECT version, checksum"):
            rows = [
                {"version": version, "checksum": checksum}
                for version, checksum in acceptance.EXPECTED_MIGRATIONS
            ]
            if self.checksum_override is not None:
                rows[-1]["checksum"] = self.checksum_override
            return _QueryResult(mappings=rows)
        if "SELECT TABLE_NAME" in sql:
            return _QueryResult(scalars=acceptance.EXPECTED_TABLES)
        if "SELECT TRIGGER_NAME" in sql:
            return _QueryResult(scalars=self.triggers)
        if "SELECT ROUTINE_NAME" in sql:
            return _QueryResult(scalars=self.routines)
        if "SELECT EVENT_NAME" in sql:
            return _QueryResult(scalars=self.events)
        if sql.startswith("SELECT fence_name, state, target_version"):
            rows = self.fence_rows
            if rows is None:
                rows = (
                    {
                        "fence_name": "execution_evidence_011_015",
                        "state": "INACTIVE",
                        "target_version": acceptance.FROZEN_EXPECTED_MIGRATIONS[-1][0],
                        "generation": 1,
                        "activated_at": "2026-08-04 00:00:00.000000",
                        "updated_at": "2026-08-04 00:00:01.000000",
                    },
                )
            return _QueryResult(mappings=rows)
        raise AssertionError(sql)


def _passing_schema_report():
    return SimpleNamespace(
        metadata_preflight_passed=True,
        production_activation_allowed=False,
        structural_blockers=(),
        guards_checked=True,
        migration_ledger_checked=True,
        activation_checks_included=True,
        activation_blockers=("ISOLATED_MYSQL_BEHAVIORAL_ACCEPTANCE_MISSING",),
        maintenance_fence_checked=True,
        maintenance_fence_active=False,
        actionable_output_allowed=False,
    )


def _empty_post_migration_row_counts() -> dict[str, int]:
    return {
        table: acceptance.EXPECTED_MIGRATION_COUNT
        if table == "schema_migration_v2"
        else 1
        if table == "schema_migration_v2_maintenance_fence"
        else 0
        for table in acceptance.EXPECTED_TABLES
    }


def test_snapshot_checks_ledger_tables_triggers_and_full_schema_gate(monkeypatch):
    engine = _ConnectableEngine(_SnapshotConnection())
    inspection_calls: list[dict[str, object]] = []

    def fake_inspection(_connection, **kwargs):
        inspection_calls.append(kwargs)
        return _passing_schema_report()

    monkeypatch.setattr(
        acceptance,
        "inspect_v2_execution_evidence_schema",
        fake_inspection,
    )
    monkeypatch.setattr(
        acceptance,
        "_post_migration_row_counts_from_connection",
        lambda _connection: _empty_post_migration_row_counts(),
    )

    snapshot = acceptance._post_migration_snapshot(engine)

    assert len(snapshot.migration_versions) == 15
    assert len(snapshot.checksums) == 15
    assert set(snapshot.observed_tables) == acceptance.EXPECTED_TABLES
    assert set(snapshot.observed_triggers) == acceptance.EXPECTED_TRIGGER_NAMES
    assert len(snapshot.evidence_tables) == 5
    assert len(snapshot.evidence_triggers) == 17
    assert snapshot.migration_ledger_rows == 15
    assert snapshot.maintenance_fence_rows == 1
    assert snapshot.maintenance_fence_state == "INACTIVE"
    assert snapshot.business_tables_empty is True
    assert snapshot.stored_routines_empty is True
    assert snapshot.scheduled_events_empty is True
    assert snapshot.metadata_preflight_passed is True
    assert snapshot.production_activation_allowed is False
    assert snapshot.actionable_output_allowed is False
    assert inspection_calls == [
        {
            "require_guards": True,
            "require_migration_ledger": True,
            "include_activation_blockers": True,
        }
    ]


def test_snapshot_rejects_any_ledger_checksum_drift(monkeypatch):
    engine = _ConnectableEngine(_SnapshotConnection(checksum_override="0" * 64))
    monkeypatch.setattr(
        acceptance,
        "inspect_v2_execution_evidence_schema",
        lambda *_args, **_kwargs: _passing_schema_report(),
    )
    monkeypatch.setattr(
        acceptance,
        "_post_migration_row_counts_from_connection",
        lambda _connection: _empty_post_migration_row_counts(),
    )

    with pytest.raises(RuntimeError, match="checksums drifted"):
        acceptance._post_migration_snapshot(engine)


@pytest.mark.parametrize(
    "fence_rows",
    (
        (),
        (
            {
                "fence_name": "execution_evidence_011_015",
                "state": "ACTIVE",
            },
        ),
        (
            {
                "fence_name": "execution_evidence_011_015",
                "state": "INACTIVE",
            },
            {
                "fence_name": "unexpected_duplicate",
                "state": "INACTIVE",
            },
        ),
    ),
)
def test_snapshot_rejects_missing_or_active_maintenance_fence(
    monkeypatch,
    fence_rows,
):
    engine = _ConnectableEngine(
        _SnapshotConnection(fence_rows=tuple(fence_rows))
    )
    monkeypatch.setattr(
        acceptance,
        "inspect_v2_execution_evidence_schema",
        lambda *_args, **_kwargs: _passing_schema_report(),
    )
    monkeypatch.setattr(
        acceptance,
        "_post_migration_row_counts_from_connection",
        lambda _connection: _empty_post_migration_row_counts(),
    )

    with pytest.raises(RuntimeError, match="maintenance fence"):
        acceptance._post_migration_snapshot(engine)


def test_snapshot_rejects_full_trigger_inventory_drift(monkeypatch):
    engine = _ConnectableEngine(
        _SnapshotConnection(
            triggers=acceptance.EXPECTED_TRIGGER_NAMES
            - {"trg_trade_account_v2_real_disabled_bi"}
        )
    )
    monkeypatch.setattr(
        acceptance,
        "inspect_v2_execution_evidence_schema",
        lambda *_args, **_kwargs: _passing_schema_report(),
    )
    monkeypatch.setattr(
        acceptance,
        "_post_migration_row_counts_from_connection",
        lambda _connection: _empty_post_migration_row_counts(),
    )

    with pytest.raises(RuntimeError, match="trigger inventory drifted"):
        acceptance._post_migration_snapshot(engine)


@pytest.mark.parametrize(
    ("routines", "events"),
    (
        (("unexpected_proc",), ()),
        ((), ("unexpected_event",)),
    ),
)
def test_snapshot_rejects_post_migration_routines_or_events(
    monkeypatch,
    routines,
    events,
):
    engine = _ConnectableEngine(
        _SnapshotConnection(routines=routines, events=events)
    )
    monkeypatch.setattr(
        acceptance,
        "inspect_v2_execution_evidence_schema",
        lambda *_args, **_kwargs: _passing_schema_report(),
    )
    monkeypatch.setattr(
        acceptance,
        "_post_migration_row_counts_from_connection",
        lambda _connection: _empty_post_migration_row_counts(),
    )

    with pytest.raises(RuntimeError, match="stored routines or scheduled events"):
        acceptance._post_migration_snapshot(engine)


def test_cli_exposes_no_cleanup_or_nonempty_bypass():
    with pytest.raises(SystemExit):
        acceptance._parser().parse_args(["--require-clean"])
    source = Path(acceptance.__file__).read_text(encoding="utf-8").upper()
    assert "DROP " not in source
    assert "TRUNCATE " not in source


def test_behavioral_mode_is_explicit_five_table_and_never_activates(monkeypatch):
    engine = _Engine()
    call_order: list[str] = []
    scenario = object()
    outcome = acceptance.BehavioralProbeOutcome(
        legal_inserted=acceptance.CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES,
        idempotent_replay=acceptance.CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES,
        identical_double_writer=("double-writer",) * 5,
        conflicting_double_writer=("conflicting-writer",) * 5,
        append_only_update_guards=("update",) * 5,
        append_only_delete_guards=("delete",) * 5,
        invalid_insert_guards=("invalid-insert",) * 5,
        replace_guards=("replace",) * 5,
        on_duplicate_key_update_guards=("on-duplicate",) * 5,
        rollback_verified=True,
        rollback_verified_evidence_types=(
            acceptance.CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES
        ),
    )

    authority_audit = acceptance.V2AuthorityStoredRowAuditReport(
        table_counts=tuple(
            (table, 1) for table in acceptance.AUTHORITY_AUDIT_TABLES
        ),
        rows_reconstructed=5,
        hashes_verified=7,
        signatures_verified=1,
        database_sha2_used=True,
        shared_row_locks_used=True,
    )
    accounting_audit = acceptance.V2AccountingEvidenceAuditReport(
        table_counts=(
            (acceptance.OUTCOME_TABLE, 1),
            (acceptance.LOT_EFFECT_TABLE, 2),
            (acceptance.FINALIZATION_TABLE, 1),
        ),
        hash_verifications=(
            (acceptance.OUTCOME_TABLE, 4),
            (acceptance.LOT_EFFECT_TABLE, 10),
            (acceptance.FINALIZATION_TABLE, 2),
        ),
        hashes_verified=16,
        rows_reconstructed=4,
        finalized_outcomes=1,
        finalized_outcome_ids=("a" * 64,),
        lot_chains_checked=2,
        lot_chain_ids=("lot-a", "lot-b"),
        parent_rows_checked=8,
        parent_row_checks=(
            ("account", "account"),
            ("cash", "cash"),
            ("fill", "fill"),
            ("fill", "old-a"),
            ("fill", "old-b"),
            ("lot", "lot-a"),
            ("lot", "lot-b"),
            ("order", "order"),
        ),
        database_sha2_used=True,
        shared_row_locks_used=True,
    )
    extended = acceptance.ExtendedBehavioralProbeOutcome(
        authority=acceptance.AuthorityBehavioralProbeOutcome(
            key_registration=("key-1",),
            receipt_registration=("receipt-1",),
            concurrent_registration=("key-race", "receipt-race"),
            nonce_replay_rejected=True,
            signature_rejected=True,
            revocations=("KEY", "RECEIPT"),
            historical_recheck=("KEY:REJECTED", "RECEIPT:REJECTED"),
        ),
        accounting=acceptance.AccountingBehavioralProbeOutcome(
            ordered_insert_tags=(
                "insert_outcome",
                "insert_lot_effect",
                "insert_lot_effect",
                "insert_finalization",
            ),
            interruption_rolled_back=True,
            whole_batch_rolled_back=True,
            exact_replay_status="IDEMPOTENT",
            different_content_conflict=True,
            fifo_lot_ids=("lot-a", "lot-b"),
        ),
        authority_audit_report=authority_audit,
        accounting_audit_report=accounting_audit,
    )

    def fake_create_tool_engine(*_args, **kwargs):
        assert kwargs["pool_size"] == 2
        assert kwargs["max_overflow"] == 0
        return engine

    monkeypatch.setattr(
        acceptance,
        "create_tool_engine",
        fake_create_tool_engine,
    )
    monkeypatch.setattr(
        acceptance,
        "_server_identity_from_connection",
        lambda _engine, database, expected_server_uuid: _accepted_identity(
            database,
            "5.7.38-log",
        )
        if expected_server_uuid == TEST_SERVER_UUID
        else pytest.fail("unexpected server UUID"),
    )
    monkeypatch.setattr(
        acceptance,
        "_schema_object_inventory_from_connection",
        lambda _engine: {
            "tables": frozenset(),
            "routines": frozenset(),
            "events": frozenset(),
        },
    )

    def fake_migrations(
        _engine,
        *,
        allow_execution_evidence=False,
        connection=None,
    ):
        assert connection is engine.connection
        assert allow_execution_evidence is True
        call_order.append("migrations")
        return _migration_run("applied")

    def fake_snapshot(_engine, *, connection=None):
        assert connection is engine.connection
        call_order.append("snapshot")
        return _snapshot()

    def fake_probes(_engine, observed_scenario):
        assert observed_scenario is scenario
        call_order.append("probes")
        return outcome

    monkeypatch.setattr(acceptance, "run_v2_migrations", fake_migrations)
    monkeypatch.setattr(acceptance, "_post_migration_snapshot", fake_snapshot)
    monkeypatch.setattr(acceptance, "build_behavioral_scenario", lambda: scenario)
    monkeypatch.setattr(acceptance, "_run_behavioral_probes", fake_probes)
    monkeypatch.setattr(
        acceptance,
        "_run_extended_behavioral_probes",
        lambda observed_engine: (
            call_order.append("extended-probes"),
            extended,
        )[1]
        if observed_engine._engine is engine
        else pytest.fail("unexpected extended probe engine"),
    )

    audit_report = V2EvidenceHashAuditReport(
        table_counts=tuple(
            (table, 1)
            for table in sorted(acceptance.EVIDENCE_TABLES)
        ),
        payload_hashes_verified=13,
        rows_reconstructed=5,
        cash_chains_checked=1,
        complete_cash_chains=0,
        order_chains_checked=1,
        complete_order_chains=0,
        external_authority_claims=2,
        database_sha2_used=True,
        shared_row_locks_used=True,
    )

    def fake_hash_audit(observed_engine):
        assert observed_engine._engine is engine
        call_order.append("hash-audit")
        return acceptance.CanonicalHashAuditAcceptanceOutcome(
            report=audit_report,
            schema_blocker_removed=True,
        )

    report = acceptance.run_mysql_behavioral_acceptance(
        "mysql+pymysql://u:p@localhost/probiga_v2_evidence_test_behavioral",
        expected_server_uuid=TEST_SERVER_UUID,
        canonical_hash_audit_runner=fake_hash_audit,
    )

    assert call_order == [
        "migrations",
        "snapshot",
        "probes",
        "extended-probes",
        "hash-audit",
    ]
    assert report.behavioral_coverage == (
        "MARKET_CALENDAR",
        "QUOTE_RECEIPT",
        "FILL_EXECUTION",
        "CASH_EVENT",
        "ORDER_TRANSITION",
        "EXTERNAL_AUTHORITY_REGISTRY",
        "ACCOUNTING_OUTCOME_FINALIZATION",
    )
    assert report.behavioral_not_covered == ()
    assert report.behavioral_probes_covered == acceptance.BEHAVIORAL_PROBES_COVERED
    assert report.behavioral_probes_not_covered == ()
    assert report.all_five_evidence_types_covered is True
    assert report.all_declared_evidence_types_covered is True
    assert report.rollback_verified is True
    assert report.identical_double_writer == ("double-writer",) * 5
    assert report.conflicting_double_writer == ("conflicting-writer",) * 5
    assert report.invalid_insert_guards == ("invalid-insert",) * 5
    assert report.replace_guards == ("replace",) * 5
    assert report.on_duplicate_key_update_guards == ("on-duplicate",) * 5
    assert (
        report.rollback_verified_evidence_types
        == acceptance.CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES
    )
    assert report.authority_nonce_replay_rejected is True
    assert report.authority_signature_rejected is True
    assert report.authority_audit_passed is True
    assert report.accounting_exact_replay_status == "IDEMPOTENT"
    assert report.accounting_different_content_conflict is True
    assert report.accounting_audit_passed is True
    assert report.three_layer_nonempty_audit_passed is True
    assert report.production_activation_allowed is False
    assert report.actionable_output_allowed is False
    assert report.canonical_hash_audit_passed is True
    assert report.canonical_hash_schema_blocker_removed is True
    assert report.canonical_hash_audit_report is audit_report
    assert report.canonical_hash_audit_report.production_activation_allowed is False
    assert report.pre_behavior_snapshot.production_activation_allowed is False
    assert report.pre_behavior_snapshot.actionable_output_allowed is False
    assert engine.disposed is True


def test_real_hash_audit_runner_passes_true_flag_to_schema_gate(monkeypatch):
    connection = object()
    engine = SimpleNamespace(
        begin=lambda: _ConnectionContext(connection),
    )
    audit_report = V2EvidenceHashAuditReport(
        table_counts=tuple(
            (table, 1) for table in sorted(acceptance.EVIDENCE_TABLES)
        ),
        payload_hashes_verified=13,
        rows_reconstructed=5,
        cash_chains_checked=1,
        complete_cash_chains=0,
        order_chains_checked=1,
        complete_order_chains=0,
        external_authority_claims=0,
        database_sha2_used=True,
        shared_row_locks_used=True,
    )
    observed_kwargs = {}
    monkeypatch.setattr(
        acceptance,
        "audit_v2_execution_evidence_database",
        lambda value: audit_report
        if value is connection
        else pytest.fail("unexpected audit connection"),
    )

    def fake_gate(value, **kwargs):
        assert value is connection
        observed_kwargs.update(kwargs)
        return SimpleNamespace(
            activation_blockers=(
                "ISOLATED_MYSQL_BEHAVIORAL_ACCEPTANCE_MISSING",
                "LEAST_PRIVILEGE_ATTESTATION_MISSING",
                "EVIDENCE_WRITER_NOT_PRODUCTION_WIRED",
            ),
            production_activation_allowed=False,
            actionable_output_allowed=False,
        )

    monkeypatch.setattr(
        acceptance,
        "inspect_v2_execution_evidence_schema",
        fake_gate,
    )

    outcome = acceptance._run_database_canonical_hash_audit(engine)

    assert outcome.report is audit_report
    assert outcome.schema_blocker_removed is True
    assert outcome.production_activation_allowed is False
    assert outcome.actionable_output_allowed is False
    assert observed_kwargs["canonical_hash_audit_passed"] is True


def test_real_hash_audit_runner_rejects_nonpassing_report(monkeypatch):
    engine = SimpleNamespace(begin=lambda: _ConnectionContext(object()))
    failed = V2EvidenceHashAuditReport(
        table_counts=tuple(
            (table, 1) for table in sorted(acceptance.EVIDENCE_TABLES)
        ),
        payload_hashes_verified=13,
        rows_reconstructed=4,
        cash_chains_checked=1,
        complete_cash_chains=0,
        order_chains_checked=1,
        complete_order_chains=0,
        external_authority_claims=0,
        database_sha2_used=True,
        shared_row_locks_used=True,
    )
    monkeypatch.setattr(
        acceptance,
        "audit_v2_execution_evidence_database",
        lambda _connection: failed,
    )
    with pytest.raises(RuntimeError, match="did not prove"):
        acceptance._run_database_canonical_hash_audit(engine)


def test_behavioral_hash_audit_injection_must_be_callable():
    with pytest.raises(TypeError, match="canonical_hash_audit_runner"):
        acceptance.run_mysql_behavioral_acceptance(
            "mysql+pymysql://u:p@localhost/probiga_v2_evidence_test_behavioral",
            expected_server_uuid=TEST_SERVER_UUID,
            canonical_hash_audit_runner=None,
        )


class _FakeTransaction:
    def __init__(self, rollbacks: list[str]) -> None:
        self.rollbacks = rollbacks

    def rollback(self) -> None:
        self.rollbacks.append("rollback")


class _GuardFailure(Exception):
    def __init__(self, message: str) -> None:
        super().__init__("wrapped guard failure")
        self.orig = Exception(1644, message)


class _GuardConnection:
    def __init__(self, cases, statements, rollbacks) -> None:
        self.cases = cases
        self.statements = statements
        self.rollbacks = rollbacks

    def begin(self):
        return _FakeTransaction(self.rollbacks)

    def execute(self, statement, params):
        sql = " ".join(str(statement).split())
        self.statements.append((sql, dict(params)))
        for case in self.cases:
            if case.table not in sql:
                continue
            if sql.startswith("UPDATE"):
                raise _GuardFailure(case.update_guard_message)
            if sql.startswith("DELETE"):
                raise _GuardFailure(case.delete_guard_message)
            if sql.startswith("SELECT COUNT(*)"):
                return _QueryResult(scalar=1)
        raise AssertionError(sql)


class _GuardEngine:
    def __init__(self, cases) -> None:
        self.statements: list[tuple[str, dict[str, object]]] = []
        self.rollbacks: list[str] = []
        self.connection = _GuardConnection(
            cases,
            self.statements,
            self.rollbacks,
        )

    def connect(self):
        return _ConnectionContext(self.connection)


def test_behavioral_append_only_probes_require_1644_and_retain_rows():
    scenario = acceptance.build_behavioral_scenario()
    engine = _GuardEngine(scenario.cases)

    updates, deletes = acceptance._run_append_only_guard_probes(engine, scenario)

    assert len(updates) == len(deletes) == 5
    assert all("1644/45000:ROW_RETAINED" in item for item in updates + deletes)
    assert len(engine.rollbacks) == 10
    assert sum(sql.startswith("SELECT COUNT(*)") for sql, _ in engine.statements) == 10


def test_behavioral_negative_probe_matrix_covers_all_five_types(monkeypatch):
    scenario = acceptance.build_behavioral_scenario()
    engine = object()

    def fake_negative_probes(observed_engine, cases):
        assert observed_engine is engine
        assert tuple(
            (case.evidence_type, case.primary_value) for case in cases
        ) == tuple(
            (case.evidence_type, case.primary_value)
            for case in scenario.cases
        )
        return tuple(
            SimpleNamespace(
                evidence_type=case.evidence_type,
                table=case.table,
                operation=operation,
                mysql_errno=1644,
                baseline_retained=True,
                row_count_before=1,
                row_count_after=1,
            )
            for case in scenario.cases
            for operation in acceptance.ALL_NEGATIVE_PROBE_OPERATIONS
        )

    monkeypatch.setattr(
        acceptance,
        "run_negative_probes",
        fake_negative_probes,
    )

    invalid, replace, on_duplicate = acceptance._run_negative_guard_probes(
        engine,
        scenario,
    )

    assert len(invalid) == len(replace) == len(on_duplicate) == 5
    assert all("INVALID_INSERT:1644/45000:ROW_RETAINED" in item for item in invalid)
    assert all("REPLACE:1644/45000:ROW_RETAINED" in item for item in replace)
    assert all(
        "ON_DUPLICATE_KEY_UPDATE:1644/45000:ROW_RETAINED" in item
        for item in on_duplicate
    )


def test_behavioral_negative_probe_matrix_rejects_retention_drift(monkeypatch):
    scenario = acceptance.build_behavioral_scenario()

    def fake_negative_probes(_engine, _cases):
        return tuple(
            SimpleNamespace(
                evidence_type=case.evidence_type,
                table=case.table,
                operation=operation,
                mysql_errno=1644,
                baseline_retained=True,
                row_count_before=1,
                row_count_after=(
                    2
                    if case is scenario.cases[0]
                    and operation
                    is acceptance.NegativeProbeOperation.INVALID_INSERT
                    else 1
                ),
            )
            for case in scenario.cases
            for operation in acceptance.ALL_NEGATIVE_PROBE_OPERATIONS
        )

    monkeypatch.setattr(
        acceptance,
        "run_negative_probes",
        fake_negative_probes,
    )

    with pytest.raises(RuntimeError, match="exact row retention"):
        acceptance._run_negative_guard_probes(object(), scenario)


def test_behavioral_guard_error_rejects_wrong_mysql_errno():
    failure = _GuardFailure("calendar evidence is append only")
    failure.orig = Exception(1062, "calendar evidence is append only")
    with pytest.raises(RuntimeError, match="errno 1644"):
        acceptance._require_1644_guard(
            failure,
            "calendar evidence is append only",
        )


class _BeginOnlyEngine:
    def __init__(self) -> None:
        self.begin_count = 0

    def begin(self):
        self.begin_count += 1
        return _ConnectionContext(SimpleNamespace())


def test_behavioral_dispatch_uses_all_five_public_writers(monkeypatch):
    scenario = acceptance.build_behavioral_scenario()
    connection = object()
    called: list[str] = []

    def fake_writer(evidence_type):
        def append(observed_connection, evidence):
            assert observed_connection is connection
            called.append(evidence_type)
            return evidence

        return append

    for evidence_type, name in (
        ("MARKET_CALENDAR", "append_market_calendar_evidence"),
        ("QUOTE_RECEIPT", "append_quote_receipt_evidence"),
        ("FILL_EXECUTION", "append_fill_execution_evidence"),
        ("CASH_EVENT", "append_cash_event_binding"),
        ("ORDER_TRANSITION", "append_order_transition_evidence"),
    ):
        monkeypatch.setattr(acceptance, name, fake_writer(evidence_type))

    returned = tuple(
        acceptance._append_behavioral_case(connection, case)
        for case in scenario.cases
    )

    assert called == list(acceptance.CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES)
    assert returned == tuple(case.evidence for case in scenario.cases)


def test_behavioral_idempotent_replay_uses_separate_transactions(monkeypatch):
    scenario = acceptance.build_behavioral_scenario()
    engine = _BeginOnlyEngine()

    def fake_append(_connection, case):
        return SimpleNamespace(
            status=acceptance.EvidenceAppendStatus.IDEMPOTENT,
            evidence_type=case.evidence_type,
        )

    monkeypatch.setattr(acceptance, "_append_behavioral_case", fake_append)

    replayed = acceptance._run_idempotent_replay_probes(
        engine,
        scenario,
    )

    assert replayed == acceptance.CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES
    assert engine.begin_count == 5


class _DoubleWriterTransaction:
    def __init__(self, engine) -> None:
        self.engine = engine
        self.finished = False

    def commit(self):
        assert self.finished is False
        self.finished = True
        self.engine.commits += 1

    def rollback(self):
        assert self.finished is False
        self.finished = True
        self.engine.transaction_rollbacks += 1


class _DoubleWriterConnection:
    def __init__(self, engine) -> None:
        self.engine = engine

    def execute(self, statement, params=None):
        sql = " ".join(str(statement).split())
        if sql.startswith("SET SESSION innodb_lock_wait_timeout"):
            self.engine.session_configurations += 1
            return _QueryResult()
        if sql.startswith("SELECT COUNT(*)"):
            assert params
            if "primary_value" not in params:
                assert any(
                    key.startswith("natural_value_") for key in params
                )
                return _QueryResult(
                    scalar=getattr(self.engine, "natural_count", 1)
                )
            retained_counts = getattr(
                self.engine,
                "retained_counts",
                {},
            )
            return _QueryResult(
                scalar=retained_counts.get(params["primary_value"], 1)
            )
        raise AssertionError(sql)

    def rollback(self):
        self.engine.checkout_rollbacks += 1

    def begin(self):
        return _DoubleWriterTransaction(self.engine)


class _DoubleWriterBeginContext:
    def __init__(self, engine) -> None:
        self.engine = engine
        self.connection = _DoubleWriterConnection(engine)
        self.transaction = _DoubleWriterTransaction(engine)

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, _exc, _tb):
        if exc_type is None:
            self.transaction.commit()
        else:
            self.transaction.rollback()
        return False


class _DoubleWriterEngine:
    def __init__(self) -> None:
        self.commits = 0
        self.transaction_rollbacks = 0
        self.checkout_rollbacks = 0
        self.session_configurations = 0

    def connect(self):
        return _ConnectionContext(_DoubleWriterConnection(self))

    def begin(self):
        return _DoubleWriterBeginContext(self)


def test_identical_double_writer_covers_all_five_and_retains_one(monkeypatch):
    scenario = acceptance.build_behavioral_scenario()
    engine = _DoubleWriterEngine()
    lock = Lock()
    calls = {case.evidence_type: 0 for case in scenario.cases}

    def fake_append(_connection, case):
        with lock:
            calls[case.evidence_type] += 1
            ordinal = calls[case.evidence_type]
        return SimpleNamespace(
            status=(
                acceptance.EvidenceAppendStatus.INSERTED
                if ordinal == 1
                else acceptance.EvidenceAppendStatus.IDEMPOTENT
            )
        )

    monkeypatch.setattr(acceptance, "_append_behavioral_case", fake_append)

    inserted, outcomes = acceptance._run_identical_double_writer_probes(
        engine,
        scenario,
    )

    assert inserted == acceptance.CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES
    assert len(outcomes) == 5
    assert all(
        "INSERTED+IDEMPOTENT:DIRECT:ONE_ROW" in item
        for item in outcomes
    )
    assert set(calls.values()) == {2}
    assert engine.commits == 10
    assert engine.transaction_rollbacks == 0
    assert engine.session_configurations == 10


def test_conflicting_double_writer_covers_five_and_proves_loser_absent(
    monkeypatch,
):
    base = acceptance.build_behavioral_scenario()
    scenario = acceptance.build_conflicting_double_writer_scenario(base)
    engine = _DoubleWriterEngine()
    engine.natural_count = 1
    engine.retained_counts = {
        pair.left.primary_value: 1
        for pair in scenario.pairs
    } | {
        pair.right.primary_value: 0
        for pair in scenario.pairs
    }
    initial_errnos = {
        "MARKET_CALENDAR": 1062,
        "QUOTE_RECEIPT": 1205,
        "FILL_EXECUTION": 1213,
    }
    lock = Lock()
    calls: dict[tuple[str, str], int] = {}
    left_ids = {
        pair.evidence_type: pair.left.primary_value
        for pair in scenario.pairs
    }

    def fake_append(_connection, case):
        key = (case.evidence_type, case.primary_value)
        with lock:
            calls[key] = calls.get(key, 0) + 1
            ordinal = calls[key]
        if case.primary_value == left_ids[case.evidence_type]:
            assert ordinal == 1
            return SimpleNamespace(
                status=acceptance.EvidenceAppendStatus.INSERTED
            )
        if ordinal == 1 and case.evidence_type in initial_errnos:
            errno = initial_errnos[case.evidence_type]
            raise Exception(errno, f"transient {errno}")
        raise acceptance.EvidenceAppendConflictError(
            "natural business key already carries different content"
        )

    monkeypatch.setattr(acceptance, "_append_behavioral_case", fake_append)

    outcomes = acceptance._run_conflicting_double_writer_probes(
        engine,
        scenario,
    )

    assert len(outcomes) == 5
    assert all(
        "CONFLICTING_DOUBLE_WRITER:ONE_INSERTED+ONE_LOSER" in item
        and "NEW_TRANSACTION_CONFLICT:WINNER_ONLY" in item
        for item in outcomes
    )
    assert "TRANSIENT_1062" in outcomes[0]
    assert "TRANSIENT_1205" in outcomes[1]
    assert "TRANSIENT_1213" in outcomes[2]
    assert "DIRECT_CONFLICT" in outcomes[3]
    assert "DIRECT_CONFLICT" in outcomes[4]
    for pair in scenario.pairs:
        assert calls[(pair.evidence_type, pair.left.primary_value)] == 1
        assert calls[(pair.evidence_type, pair.right.primary_value)] == 2
    assert engine.commits == 5
    assert engine.transaction_rollbacks == 10
    assert engine.session_configurations == 10


@pytest.mark.parametrize("final_errno", (1062, 1205, 1213))
def test_conflicting_double_writer_never_accepts_transient_as_final_signal(
    monkeypatch,
    final_errno,
):
    scenario = acceptance.build_conflicting_double_writer_scenario(
        acceptance.build_behavioral_scenario()
    )
    first = scenario.pairs[0]
    engine = _DoubleWriterEngine()
    lock = Lock()
    loser_calls = 0

    def fake_append(_connection, case):
        nonlocal loser_calls
        if case.primary_value == first.left.primary_value:
            return SimpleNamespace(
                status=acceptance.EvidenceAppendStatus.INSERTED
            )
        with lock:
            loser_calls += 1
        raise Exception(final_errno, f"transient {final_errno}")

    monkeypatch.setattr(acceptance, "_append_behavioral_case", fake_append)

    with pytest.raises(Exception, match=f"transient {final_errno}"):
        acceptance._run_conflicting_double_writer_probes(engine, scenario)

    assert loser_calls == 2


def test_conflicting_double_writer_requires_explicit_fresh_tx_conflict(
    monkeypatch,
):
    scenario = acceptance.build_conflicting_double_writer_scenario(
        acceptance.build_behavioral_scenario()
    )
    first = scenario.pairs[0]
    engine = _DoubleWriterEngine()
    lock = Lock()
    loser_calls = 0

    def fake_append(_connection, case):
        nonlocal loser_calls
        if case.primary_value == first.left.primary_value:
            return SimpleNamespace(
                status=acceptance.EvidenceAppendStatus.INSERTED
            )
        with lock:
            loser_calls += 1
            ordinal = loser_calls
        if ordinal == 1:
            raise acceptance.EvidenceAppendConflictError("initial conflict")
        return SimpleNamespace(
            status=acceptance.EvidenceAppendStatus.IDEMPOTENT
        )

    monkeypatch.setattr(acceptance, "_append_behavioral_case", fake_append)

    with pytest.raises(
        RuntimeError,
        match="did not raise EvidenceAppendConflictError",
    ):
        acceptance._run_conflicting_double_writer_probes(engine, scenario)

    assert loser_calls == 2
    assert engine.transaction_rollbacks == 2


@pytest.mark.parametrize(
    ("errno", "message"),
    (
        (1062, "duplicate primary key"),
        (1213, "deadlock victim"),
    ),
)
def test_calendar_double_writer_conflict_retries_in_new_transaction(
    monkeypatch,
    errno,
    message,
):
    calendar = acceptance.build_behavioral_scenario().cases[0]
    scenario = SimpleNamespace(cases=(calendar,))
    engine = _DoubleWriterEngine()
    lock = Lock()
    calls = 0

    def fake_append(_connection, _case):
        nonlocal calls
        with lock:
            calls += 1
            ordinal = calls
        if ordinal == 1:
            return SimpleNamespace(status=acceptance.EvidenceAppendStatus.INSERTED)
        if ordinal == 2:
            raise Exception(errno, message)
        return SimpleNamespace(status=acceptance.EvidenceAppendStatus.IDEMPOTENT)

    monkeypatch.setattr(acceptance, "_append_behavioral_case", fake_append)

    inserted, outcomes = acceptance._run_identical_double_writer_probes(
        engine,
        scenario,
    )

    assert inserted == ("MARKET_CALENDAR",)
    assert outcomes == (
        "st_market_calendar_evidence_v2:IDENTICAL_DOUBLE_WRITER:"
        f"INSERTED+IDEMPOTENT:RETRY_{errno}:ONE_ROW",
    )
    assert calls == 3
    assert engine.transaction_rollbacks == 1


@pytest.mark.parametrize(
    ("case_index", "errno", "message"),
    (
        (0, 1205, "lock wait timeout"),
        (1, 1213, "quote deadlock"),
    ),
)
def test_double_writer_rejects_non_retryable_database_errors(
    monkeypatch,
    case_index,
    errno,
    message,
):
    case = acceptance.build_behavioral_scenario().cases[case_index]
    scenario = SimpleNamespace(cases=(case,))
    engine = _DoubleWriterEngine()
    lock = Lock()
    calls = 0

    def fake_append(_connection, _case):
        nonlocal calls
        with lock:
            calls += 1
            ordinal = calls
        if ordinal == 1:
            return SimpleNamespace(status=acceptance.EvidenceAppendStatus.INSERTED)
        raise Exception(errno, message)

    monkeypatch.setattr(acceptance, "_append_behavioral_case", fake_append)

    with pytest.raises(Exception, match=message):
        acceptance._run_identical_double_writer_probes(engine, scenario)

    assert calls == 2
    assert engine.transaction_rollbacks == 1


def test_behavioral_orchestrator_requires_double_writer_and_negative_matrix(
    monkeypatch,
):
    scenario = acceptance.build_behavioral_scenario()
    conflicting_scenario = object()
    engine = object()
    calls: list[str] = []
    covered = acceptance.CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES

    monkeypatch.setattr(
        acceptance,
        "build_conflicting_double_writer_scenario",
        lambda observed_scenario: conflicting_scenario
        if observed_scenario is scenario
        else pytest.fail("unexpected conflicting scenario input"),
    )
    monkeypatch.setattr(
        acceptance,
        "_insert_behavioral_seed",
        lambda observed_engine, observed_scenario: calls.append(
            "seed" if observed_scenario is scenario else "conflict-seed"
        )
        if observed_engine is engine
        and (
            observed_scenario is scenario
            or observed_scenario is conflicting_scenario
        )
        else pytest.fail("unexpected seed inputs"),
    )
    monkeypatch.setattr(
        acceptance,
        "_run_outer_transaction_rollback_probe",
        lambda *_args: (calls.append("rollback"), covered)[1],
    )
    monkeypatch.setattr(
        acceptance,
        "_run_identical_double_writer_probes",
        lambda *_args: (
            calls.append("double-writer"),
            (covered, ("double",) * 5),
        )[1],
    )
    monkeypatch.setattr(
        acceptance,
        "_run_conflicting_double_writer_probes",
        lambda observed_engine, observed_scenario: (
            calls.append("conflicting-double-writer"),
            ("conflict",) * 5,
        )[1]
        if observed_engine is engine
        and observed_scenario is conflicting_scenario
        else pytest.fail("unexpected conflicting probe inputs"),
    )
    monkeypatch.setattr(
        acceptance,
        "_run_idempotent_replay_probes",
        lambda *_args: (calls.append("replay"), covered)[1],
    )
    monkeypatch.setattr(
        acceptance,
        "_run_negative_guard_probes",
        lambda *_args: (
            calls.append("negative"),
            (("invalid",) * 5, ("replace",) * 5, ("ondup",) * 5),
        )[1],
    )
    monkeypatch.setattr(
        acceptance,
        "_run_append_only_guard_probes",
        lambda *_args: (
            calls.append("append-only"),
            (("update",) * 5, ("delete",) * 5),
        )[1],
    )

    outcome = acceptance._run_behavioral_probes(engine, scenario)

    assert calls == [
        "seed",
        "conflict-seed",
        "rollback",
        "double-writer",
        "conflicting-double-writer",
        "replay",
        "negative",
        "append-only",
    ]
    assert outcome.legal_inserted == covered
    assert outcome.identical_double_writer == ("double",) * 5
    assert outcome.conflicting_double_writer == ("conflict",) * 5
    assert outcome.invalid_insert_guards == ("invalid",) * 5


class _TransactionContext:
    def __init__(self, engine) -> None:
        self.engine = engine
        self.connection = SimpleNamespace(pending=set())

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, _exc, _tb):
        if exc_type is None:
            self.engine.committed.update(self.connection.pending)
        else:
            self.engine.rollback_count += 1
        return False


class _RollbackReadConnection:
    def __init__(self, engine) -> None:
        self.engine = engine

    def execute(self, _statement, params):
        return _QueryResult(
            scalar=int(params["primary_value"] in self.engine.committed)
        )


class _RollbackEngine:
    def __init__(self) -> None:
        self.committed: set[str] = set()
        self.rollback_count = 0

    def begin(self):
        return _TransactionContext(self)

    def connect(self):
        return _ConnectionContext(_RollbackReadConnection(self))


def test_behavioral_outer_exception_rolls_back_insert(monkeypatch):
    scenario = acceptance.build_behavioral_scenario()
    engine = _RollbackEngine()
    calls: list[str] = []

    def fake_append(connection, case):
        calls.append(case.evidence_type)
        connection.pending.add(case.primary_value)
        return SimpleNamespace(status=acceptance.EvidenceAppendStatus.INSERTED)

    monkeypatch.setattr(
        acceptance,
        "_append_behavioral_case",
        fake_append,
    )

    assert acceptance._run_outer_transaction_rollback_probe(
        engine,
        scenario,
    ) == acceptance.CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES
    assert calls == [
        "MARKET_CALENDAR",
        "QUOTE_RECEIPT",
        "MARKET_CALENDAR",
        "QUOTE_RECEIPT",
        "FILL_EXECUTION",
        "CASH_EVENT",
        "ORDER_TRANSITION",
    ]
    assert engine.committed == set()
    assert engine.rollback_count == 5


def test_direct_script_help_bootstraps_project_imports(tmp_path):
    completed = subprocess.run(
        [sys.executable, acceptance.__file__, "--help"],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "serial-replay" in completed.stdout
    assert "concurrent-initial" in completed.stdout
    assert "behavioral" in completed.stdout


def _decode_base64url(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def test_extended_authority_scenario_binds_signatures_nonce_and_revocations():
    now = datetime(2026, 8, 4, 1, 2, 3, 456789, tzinfo=timezone.utc)
    scenario = acceptance.build_authority_behavioral_scenario(now)

    assert len(scenario.cases) == 2
    assert scenario.available_at > now
    assert scenario.available_at.microsecond == 0
    assert {case.revocation_kind for case in scenario.cases} == {
        "KEY",
        "RECEIPT",
    }
    for case in scenario.cases:
        Ed25519PublicKey.from_public_bytes(case.public_key).verify(
            _decode_base64url(case.receipt.signature),
            case.receipt.signature_message,
        )
        assert case.receipt.claim_hash == case.claim.claim_hash
        assert case.receipt_values["envelope_hash"] == case.receipt.envelope_hash
        assert case.trust_key_values["public_key_hash"] == hashlib.sha256(
            case.public_key
        ).hexdigest()

    first = scenario.cases[0]
    nonce = scenario.nonce_replay_case
    assert nonce.claim.claim_hash != first.claim.claim_hash
    assert (
        nonce.claim.source_provider,
        nonce.receipt.key_id,
        nonce.receipt.key_version,
        nonce.receipt.replay_nonce,
    ) == (
        first.claim.source_provider,
        first.receipt.key_id,
        first.receipt.key_version,
        first.receipt.replay_nonce,
    )

    bad = scenario.invalid_signature_case
    receipt_case = scenario.cases[1]
    with pytest.raises(InvalidSignature):
        Ed25519PublicKey.from_public_bytes(receipt_case.public_key).verify(
            _decode_base64url(bad.receipt.signature),
            bad.receipt.signature_message,
        )

    revoked_at = scenario.available_at + timedelta(microseconds=1)
    for case in scenario.cases:
        values = case.revocation_values(revoked_at)
        assert len(str(values["revocation_hash"])) == 64
        assert values["revoked_at"] == revoked_at.replace(tzinfo=None)


def test_extended_authority_calendar_reaches_real_evidence_writer(monkeypatch):
    """Freeze the scenario against the production writer's exact payload contract."""

    scenario = acceptance.build_authority_behavioral_scenario(
        datetime(2026, 8, 4, 1, 2, 3, 456789, tzinfo=timezone.utc)
    )
    connection = object()
    expected_result = object()

    monkeypatch.setattr(evidence_writer, "_active_connection", lambda value: value)
    monkeypatch.setattr(
        evidence_writer,
        "_verify_external_authority",
        lambda observed_connection, evidence, *, verifier: None,
    )
    monkeypatch.setattr(
        evidence_writer,
        "_append_storage",
        lambda observed_connection, **kwargs: expected_result,
    )

    for case in scenario.cases:
        assert (
            evidence_writer.append_market_calendar_evidence(
                connection,
                case.evidence,
            )
            is expected_result
        )


def test_extended_accounting_scenario_is_two_lot_fifo_and_same_fill_conflict():
    scenario = acceptance.build_accounting_behavioral_scenario()

    assert len(scenario.seed_rows) == 9
    assert len(scenario.cash_evidence_rows) == 2
    assert scenario.cash_evidence_rows[1].previous_binding_id == (
        scenario.cash_evidence_rows[0].cash_binding_id
    )
    effects = scenario.outcome.lot_effects
    assert tuple(effect.effect_kind.value for effect in effects) == (
        "SELL_FIFO_CONSUME",
        "SELL_FIFO_CONSUME",
    )
    assert tuple(effect.consumed_quantity for effect in effects) == (100, 50)
    assert tuple(effect.after_lot.lot_id for effect in effects) == (
        "mysql57-accounting-lot-a",
        "mysql57-accounting-lot-b",
    )
    assert effects[0].before_lot.opened_trade_date < (
        effects[1].before_lot.opened_trade_date
    )
    assert scenario.conflicting_outcome.fill_execution_evidence.fill_id == (
        scenario.outcome.fill_execution_evidence.fill_id
    )
    assert scenario.conflicting_outcome.accounting_outcome_id != (
        scenario.outcome.accounting_outcome_id
    )


class _AccountingProbeConnection:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def in_transaction(self) -> bool:
        return True

    def execute(self, statement, parameters=None):
        self.calls.append(str(statement))
        return SimpleNamespace(parameters=parameters)


def test_accounting_execution_probe_interrupts_exactly_before_final_marker():
    connection = _AccountingProbeConnection()
    probe = acceptance._AccountingExecutionProbe(
        connection,
        interrupt_tag="insert_finalization",
    )
    probe.execute(
        text("/* v2ao:insert_outcome */ INSERT INTO outcome VALUES (1)"),
        {},
    )
    with pytest.raises(
        acceptance._InjectedAccountingInterruption,
        match="insert_finalization",
    ):
        probe.execute(
            text(
                "/* v2ao:insert_finalization */ "
                "INSERT INTO finalization VALUES (1)"
            ),
            {},
        )

    assert probe.tags == ["insert_outcome", "insert_finalization"]
    assert len(connection.calls) == 1


def _nonempty_three_layer_reports():
    core = V2EvidenceHashAuditReport(
        table_counts=tuple(
            (table, 1) for table in sorted(acceptance.EVIDENCE_TABLES)
        ),
        payload_hashes_verified=13,
        rows_reconstructed=5,
        cash_chains_checked=1,
        complete_cash_chains=0,
        order_chains_checked=1,
        complete_order_chains=0,
        external_authority_claims=1,
        database_sha2_used=True,
        shared_row_locks_used=True,
    )
    authority = acceptance.V2AuthorityStoredRowAuditReport(
        table_counts=tuple(
            (table, 1) for table in acceptance.AUTHORITY_AUDIT_TABLES
        ),
        rows_reconstructed=5,
        hashes_verified=7,
        signatures_verified=1,
        database_sha2_used=True,
        shared_row_locks_used=True,
    )
    accounting = acceptance.V2AccountingEvidenceAuditReport(
        table_counts=(
            (acceptance.OUTCOME_TABLE, 1),
            (acceptance.LOT_EFFECT_TABLE, 2),
            (acceptance.FINALIZATION_TABLE, 1),
        ),
        hash_verifications=(
            (acceptance.OUTCOME_TABLE, 4),
            (acceptance.LOT_EFFECT_TABLE, 10),
            (acceptance.FINALIZATION_TABLE, 2),
        ),
        hashes_verified=16,
        rows_reconstructed=4,
        finalized_outcomes=1,
        finalized_outcome_ids=("a" * 64,),
        lot_chains_checked=2,
        lot_chain_ids=("lot-a", "lot-b"),
        parent_rows_checked=8,
        parent_row_checks=(
            ("account", "account"),
            ("cash", "cash"),
            ("fill", "fill"),
            ("fill", "old-a"),
            ("fill", "old-b"),
            ("lot", "lot-a"),
            ("lot", "lot-b"),
            ("order", "order"),
        ),
        database_sha2_used=True,
        shared_row_locks_used=True,
    )
    return core, authority, accounting


def test_three_layer_audit_requires_every_layer_to_be_nonempty():
    core, authority, accounting = _nonempty_three_layer_reports()
    assert acceptance._require_nonempty_three_layer_audits(
        core,
        authority,
        accounting,
    ) is True

    empty_authority = replace(
        authority,
        table_counts=tuple(
            (table, 0) for table in acceptance.AUTHORITY_AUDIT_TABLES
        ),
        rows_reconstructed=0,
        hashes_verified=0,
        signatures_verified=0,
    )
    assert empty_authority.audit_passed is True
    with pytest.raises(RuntimeError, match="not non-empty"):
        acceptance._require_nonempty_three_layer_audits(
            core,
            empty_authority,
            accounting,
        )


def test_extended_probe_orchestrator_runs_014_then_015_then_both_auditors(
    monkeypatch,
):
    engine = object()
    now = datetime(2026, 8, 4, 1, 2, 3, tzinfo=timezone.utc)
    authority_scenario = object()
    accounting_scenario = object()
    authority_outcome = object()
    accounting_outcome = object()
    final_outcome = object()
    calls: list[str] = []

    monkeypatch.setattr(
        acceptance,
        "_database_utc_now",
        lambda observed: (calls.append("clock"), now)[1]
        if observed is engine
        else pytest.fail("unexpected clock engine"),
    )
    monkeypatch.setattr(
        acceptance,
        "build_authority_behavioral_scenario",
        lambda observed: (calls.append("build-authority"), authority_scenario)[1]
        if observed is now
        else pytest.fail("unexpected authority time"),
    )
    monkeypatch.setattr(
        acceptance,
        "_run_authority_behavioral_probes",
        lambda observed_engine, observed_scenario: (
            calls.append("authority"),
            authority_outcome,
        )[1]
        if observed_engine is engine and observed_scenario is authority_scenario
        else pytest.fail("unexpected authority inputs"),
    )
    monkeypatch.setattr(
        acceptance,
        "build_accounting_behavioral_scenario",
        lambda: (calls.append("build-accounting"), accounting_scenario)[1],
    )
    monkeypatch.setattr(
        acceptance,
        "_run_accounting_behavioral_probes",
        lambda observed_engine, observed_scenario: (
            calls.append("accounting"),
            accounting_outcome,
        )[1]
        if observed_engine is engine and observed_scenario is accounting_scenario
        else pytest.fail("unexpected accounting inputs"),
    )
    monkeypatch.setattr(
        acceptance,
        "_run_extended_database_audits",
        lambda observed_engine, authority, accounting: (
            calls.append("auditors"),
            final_outcome,
        )[1]
        if observed_engine is engine
        and authority is authority_outcome
        and accounting is accounting_outcome
        else pytest.fail("unexpected audit inputs"),
    )

    assert acceptance._run_extended_behavioral_probes(engine) is final_outcome
    assert calls == [
        "clock",
        "build-authority",
        "authority",
        "build-accounting",
        "accounting",
        "auditors",
    ]
