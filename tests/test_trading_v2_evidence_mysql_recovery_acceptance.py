from __future__ import annotations

from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from server.db import migrations_v2
from tools import trading_v2_evidence_mysql_recovery_acceptance as recovery


TEST_SERVER_UUID = "123e4567-e89b-12d3-a456-426614174000"
TEST_DATABASE = "probiga_v2_evidence_test_recovery"
TEST_URL = f"mysql+pymysql://u:p@localhost/{TEST_DATABASE}"
ORACLE_COMMENT = "MySQL Community Server (GPL)"


class _ScalarResult:
    def __init__(self, value=None) -> None:
        self.value = value

    def scalar(self):
        return self.value


class _RunnerConnection:
    def __init__(self) -> None:
        self.dialect = SimpleNamespace(name="mysql")
        self.sql: list[str] = []
        self.commits = 0

    def execute(self, statement, params=None):
        self.sql.append(str(statement))
        return _ScalarResult()

    def commit(self) -> None:
        self.commits += 1


class _RunnerEngine:
    dialect = SimpleNamespace(name="mysql")


def test_fault_hook_is_strict_one_shot_and_carries_the_exact_boundary():
    hook = migrations_v2.V2MigrationAcceptanceFaultHook.after_ddl_commit(
        migrations_v2.EVIDENCE_BINDING_VERSION,
        2,
    )
    hook._validate_declaration(migrations_v2.MIGRATIONS)
    hook._raise_if_matches(
        version=migrations_v2.EVIDENCE_BINDING_VERSION,
        phase=migrations_v2.V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
        committed_statement_count=1,
    )
    assert hook.triggered is False

    with pytest.raises(
        migrations_v2.V2MigrationAcceptanceFault,
        match="after 2 committed statements",
    ) as caught:
        hook._raise_if_matches(
            version=migrations_v2.EVIDENCE_BINDING_VERSION,
            phase=migrations_v2.V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
            committed_statement_count=2,
        )

    assert hook.triggered is True
    assert caught.value.version == migrations_v2.EVIDENCE_BINDING_VERSION
    assert caught.value.phase == migrations_v2.V2_MIGRATION_FAULT_AFTER_DDL_COMMIT
    assert caught.value.committed_statement_count == 2
    with pytest.raises(RuntimeError, match="cannot be reused"):
        hook._validate_declaration(migrations_v2.MIGRATIONS)


@pytest.mark.parametrize(
    "kwargs",
    (
        {
            "version": "20260803_013_not_an_interrupt_target",
            "phase": migrations_v2.V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE,
        },
        {
            "version": migrations_v2.EVIDENCE_BINDING_VERSION,
            "phase": "BEFORE_RANDOM_WRITE",
        },
        {
            "version": migrations_v2.EVIDENCE_BINDING_VERSION,
            "phase": migrations_v2.V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
            "committed_statement_count": 0,
        },
        {
            "version": migrations_v2.EVIDENCE_BINDING_VERSION,
            "phase": migrations_v2.V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE,
            "committed_statement_count": 1,
        },
    ),
)
def test_fault_hook_rejects_non_evidence_or_unsafe_boundaries(kwargs):
    with pytest.raises(ValueError):
        migrations_v2.V2MigrationAcceptanceFaultHook(**kwargs)


def test_fault_hook_rejects_statement_count_past_declared_ddl():
    statements = tuple(
        next(
            item
            for item in migrations_v2.MIGRATIONS
            if item["version"] == migrations_v2.EVIDENCE_BINDING_VERSION
        )["statements"]
    )
    hook = migrations_v2.V2MigrationAcceptanceFaultHook.after_ddl_commit(
        migrations_v2.EVIDENCE_BINDING_VERSION,
        len(statements) + 1,
    )
    with pytest.raises(ValueError, match="exceeds"):
        hook._validate_declaration(migrations_v2.MIGRATIONS)


def test_fault_hook_is_unavailable_for_dry_run_and_rejects_arbitrary_objects():
    hook = migrations_v2.V2MigrationAcceptanceFaultHook.before_ledger_write(
        migrations_v2.EVIDENCE_GUARD_VERSION
    )
    with pytest.raises(ValueError, match="unavailable for dry-run"):
        migrations_v2.run_v2_migrations(
            _RunnerEngine(),
            dry_run=True,
            acceptance_fault_hook=hook,
        )
    with pytest.raises(TypeError, match="V2MigrationAcceptanceFaultHook"):
        migrations_v2.run_v2_migrations(
            _RunnerEngine(),
            acceptance_fault_hook=object(),
        )


def test_runner_interrupts_only_after_the_requested_ddl_commit(monkeypatch):
    synthetic = (
        {
            "version": migrations_v2.EVIDENCE_BINDING_VERSION,
            "statements": ("DDL ONE", "DDL TWO"),
        },
    )
    monkeypatch.setattr(migrations_v2, "MIGRATIONS", synthetic)
    monkeypatch.setattr(migrations_v2, "_migration_table_exists", lambda _c: True)
    monkeypatch.setattr(
        migrations_v2,
        "_applied_checksum",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        migrations_v2,
        "_validate_evidence_server",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations_v2,
        "_validate_evidence_schema_for_version",
        lambda _connection, _version, **_kwargs: None,
    )
    monkeypatch.setattr(
        migrations_v2,
        "_bootstrap_maintenance_fence",
        lambda _connection: migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
    )
    monkeypatch.setattr(
        migrations_v2,
        "_activate_maintenance_fence",
        lambda _connection, *, target_version: None,
    )
    connection = _RunnerConnection()
    hook = migrations_v2.V2MigrationAcceptanceFaultHook.after_ddl_commit(
        migrations_v2.EVIDENCE_BINDING_VERSION,
        1,
    )

    with pytest.raises(migrations_v2.V2MigrationAcceptanceFault):
        migrations_v2._run_v2_migrations_unlocked(
            _RunnerEngine(),
            dry_run=False,
            allow_execution_evidence=True,
            connection=connection,
            acceptance_fault_hook=hook,
        )

    assert hook.triggered is True
    assert any(sql == "DDL ONE" for sql in connection.sql)
    assert all(sql != "DDL TWO" for sql in connection.sql)
    # One explicit migration-table commit, then the committed DDL boundary.
    assert connection.commits == 2


def test_runner_interrupts_before_ledger_insert_not_after_it(monkeypatch):
    synthetic = (
        {
            "version": migrations_v2.EVIDENCE_GUARD_VERSION,
            "statements": ("DROP TRIGGER t", "CREATE TRIGGER t"),
        },
    )
    monkeypatch.setattr(migrations_v2, "MIGRATIONS", synthetic)
    monkeypatch.setattr(migrations_v2, "_migration_table_exists", lambda _c: True)
    monkeypatch.setattr(
        migrations_v2,
        "_applied_checksum",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        migrations_v2,
        "_validate_evidence_server",
        lambda _connection: None,
    )
    monkeypatch.setattr(
        migrations_v2,
        "_validate_evidence_schema_for_version",
        lambda _connection, _version, **_kwargs: None,
    )
    monkeypatch.setattr(
        migrations_v2,
        "_bootstrap_maintenance_fence",
        lambda _connection: migrations_v2.V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
    )
    monkeypatch.setattr(
        migrations_v2,
        "_activate_maintenance_fence",
        lambda _connection, *, target_version: None,
    )
    monkeypatch.setattr(
        migrations_v2,
        "_audit_evidence_rows_before_ledger",
        lambda _connection, *, version: None,
    )
    connection = _RunnerConnection()
    hook = migrations_v2.V2MigrationAcceptanceFaultHook.before_ledger_write(
        migrations_v2.EVIDENCE_GUARD_VERSION
    )

    with pytest.raises(migrations_v2.V2MigrationAcceptanceFault):
        migrations_v2._run_v2_migrations_unlocked(
            _RunnerEngine(),
            dry_run=False,
            allow_execution_evidence=True,
            connection=connection,
            acceptance_fault_hook=hook,
        )

    assert hook.triggered is True
    assert any(sql == "DROP TRIGGER t" for sql in connection.sql)
    assert any(sql == "CREATE TRIGGER t" for sql in connection.sql)
    assert not any("INSERT IGNORE INTO schema_migration_v2" in sql for sql in connection.sql)


class _ConnectionContext:
    def __init__(self, connection: "_RecoveryConnection") -> None:
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        return False


class _RecoveryConnection:
    def __init__(self, identifier: int, engine: "_RecoveryEngine") -> None:
        self.identifier = identifier
        self.engine = engine
        self.dialect = SimpleNamespace(name="mysql")
        self.rollbacks = 0

    def rollback(self) -> None:
        self.rollbacks += 1


class _RecoveryEngine:
    def __init__(self) -> None:
        self.dialect = SimpleNamespace(name="mysql")
        self.connections: list[_RecoveryConnection] = []
        self.disposals = 0

    def connect(self):
        connection = _RecoveryConnection(len(self.connections) + 1, self)
        self.connections.append(connection)
        return _ConnectionContext(connection)

    def dispose(self) -> None:
        self.disposals += 1


def _snapshot(
    *,
    production: bool = False,
    actionable: bool = False,
    table_count: int = 52,
    trigger_count: int = 43,
    maintenance_fence_rows: int = 1,
    maintenance_fence_state: str = "INACTIVE",
):
    expected = recovery.base_acceptance.FROZEN_EXPECTED_MIGRATIONS
    return recovery.base_acceptance.EvidenceAcceptanceSnapshot(
        migration_versions=tuple(version for version, _checksum in expected),
        checksums=tuple(checksum for _version, checksum in expected),
        observed_tables=tuple(f"table_{index}" for index in range(table_count)),
        observed_triggers=tuple(
            f"trigger_{index}" for index in range(trigger_count)
        ),
        evidence_tables=tuple(f"evidence_table_{index}" for index in range(5)),
        evidence_triggers=tuple(
            f"evidence_trigger_{index}" for index in range(17)
        ),
        migration_ledger_rows=recovery.base_acceptance.EXPECTED_MIGRATION_COUNT,
        maintenance_fence_rows=maintenance_fence_rows,
        maintenance_fence_state=maintenance_fence_state,
        business_tables_empty=True,
        stored_routines_empty=True,
        scheduled_events_empty=True,
        metadata_preflight_passed=True,
        production_activation_allowed=production,
        actionable_output_allowed=actionable,
    )


def _migration_results(statuses: tuple[str, ...]):
    return tuple(
        migrations_v2.V2MigrationResult(version, status, 0)
        for (version, _checksum), status in zip(
            recovery.base_acceptance.FROZEN_EXPECTED_MIGRATIONS,
            statuses,
            strict=True,
        )
    )


def _install_orchestration_fakes(monkeypatch, scenario_name: str):
    engine = _RecoveryEngine()
    selected = recovery.RECOVERY_SCENARIOS[scenario_name]
    migration_calls: list[tuple[int, object]] = []
    empty_checks: list[int] = []
    monkeypatch.setattr(recovery, "_assert_recovery_contract", lambda: None)
    monkeypatch.setattr(recovery, "create_tool_engine", lambda *_a, **_kw: engine)
    monkeypatch.setattr(
        recovery.base_acceptance,
        "_server_identity_from_connection",
        lambda connection, _database, _uuid: (
            TEST_DATABASE,
            "5.7.38",
            TEST_SERVER_UUID,
            ORACLE_COMMENT,
        ),
    )

    def assert_empty(connection, *, mode):
        empty_checks.append(connection.identifier)

    monkeypatch.setattr(
        recovery.base_acceptance,
        "_assert_empty_on_connection",
        assert_empty,
    )
    monkeypatch.setattr(
        recovery.base_acceptance,
        "_validate_run",
        lambda results, *, allowed_statuses: tuple(results),
    )
    monkeypatch.setattr(
        recovery.base_acceptance,
        "_post_migration_snapshot",
        lambda _engine, *, connection: _snapshot(),
    )

    def inspect(connection, spec, *, attempt):
        return recovery.RecoveryInterruptionObservation(
            attempt=attempt,
            version=spec.version,
            phase=spec.phase,
            committed_statement_count=spec.committed_statement_count,
            migration_ledger_versions=recovery._expected_ledger_prefix(
                spec.version
            ),
            observed_table_count=spec.expected_partial_table_count,
            observed_trigger_count=spec.expected_partial_trigger_count,
            maintenance_fence_rows=1,
            maintenance_fence_state="ACTIVE",
            business_tables_empty=True,
            stored_routines_empty=True,
            scheduled_events_empty=True,
        )

    monkeypatch.setattr(recovery, "_inspect_interrupted_state", inspect)
    completed = False

    def run(_engine, *, allow_execution_evidence, connection, acceptance_fault_hook=None):
        nonlocal completed
        migration_calls.append((connection.identifier, acceptance_fault_hook))
        if acceptance_fault_hook is not None:
            acceptance_fault_hook._raise_if_matches(
                version=acceptance_fault_hook.version,
                phase=acceptance_fault_hook.phase,
                committed_statement_count=(
                    acceptance_fault_hook.committed_statement_count
                ),
            )
            raise AssertionError("fault hook did not fire")
        count = recovery.base_acceptance.EXPECTED_MIGRATION_COUNT
        if not completed:
            versions = tuple(
                version
                for version, _checksum in (
                    recovery.base_acceptance.FROZEN_EXPECTED_MIGRATIONS
                )
            )
            boundary = versions.index(selected.target_version)
            completed = True
            return _migration_results(
                ("exists",) * boundary + ("applied",) * (count - boundary)
            )
        return _migration_results(("exists",) * count)

    monkeypatch.setattr(recovery, "run_v2_migrations", run)
    return engine, migration_calls, empty_checks


@pytest.mark.parametrize("scenario_name", tuple(recovery.RECOVERY_SCENARIOS))
def test_recovery_scenarios_use_new_connections_and_finish_full_blocked_state(
    monkeypatch,
    scenario_name,
):
    engine, migration_calls, empty_checks = _install_orchestration_fakes(
        monkeypatch,
        scenario_name,
    )
    selected = recovery.RECOVERY_SCENARIOS[scenario_name]

    report = recovery.run_mysql_recovery_acceptance(
        TEST_URL,
        expected_server_uuid=TEST_SERVER_UUID,
        scenario=scenario_name,
    )

    expected_count = recovery.base_acceptance.EXPECTED_MIGRATION_COUNT
    assert report.interruption_count == len(selected.faults)
    assert report.final_migration_ledger_rows == expected_count
    assert report.final_table_count == 52
    assert report.final_trigger_count == 43
    assert report.final_maintenance_fence_rows == 1
    assert report.final_maintenance_fence_state == "INACTIVE"
    assert report.business_tables_empty is True
    assert report.stored_routines_empty is True
    assert report.scheduled_events_empty is True
    assert report.schema_gate_passed is True
    assert report.production_activation_allowed is False
    assert report.actionable_output_allowed is False
    assert report.serial_replay == ("exists",) * expected_count
    connection_ids = [identifier for identifier, _hook in migration_calls]
    assert len(connection_ids) == len(selected.faults) + 2
    assert len(set(connection_ids)) == len(connection_ids)
    assert empty_checks == [1]
    assert engine.disposals >= len(selected.faults) + 2


def test_nonempty_preflight_has_no_cleanup_or_bypass(monkeypatch):
    engine = _RecoveryEngine()
    migration_called = False
    monkeypatch.setattr(recovery, "_assert_recovery_contract", lambda: None)
    monkeypatch.setattr(recovery, "create_tool_engine", lambda *_a, **_kw: engine)
    monkeypatch.setattr(
        recovery.base_acceptance,
        "_server_identity_from_connection",
        lambda *_args: (
            TEST_DATABASE,
            "5.7.38",
            TEST_SERVER_UUID,
            ORACLE_COMMENT,
        ),
    )
    monkeypatch.setattr(
        recovery.base_acceptance,
        "_assert_empty_on_connection",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("must start empty")
        ),
    )

    def unexpected_migration(*_args, **_kwargs):
        nonlocal migration_called
        migration_called = True

    monkeypatch.setattr(recovery, "run_v2_migrations", unexpected_migration)

    with pytest.raises(RuntimeError, match="must start empty"):
        recovery.run_mysql_recovery_acceptance(
            TEST_URL,
            expected_server_uuid=TEST_SERVER_UUID,
            scenario="011-ddl-prefix",
        )

    assert migration_called is False
    assert engine.disposals == 1


class _InspectionResult:
    def __init__(self, *, scalars=(), scalar=0, mappings=()) -> None:
        self._scalars = tuple(scalars)
        self._scalar = scalar
        self._mappings = tuple(mappings)

    def scalars(self):
        return iter(self._scalars)

    def scalar(self):
        return self._scalar

    def mappings(self):
        return iter(self._mappings)


class _InspectionConnection:
    def __init__(
        self,
        ledger_versions,
        triggers,
        *,
        nonempty_table=None,
        maintenance_fence_rows=None,
    ) -> None:
        self.ledger_versions = tuple(ledger_versions)
        self.triggers = tuple(triggers)
        self.nonempty_table = nonempty_table
        self.maintenance_fence_rows = maintenance_fence_rows

    def execute(self, statement):
        sql = str(statement)
        if "SELECT version FROM schema_migration_v2" in sql:
            return _InspectionResult(scalars=self.ledger_versions)
        if "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS" in sql:
            return _InspectionResult(scalars=self.triggers)
        if sql.startswith("SELECT fence_name, state, target_version"):
            rows = self.maintenance_fence_rows
            if rows is None:
                rows = (
                    {
                        "fence_name": "execution_evidence_011_015",
                        "state": "ACTIVE",
                        "target_version": migrations_v2.EVIDENCE_BINDING_VERSION,
                        "generation": 1,
                        "activated_at": "2026-08-04 00:00:00.000000",
                        "updated_at": "2026-08-04 00:00:00.000000",
                    },
                )
            return _InspectionResult(mappings=rows)
        if "SELECT COUNT(*) FROM" in sql:
            count = int(
                self.nonempty_table is not None
                and self.nonempty_table in sql
            )
            return _InspectionResult(scalar=count)
        raise AssertionError(f"unexpected inspection SQL: {sql}")


def test_interrupted_state_requires_exact_ledger_inventory_and_empty_rows(monkeypatch):
    spec = recovery.RECOVERY_SCENARIOS["011-ddl-prefix"].faults[0]
    tables = frozenset(
        sorted(recovery.base_acceptance.EXPECTED_TABLES)[
            : spec.expected_partial_table_count
        ]
    )
    triggers = tuple(
        sorted(recovery.base_acceptance.EXPECTED_TRIGGER_NAMES)[
            : spec.expected_partial_trigger_count
        ]
    )
    monkeypatch.setattr(
        recovery.base_acceptance,
        "_schema_object_inventory_from_connection",
        lambda _connection: {
            "tables": tables,
            "routines": frozenset(),
            "events": frozenset(),
        },
    )
    ledger = recovery._expected_ledger_prefix(spec.version)
    observed = recovery._inspect_interrupted_state(
        _InspectionConnection(
            ledger,
            triggers,
            nonempty_table="`schema_migration_v2_maintenance_fence`",
        ),
        spec,
        attempt=1,
    )

    assert observed.migration_ledger_versions == ledger
    assert observed.observed_table_count == spec.expected_partial_table_count
    assert observed.observed_trigger_count == spec.expected_partial_trigger_count
    assert observed.maintenance_fence_rows == 1
    assert observed.maintenance_fence_state == "ACTIVE"
    assert observed.business_tables_empty is True


@pytest.mark.parametrize(
    "maintenance_fence_rows",
    (
        (),
        (
            {
                "fence_name": "execution_evidence_011_015",
                "state": "INACTIVE",
            },
        ),
    ),
)
def test_interrupted_state_requires_one_active_maintenance_fence(
    monkeypatch,
    maintenance_fence_rows,
):
    spec = recovery.RECOVERY_SCENARIOS["011-ddl-prefix"].faults[0]
    tables = frozenset(
        sorted(recovery.base_acceptance.EXPECTED_TABLES)[
            : spec.expected_partial_table_count
        ]
    )
    triggers = tuple(
        sorted(recovery.base_acceptance.EXPECTED_TRIGGER_NAMES)[
            : spec.expected_partial_trigger_count
        ]
    )
    monkeypatch.setattr(
        recovery.base_acceptance,
        "_schema_object_inventory_from_connection",
        lambda _connection: {
            "tables": tables,
            "routines": frozenset(),
            "events": frozenset(),
        },
    )

    with pytest.raises(RuntimeError, match="maintenance fence"):
        recovery._inspect_interrupted_state(
            _InspectionConnection(
                recovery._expected_ledger_prefix(spec.version),
                triggers,
                maintenance_fence_rows=tuple(maintenance_fence_rows),
            ),
            spec,
            attempt=1,
        )


def test_interrupted_state_rejects_business_rows(monkeypatch):
    spec = recovery.RECOVERY_SCENARIOS["012-before-ledger"].faults[0]
    tables = frozenset(
        sorted(recovery.base_acceptance.EXPECTED_TABLES)[
            : spec.expected_partial_table_count
        ]
    )
    business_table = next(
        item
        for item in sorted(tables)
        if item
        not in {
            "schema_migration_v2",
            "schema_migration_v2_maintenance_fence",
        }
    )
    monkeypatch.setattr(
        recovery.base_acceptance,
        "_schema_object_inventory_from_connection",
        lambda _connection: {
            "tables": tables,
            "routines": frozenset(),
            "events": frozenset(),
        },
    )
    with pytest.raises(RuntimeError, match="contains business rows"):
        recovery._inspect_interrupted_state(
            _InspectionConnection(
                recovery._expected_ledger_prefix(spec.version),
                tuple(sorted(recovery.base_acceptance.EXPECTED_TRIGGER_NAMES))[
                    : spec.expected_partial_trigger_count
                ],
                nonempty_table=f"`{business_table}`",
            ),
            spec,
            attempt=1,
        )


@pytest.mark.parametrize(
    "url",
    (
        "mysql+pymysql://u:p@localhost/production",
        "mysql+pymysql://u:p@localhost/probiga_v2_evidence_test?database=prod",
        "sqlite:///probiga_v2_evidence_test",
    ),
)
def test_recovery_reuses_strict_url_gate_before_engine_creation(monkeypatch, url):
    monkeypatch.setattr(recovery, "_assert_recovery_contract", lambda: None)
    monkeypatch.setattr(
        recovery,
        "create_tool_engine",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            AssertionError("unsafe URL reached engine creation")
        ),
    )
    with pytest.raises(ValueError):
        recovery.run_mysql_recovery_acceptance(
            url,
            expected_server_uuid=TEST_SERVER_UUID,
            scenario="011-ddl-prefix",
        )


def test_unknown_scenario_is_rejected_before_engine_creation(monkeypatch):
    monkeypatch.setattr(recovery, "_assert_recovery_contract", lambda: None)
    monkeypatch.setattr(
        recovery,
        "create_tool_engine",
        lambda *_a, **_kw: (_ for _ in ()).throw(
            AssertionError("unknown scenario reached engine creation")
        ),
    )
    with pytest.raises(ValueError, match="scenario must be one of"):
        recovery.run_mysql_recovery_acceptance(
            TEST_URL,
            expected_server_uuid=TEST_SERVER_UUID,
            scenario="all-in-one-dirty-reuse",
        )


@pytest.mark.parametrize(
    "snapshot,match",
    (
        (_snapshot(production=True), "cannot enable production"),
        (_snapshot(actionable=True), "cannot enable production"),
        (_snapshot(table_count=51), "exactly 52 tables"),
        (_snapshot(trigger_count=42), "exactly 43 triggers"),
        (
            _snapshot(maintenance_fence_rows=0),
            "maintenance fence",
        ),
        (
            _snapshot(maintenance_fence_rows=2),
            "maintenance fence",
        ),
        (
            _snapshot(maintenance_fence_state="ACTIVE"),
            "maintenance fence",
        ),
    ),
)
def test_final_snapshot_is_fail_closed(snapshot, match):
    with pytest.raises(RuntimeError, match=match):
        recovery._validate_final_snapshot(snapshot)


def test_recovery_contract_declares_precise_011_015_boundaries():
    recovery._assert_recovery_contract()
    assert recovery.EXPECTED_MIGRATION_OWNED_TABLE_COUNT == 51
    assert recovery.EXPECTED_RUNNER_BOOTSTRAP_TABLE_COUNT == 1
    assert recovery.EXPECTED_FINAL_TABLE_COUNT == 52
    assert tuple(recovery.RECOVERY_SCENARIOS) == (
        "011-ddl-prefix",
        "012-drop-create-boundary",
        "012-before-ledger",
        "013-after-ddl",
        "013-before-ledger",
        "014-ddl-prefix",
        "014-drop-create-boundary",
        "014-before-ledger",
        "015-drop-create-boundary",
        "015-ddl-prefix",
        "015-before-ledger",
    )
    drop_create = recovery.RECOVERY_SCENARIOS["012-drop-create-boundary"]
    assert tuple(item.committed_statement_count for item in drop_create.faults) == (
        1,
    )
    assert tuple(item.expected_partial_trigger_count for item in drop_create.faults) == (
        2,
    )
    assert recovery.RECOVERY_SCENARIOS["011-ddl-prefix"].faults[
        0
    ].expected_partial_table_count == 41


def test_recovery_help_is_available_without_database():
    script = (
        Path(__file__).resolve().parents[1]
        / "tools"
        / "trading_v2_evidence_mysql_recovery_acceptance.py"
    )
    completed = subprocess.run(
        [sys.executable, str(script), "--help"],
        check=True,
        capture_output=True,
        text=True,
    )
    assert "011-ddl-prefix" in completed.stdout
    assert "012-drop-create-boundary" in completed.stdout
    assert "012-before-ledger" in completed.stdout
    assert "013-after-ddl" in completed.stdout
    assert "014-drop-create-boundary" in completed.stdout
    assert "015-before-ledger" in completed.stdout
