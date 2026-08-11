"""Isolated MySQL recovery acceptance for V2 evidence migrations 011-015.

Each invocation accepts exactly one recovery scenario and requires a separately
provisioned, completely empty V2-evidence test/CI schema.  It never drops,
cleans, truncates, or bypasses the empty-schema preflight.  The final state is
the complete frozen migration contract (including migrations added after 012),
while fault injection remains limited to committed boundaries in 011-015.
Every interrupted boundary must retain one ACTIVE maintenance-fence row, and
the recovered final report requires exactly one INACTIVE row.  Recovery remains
non-production and non-actionable.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import sys
from types import MappingProxyType
from typing import Mapping


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, make_url

from server.common.mysql_version_policy import isolated_acceptance_versions_label
from server.db.migrations_v2 import (
    EVIDENCE_BINDING_VERSION,
    EVIDENCE_GUARD_VERSION,
    EVIDENCE_NATURAL_KEY_VERSION,
    EVIDENCE_AUTHORITY_VERSION,
    EVIDENCE_ACCOUNTING_VERSION,
    MIGRATIONS,
    V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE,
    V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
    V2_EVIDENCE_MAINTENANCE_FENCE_TABLE,
    V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
    V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE,
    V2MigrationAcceptanceFault,
    V2MigrationAcceptanceFaultHook,
    V2MigrationResult,
    run_v2_migrations,
)
from tools import trading_v2_evidence_mysql_acceptance as base_acceptance
from tools.mysql_acceptance_tls import (
    MySQLAcceptanceTLSConfig,
    create_mysql_acceptance_engine as create_tool_engine,
    resolve_mysql_acceptance_tls_config,
)


DEFAULT_URL_ENV = "V2_EVIDENCE_TEST_RECOVERY_MYSQL_URL"
DEFAULT_SERVER_UUID_ENV = "V2_EVIDENCE_TEST_RECOVERY_MYSQL_SERVER_UUID"
DEFAULT_SSL_CA_ENV = "V2_EVIDENCE_TEST_RECOVERY_MYSQL_SSL_CA"
EXPECTED_MIGRATION_OWNED_TABLE_COUNT = 51
EXPECTED_RUNNER_BOOTSTRAP_TABLE_COUNT = 1
EXPECTED_FINAL_TABLE_COUNT = 52
EXPECTED_FINAL_TRIGGER_COUNT = 43
EXPECTED_EVIDENCE_TABLE_COUNT = 5
EXPECTED_EVIDENCE_TRIGGER_COUNT = 17


@dataclass(frozen=True, slots=True)
class RecoveryFaultSpec:
    version: str
    phase: str
    committed_statement_count: int | None
    expected_partial_table_count: int
    expected_partial_trigger_count: int

    def build_hook(self) -> V2MigrationAcceptanceFaultHook:
        if self.phase == V2_MIGRATION_FAULT_AFTER_DDL_COMMIT:
            return V2MigrationAcceptanceFaultHook.after_ddl_commit(
                self.version,
                int(self.committed_statement_count or 0),
            )
        if self.phase == V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE:
            return V2MigrationAcceptanceFaultHook.before_ledger_write(
                self.version
            )
        raise RuntimeError(f"unsupported recovery fault phase: {self.phase}")


@dataclass(frozen=True, slots=True)
class RecoveryScenario:
    name: str
    target_version: str
    faults: tuple[RecoveryFaultSpec, ...]


@dataclass(frozen=True, slots=True)
class RecoveryInterruptionObservation:
    attempt: int
    version: str
    phase: str
    committed_statement_count: int | None
    migration_ledger_versions: tuple[str, ...]
    observed_table_count: int
    observed_trigger_count: int
    maintenance_fence_rows: int
    maintenance_fence_state: str
    business_tables_empty: bool
    stored_routines_empty: bool
    scheduled_events_empty: bool


@dataclass(frozen=True, slots=True)
class RecoveryAcceptanceReport:
    mode: str
    scenario: str
    database: str
    server_version: str
    server_version_comment: str
    server_uuid: str
    least_privilege_attested: bool
    started_empty: bool
    interruption_count: int
    interruptions: tuple[RecoveryInterruptionObservation, ...]
    recovery_migration: tuple[str, ...]
    serial_replay: tuple[str, ...]
    final_migration_ledger_rows: int
    final_table_count: int
    final_trigger_count: int
    final_maintenance_fence_rows: int
    final_maintenance_fence_state: str
    business_tables_empty: bool
    stored_routines_empty: bool
    scheduled_events_empty: bool
    schema_gate_passed: bool
    production_activation_allowed: bool
    actionable_output_allowed: bool
    snapshot: base_acceptance.EvidenceAcceptanceSnapshot

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


_CORE_EVIDENCE_TRIGGER_COUNT = 15
_AUTHORITY_TABLE_COUNT = 5
_AUTHORITY_TABLE_TRIGGER_COUNT = 15
_AUTHORITY_EVIDENCE_TRIGGER_COUNT = 2
_ACCOUNTING_TABLE_COUNT = 3
_ACCOUNTING_TRIGGER_COUNT = 9
_PRE_EVIDENCE_TABLE_COUNT = (
    EXPECTED_FINAL_TABLE_COUNT
    - EXPECTED_EVIDENCE_TABLE_COUNT
    - _AUTHORITY_TABLE_COUNT
    - _ACCOUNTING_TABLE_COUNT
)
_PRE_EVIDENCE_TRIGGER_COUNT = (
    EXPECTED_FINAL_TRIGGER_COUNT
    - _CORE_EVIDENCE_TRIGGER_COUNT
    - _AUTHORITY_TABLE_TRIGGER_COUNT
    - _AUTHORITY_EVIDENCE_TRIGGER_COUNT
    - _ACCOUNTING_TRIGGER_COUNT
)
_POST_BINDING_TABLE_COUNT = (
    _PRE_EVIDENCE_TABLE_COUNT + EXPECTED_EVIDENCE_TABLE_COUNT
)
_POST_GUARD_TRIGGER_COUNT = (
    _PRE_EVIDENCE_TRIGGER_COUNT + _CORE_EVIDENCE_TRIGGER_COUNT
)
_POST_AUTHORITY_TABLE_COUNT = _POST_BINDING_TABLE_COUNT + _AUTHORITY_TABLE_COUNT
_POST_AUTHORITY_TRIGGER_COUNT = (
    _POST_GUARD_TRIGGER_COUNT
    + _AUTHORITY_TABLE_TRIGGER_COUNT
    + _AUTHORITY_EVIDENCE_TRIGGER_COUNT
)

RECOVERY_SCENARIOS: Mapping[str, RecoveryScenario] = MappingProxyType(
    {
        "011-ddl-prefix": RecoveryScenario(
            name="011-ddl-prefix",
            target_version=EVIDENCE_BINDING_VERSION,
            faults=(
                RecoveryFaultSpec(
                    version=EVIDENCE_BINDING_VERSION,
                    phase=V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
                    committed_statement_count=2,
                    expected_partial_table_count=_PRE_EVIDENCE_TABLE_COUNT + 2,
                    expected_partial_trigger_count=_PRE_EVIDENCE_TRIGGER_COUNT,
                ),
            ),
        ),
        "012-drop-create-boundary": RecoveryScenario(
            name="012-drop-create-boundary",
            target_version=EVIDENCE_GUARD_VERSION,
            faults=(
                # Stop after the first committed DROP and before its paired
                # CREATE.  The next run must restore the missing trigger.
                RecoveryFaultSpec(
                    version=EVIDENCE_GUARD_VERSION,
                    phase=V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
                    committed_statement_count=1,
                    expected_partial_table_count=_POST_BINDING_TABLE_COUNT,
                    expected_partial_trigger_count=_PRE_EVIDENCE_TRIGGER_COUNT,
                ),
            ),
        ),
        "012-before-ledger": RecoveryScenario(
            name="012-before-ledger",
            target_version=EVIDENCE_GUARD_VERSION,
            faults=(
                RecoveryFaultSpec(
                    version=EVIDENCE_GUARD_VERSION,
                    phase=V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE,
                    committed_statement_count=None,
                    expected_partial_table_count=_POST_BINDING_TABLE_COUNT,
                    expected_partial_trigger_count=_POST_GUARD_TRIGGER_COUNT,
                ),
            ),
        ),
        "013-after-ddl": RecoveryScenario(
            name="013-after-ddl",
            target_version=EVIDENCE_NATURAL_KEY_VERSION,
            faults=(
                RecoveryFaultSpec(
                    version=EVIDENCE_NATURAL_KEY_VERSION,
                    phase=V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
                    committed_statement_count=1,
                    expected_partial_table_count=_POST_BINDING_TABLE_COUNT,
                    expected_partial_trigger_count=_POST_GUARD_TRIGGER_COUNT,
                ),
            ),
        ),
        "013-before-ledger": RecoveryScenario(
            name="013-before-ledger",
            target_version=EVIDENCE_NATURAL_KEY_VERSION,
            faults=(
                RecoveryFaultSpec(
                    version=EVIDENCE_NATURAL_KEY_VERSION,
                    phase=V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE,
                    committed_statement_count=None,
                    expected_partial_table_count=_POST_BINDING_TABLE_COUNT,
                    expected_partial_trigger_count=_POST_GUARD_TRIGGER_COUNT,
                ),
            ),
        ),
        "014-ddl-prefix": RecoveryScenario(
            name="014-ddl-prefix",
            target_version=EVIDENCE_AUTHORITY_VERSION,
            faults=(
                RecoveryFaultSpec(
                    version=EVIDENCE_AUTHORITY_VERSION,
                    phase=V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
                    committed_statement_count=2,
                    expected_partial_table_count=_POST_BINDING_TABLE_COUNT + 2,
                    expected_partial_trigger_count=_POST_GUARD_TRIGGER_COUNT,
                ),
            ),
        ),
        "014-drop-create-boundary": RecoveryScenario(
            name="014-drop-create-boundary",
            target_version=EVIDENCE_AUTHORITY_VERSION,
            faults=(
                RecoveryFaultSpec(
                    version=EVIDENCE_AUTHORITY_VERSION,
                    phase=V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
                    committed_statement_count=6,
                    expected_partial_table_count=_POST_AUTHORITY_TABLE_COUNT,
                    expected_partial_trigger_count=_POST_GUARD_TRIGGER_COUNT,
                ),
            ),
        ),
        "014-before-ledger": RecoveryScenario(
            name="014-before-ledger",
            target_version=EVIDENCE_AUTHORITY_VERSION,
            faults=(
                RecoveryFaultSpec(
                    version=EVIDENCE_AUTHORITY_VERSION,
                    phase=V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE,
                    committed_statement_count=None,
                    expected_partial_table_count=_POST_AUTHORITY_TABLE_COUNT,
                    expected_partial_trigger_count=_POST_AUTHORITY_TRIGGER_COUNT,
                ),
            ),
        ),
        "015-drop-create-boundary": RecoveryScenario(
            name="015-drop-create-boundary",
            target_version=EVIDENCE_ACCOUNTING_VERSION,
            faults=(
                RecoveryFaultSpec(
                    version=EVIDENCE_ACCOUNTING_VERSION,
                    phase=V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
                    committed_statement_count=4,
                    expected_partial_table_count=EXPECTED_FINAL_TABLE_COUNT,
                    expected_partial_trigger_count=_POST_AUTHORITY_TRIGGER_COUNT,
                ),
            ),
        ),
        "015-ddl-prefix": RecoveryScenario(
            name="015-ddl-prefix",
            target_version=EVIDENCE_ACCOUNTING_VERSION,
            faults=(
                RecoveryFaultSpec(
                    version=EVIDENCE_ACCOUNTING_VERSION,
                    phase=V2_MIGRATION_FAULT_AFTER_DDL_COMMIT,
                    committed_statement_count=2,
                    expected_partial_table_count=_POST_AUTHORITY_TABLE_COUNT + 2,
                    expected_partial_trigger_count=_POST_AUTHORITY_TRIGGER_COUNT,
                ),
            ),
        ),
        "015-before-ledger": RecoveryScenario(
            name="015-before-ledger",
            target_version=EVIDENCE_ACCOUNTING_VERSION,
            faults=(
                RecoveryFaultSpec(
                    version=EVIDENCE_ACCOUNTING_VERSION,
                    phase=V2_MIGRATION_FAULT_BEFORE_LEDGER_WRITE,
                    committed_statement_count=None,
                    expected_partial_table_count=EXPECTED_FINAL_TABLE_COUNT,
                    expected_partial_trigger_count=EXPECTED_FINAL_TRIGGER_COUNT,
                ),
            ),
        ),
    }
)


def _migration(version: str) -> Mapping[str, object]:
    try:
        return next(item for item in MIGRATIONS if str(item["version"]) == version)
    except StopIteration as exc:
        raise RuntimeError(f"recovery target migration is missing: {version}") from exc


def _assert_recovery_contract() -> None:
    base_acceptance._assert_frozen_migration_contract()
    if len(base_acceptance.EXPECTED_TABLES) != EXPECTED_FINAL_TABLE_COUNT:
        raise RuntimeError("recovery contract requires exactly 52 final tables")
    if (
        len(base_acceptance.EXPECTED_MIGRATION_OWNED_TABLES)
        != EXPECTED_MIGRATION_OWNED_TABLE_COUNT
        or len(base_acceptance.EXPECTED_RUNNER_BOOTSTRAP_TABLES)
        != EXPECTED_RUNNER_BOOTSTRAP_TABLE_COUNT
        or base_acceptance.EXPECTED_RUNNER_BOOTSTRAP_TABLES
        != frozenset({V2_EVIDENCE_MAINTENANCE_FENCE_TABLE})
    ):
        raise RuntimeError(
            "recovery contract requires 51 migration-owned tables plus "
            "one runner bootstrap maintenance-fence table"
        )
    if len(base_acceptance.EXPECTED_TRIGGER_NAMES) != EXPECTED_FINAL_TRIGGER_COUNT:
        raise RuntimeError("recovery contract requires exactly 43 final triggers")
    binding_statements = tuple(_migration(EVIDENCE_BINDING_VERSION)["statements"])
    guard_statements = tuple(_migration(EVIDENCE_GUARD_VERSION)["statements"])
    natural_key_statements = tuple(
        _migration(EVIDENCE_NATURAL_KEY_VERSION)["statements"]
    )
    authority_statements = tuple(
        _migration(EVIDENCE_AUTHORITY_VERSION)["statements"]
    )
    accounting_statements = tuple(
        _migration(EVIDENCE_ACCOUNTING_VERSION)["statements"]
    )
    if len(binding_statements) < 2:
        raise RuntimeError("evidence migration 011 no longer has a DDL prefix")
    if len(guard_statements) < 2:
        raise RuntimeError("evidence migration 012 no longer has DROP/CREATE pairs")
    if not guard_statements[0].strip().upper().startswith("DROP TRIGGER"):
        raise RuntimeError("evidence migration 012 no longer starts with DROP TRIGGER")
    if "CREATE TRIGGER" not in guard_statements[1].upper():
        raise RuntimeError("evidence migration 012 DROP/CREATE boundary drifted")
    if len(natural_key_statements) != 1 or not natural_key_statements[0].strip().upper().startswith(
        "ALTER TABLE"
    ):
        raise RuntimeError("evidence migration 013 recovery boundary drifted")
    if (
        len(authority_statements) != 39
        or any(
            not item.strip().upper().startswith("CREATE TABLE")
            for item in authority_statements[:5]
        )
        or not authority_statements[5].strip().upper().startswith("DROP TRIGGER")
        or "CREATE TRIGGER" not in authority_statements[6].upper()
    ):
        raise RuntimeError("evidence migration 014 recovery boundaries drifted")
    if (
        len(accounting_statements) != 21
        or any(
            not item.strip().upper().startswith("CREATE TABLE")
            for item in accounting_statements[:3]
        )
        or not accounting_statements[3].strip().upper().startswith("DROP TRIGGER")
        or "CREATE TRIGGER" not in accounting_statements[4].upper()
    ):
        raise RuntimeError("evidence migration 015 recovery boundaries drifted")
    for scenario in RECOVERY_SCENARIOS.values():
        if not scenario.faults:
            raise RuntimeError("recovery scenario must contain an interruption")
        if any(item.version != scenario.target_version for item in scenario.faults):
            raise RuntimeError("recovery scenario target and fault version differ")


def require_recovery_scenario(value: object) -> RecoveryScenario:
    if type(value) is not str or value not in RECOVERY_SCENARIOS:
        raise ValueError(
            "recovery scenario must be one of: "
            + ", ".join(sorted(RECOVERY_SCENARIOS))
        )
    return RECOVERY_SCENARIOS[value]


def _identity(
    connection: Connection,
    *,
    expected_database: str,
    expected_server_uuid: str,
    expected_identity: tuple[str, str, str, str] | None,
) -> tuple[str, str, str, str]:
    observed = base_acceptance._server_identity_from_connection(
        connection,
        expected_database,
        expected_server_uuid,
    )
    if expected_identity is not None and observed != expected_identity:
        raise RuntimeError("recovery acceptance connection identity changed")
    return observed


def _expected_ledger_prefix(version: str) -> tuple[str, ...]:
    versions = tuple(
        item_version
        for item_version, _checksum in base_acceptance.FROZEN_EXPECTED_MIGRATIONS
    )
    try:
        boundary = versions.index(version)
    except ValueError as exc:
        raise RuntimeError("fault target is absent from frozen migration ledger") from exc
    return versions[:boundary]


def _inspect_interrupted_state(
    connection: Connection,
    spec: RecoveryFaultSpec,
    *,
    attempt: int,
) -> RecoveryInterruptionObservation:
    ledger_versions = tuple(
        str(item)
        for item in connection.execute(
            text("SELECT version FROM schema_migration_v2 ORDER BY version")
        ).scalars()
    )
    expected_ledger = _expected_ledger_prefix(spec.version)
    if ledger_versions != expected_ledger:
        raise RuntimeError(
            "interrupted migration ledger is not the exact pre-target prefix"
        )
    inventory = base_acceptance._schema_object_inventory_from_connection(connection)
    tables = inventory["tables"]
    routines = inventory["routines"]
    events = inventory["events"]
    triggers = frozenset(
        str(item).lower()
        for item in connection.execute(
            text(
                "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS "
                "WHERE TRIGGER_SCHEMA = DATABASE() ORDER BY TRIGGER_NAME"
            )
        ).scalars()
    )
    if not tables <= base_acceptance.EXPECTED_TABLES:
        raise RuntimeError("interruption created an unexpected table")
    if not triggers <= base_acceptance.EXPECTED_TRIGGER_NAMES:
        raise RuntimeError("interruption created an unexpected trigger")
    if routines or events:
        raise RuntimeError("interruption left a stored routine or scheduled event")
    if len(tables) != spec.expected_partial_table_count:
        raise RuntimeError(
            "interruption did not stop at the expected table DDL boundary"
        )
    if len(triggers) != spec.expected_partial_trigger_count:
        raise RuntimeError(
            "interruption did not stop at the expected trigger DDL boundary"
        )
    maintenance_fence_rows, maintenance_fence_state = (
        base_acceptance._maintenance_fence_state_from_connection(
            connection,
            expected_state=V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE,
        )
    )
    nonempty_business_tables: list[str] = []
    for table in sorted(tables):
        if base_acceptance._SAFE_SQL_IDENTIFIER_RE.fullmatch(table) is None:
            raise RuntimeError("interrupted table inventory contains an unsafe name")
        count = connection.execute(
            text(f"SELECT COUNT(*) FROM `{table}`")
        ).scalar()
        if (
            table
            not in {
                "schema_migration_v2",
                V2_EVIDENCE_MAINTENANCE_FENCE_TABLE,
            }
            and int(count or 0) != 0
        ):
            nonempty_business_tables.append(table)
    if nonempty_business_tables:
        raise RuntimeError(
            "interrupted recovery acceptance contains business rows: "
            + ", ".join(nonempty_business_tables)
        )
    return RecoveryInterruptionObservation(
        attempt=attempt,
        version=spec.version,
        phase=spec.phase,
        committed_statement_count=spec.committed_statement_count,
        migration_ledger_versions=ledger_versions,
        observed_table_count=len(tables),
        observed_trigger_count=len(triggers),
        maintenance_fence_rows=maintenance_fence_rows,
        maintenance_fence_state=maintenance_fence_state,
        business_tables_empty=True,
        stored_routines_empty=True,
        scheduled_events_empty=True,
    )


def _validate_recovery_statuses(
    results: tuple[V2MigrationResult, ...],
    target_version: str,
) -> tuple[V2MigrationResult, ...]:
    validated = base_acceptance._validate_run(
        results,
        allowed_statuses=frozenset({"applied", "exists"}),
    )
    versions = tuple(
        version for version, _checksum in base_acceptance.FROZEN_EXPECTED_MIGRATIONS
    )
    target_index = versions.index(target_version)
    expected = (
        ("exists",) * target_index
        + ("applied",) * (len(versions) - target_index)
    )
    if base_acceptance._result_statuses(validated) != expected:
        raise RuntimeError("recovery migration statuses do not match the fault boundary")
    return validated


def _validate_final_snapshot(
    snapshot: base_acceptance.EvidenceAcceptanceSnapshot,
) -> None:
    if snapshot.migration_ledger_rows != base_acceptance.EXPECTED_MIGRATION_COUNT:
        raise RuntimeError("recovery did not produce the complete migration ledger")
    if len(snapshot.observed_tables) != EXPECTED_FINAL_TABLE_COUNT:
        raise RuntimeError("recovery did not produce exactly 52 tables")
    if len(snapshot.observed_triggers) != EXPECTED_FINAL_TRIGGER_COUNT:
        raise RuntimeError("recovery did not produce exactly 43 triggers")
    if len(snapshot.evidence_tables) != EXPECTED_EVIDENCE_TABLE_COUNT:
        raise RuntimeError("recovery did not produce exactly 5 evidence tables")
    if len(snapshot.evidence_triggers) != EXPECTED_EVIDENCE_TRIGGER_COUNT:
        raise RuntimeError("recovery did not produce exactly 17 evidence triggers")
    if (
        snapshot.maintenance_fence_rows != 1
        or snapshot.maintenance_fence_state
        != V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
    ):
        raise RuntimeError(
            "recovery maintenance fence must finish with exactly one "
            "INACTIVE row"
        )
    if not (
        snapshot.business_tables_empty
        and snapshot.stored_routines_empty
        and snapshot.scheduled_events_empty
        and snapshot.metadata_preflight_passed
    ):
        raise RuntimeError("recovery final structural or row-state checks failed")
    if (
        snapshot.production_activation_allowed
        or snapshot.actionable_output_allowed
    ):
        raise RuntimeError(
            "recovery acceptance cannot enable production or actionable output"
        )


def run_mysql_recovery_acceptance(
    url: str,
    *,
    expected_server_uuid: str,
    scenario: str,
    tls_config: MySQLAcceptanceTLSConfig | None = None,
) -> RecoveryAcceptanceReport:
    """Interrupt and recover one scenario in a dedicated, initially empty DB."""

    _assert_recovery_contract()
    selected = require_recovery_scenario(scenario)
    safe_url = base_acceptance.require_dedicated_test_url(url)
    expected_uuid = base_acceptance.require_expected_server_uuid(
        expected_server_uuid
    )
    expected_database = str(make_url(safe_url).database)
    engine = create_tool_engine(
        safe_url,
        tls_config=tls_config,
        future=True,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    identity: tuple[str, str, str, str] | None = None
    observations: list[RecoveryInterruptionObservation] = []
    try:
        for attempt, spec in enumerate(selected.faults, start=1):
            hook = spec.build_hook()
            with engine.connect() as connection:
                observed_identity = _identity(
                    connection,
                    expected_database=expected_database,
                    expected_server_uuid=expected_uuid,
                    expected_identity=identity,
                )
                if identity is None:
                    identity = observed_identity
                    base_acceptance._assert_empty_on_connection(
                        connection,
                        mode=f"recovery-{selected.name}",
                    )
                connection.rollback()
                try:
                    run_v2_migrations(
                        engine,
                        allow_execution_evidence=True,
                        connection=connection,
                        acceptance_fault_hook=hook,
                    )
                except V2MigrationAcceptanceFault as exc:
                    if (
                        exc.version != spec.version
                        or exc.phase != spec.phase
                        or exc.committed_statement_count
                        != spec.committed_statement_count
                        or not hook.triggered
                    ):
                        raise RuntimeError(
                            "migration interrupted at an unexpected fault point"
                        ) from exc
                else:
                    raise RuntimeError("migration acceptance fault did not fire")
                connection.rollback()
                observations.append(
                    _inspect_interrupted_state(
                        connection,
                        spec,
                        attempt=attempt,
                    )
                )
                connection.rollback()
            # Close every checked-in DBAPI connection so the retry cannot reuse
            # the checkout that observed the intentional failure.
            engine.dispose()

        if identity is None:  # pragma: no cover - scenario contract is non-empty
            raise RuntimeError("recovery acceptance did not establish identity")
        with engine.connect() as connection:
            _identity(
                connection,
                expected_database=expected_database,
                expected_server_uuid=expected_uuid,
                expected_identity=identity,
            )
            connection.rollback()
            recovery = _validate_recovery_statuses(
                tuple(
                    run_v2_migrations(
                        engine,
                        allow_execution_evidence=True,
                        connection=connection,
                    )
                ),
                selected.target_version,
            )
            connection.rollback()
        engine.dispose()

        with engine.connect() as connection:
            _identity(
                connection,
                expected_database=expected_database,
                expected_server_uuid=expected_uuid,
                expected_identity=identity,
            )
            connection.rollback()
            replay = base_acceptance._validate_run(
                run_v2_migrations(
                    engine,
                    allow_execution_evidence=True,
                    connection=connection,
                ),
                allowed_statuses=frozenset({"exists"}),
            )
            if base_acceptance._result_statuses(replay) != (
                "exists",
            ) * base_acceptance.EXPECTED_MIGRATION_COUNT:
                raise RuntimeError("post-recovery replay was not fully idempotent")
            connection.rollback()
            snapshot = base_acceptance._post_migration_snapshot(
                engine,
                connection=connection,
            )
            connection.rollback()
        _validate_final_snapshot(snapshot)
    finally:
        engine.dispose()

    database, server_version, server_uuid, version_comment = identity
    return RecoveryAcceptanceReport(
        mode="ddl-interruption-recovery",
        scenario=selected.name,
        database=database,
        server_version=server_version,
        server_version_comment=version_comment,
        server_uuid=server_uuid,
        least_privilege_attested=True,
        started_empty=True,
        interruption_count=len(observations),
        interruptions=tuple(observations),
        recovery_migration=base_acceptance._result_statuses(recovery),
        serial_replay=base_acceptance._result_statuses(replay),
        final_migration_ledger_rows=snapshot.migration_ledger_rows,
        final_table_count=len(snapshot.observed_tables),
        final_trigger_count=len(snapshot.observed_triggers),
        final_maintenance_fence_rows=snapshot.maintenance_fence_rows,
        final_maintenance_fence_state=snapshot.maintenance_fence_state,
        business_tables_empty=snapshot.business_tables_empty,
        stored_routines_empty=snapshot.stored_routines_empty,
        scheduled_events_empty=snapshot.scheduled_events_empty,
        schema_gate_passed=snapshot.metadata_preflight_passed,
        production_activation_allowed=False,
        actionable_output_allowed=False,
        snapshot=snapshot,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run one V2 evidence 011-015 interruption-recovery scenario in "
            "a separately provisioned empty Oracle MySQL "
            f"{isolated_acceptance_versions_label()} database; "
            "require ACTIVE fencing while interrupted and one INACTIVE row "
            "after recovery without authorizing production/actionable output."
        )
    )
    parser.add_argument(
        "--scenario",
        required=True,
        choices=tuple(sorted(RECOVERY_SCENARIOS)),
    )
    parser.add_argument("--url-env", default=DEFAULT_URL_ENV)
    parser.add_argument(
        "--server-uuid-env",
        default=DEFAULT_SERVER_UUID_ENV,
    )
    parser.add_argument(
        "--ssl-ca-env",
        default=DEFAULT_SSL_CA_ENV,
        help="dedicated V2 evidence TEST/CI variable containing the SSL CA file",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    report = run_mysql_recovery_acceptance(
        base_acceptance.resolve_test_url(args.url_env),
        expected_server_uuid=base_acceptance.resolve_server_uuid(
            args.server_uuid_env
        ),
        scenario=args.scenario,
        tls_config=resolve_mysql_acceptance_tls_config(
            "V2_EVIDENCE",
            args.ssl_ca_env,
        ),
    )
    print(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised via subprocess
    raise SystemExit(main())


__all__ = [
    "DEFAULT_SERVER_UUID_ENV",
    "DEFAULT_SSL_CA_ENV",
    "DEFAULT_URL_ENV",
    "EXPECTED_EVIDENCE_TABLE_COUNT",
    "EXPECTED_EVIDENCE_TRIGGER_COUNT",
    "EXPECTED_FINAL_TABLE_COUNT",
    "EXPECTED_FINAL_TRIGGER_COUNT",
    "EXPECTED_MIGRATION_OWNED_TABLE_COUNT",
    "EXPECTED_RUNNER_BOOTSTRAP_TABLE_COUNT",
    "RECOVERY_SCENARIOS",
    "RecoveryAcceptanceReport",
    "RecoveryFaultSpec",
    "RecoveryInterruptionObservation",
    "RecoveryScenario",
    "main",
    "require_recovery_scenario",
    "run_mysql_recovery_acceptance",
]
