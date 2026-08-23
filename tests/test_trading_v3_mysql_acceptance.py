from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine

import server.db.migrations_v3 as migrations_v3
from server.db.migrations_v3 import (
    FORWARD_EXIT_ALLOCATION_DDL,
    FORWARD_EXIT_ALLOCATION_RDS_DDL,
    FORWARD_EXIT_ALLOCATION_MIGRATION_VERSION,
    FORWARD_STRATEGY_VERSION_DDL,
    FORWARD_STRATEGY_VERSION_RDS_DDL,
    FORWARD_STRATEGY_VERSION_MIGRATION_VERSION,
    V2_RAW_LEDGER_IMMUTABILITY_DDL,
    V2_RAW_LEDGER_IMMUTABILITY_RDS_DDL,
    V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION,
    MIGRATION_PROGRESS_TABLE_DDL,
    MIGRATION_TABLE_DDL,
    MIGRATIONS,
    SHADOW_INTELLIGENCE_RDS_DDL,
    HORIZON_CANDIDATE_LEDGER_RDS_DDL,
    HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION,
    HORIZON_PROTOCOL_V2_RDS_DDL,
    HORIZON_PROTOCOL_V2_MIGRATION_VERSION,
    V3MigrationAcceptanceFaultHook,
    V3_PROJECTION_OUTBOX_MIGRATION_VERSION,
    run_v3_migrations,
)
from server.trading_v3.horizon_candidate_ledger_schema import (
    validate_horizon_candidate_ledger_server,
)
from tools import trading_v3_mysql_acceptance as acceptance


def test_frozen_v3_acceptance_contract_matches_source():
    acceptance._assert_frozen_contract()
    assert len(acceptance.FROZEN_EXPECTED_V3_MIGRATIONS) == 27
    assert acceptance.FROZEN_EXPECTED_V3_MIGRATIONS[-1] == (
        FORWARD_EXIT_ALLOCATION_MIGRATION_VERSION,
        "f2e99ea79df11e578e17298ebd9a829cc0715d334708ca760bd99970a6a5d460",
        1,
    )
    assert acceptance.FROZEN_EXPECTED_V3_MIGRATIONS[-2] == (
        V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        0,
    )
    assert acceptance.FROZEN_EXPECTED_V3_MIGRATIONS[-3] == (
        FORWARD_STRATEGY_VERSION_MIGRATION_VERSION,
        "1804a2d2c3473e98c1be77d03d324e61cb5cdb5682e7d87cf647841218b756e6",
        3,
    )
    assert acceptance.FROZEN_EXPECTED_V3_MIGRATIONS[-4] == (
        HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION,
        "82e113beb6328c8590e66173ea2aca0b832650ec3796539a4ba9bd37bc29ab05",
        1,
    )
    assert acceptance.FROZEN_EXPECTED_V3_MIGRATIONS[-5] == (
        HORIZON_PROTOCOL_V2_MIGRATION_VERSION,
        "6eb0c66266b2c9103f2d65bf932dc1576d16c9b249a78a2704376b4f19e32a3b",
        2,
    )
    assert acceptance.FROZEN_EXPECTED_V3_MIGRATIONS[-6] == (
        V3_PROJECTION_OUTBOX_MIGRATION_VERSION,
        "5f53bf5258705e410b93db2b5034bbf9683ebe03b17f854625f6854fb55f7e78",
        4,
    )
    assert len(acceptance.FROZEN_V3_TABLES) == 30


def test_shadow_and_horizon_protocol_migrations_are_rds_portable():
    old = next(
        item for item in MIGRATIONS
        if item["version"] == "20260804_000_shadow_intelligence_runtime"
    )
    old_statements = tuple(old["statements"])
    assert old_statements == SHADOW_INTELLIGENCE_RDS_DDL
    assert len(old_statements) == 10
    assert migrations_v3._checksum(old_statements) == (
        "25f3a14c56c3cb7ec701192cb4d736eaad6c2592c6ad9e4b698081c549f1c3c6"
    )
    assert all("TRIGGER" not in statement.upper() for statement in old_statements)
    migration = next(
        item for item in MIGRATIONS
        if item["version"] == HORIZON_PROTOCOL_V2_MIGRATION_VERSION
    )
    statements = tuple(str(item) for item in migration["statements"])
    assert statements == HORIZON_PROTOCOL_V2_RDS_DDL
    assert len(statements) == 2
    assert statements[0].lstrip().upper().startswith("ALTER TABLE")
    assert statements[1].lstrip().upper().startswith("CREATE INDEX")
    assert all("TRIGGER" not in statement.upper() for statement in statements)


def test_horizon_candidate_ledger_migration_is_additive_and_rds_portable():
    migration = next(
        item for item in MIGRATIONS
        if item["version"] == HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION
    )
    statements = tuple(str(item) for item in migration["statements"])
    assert statements == HORIZON_CANDIDATE_LEDGER_RDS_DDL
    assert len(statements) == 1
    assert statements[0].lstrip().upper().startswith("ALTER TABLE")
    assert "DROP CHECK chk_v3_horizon_model_protocol_projection" in statements[0]
    assert statements[0].count("ADD COLUMN") == 5
    assert "TRIGGER" not in statements[0].upper()


def test_forward_strategy_version_migration_is_additive_and_rds_portable():
    migration = next(
        item for item in MIGRATIONS
        if item["version"] == FORWARD_STRATEGY_VERSION_MIGRATION_VERSION
    )
    statements = tuple(str(item) for item in migration["statements"])
    assert statements == FORWARD_STRATEGY_VERSION_RDS_DDL
    assert len(statements) == 3
    assert "ADD COLUMN strategy_version VARCHAR(160) NOT NULL DEFAULT ''" in (
        statements[0]
    )
    assert "AFTER strategy_key" in statements[0]
    assert "idx_v3_forward_strategy_version" in statements[1]
    assert (
        "strategy_key, strategy_version, evidence_status, exit_at"
        in " ".join(statements[1].split())
    )
    backfill = statements[2]
    assert "UPDATE st_forward_trade_evidence_v3 e" in backfill
    assert "JOIN st_decision_run_v3 r" in backfill
    assert "'$.primary_forecast_id'" in backfill
    assert "'$.ownership_hash'" in backfill
    assert "SET e.strategy_version = CONCAT(" in backfill
    assert all("TRIGGER" not in statement.upper() for statement in statements)
    assert len(FORWARD_STRATEGY_VERSION_DDL) == 7


def test_raw_fill_and_cash_ledger_migration_is_rds_portable_noop():
    migration = next(
        item for item in MIGRATIONS
        if item["version"] == V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION
    )
    statements = tuple(str(item) for item in migration["statements"])
    assert statements == V2_RAW_LEDGER_IMMUTABILITY_RDS_DDL == ()
    assert len(V2_RAW_LEDGER_IMMUTABILITY_DDL) == 8


def test_forward_exit_allocation_migration_is_normalized_and_rds_portable():
    migration = next(
        item for item in MIGRATIONS
        if item["version"] == FORWARD_EXIT_ALLOCATION_MIGRATION_VERSION
    )
    statements = tuple(str(item) for item in migration["statements"])
    assert statements == FORWARD_EXIT_ALLOCATION_RDS_DDL
    assert len(statements) == 1
    table = statements[0]
    assert "CREATE TABLE IF NOT EXISTS st_forward_exit_allocation_v3" in table
    assert "UNIQUE KEY uk_v3_forward_exit_evidence_fill" in table
    assert "UNIQUE KEY uk_v3_forward_exit_fill_sequence" in table
    assert "UNIQUE KEY uk_v3_forward_exit_fill_entry" in table
    assert "KEY idx_v3_forward_exit_evidence" in table
    assert "attribution_status VARCHAR(32) NOT NULL" in table
    assert "allocation_sequence BIGINT NOT NULL" in table
    assert "FOREIGN KEY (evidence_id)" in table
    assert "FOREIGN KEY (exit_fill_id)" in table
    assert "TRIGGER" not in table.upper()
    assert len(FORWARD_EXIT_ALLOCATION_DDL) == 5


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


@pytest.mark.parametrize("version", ("5.7.38-log", "8.0.45", "8.4.12"))
def test_candidate_ledger_migration_rejects_non_frozen_mysql84(version):
    class Result:
        def __init__(self, value):
            self.value = value

        def scalar(self):
            return self.value

    class Connection:
        def execute(self, statement):
            return Result(
                "MySQL Community Server (GPL)"
                if "VERSION_COMMENT" in str(statement).upper()
                else version
            )

    with pytest.raises(RuntimeError, match="candidate-ledger migration"):
        validate_horizon_candidate_ledger_server(Connection())  # type: ignore[arg-type]


def test_candidate_ledger_migration_accepts_frozen_mysql8411():
    class Result:
        def __init__(self, value):
            self.value = value

        def scalar(self):
            return self.value

    class Connection:
        def execute(self, statement):
            return Result(
                "MySQL Enterprise Server"
                if "VERSION_COMMENT" in str(statement).upper()
                else "8.4.11-commercial"
            )

    validate_horizon_candidate_ledger_server(Connection())  # type: ignore[arg-type]


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
    assert len(acceptance._expected_final_tables()) == 86
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


def test_v3_dry_run_rejects_trigger_executor_before_any_write():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    try:
        with pytest.raises(ValueError, match="unavailable for dry-run"):
            run_v3_migrations(
                engine,
                dry_run=True,
                trigger_ddl_executor=lambda _statement: None,
            )
    finally:
        engine.dispose()


def test_v3_runner_delegates_only_missing_create_trigger(monkeypatch):
    table_ddl = "CREATE TABLE st_callback_test (id BIGINT PRIMARY KEY)"
    trigger_ddl = (
        "CREATE TRIGGER trg_callback_test BEFORE UPDATE ON st_callback_test "
        "FOR EACH ROW SET NEW.id=OLD.id"
    )
    statements = (table_ddl, trigger_ddl)
    checksum = migrations_v3._checksum(statements)
    executed: list[str] = []
    delegated: list[str] = []
    progress: list[tuple[int, int]] = []
    trigger_installed = False
    applied_reads = 0

    class _Connection:
        def execute(self, statement, _params=None):
            executed.append(str(statement).strip())
            return SimpleNamespace()

        def commit(self):
            return None

        def rollback(self):
            return None

    connection = _Connection()

    def applied_record(_connection, _version):
        nonlocal applied_reads
        applied_reads += 1
        if applied_reads == 1:
            return None
        return SimpleNamespace(
            checksum=checksum,
            statement_count=len(statements),
        )

    def already_applied(_connection, statement):
        if str(statement) == trigger_ddl:
            return trigger_installed
        return False

    def trigger_executor(statement):
        nonlocal trigger_installed
        delegated.append(statement)
        trigger_installed = True

    monkeypatch.setattr(migrations_v3, "_mysql_dialect", lambda _engine: True)
    monkeypatch.setattr(migrations_v3, "MIGRATIONS", ({
        "version": "test_trigger_callback",
        "statements": statements,
    },))
    monkeypatch.setattr(migrations_v3, "_validate_migration_server", lambda *_: None)
    monkeypatch.setattr(migrations_v3, "_ensure_migration_metadata", lambda *_: None)
    monkeypatch.setattr(migrations_v3, "_applied_record", applied_record)
    monkeypatch.setattr(migrations_v3, "_progress_count", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        migrations_v3,
        "_ddl_statement_already_applied",
        already_applied,
    )
    monkeypatch.setattr(
        migrations_v3,
        "_mark_progress",
        lambda _connection, *, previous_count, next_count, **_kwargs: (
            progress.append((previous_count, next_count))
        ),
    )
    monkeypatch.setattr(
        migrations_v3,
        "_validate_applied_migration",
        lambda *_args, **_kwargs: None,
    )

    result = migrations_v3._run_v3_migrations_unlocked(
        SimpleNamespace(),
        dry_run=False,
        connection=connection,
        acceptance_fault_hook=None,
        trigger_ddl_executor=trigger_executor,
    )

    assert delegated == [trigger_ddl]
    assert table_ddl in executed
    assert trigger_ddl not in executed
    assert progress == [(0, 1), (1, 2)]
    assert [(item.version, item.status, item.statement_count) for item in result] == [
        ("test_trigger_callback", "applied", 2)
    ]


def test_v3_runner_refuses_missing_trigger_without_explicit_executor(monkeypatch):
    trigger_ddl = (
        "CREATE TRIGGER trg_callback_test BEFORE UPDATE ON st_callback_test "
        "FOR EACH ROW SET NEW.id=OLD.id"
    )

    class _Connection:
        def execute(self, _statement, _params=None):
            return SimpleNamespace()

        def commit(self):
            return None

        def rollback(self):
            return None

    monkeypatch.setattr(migrations_v3, "_mysql_dialect", lambda _engine: True)
    monkeypatch.setattr(migrations_v3, "MIGRATIONS", ({
        "version": "test_missing_trigger_executor",
        "statements": (trigger_ddl,),
    },))
    monkeypatch.setattr(migrations_v3, "_validate_migration_server", lambda *_: None)
    monkeypatch.setattr(migrations_v3, "_ensure_migration_metadata", lambda *_: None)
    monkeypatch.setattr(migrations_v3, "_applied_record", lambda *_: None)
    monkeypatch.setattr(migrations_v3, "_progress_count", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(
        migrations_v3,
        "_ddl_statement_already_applied",
        lambda *_args: False,
    )

    with pytest.raises(RuntimeError, match="explicit trigger DDL executor"):
        migrations_v3._run_v3_migrations_unlocked(
            SimpleNamespace(),
            dry_run=False,
            connection=_Connection(),
            acceptance_fault_hook=None,
        )


def test_v3_mysql_dry_run_validates_ledger_count_and_physical_contract(
    monkeypatch,
):
    statements = ("CREATE TABLE st_read_only_contract (id BIGINT)",)
    version = "test_read_only_contract"
    checksum = migrations_v3._checksum(statements)
    validations: list[tuple[str, bool]] = []

    class _Rows:
        def mappings(self):
            return self

        def first(self):
            return {"checksum": checksum, "statement_count": 1}

    class _Connection:
        def execute(self, sql, _params=None):
            rendered = str(sql)
            assert "SELECT checksum, statement_count" in rendered
            return _Rows()

        def close(self):
            return None

    class _Engine:
        def connect(self):
            return _Connection()

    monkeypatch.setattr(migrations_v3, "_mysql_dialect", lambda _engine: True)
    monkeypatch.setattr(migrations_v3, "_table_exists", lambda *_args: True)
    monkeypatch.setattr(
        migrations_v3,
        "MIGRATIONS",
        ({"version": version, "statements": statements},),
    )
    monkeypatch.setattr(
        migrations_v3,
        "_validate_applied_migration",
        lambda _connection, *, version, reconcile_legacy_rows: validations.append(
            (version, reconcile_legacy_rows)
        ),
    )

    results = migrations_v3._run_v3_migrations_unlocked(
        _Engine(),  # type: ignore[arg-type]
        dry_run=True,
        connection=None,
        acceptance_fault_hook=None,
    )

    assert [(item.version, item.status) for item in results] == [
        (version, "exists")
    ]
    assert validations == [(version, False)]


def test_v3_mysql_dry_run_rejects_statement_count_drift(monkeypatch):
    statements = ("CREATE TABLE st_read_only_contract (id BIGINT)",)
    checksum = migrations_v3._checksum(statements)

    class _Rows:
        def mappings(self):
            return self

        def first(self):
            return {"checksum": checksum, "statement_count": 2}

    class _Connection:
        def execute(self, _sql, _params=None):
            return _Rows()

        def close(self):
            return None

    class _Engine:
        def connect(self):
            return _Connection()

    monkeypatch.setattr(migrations_v3, "_mysql_dialect", lambda _engine: True)
    monkeypatch.setattr(migrations_v3, "_table_exists", lambda *_args: True)
    monkeypatch.setattr(
        migrations_v3,
        "MIGRATIONS",
        ({"version": "test_count_drift", "statements": statements},),
    )

    with pytest.raises(RuntimeError, match="contract changed"):
        migrations_v3._run_v3_migrations_unlocked(
            _Engine(),  # type: ignore[arg-type]
            dry_run=True,
            connection=None,
            acceptance_fault_hook=None,
        )


def test_trigger_metadata_normalization_preserves_sqlstate_value_literals():
    normalized = migrations_v3._normalized_trigger_sql(
        "BEGIN SIGNAL SQLSTATE VALUE '45000' "
        "SET MESSAGE_TEXT='Keep SQLSTATE VALUE  exactly'; END"
    )

    assert "signal sqlstate '45000'" in normalized
    assert "'Keep SQLSTATE VALUE  exactly'" in normalized


def test_outbox_acceptance_fault_hook_is_narrow_and_one_shot():
    hook = V3MigrationAcceptanceFaultHook.after_outbox_ddl_commit(2)
    assert hook.version == V3_PROJECTION_OUTBOX_MIGRATION_VERSION
    assert hook.committed_statement_count == 2
    with pytest.raises(ValueError, match="incomplete"):
        V3MigrationAcceptanceFaultHook.after_outbox_ddl_commit(4)
    with pytest.raises(ValueError, match="unsupported"):
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


@pytest.mark.parametrize("committed_count", (1,))
def test_horizon_protocol_v2_fault_hook_covers_recoverable_ddl_boundaries(
    committed_count,
):
    hook = V3MigrationAcceptanceFaultHook.after_horizon_protocol_v2_ddl_commit(
        committed_count
    )
    assert hook.version == HORIZON_PROTOCOL_V2_MIGRATION_VERSION
    hook.validate()
    with pytest.raises(
        migrations_v3.V3MigrationAcceptanceFault,
        match="after committed DDL",
    ):
        hook.raise_if_matches(
            version=HORIZON_PROTOCOL_V2_MIGRATION_VERSION,
            statement_count=committed_count,
        )
    assert hook.triggered is True


@pytest.mark.parametrize("unsupported_count", (0, 2, 3, 4, 6, 8, 10))
def test_horizon_protocol_v2_fault_hook_rejects_non_firing_boundaries(
    unsupported_count,
):
    with pytest.raises(ValueError, match="recoverable committed DDL"):
        V3MigrationAcceptanceFaultHook.after_horizon_protocol_v2_ddl_commit(
            unsupported_count
        )


@pytest.mark.parametrize("committed_count", (1,))
def test_horizon_candidate_ledger_fault_hook_covers_recoverable_boundaries(
    committed_count,
):
    hook = (
        V3MigrationAcceptanceFaultHook
        .after_horizon_candidate_ledger_ddl_commit(committed_count)
    )
    assert hook.version == HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION
    hook.validate()


@pytest.mark.parametrize("unsupported_count", (0, 2, 3, 4, 5, 7, 9))
def test_horizon_candidate_ledger_fault_hook_rejects_other_boundaries(
    unsupported_count,
):
    with pytest.raises(ValueError, match="recoverable committed DDL"):
        V3MigrationAcceptanceFaultHook.after_horizon_candidate_ledger_ddl_commit(
            unsupported_count
        )


def test_shadow_fk_acceptance_fault_hooks_cover_both_recoverable_alters():
    hooks = [
        V3MigrationAcceptanceFaultHook.after_shadow_fk_ddl_commit(name)
        for name in (
            "fk_v3_calibration_gate_learning_run",
            "fk_v3_shadow_release_gate",
        )
    ]
    assert len({hook.committed_statement_count for hook in hooks}) == 2
    for hook in hooks:
        hook.validate()
        with pytest.raises(
            migrations_v3.V3MigrationAcceptanceFault,
            match="after committed DDL",
        ):
            hook.raise_if_matches(
                version=hook.version,
                statement_count=hook.committed_statement_count,
            )
        assert hook.triggered is True
    with pytest.raises(ValueError, match="recoverable ALTER FK"):
        V3MigrationAcceptanceFaultHook.after_shadow_fk_ddl_commit(
            "fk_not_supported"
        )


def test_shadow_fk_recovery_requires_exact_metadata_and_restrict_rules():
    statements = {
        name: next(
            statement
            for statement in migrations_v3.SHADOW_INTELLIGENCE_DDL
            if f"ADD CONSTRAINT {name}" in statement
        )
        for name in (
            "fk_v3_calibration_gate_learning_run",
            "fk_v3_shadow_release_gate",
        )
    }
    metadata = {
        "fk_v3_calibration_gate_learning_run": {
            "TABLE_NAME": "st_calibration_gate_v3",
            "COLUMN_NAME": "learning_run_id",
            "REFERENCED_TABLE_NAME": "st_counterfactual_learning_run_v3",
            "REFERENCED_COLUMN_NAME": "learning_run_id",
            "UPDATE_RULE": "RESTRICT",
            "DELETE_RULE": "RESTRICT",
        },
        "fk_v3_shadow_release_gate": {
            "TABLE_NAME": "st_shadow_release_v3",
            "COLUMN_NAME": "gate_evaluation_id",
            "REFERENCED_TABLE_NAME": "st_calibration_gate_v3",
            "REFERENCED_COLUMN_NAME": "gate_evaluation_id",
            "UPDATE_RULE": "NO ACTION",
            "DELETE_RULE": "NO ACTION",
        },
    }

    class _Result:
        def __init__(self, rows):
            self.rows = rows

        def mappings(self):
            return self.rows

    class _Connection:
        def __init__(self, row):
            self.row = row

        def execute(self, _statement, _parameters):
            return _Result([self.row])

    for name, statement in statements.items():
        assert migrations_v3._ddl_statement_already_applied(
            _Connection(metadata[name]),  # type: ignore[arg-type]
            statement,
        ) is True
        drifted = {**metadata[name], "DELETE_RULE": "CASCADE"}
        with pytest.raises(RuntimeError, match="drifted foreign key"):
            migrations_v3._ddl_statement_already_applied(
                _Connection(drifted),  # type: ignore[arg-type]
                statement,
            )


def test_acceptance_modes_and_outputs_remain_non_production():
    parser = acceptance._parser()
    assert parser.parse_args([]).mode == "serial-replay"
    for mode in (
        "serial-replay",
        "concurrent-initial",
        "partial-recovery",
        "horizon-v2-partial-recovery",
    ):
        assert parser.parse_args(["--mode", mode]).mode == mode
    fields = acceptance.V3MySQLAcceptanceReport.__dataclass_fields__
    for field_name in (
        "outbox_runtime_enabled",
        "production_activation_allowed",
        "actionable_output_allowed",
        "worker_activation_allowed",
    ):
        assert field_name in fields


def test_partial_recovery_statuses_include_later_unattempted_migrations():
    expected = acceptance._expected_recovery_statuses(
        V3_PROJECTION_OUTBOX_MIGRATION_VERSION
    )
    outbox_index = next(
        index for index, item in enumerate(MIGRATIONS)
        if item["version"] == V3_PROJECTION_OUTBOX_MIGRATION_VERSION
    )
    assert expected[:outbox_index] == ("exists",) * outbox_index
    assert expected[outbox_index:] == ("applied",) * (
        len(MIGRATIONS) - outbox_index
    )
    for version in (
        HORIZON_PROTOCOL_V2_MIGRATION_VERSION,
        HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION,
        FORWARD_STRATEGY_VERSION_MIGRATION_VERSION,
        V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION,
        FORWARD_EXIT_ALLOCATION_MIGRATION_VERSION,
    ):
        migration_index = next(
            index for index, item in enumerate(MIGRATIONS)
            if item["version"] == version
        )
        assert acceptance._expected_recovery_statuses(version) == (
            ("exists",) * migration_index
            + ("applied",) * (len(MIGRATIONS) - migration_index)
        )


def test_acceptance_tool_never_reads_generic_application_database_url():
    source = Path("tools/trading_v3_mysql_acceptance.py").read_text(
        encoding="utf-8"
    )
    assert "create_tool_engine()" not in source
    assert "load_project_env" not in source
    assert "_FORBIDDEN_URL_ENVS" in source
