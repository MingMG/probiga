from __future__ import annotations

import copy
import json
import os
import stat
import subprocess
import sys
from types import SimpleNamespace
from pathlib import Path
from typing import Any, Mapping, Sequence

import pytest

from tools import migrate_qmt_attestation_collation as migration


AUDITOR_GRANTS = (
    "GRANT USAGE ON *.* TO `probiga_qmt_auditor`@`127.0.0.1` REQUIRE SSL",
    "GRANT SELECT ON `probiga`.* TO `probiga_qmt_auditor`@`127.0.0.1`",
)
MIGRATOR_GRANTS = (
    "GRANT USAGE ON *.* TO `probiga_migrator`@`127.0.0.1` REQUIRE SSL",
    *(
        "GRANT SELECT, ALTER ON `probiga`."
        f"`{table}` TO `probiga_migrator`@`127.0.0.1`"
        for table in migration.QMT_TABLES
    ),
)
PEER_CERT_SHA256 = "a" * 64


def _target_state(**changes: Any) -> dict[str, Any]:
    state = {
        "mysql_version": migration.EXPECTED_MYSQL_VERSION,
        "version_comment": "MySQL Community Server - GPL",
        "database_name": migration.DATABASE_NAME,
        "authenticated_user": migration.EXPECTED_AUDITOR_USER,
        "active_roles": "NONE",
        "server_uuid": migration.EXPECTED_SERVER_UUID,
        "server_port": migration.EXPECTED_SERVER_PORT,
        "server_hostname": migration.EXPECTED_SERVER_HOSTNAME,
        "log_bin": 1,
        "binlog_format": "ROW",
        "trust_creators": 0,
        "session_sql_mode": migration.EXPECTED_SQL_MODE,
        "character_set_client": "utf8mb4",
        "collation_connection": migration.SOURCE_COLLATION,
        "database_collation": migration.TARGET_COLLATION,
        "tls_cipher": "TLS_AES_256_GCM_SHA384",
        "tls_version": "TLSv1.3",
        "tls_peer_cert_sha256": PEER_CERT_SHA256,
        "expected_tls_peer_cert_sha256": PEER_CERT_SHA256,
    }
    state.update(changes)
    return state


def _snapshot(
    table_name: str,
    *,
    ordinal: int,
    status: str = "source",
) -> dict[str, Any]:
    data_bytes = 16_384 * ordinal
    index_bytes = 8_192 * ordinal
    return {
        **migration._frozen_manifest(table_name, status),
        "exact_row_count": ordinal,
        "canonical_row_sha256": f"{ordinal:064x}",
        "row_checksum": 900_000 + ordinal,
        "data_bytes": data_bytes,
        "index_bytes": index_bytes,
        "allocated_bytes": data_bytes + index_bytes,
    }


class _Connection:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeDatabase:
    def __init__(self, *, role: str = "auditor") -> None:
        if role == "auditor":
            self.state = _target_state()
            self.grant_rows: Sequence[str] = AUDITOR_GRANTS
        elif role == "migrator":
            self.state = _target_state(
                authenticated_user=migration.EXPECTED_MIGRATOR_USER
            )
            self.grant_rows = MIGRATOR_GRANTS
        else:
            raise AssertionError("unexpected fake role")
        self.governance_inventory: set[str] = set()
        self.snapshots = {
            table: _snapshot(table, ordinal=ordinal)
            for ordinal, table in enumerate(migration.QMT_TABLES, start=1)
        }
        self.connection = _Connection()
        self.snapshot_calls = {table: 0 for table in migration.QMT_TABLES}
        self.drift_on_second_read: str | None = None

    def target_state(self) -> Mapping[str, Any]:
        return copy.deepcopy(self.state)

    def grants(self) -> Sequence[str]:
        return tuple(self.grant_rows)

    def table_inventory(self, names: Sequence[str]) -> set[str]:
        if tuple(names) == migration.QMT_TABLES:
            return set(self.snapshots)
        if tuple(names) == migration.GOVERNANCE_TABLES:
            return set(self.governance_inventory)
        raise AssertionError("unexpected inventory")

    def snapshot(self, table_name: str) -> Mapping[str, Any]:
        table_name = migration._require_table(table_name)
        self.snapshot_calls[table_name] += 1
        snapshot = copy.deepcopy(self.snapshots[table_name])
        if self.drift_on_second_read == table_name and self.snapshot_calls[table_name] == 2:
            snapshot["canonical_row_sha256"] = "f" * 64
        return snapshot


def _audit(
    database: FakeDatabase,
    migrator_database: FakeDatabase | None = None,
) -> dict[str, Any]:
    return migration.audit_database(
        database,
        migrator_database or FakeDatabase(role="migrator"),
    )


def test_default_parser_is_read_only_and_has_no_writer_assertion():
    args = migration._parser().parse_args([])

    assert args.command == "audit"
    assert not hasattr(args, "writers_fenced")
    with pytest.raises(SystemExit):
        migration._parser().parse_args(["apply", "--writers-fenced"])


def test_audit_uses_frozen_manifests_and_canonical_primary_key_row_hashes():
    database = FakeDatabase()

    first = _audit(database)
    database = FakeDatabase()
    second = _audit(database)

    assert first == second
    assert first["status"] == "AUDIT_ONLY"
    assert first["ddl_executed"] is False
    assert first["zero_ddl"] is True
    assert first["apply_eligible"] is False
    assert first["schema_eligible"] is True
    assert first["plan_sha256"] == migration._digest(first["plan"])
    assert first["plan"]["frozen_schema_contract_sha256"] == (
        migration.FROZEN_SCHEMA_CONTRACT_SHA256
    )
    assert first["plan"]["row_content_measurement"] == (
        "CANONICAL_SHA256_PRIMARY_KEY_ORDER_V1"
    )
    assert first["plan"]["row_count_measurement"] == (
        "CANONICAL_STREAM_ROW_COUNT_V1"
    )
    assert first["plan"]["stability_measurement"] == "FULL_DOUBLE_READ_EXACT_V1"
    assert [item["canonical_row_sha256"] for item in first["plan"]["operations"]] == [
        f"{ordinal:064x}" for ordinal in range(1, 5)
    ]
    assert all(item["action"] == "convert" for item in first["plan"]["operations"])
    assert set(database.snapshot_calls.values()) == {2}


def test_audit_never_claims_apply_eligible_even_when_tables_are_exact_source():
    result = _audit(FakeDatabase())

    assert result["schema_eligible"] is True
    assert result["apply_eligible"] is False
    assert result["plan"]["apply_disabled_reason"] == (
        "INDEPENDENT_SAFETY_PROOF_NOT_YET_COMPLETE"
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mysql_version", "8.4.10"),
        ("mysql_version", "8.4.12"),
        ("authenticated_user", migration.EXPECTED_MIGRATOR_USER),
        ("server_uuid", "0" * 36),
        ("server_port", 3307),
        ("server_hostname", "OTHER-HOST"),
        ("trust_creators", 1),
        ("tls_cipher", ""),
        ("tls_version", "TLSv1.2"),
        ("tls_peer_cert_sha256", "b" * 64),
        ("collation_connection", migration.TARGET_COLLATION),
        ("database_collation", migration.SOURCE_COLLATION),
    ],
)
def test_audit_rejects_mysql_identity_tls_trust_or_collation_drift(field, value):
    database = FakeDatabase()
    database.state[field] = value

    with pytest.raises(migration.CollationMigrationError) as error:
        _audit(database)

    assert error.value.code == "TARGET_BOUNDARY_MISMATCH"


@pytest.mark.parametrize(
    "grants",
    [
        AUDITOR_GRANTS[:1],
        AUDITOR_GRANTS + (
            "GRANT SELECT ON `other`.* TO `probiga_qmt_auditor`@`127.0.0.1`",
        ),
        (
            AUDITOR_GRANTS[0],
            "GRANT ALL PRIVILEGES ON `probiga`.* TO "
            "`probiga_qmt_auditor`@`127.0.0.1`",
        ),
        (
            AUDITOR_GRANTS[0],
            AUDITOR_GRANTS[1] + " WITH GRANT OPTION",
        ),
        tuple(item.replace("probiga_qmt_auditor", "root") for item in AUDITOR_GRANTS),
    ],
)
def test_audit_rejects_non_exact_read_inventory_account(grants):
    database = FakeDatabase()
    database.grant_rows = grants

    with pytest.raises(migration.CollationMigrationError) as error:
        _audit(database)

    assert error.value.code == "AUDITOR_GRANTS_INVALID"


def test_migrator_privileges_are_exactly_select_alter_on_four_tables():
    migration._validate_migrator_grants(MIGRATOR_GRANTS)

    forbidden = (
        MIGRATOR_GRANTS[0],
        "GRANT ALL PRIVILEGES ON `probiga`.* TO "
        "`probiga_migrator`@`127.0.0.1`",
    )
    with pytest.raises(migration.CollationMigrationError) as all_error:
        migration._validate_migrator_grants(forbidden)
    assert all_error.value.code == "MIGRATOR_GRANTS_INVALID"

    for privilege in ("INSERT", "UPDATE", "DELETE", "DROP", "TRIGGER", "EVENT", "EXECUTE"):
        changed = list(MIGRATOR_GRANTS)
        changed[1] = changed[1].replace("SELECT, ALTER", f"SELECT, ALTER, {privilege}")
        with pytest.raises(migration.CollationMigrationError):
            migration._validate_migrator_grants(tuple(changed))


def test_migrator_rejects_schema_scope_missing_table_and_grant_option():
    schema_scope = (
        MIGRATOR_GRANTS[0],
        "GRANT SELECT, ALTER ON `probiga`.* TO "
        "`probiga_migrator`@`127.0.0.1`",
    )
    with pytest.raises(migration.CollationMigrationError):
        migration._validate_migrator_grants(schema_scope)
    with pytest.raises(migration.CollationMigrationError):
        migration._validate_migrator_grants(MIGRATOR_GRANTS[:-1])
    changed = list(MIGRATOR_GRANTS)
    changed[-1] += " WITH GRANT OPTION"
    with pytest.raises(migration.CollationMigrationError):
        migration._validate_migrator_grants(tuple(changed))


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        (lambda snapshot: snapshot["table"].update(engine="MyISAM"), "TABLE_SCHEMA_DRIFT"),
        (lambda snapshot: snapshot["columns"].append(copy.deepcopy(snapshot["columns"][-1])), "TABLE_SCHEMA_DRIFT"),
        (lambda snapshot: snapshot["indexes"].pop(), "TABLE_SCHEMA_DRIFT"),
        (lambda snapshot: snapshot["triggers"].append({}), "TABLE_SCHEMA_DRIFT"),
    ],
)
def test_audit_rejects_engine_column_index_or_extra_trigger_drift(mutation, expected_code):
    database = FakeDatabase()
    snapshot = database.snapshots[migration.QMT_TABLES[2]]
    mutation(snapshot)

    with pytest.raises(migration.CollationMigrationError) as error:
        _audit(database)

    assert error.value.code == expected_code


@pytest.mark.parametrize("field", ["definer", "sql_mode", "action_statement"])
def test_audit_rejects_trigger_body_definer_or_sql_mode_drift(field):
    database = FakeDatabase()
    trigger = database.snapshots[migration.QMT_TABLES[2]]["triggers"][0]
    trigger[field] = "DIFFERENT"

    with pytest.raises(migration.CollationMigrationError) as error:
        _audit(database)

    assert error.value.code == "TABLE_SCHEMA_DRIFT"


def test_audit_rejects_partial_inventory_existing_governance_or_bad_row_sha():
    database = FakeDatabase()
    database.snapshots.pop(migration.QMT_TABLES[-1])
    with pytest.raises(migration.CollationMigrationError) as missing:
        _audit(database)
    assert missing.value.code == "QMT_TABLE_INVENTORY_MISMATCH"

    database = FakeDatabase()
    database.governance_inventory.add(migration.GOVERNANCE_TABLES[0])
    with pytest.raises(migration.CollationMigrationError) as governance:
        _audit(database)
    assert governance.value.code == "GOVERNANCE_SCHEMA_ALREADY_PRESENT"

    database = FakeDatabase()
    database.snapshots[migration.QMT_TABLES[0]]["canonical_row_sha256"] = "CHECKSUM_ONLY"
    with pytest.raises(migration.CollationMigrationError) as row_proof:
        _audit(database)
    assert row_proof.value.code == "TABLE_PROOF_INVALID"


def test_apply_is_unconditionally_blocked_and_cannot_accept_boolean_bypass():
    for kwargs in (
        {},
        {"expected_plan_sha256": "0" * 64},
        {"writers_fenced": True, "expected_plan_sha256": "0" * 64},
    ):
        with pytest.raises(migration.CollationMigrationError) as error:
            migration.apply_migration(**kwargs)
        assert error.value.code == "APPLY_SAFETY_PROOF_UNAVAILABLE"


def test_frozen_schema_manifest_is_deeply_immutable_and_rechecked_each_audit(
    monkeypatch,
):
    table_name = migration.QMT_TABLES[0]
    with pytest.raises(TypeError):
        migration.FROZEN_SCHEMA_MANIFESTS[table_name]["source"]["table"][
            "engine"
        ] = "MyISAM"
    with pytest.raises(TypeError):
        migration.FROZEN_SCHEMA_MANIFESTS[table_name]["source"]["columns"][0][
            "column_name"
        ] = "tampered"

    tampered = migration._deep_thaw(migration.FROZEN_SCHEMA_MANIFESTS)
    tampered[table_name]["source"]["table"]["engine"] = "MyISAM"
    monkeypatch.setattr(migration, "FROZEN_SCHEMA_MANIFESTS", tampered)
    with pytest.raises(migration.CollationMigrationError) as error:
        _audit(FakeDatabase())
    assert error.value.code == "FROZEN_SCHEMA_CONTRACT_TAMPERED"


def test_frozen_schema_digest_is_independent_reviewed_literal():
    assert migration.FROZEN_SCHEMA_CONTRACT_SHA256 == (
        "55d490f291227c940f66271c30fe683f30983faec2dd50a73542d8c0ae57544e"
    )
    migration._assert_frozen_schema_integrity()


def test_self_contained_contract_matches_shared_runtime_contract():
    from server.common import qmt_attestation_contract as shared

    assert migration.QMT_ATTESTATION_COLUMN_SPECS == dict(
        shared.QMT_ATTESTATION_COLUMN_SPECS
    )
    assert migration.QMT_ATTESTATION_INDEX_SPECS == {
        table: dict(indexes)
        for table, indexes in shared.QMT_ATTESTATION_INDEX_SPECS.items()
    }
    assert migration.QMT_ATTESTATION_TRIGGER_SPECS == dict(
        shared.QMT_ATTESTATION_TRIGGER_SPECS
    )
    assert migration.TARGET_COLLATION == shared.QMT_ATTESTATION_COLLATION


def test_audit_requires_independent_migrator_connection_and_real_grants():
    with pytest.raises(migration.CollationMigrationError) as missing:
        migration.audit_database(FakeDatabase())
    assert missing.value.code == "MIGRATOR_PROOF_CONNECTION_REQUIRED"

    migrator = FakeDatabase(role="migrator")
    migrator.grant_rows = MIGRATOR_GRANTS[:-1]
    with pytest.raises(migration.CollationMigrationError) as grants:
        _audit(FakeDatabase(), migrator)
    assert grants.value.code == "MIGRATOR_GRANTS_INVALID"

    migrator = FakeDatabase(role="migrator")
    migrator.state["tls_peer_cert_sha256"] = "b" * 64
    with pytest.raises(migration.CollationMigrationError) as identity:
        _audit(FakeDatabase(), migrator)
    assert identity.value.code == "TARGET_BOUNDARY_MISMATCH"


def test_audit_rejects_full_table_drift_between_two_read_passes():
    database = FakeDatabase()
    table_name = migration.QMT_TABLES[2]
    database.drift_on_second_read = table_name

    with pytest.raises(migration.CollationMigrationError) as error:
        _audit(database)

    assert error.value.code == "AUDIT_SNAPSHOT_UNSTABLE"
    assert error.value.evidence == {"table_name": table_name}


def test_static_sql_inventory_and_primary_key_order_are_frozen():
    assert set(migration.ALTER_DDL) == set(migration.QMT_TABLES)
    assert set(migration.ROW_PROOF_SQL) == set(migration.QMT_TABLES)
    assert set(migration.CHECKSUM_SQL) == set(migration.QMT_TABLES)
    assert all(";" not in sql and "%s" not in sql for sql in migration.ALTER_DDL.values())
    assert "ORDER BY BINARY `run_id`" in migration.ROW_PROOF_SQL[migration.QMT_TABLES[0]]
    assert "ORDER BY `id`" in migration.ROW_PROOF_SQL[migration.QMT_TABLES[1]]
    assert "ORDER BY BINARY `attestation_id`" in migration.ROW_PROOF_SQL[migration.QMT_TABLES[2]]
    assert "ORDER BY BINARY `migration_key`" in migration.ROW_PROOF_SQL[migration.QMT_TABLES[3]]


def test_pymysql_is_not_imported_at_module_load():
    source = Path(migration.__file__).read_text(encoding="utf-8")

    assert "import pymysql" not in source
    assert "from pymysql" not in source
    assert 'importlib.import_module("pymysql")' in source
    assert "ssl_verify_identity=True" in source


def test_read_connection_uses_real_server_side_dict_cursor_and_migrator_option(
    monkeypatch,
):
    real_pymysql = __import__("pymysql")
    captured: dict[str, Any] = {}

    class Socket:
        def getpeercert(self, *, binary_form: bool):
            assert binary_form is True
            return b"fixed-peer-certificate"

    class Connection:
        _sock = Socket()

        def close(self):
            captured["closed"] = True

    def connect(**kwargs):
        captured.update(kwargs)
        return Connection()

    expected_peer = migration.hashlib.sha256(b"fixed-peer-certificate").hexdigest()
    monkeypatch.setattr(migration, "_require_isolated_interpreter", lambda: None)
    monkeypatch.setattr(
        migration,
        "_read_credential",
        lambda path, *, expected_user: (
            captured.update(option_path=path, expected_user=expected_user)
            or migration.OptionCredential("127.0.0.1", 3306, "probiga_migrator", "A" * 64)
        ),
    )
    monkeypatch.setattr(
        migration,
        "_tls_material",
        lambda: migration.TlsMaterial(
            Path("/ca"), Path("/cert"), Path("/key"), expected_peer
        ),
    )
    monkeypatch.setattr(
        migration.importlib,
        "import_module",
        lambda name: (
            SimpleNamespace(connect=connect)
            if name == "pymysql"
            else real_pymysql.cursors
        ),
    )

    database = migration._open_read_database(role="migrator")

    assert isinstance(database, migration.PymysqlReadProofDatabase)
    assert captured["option_path"] == migration.MIGRATOR_OPTION_FILE
    assert captured["expected_user"] == migration.EXPECTED_MIGRATOR_USER
    assert captured["cursorclass"] is real_pymysql.cursors.SSDictCursor
    assert captured["ssl_verify_identity"] is True
    assert captured["program_name"] == "probiga-qmt-collation-migrator-proof-v2"


def test_canonical_row_hash_streams_large_input_in_bounded_batches():
    table_name = migration.QMT_TABLES[0]
    column_names = [
        spec[0] for spec in migration.QMT_ATTESTATION_COLUMN_SPECS[table_name]
    ]
    total_rows = 10_001

    class StreamingCursor:
        def __init__(self):
            self.offset = 0
            self.fetch_sizes: list[int] = []
            self.sql = ""

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql):
            self.sql = sql

        def fetchmany(self, size):
            self.fetch_sizes.append(size)
            remaining = total_rows - self.offset
            count = min(size, max(remaining, 0))
            rows = [
                {
                    name: f"{name}:{row_number}"
                    for name in column_names
                }
                for row_number in range(self.offset, self.offset + count)
            ]
            self.offset += count
            return rows

        def fetchall(self):
            raise AssertionError("canonical full-table proof must never call fetchall")

    class StreamingConnection:
        def __init__(self):
            self.cursors: list[StreamingCursor] = []

        def cursor(self):
            cursor = StreamingCursor()
            self.cursors.append(cursor)
            return cursor

    first_connection = StreamingConnection()
    second_connection = StreamingConnection()
    first_database = migration.PymysqlReadProofDatabase(
        first_connection,
        expected_peer_cert_sha256=PEER_CERT_SHA256,
        observed_peer_cert_sha256=PEER_CERT_SHA256,
    )
    second_database = migration.PymysqlReadProofDatabase(
        second_connection,
        expected_peer_cert_sha256=PEER_CERT_SHA256,
        observed_peer_cert_sha256=PEER_CERT_SHA256,
    )

    first_count, first_sha = first_database._canonical_row_proof(table_name)
    second_count, second_sha = second_database._canonical_row_proof(table_name)

    assert first_count == second_count == total_rows
    assert first_sha == second_sha
    assert migration._LOWER_SHA256_RE.fullmatch(first_sha)
    assert first_connection.cursors[0].sql == migration.ROW_PROOF_SQL[table_name]
    assert set(first_connection.cursors[0].fetch_sizes) == {512}
    assert len(first_connection.cursors[0].fetch_sizes) > 10


def test_cli_main_empty_arguments_runs_read_only_audit(monkeypatch, capsys):
    database = FakeDatabase()
    migrator_database = FakeDatabase(role="migrator")
    monkeypatch.setattr(migration, "_require_isolated_interpreter", lambda: None)
    monkeypatch.setattr(migration, "_require_root_execution", lambda: None)
    monkeypatch.setattr(migration, "_open_auditor_database", lambda: database)
    monkeypatch.setattr(
        migration,
        "_open_migrator_proof_database",
        lambda: migrator_database,
    )

    exit_code = migration.main([])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == "AUDIT_ONLY"
    assert payload["ddl_executed"] is False
    assert payload["apply_eligible"] is False
    assert database.connection.closed is True
    assert migrator_database.connection.closed is True


def test_cli_apply_fails_before_opening_database(monkeypatch, capsys):
    monkeypatch.setattr(
        migration,
        "_open_auditor_database",
        lambda: pytest.fail("apply must not open the database"),
    )

    exit_code = migration.main(["apply", "--expected-plan-sha", "0" * 64])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["error_code"] == "APPLY_SAFETY_PROOF_UNAVAILABLE"


def test_cli_closes_auditor_if_independent_migrator_connection_fails(
    monkeypatch,
    capsys,
):
    database = FakeDatabase()
    monkeypatch.setattr(migration, "_require_isolated_interpreter", lambda: None)
    monkeypatch.setattr(migration, "_require_root_execution", lambda: None)
    monkeypatch.setattr(migration, "_open_auditor_database", lambda: database)
    monkeypatch.setattr(
        migration,
        "_open_migrator_proof_database",
        lambda: (_ for _ in ()).throw(
            migration.CollationMigrationError(
                "MIGRATOR_PROOF_CONNECTION_FAILED",
                "independent proof connection failed",
            )
        ),
    )

    exit_code = migration.main([])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["error_code"] == "MIGRATOR_PROOF_CONNECTION_FAILED"
    assert database.connection.closed is True


def test_real_python_isolated_startup_ignores_malicious_worktree_module(
    tmp_path,
):
    marker = tmp_path / "project-module-imported"
    malicious = tmp_path / "server" / "common"
    malicious.mkdir(parents=True)
    (tmp_path / "server" / "__init__.py").write_text("", encoding="utf-8")
    (malicious / "__init__.py").write_text("", encoding="utf-8")
    (malicious / "qmt_attestation_contract.py").write_text(
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text('imported', encoding='utf-8')\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(tmp_path)

    completed = subprocess.run(
        [
            sys.executable,
            "-I",
            str(Path(migration.__file__).resolve()),
            "startup-check",
        ],
        cwd=tmp_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["status"] == "STARTUP_BOUNDARY_VERIFIED"
    assert payload["isolated_interpreter"] is True
    assert payload["project_module_imported"] is False
    assert marker.exists() is False


def test_executable_startup_check_rejects_nonisolated_python(tmp_path):
    completed = subprocess.run(
        [sys.executable, str(Path(migration.__file__).resolve()), "startup-check"],
        cwd=tmp_path,
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert completed.returncode == 2
    assert json.loads(completed.stdout)["error_code"] == (
        "ISOLATED_INTERPRETER_REQUIRED"
    )


@pytest.mark.skipif(
    os.name != "posix" or not hasattr(os, "geteuid") or os.geteuid() != 0,
    reason="real root-owned POSIX mode proof requires a root POSIX runner",
)
def test_real_posix_root_owned_option_file_boundary(tmp_path, monkeypatch):
    os.chmod(tmp_path, 0o700)
    option_file = tmp_path / "auditor.ini"
    credential_field = "pass" + "word="
    option_file.write_text(
        "[client]\nprotocol=tcp\nhost=127.0.0.1\nport=3306\n"
        "user=probiga_qmt_auditor\n" + credential_field + "A" * 64 + "\n",
        encoding="utf-8",
    )
    os.chmod(option_file, 0o600)

    credential = migration._read_credential(
        option_file, expected_user=migration.EXPECTED_AUDITOR_USER
    )

    assert credential.user == "probiga_qmt_auditor"
    os.chmod(option_file, 0o644)
    with pytest.raises(migration.CollationMigrationError) as error:
        migration._read_credential(
            option_file, expected_user=migration.EXPECTED_AUDITOR_USER
        )
    assert error.value.code == "PROTECTED_FILE_INVALID"
