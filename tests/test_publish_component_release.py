from __future__ import annotations

import json
import re
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from server.common.component_release import build_component_release
from server.common import component_release_ledger as ledger
from tools import publish_component_release as publisher

LINUX = "a" * 40
ANCHOR = "b" * 40
PARENT = "c" * 40
DIGEST = "d" * 64
RUNTIME_GRANTS = (
    "GRANT USAGE ON *.* TO `probiga_runtime`@`127.0.0.1` REQUIRE SSL",
    "GRANT SELECT ON `biga`.* TO `probiga_runtime`@`127.0.0.1`",
    "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE TEMPORARY TABLES ON `probiga`.* TO `probiga_runtime`@`127.0.0.1`",
    "GRANT SELECT ON `probiga_qmt_history`.* TO `probiga_runtime`@`127.0.0.1`",
)


def _manifest(**overrides):
    fields = dict(
        linux_build_sha=LINUX, windows_build_sha=ANCHOR,
        contract_build_sha=ANCHOR, contract_sha256=DIGEST,
        parent_linux_build_sha=PARENT, scope="LINUX",
        created_at="2026-09-19T03:00:00Z",
    )
    fields.update(overrides)
    return build_component_release(**fields)


class _Result:
    def __init__(self, rows=(), scalar=None):
        self.rows = list(rows)
        self.scalar = scalar

    def __iter__(self):
        return iter(self.rows)

    def one(self):
        assert len(self.rows) == 1
        return self.rows[0]

    def scalar_one(self):
        return self.scalar

    def mappings(self):
        return self

    def all(self):
        return self.rows


class _Database:
    def __init__(self):
        self.tables = {}
        self.records = {}
        self.key = Ed25519PrivateKey.generate()
        self.commits = 0
        self.statements = []
        self.grants = RUNTIME_GRANTS
        self.lock_available = True
        self.lock_held = False
        self.lock_release = 1
        self.create_error = None

    def install(self, manifest):
        table = ledger.TABLE_NAME
        self.tables[table] = dict(
            table_schema="probiga", table_name=table,
            table_type="BASE TABLE", engine="InnoDB",
            table_comment=ledger.build_component_ledger_metadata(ledger.signing_public_key(self.key)),
            current_database="probiga",
        )
        encoded, signature = ledger.sign_component_manifest(manifest, self.key)
        self.records[manifest["linux_build_sha"]] = {
            "linux_build_sha": manifest["linux_build_sha"], "manifest_json": encoded,
            "signature": signature,
        }


class _Connection:
    def __init__(self, database, role):
        self.database = database
        self.role = role

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def commit(self):
        self.database.commits += 1

    def execute(self, statement, parameters=None):
        sql = str(statement)
        params = parameters or {}
        db = self.database
        db.statements.append((self.role, sql, params))
        if sql == "SHOW GRANTS FOR CURRENT_USER()":
            assert self.role == "runtime"
            return _Result([(item,) for item in db.grants])
        if sql == "SHOW CREATE USER CURRENT_USER()":
            return _Result([("CREATE USER `probiga_runtime`@`127.0.0.1` REQUIRE SSL",)])
        if "GET_LOCK(" in sql:
            assert self.role == "migrator"
            assert params["lock_name"] == publisher.PUBLICATION_LOCK_NAME
            if db.lock_available:
                db.lock_held = True
                return _Result(scalar=1)
            return _Result(scalar=0)
        if "RELEASE_LOCK(" in sql:
            assert self.role == "migrator"
            db.lock_held = False
            return _Result(scalar=db.lock_release)
        if sql.startswith("SELECT COUNT(*) FROM information_schema.TABLES"):
            return _Result(scalar=int(params["table_name"] in db.tables))
        if sql.startswith("SELECT COUNT(*) FROM probiga.st_component_release_manifest"):
            return _Result(scalar=int(params["linux_build_sha"] in db.records))
        if "FROM information_schema.COLUMNS" in sql:
            return _Result([
                {"column_name": name, "column_type": kind, "is_nullable": "NO", "column_default": None,
                 "extra": "", "collation_name": collation, "ordinal_position": i}
                for i, (name, kind, collation) in enumerate([
                    ("linux_build_sha", "char(40)", "ascii_bin"),
                    ("manifest_json", "text", "utf8mb4_bin"),
                    ("signature", "char(88)", "ascii_bin"),
                ], 1)
            ])
        if "FROM information_schema.STATISTICS" in sql:
            return _Result([{"index_name": "PRIMARY", "non_unique": 0, "seq_in_index": 1,
                             "column_name": "linux_build_sha", "sub_part": None, "expression": None}])
        if "FROM information_schema.TABLES" in sql:
            row = db.tables.get(params["table_name"])
            return _Result([row] if row else [])
        if sql.startswith("CREATE TABLE"):
            assert self.role == "migrator" and db.lock_held
            assert " ENGINE=InnoDB " in sql
            assert "COMMENT=:component_comment" in sql
            table = ledger.TABLE_NAME
            assert f"CREATE TABLE `probiga`.`{table}`" in sql
            assert table not in db.tables
            if db.create_error:
                raise RuntimeError(db.create_error)
            db.tables[table] = dict(table_schema="probiga", table_name=table, table_type="BASE TABLE",
                                    engine="InnoDB", table_comment=params["component_comment"], current_database="probiga")
            return _Result()
        if sql.startswith("SELECT linux_build_sha, manifest_json, signature"):
            row = db.records.get(params["linux_build_sha"])
            return _Result([row] if row else [])
        if sql.startswith("INSERT INTO probiga.st_component_release_manifest"):
            assert self.role == "migrator" and db.lock_held
            assert params["linux_build_sha"] not in db.records
            if db.create_error:
                raise RuntimeError(db.create_error)
            db.records[params["linux_build_sha"]] = dict(params)
            return _Result()
        raise AssertionError(f"unexpected SQL: {sql}")


class _Engine:
    def __init__(self, database, role):
        self.database = database
        self.role = role
        self.disposed = False

    def connect(self):
        return _Connection(self.database, self.role)

    def dispose(self):
        self.disposed = True


@pytest.fixture(autouse=True)
def _signing_key_boundary(monkeypatch):
    # SQL tests exercise real signatures; filesystem key protection has its
    # own tests, so no test writes the production signing key.
    monkeypatch.setattr(publisher, "load_signing_key", lambda **_: _ACTIVE_DATABASE.key)


_ACTIVE_DATABASE = None


def _boundary():
    global _ACTIVE_DATABASE
    db = _Database()
    _ACTIVE_DATABASE = db
    db.install(_manifest(linux_build_sha=ANCHOR, scope="COORDINATED"))
    db.install(_manifest(linux_build_sha=PARENT, parent_linux_build_sha=ANCHOR))
    boundary = SimpleNamespace(
        runtime_engine=_Engine(db, "runtime"), migrator_engine=_Engine(db, "migrator"),
    )
    return boundary, db


def _creates(database):
    return [sql for _role, sql, _params in database.statements if sql.startswith("CREATE TABLE")]


def test_publish_inserts_signed_record_without_ddl_and_reads_as_runtime():
    boundary, db = _boundary()
    manifest = _manifest()
    result = publisher._apply(boundary, manifest, mode="publish")
    assert result == {"status": "created", "linux_build_sha": LINUX}
    assert _creates(db) == []
    assert db.commits == 1
    assert db.lock_held is False
    assert db.statements[-1][0] == "runtime"
    assert "FROM probiga.st_component_release_manifest" in db.statements[-1][1]
    comment = db.records[LINUX]["manifest_json"]
    assert json.loads(comment) == manifest
    assert comment == json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert not any(re.match(r"(CREATE|ALTER|DROP|GRANT|UPDATE|DELETE|SET)\b", sql) for _, sql, _ in db.statements)


def test_identical_publish_is_idempotent_without_alter_or_replacement():
    boundary, db = _boundary()
    manifest = _manifest()
    db.install(manifest)
    original = db.records[LINUX].copy()
    assert publisher._apply(boundary, manifest, mode="publish")["status"] == "existing"
    assert _creates(db) == []
    assert db.records[LINUX] == original


def test_existing_same_build_identity_must_match_timestamp_and_entire_seal():
    boundary, db = _boundary()
    prior = _manifest(created_at="2026-09-19T01:00:00Z")
    db.install(prior)
    with pytest.raises(publisher.ComponentPublicationError, match="COMPONENT_IDENTITY_CONFLICT"):
        publisher._apply(boundary, _manifest(), mode="publish")
    assert _creates(db) == []
    assert json.loads(db.records[LINUX]["manifest_json"]) == prior
    assert not db.lock_held


def test_coordinated_initialize_creates_fixed_table_once_and_no_records():
    boundary, db = _boundary()
    db.tables.clear()
    db.records.clear()
    manifest = _manifest(linux_build_sha=ANCHOR, scope="COORDINATED")
    assert publisher._apply(boundary, manifest, mode="initialize")["status"] == "initialized"
    assert len(db.tables) == 1
    assert db.records == {}
    assert publisher._apply(boundary, manifest, mode="initialize")["status"] == "initialized"
    assert len(_creates(db)) == 1


@pytest.mark.parametrize("scope", ["LINUX", "COORDINATED"])
def test_publish_never_initializes_missing_table_even_for_old_coordinated_manifest(scope):
    boundary, db = _boundary()
    db.tables.clear()
    manifest = _manifest(linux_build_sha=ANCHOR, scope="COORDINATED") if scope == "COORDINATED" else _manifest()
    with pytest.raises(RuntimeError, match="metadata unavailable"):
        publisher._apply(boundary, manifest, mode="publish")
    assert _creates(db) == []


def test_initialize_rejects_linux_scope_and_existing_pubkey_drift():
    boundary, db = _boundary()
    with pytest.raises(publisher.ComponentPublicationError, match="COORDINATED_INITIALIZATION_REQUIRED"):
        publisher._apply(boundary, _manifest(), mode="initialize")
    db.key = Ed25519PrivateKey.generate()
    with pytest.raises(publisher.ComponentPublicationError, match="COMPONENT_SIGNING_KEY_DIFFERS"):
        publisher._apply(boundary, _manifest(linux_build_sha=ANCHOR, scope="COORDINATED"), mode="initialize")
    assert _creates(db) == []


def test_missing_private_key_cannot_generate_key_in_publish(monkeypatch):
    boundary, db = _boundary()
    def missing(*, create):
        assert create is False
        raise ledger.ComponentLedgerError("signing key unavailable")
    monkeypatch.setattr(publisher, "load_signing_key", missing)
    with pytest.raises(ledger.ComponentLedgerError, match="unavailable"):
        publisher._apply(boundary, _manifest(), mode="publish")
    assert _creates(db) == [] and LINUX not in db.records


@pytest.mark.parametrize("failure", ["missing-parent", "missing-anchor", "different-digest", "different-window", "uncoordinated-anchor", "self-parent"])
def test_linux_publication_requires_complete_existing_contract_lineage(failure):
    boundary, db = _boundary()
    manifest = _manifest()
    if failure == "missing-parent":
        del db.records[PARENT]
    elif failure == "missing-anchor":
        del db.records[ANCHOR]
    elif failure == "different-digest":
        db.install(_manifest(linux_build_sha=PARENT, parent_linux_build_sha=ANCHOR, contract_sha256="e" * 64))
    elif failure == "different-window":
        db.install(_manifest(linux_build_sha=PARENT, windows_build_sha=PARENT, contract_build_sha=PARENT, scope="COORDINATED"))
    elif failure == "uncoordinated-anchor":
        db.install(_manifest(linux_build_sha=ANCHOR, scope="LINUX"))
    else:
        manifest = _manifest(parent_linux_build_sha=LINUX)
    with pytest.raises(publisher.ComponentPublicationError, match="COMPONENT_LINEAGE_INVALID"):
        publisher._apply(boundary, manifest, mode="publish")
    assert _creates(db) == []
    assert not db.lock_held


@pytest.mark.parametrize("privilege", ["CREATE", "ALTER", "DROP", "INDEX", "REFERENCES", "TRIGGER", "EVENT", "CREATE ROUTINE", "ALTER ROUTINE", "EXECUTE", "ALL PRIVILEGES", "GRANT OPTION"])
def test_unsafe_runtime_privileges_prevent_trusting_or_creating_attestation(privilege):
    boundary, db = _boundary()
    grants = list(RUNTIME_GRANTS)
    if privilege == "GRANT OPTION":
        grants[2] += " WITH GRANT OPTION"
    else:
        grants[2] = grants[2].replace("GRANT SELECT,", f"GRANT {privilege}, SELECT,")
    db.grants = grants
    with pytest.raises(publisher.ComponentPublicationError, match="RUNTIME_METADATA_AUTHORITY_UNSAFE"):
        publisher._apply(boundary, _manifest(), mode="publish")
    assert _creates(db) == []
    assert not any("information_schema.TABLES" in sql for _, sql, _ in db.statements)


def test_even_frozen_legacy_ddl_contract_is_unsafe_for_metadata_seals():
    boundary, db = _boundary()
    grants = list(RUNTIME_GRANTS)
    grants[2] = grants[2].replace("GRANT SELECT,", "GRANT CREATE, ALTER, DROP, INDEX, REFERENCES, SELECT,")
    # Existing governance policy accepts this frozen historic contract. The new
    # metadata trust boundary must refuse it without changing that policy.
    publisher.boundary_policy._validate_runtime_grants(grants)
    db.grants = grants
    with pytest.raises(publisher.ComponentPublicationError, match="RUNTIME_METADATA_AUTHORITY_UNSAFE"):
        publisher._apply(boundary, _manifest(), mode="publish")
    assert _creates(db) == []


def test_verify_is_read_only_and_also_requires_runtime_authority():
    boundary, db = _boundary()
    db.install(_manifest())
    assert publisher._apply(boundary, _manifest(), mode="verify")["status"] == "verified"
    assert all(role == "runtime" for role, _, _ in db.statements)
    assert _creates(db) == []
    db.grants = (RUNTIME_GRANTS[0] + " WITH GRANT OPTION", *RUNTIME_GRANTS[1:])
    with pytest.raises(publisher.ComponentPublicationError, match="RUNTIME_METADATA_AUTHORITY_UNSAFE"):
        publisher._apply(boundary, _manifest(), mode="verify")


def test_named_lock_contention_prevents_create():
    boundary, db = _boundary()
    db.lock_available = False
    with pytest.raises(publisher.ComponentPublicationError, match="COMPONENT_LOCK_BUSY"):
        publisher._apply(boundary, _manifest(), mode="publish")
    assert _creates(db) == []


def test_failed_create_releases_lock_and_never_repairs_or_alters_existing_tables():
    boundary, db = _boundary()
    db.create_error = "database rejected CREATE"
    with pytest.raises(RuntimeError, match="database rejected"):
        publisher._apply(boundary, _manifest(), mode="publish")
    assert not db.lock_held
    assert LINUX not in db.records
    assert not any(sql.startswith("ALTER") for _, sql, _ in db.statements)


def test_failed_lock_release_is_failure_even_after_creation():
    boundary, db = _boundary()
    db.lock_release = 0
    with pytest.raises(publisher.ComponentPublicationError, match="COMPONENT_LOCK_RELEASE_FAILED"):
        publisher._apply(boundary, _manifest(), mode="publish")


def test_public_entry_uses_fixed_credential_boundary_and_closes_engines(monkeypatch):
    boundary, db = _boundary()
    calls = []
    monkeypatch.setattr(publisher, "_safe_manifest", lambda path: _manifest())
    monkeypatch.setattr(publisher, "_load_protected_runtime_env", lambda: None)
    def open_boundary(**kwargs):
        calls.append(kwargs)
        return boundary
    monkeypatch.setattr(publisher.boundary_policy, "_open_boundary", open_boundary)
    result = publisher.publish_component_release("/fixed/manifest", mode="publish")
    assert calls == [{"include_migrator": True, "expected_trust": 0}]
    assert result["status"] == "created"
    assert boundary.runtime_engine.disposed and boundary.migrator_engine.disposed


def test_public_errors_are_fixed_categories_without_database_secret_text(monkeypatch, capsys):
    boundary, db = _boundary()
    db.create_error = "mysql://user:secret@private-server SQL CREATE TABLE"
    monkeypatch.setattr(publisher, "_safe_manifest", lambda path: _manifest())
    monkeypatch.setattr(publisher, "_load_protected_runtime_env", lambda: None)
    monkeypatch.setattr(publisher.boundary_policy, "_open_boundary", lambda **_: boundary)
    assert publisher.main(["--manifest", "/fixed/manifest", "--mode", "publish"]) == 1
    assert json.loads(capsys.readouterr().out) == {"status": "error", "category": "COMPONENT_PUBLICATION_FAILED"}
    assert boundary.runtime_engine.disposed and boundary.migrator_engine.disposed
    assert not db.lock_held


def test_manifest_argument_does_not_override_existing_runtime_path(monkeypatch):
    monkeypatch.setenv("PROBIGA_COMPONENT_RELEASE_PATH", "/already-set")
    monkeypatch.setattr(publisher, "load_runtime_component_release", lambda: pytest.fail("mismatched path reached loader"))
    with pytest.raises(publisher.ComponentPublicationError, match="MANIFEST_INVALID"):
        publisher._safe_manifest("/different")


def test_manifest_load_failure_is_sanitized_and_restores_environment(monkeypatch):
    import os
    monkeypatch.delenv("PROBIGA_COMPONENT_RELEASE_PATH", raising=False)
    def broken():
        assert os.environ["PROBIGA_COMPONENT_RELEASE_PATH"] == "/fixed/path"
        raise RuntimeError("internal sensitive data")
    monkeypatch.setattr(publisher, "load_runtime_component_release", broken)
    with pytest.raises(publisher.ComponentPublicationError, match="^MANIFEST_INVALID$"):
        publisher._safe_manifest("/fixed/path")
    assert "PROBIGA_COMPONENT_RELEASE_PATH" not in os.environ


def test_publisher_loads_only_fixed_protected_runtime_configuration(monkeypatch):
    protected = publisher._RUNTIME_CONFIG_PATH
    assert protected.as_posix() == "/opt/ProBigA/.env"
    observed = []
    monkeypatch.setattr(publisher.boundary_policy, "_require_root_execution", lambda: observed.append("root"))
    monkeypatch.setattr(publisher.boundary_policy, "load_project_env", lambda path: observed.append(path))
    def metadata(path):
        if path == protected:
            return SimpleNamespace(st_mode=stat.S_IFREG | 0o640, st_uid=0, st_nlink=1)
        assert path == protected.parent
        return SimpleNamespace(st_mode=stat.S_IFDIR | 0o755, st_uid=0)
    monkeypatch.setattr(Path, "lstat", metadata)
    monkeypatch.setattr(Path, "resolve", lambda path, **_: path)
    publisher._load_protected_runtime_env()
    assert observed == ["root", protected]


@pytest.mark.parametrize(("target", "field", "value"), [
    ("file", "st_mode", stat.S_IFLNK | 0o640),
    ("file", "st_mode", stat.S_IFREG | 0o666),
    ("file", "st_uid", 1000), ("file", "st_nlink", 2),
    ("parent", "st_mode", stat.S_IFDIR | 0o777),
    ("parent", "st_mode", stat.S_IFLNK | 0o755),
    ("parent", "st_uid", 1000),
])
def test_runtime_configuration_trust_failure_never_reads_credentials(monkeypatch, target, field, value):
    protected = publisher._RUNTIME_CONFIG_PATH
    monkeypatch.setattr(publisher.boundary_policy, "_require_root_execution", lambda: None)
    monkeypatch.setattr(publisher.boundary_policy, "load_project_env", lambda path: pytest.fail("untrusted credentials were read"))
    def metadata(path):
        info = dict(st_mode=stat.S_IFREG | 0o640, st_uid=0, st_nlink=1) if path == protected else dict(st_mode=stat.S_IFDIR | 0o755, st_uid=0)
        if (target == "file") == (path == protected):
            info[field] = value
        return SimpleNamespace(**info)
    monkeypatch.setattr(Path, "lstat", metadata)
    monkeypatch.setattr(Path, "resolve", lambda path, **_: path)
    with pytest.raises(publisher.ComponentPublicationError, match="^RUNTIME_CONFIGURATION_UNSAFE$"):
        publisher._load_protected_runtime_env()


def test_runtime_configuration_is_not_read_before_root_attestation(monkeypatch):
    def denied():
        raise PermissionError("not root")
    monkeypatch.setattr(publisher.boundary_policy, "_require_root_execution", denied)
    monkeypatch.setattr(Path, "lstat", lambda _: pytest.fail("metadata read before root attestation"))
    monkeypatch.setattr(publisher.boundary_policy, "load_project_env", lambda _: pytest.fail("credentials read before root attestation"))
    with pytest.raises(publisher.ComponentPublicationError, match="^RUNTIME_CONFIGURATION_UNSAFE$"):
        publisher._load_protected_runtime_env()
