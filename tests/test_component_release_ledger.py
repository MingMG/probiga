from __future__ import annotations

import base64
import copy
import json
import os
import stat
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from server.common.component_release import build_component_release
from server.common import component_release_ledger as ledger

BUILD = "a" * 40
ANCHOR = "b" * 40


def _manifest(**overrides):
    value = dict(linux_build_sha=BUILD, windows_build_sha=ANCHOR, contract_build_sha=ANCHOR,
                 parent_linux_build_sha=ANCHOR, contract_sha256="d" * 64,
                 scope="LINUX", created_at="2026-09-19T10:00:00Z")
    value.update(overrides)
    return build_component_release(**value)


class _Result:
    def __init__(self, rows): self.rows = rows
    def mappings(self): return self
    def all(self): return copy.deepcopy(self.rows)


class _Connection:
    def __init__(self):
        self.key = Ed25519PrivateKey.generate()
        self.public_key = ledger.signing_public_key(self.key)
        self.metadata = [{"table_schema": "probiga", "table_name": ledger.TABLE_NAME,
                          "table_type": "BASE TABLE", "engine": "InnoDB", "current_database": "probiga",
                          "table_comment": ledger.build_component_ledger_metadata(self.public_key)}]
        self.columns = [{"column_name": name, "column_type": kind, "is_nullable": "NO",
                         "column_default": None, "extra": "", "collation_name": collation, "ordinal_position": i}
                        for i, (name, kind, collation) in enumerate([
                            ("linux_build_sha", "char(40)", "ascii_bin"),
                            ("manifest_json", "text", "utf8mb4_bin"), ("signature", "char(88)", "ascii_bin"),
                        ], 1)]
        self.indexes = [{"index_name": "PRIMARY", "non_unique": 0, "seq_in_index": 1,
                         "column_name": "linux_build_sha", "sub_part": None, "expression": None}]
        encoded, signature = ledger.sign_component_manifest(_manifest(), self.key)
        self.rows = [{"linux_build_sha": BUILD, "manifest_json": encoded, "signature": signature}]
        self.queries = []

    def execute(self, statement, params):
        sql = str(statement)
        self.queries.append(sql)
        if "information_schema.TABLES" in sql: return _Result(self.metadata)
        if "information_schema.COLUMNS" in sql: return _Result(self.columns)
        if "information_schema.STATISTICS" in sql: return _Result(self.indexes)
        assert "FROM probiga.st_component_release_manifest" in sql
        assert params == {"linux_build_sha": BUILD}
        return _Result(self.rows)


def test_real_ed25519_signature_round_trip_binds_complete_manifest():
    conn = _Connection()
    assert ledger.load_component_release_row(conn, BUILD) == _manifest()
    encoded = conn.rows[0]["manifest_json"]
    signature = conn.rows[0]["signature"]
    for changed in (
        _manifest(contract_sha256="e" * 64),
        _manifest(created_at="2026-09-19T11:00:00Z"),
        _manifest(parent_linux_build_sha="c" * 40),
    ):
        changed_json, _ = ledger.sign_component_manifest(changed, conn.key)
        with pytest.raises(ledger.ComponentLedgerError, match="signature"):
            ledger.verify_signed_component_manifest(changed_json, signature, conn.public_key, BUILD)
    with pytest.raises(ledger.ComponentLedgerError, match="signature"):
        ledger.verify_signed_component_manifest(encoded, signature, conn.public_key, ANCHOR)


def test_runtime_cannot_forge_a_row_by_rehashing_or_signing_with_its_own_key():
    conn = _Connection()
    fake_key = Ed25519PrivateKey.generate()
    # This covers a mutable ordinary row and a session-local temporary table:
    # the real table's privileged metadata key remains the verification anchor.
    encoded, signature = ledger.sign_component_manifest(_manifest(contract_sha256="e" * 64), fake_key)
    conn.rows[0].update(manifest_json=encoded, signature=signature)
    with pytest.raises(ledger.ComponentLedgerError, match="signature"):
        ledger.load_component_release_row(conn, BUILD)
    conn.rows[0]["public_key"] = ledger.signing_public_key(fake_key)
    with pytest.raises(ledger.ComponentLedgerError, match="row"):
        ledger.load_component_release_row(conn, BUILD)


def test_signatures_are_domain_separated():
    conn = _Connection()
    row = conn.rows[0]
    wrong_domain = conn.key.sign(row["manifest_json"].encode("utf-8"))
    row["signature"] = base64.b64encode(wrong_domain).decode("ascii")
    with pytest.raises(ledger.ComponentLedgerError, match="signature"):
        ledger.load_component_release_row(conn, BUILD)


@pytest.mark.parametrize("change", ["duplicate", "spaces", "extra", "algorithm", "bad-base64", "wrong-key-size"])
def test_metadata_is_exact_canonical_public_key_record(change):
    conn = _Connection()
    comment = conn.metadata[0]["table_comment"]
    value = json.loads(comment)
    if change == "duplicate": comment = '{"schema":"x",' + comment[1:]
    elif change == "spaces": comment = json.dumps(value)
    else:
        if change == "extra": value["allow_unsigned"] = True
        if change == "algorithm": value["algorithm"] = "none"
        if change == "bad-base64": value["public_key"] += "\n"
        if change == "wrong-key-size": value["public_key"] = base64.b64encode(b"x" * 31).decode()
        comment = json.dumps(value, sort_keys=True, separators=(",", ":"))
    with pytest.raises(ledger.ComponentLedgerError, match="metadata"):
        ledger.parse_component_ledger_metadata(comment)


@pytest.mark.parametrize(("field", "value"), [
    ("current_database", "other"), ("table_schema", "other"),
    ("table_name", "st_component_release_forged"), ("table_type", "VIEW"), ("engine", "MEMORY"),
])
def test_foreign_database_view_and_non_innodb_table_rejected(field, value):
    conn = _Connection()
    conn.metadata[0][field] = value
    with pytest.raises(ledger.ComponentLedgerError, match="physical identity"):
        ledger.load_component_release_row(conn, BUILD)
    assert len(conn.queries) == 1


@pytest.mark.parametrize(("field", "value"), [
    ("column_type", "varchar(40)"), ("collation_name", "ascii_general_ci"),
    ("is_nullable", "YES"), ("column_default", ""), ("extra", "STORED GENERATED"), ("ordinal_position", 2),
])
def test_exact_column_contract_required(field, value):
    conn = _Connection()
    conn.columns[0][field] = value
    with pytest.raises(ledger.ComponentLedgerError, match="columns"):
        ledger.load_component_release_row(conn, BUILD)


@pytest.mark.parametrize(("field", "value"), [
    ("index_name", "untrusted"), ("non_unique", 1), ("column_name", "signature"),
    ("sub_part", 20), ("expression", "(1)"),
])
def test_unique_full_linux_sha_primary_key_required(field, value):
    conn = _Connection()
    conn.indexes[0][field] = value
    with pytest.raises(ledger.ComponentLedgerError, match="primary key"):
        ledger.load_component_release_row(conn, BUILD)


@pytest.mark.parametrize("change", ["missing", "duplicate", "wrong-sha", "signature-padding", "json-spaces"])
def test_missing_tampered_or_ambiguous_rows_fail_closed(change):
    conn = _Connection()
    if change == "missing": conn.rows.clear()
    elif change == "duplicate": conn.rows *= 2
    elif change == "wrong-sha": conn.rows[0]["linux_build_sha"] = ANCHOR
    elif change == "signature-padding": conn.rows[0]["signature"] += "="
    else: conn.rows[0]["manifest_json"] += " "
    with pytest.raises(ledger.ComponentLedgerError):
        ledger.load_component_release_row(conn, BUILD)


def test_non_root_cannot_read_or_create_signing_key(monkeypatch):
    monkeypatch.setattr(os, "geteuid", lambda: 1000, raising=False)
    with pytest.raises(ledger.ComponentLedgerError, match="requires root"):
        ledger.load_signing_key(create=True)


@pytest.mark.skipif(os.name != "posix" or not hasattr(os, "geteuid") or os.geteuid() != 0, reason="requires POSIX root for real key ownership/fsync")
def test_signing_key_is_permanent_private_and_rejects_real_file_tampering(monkeypatch, tmp_path):
    # tmp_path's ancestors can include /tmp; use an isolated root-owned parent
    # assertion stub only for the temporary hierarchy, retaining real file IO.
    path = tmp_path / "signing.key"
    tmp_path.chmod(0o755)
    monkeypatch.setattr(ledger, "SIGNING_KEY_PATH", path)
    original_lstat = ledger.Path.lstat
    def guarded_lstat(candidate):
        info = original_lstat(candidate)
        if candidate in path.parents and candidate != tmp_path:
            return SimpleNamespace(st_mode=stat.S_IFDIR | 0o755, st_uid=0)
        return info
    monkeypatch.setattr(ledger.Path, "lstat", guarded_lstat)
    install = ledger._atomic_install_noreplace
    def interrupted(source, destination):
        install(source, destination)
        assert not source.exists() and destination.stat().st_nlink == 1
        raise KeyboardInterrupt("simulated termination after key publication")
    monkeypatch.setattr(ledger, "_atomic_install_noreplace", interrupted)
    with pytest.raises(KeyboardInterrupt):
        ledger.load_signing_key(create=True)
    first_bytes = path.read_bytes()
    monkeypatch.setattr(ledger, "_atomic_install_noreplace", install)
    first = ledger.load_signing_key(create=True)
    assert path.read_bytes() == first_bytes
    assert ledger.signing_public_key(ledger.load_signing_key(create=True)) == ledger.signing_public_key(first)
    assert path.stat().st_mode & 0o777 == 0o600
    assert path.stat().st_nlink == 1
    path.chmod(0o644)
    with pytest.raises(ledger.ComponentLedgerError, match="unsafe"):
        ledger.load_signing_key()
