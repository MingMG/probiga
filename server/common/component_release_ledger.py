"""Authenticity of component release records in a fixed publication ledger.

Runtime may write ordinary rows, so row hashes alone are not trusted. The
Ed25519 public key is anchored in privileged table metadata; the signing key
is read only by the root release publisher and never sent to MySQL.
"""
from __future__ import annotations

import base64
import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from sqlalchemy import text

from server.common.component_release import _atomic_install_noreplace, validate_component_release

TABLE_NAME = "st_component_release_manifest"
LEDGER_SCHEMA = "probiga.component-release-ledger.v1"
SIGNATURE_DOMAIN = b"probiga.component-release-attestation.v1\x00"
SIGNING_KEY_PATH = Path("/etc/probiga/component-release-signing.key")


class ComponentLedgerError(RuntimeError):
    pass


def _canonical(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ComponentLedgerError("duplicate component ledger field")
        result[key] = value
    return result


def _base64(value: object, length: int) -> bytes:
    try:
        if not isinstance(value, str):
            raise ValueError
        raw = base64.b64decode(value.encode("ascii"), validate=True)
        if len(raw) != length or base64.b64encode(raw).decode("ascii") != value:
            raise ValueError
        return raw
    except (ValueError, UnicodeError):
        raise ComponentLedgerError("component ledger encoding differs") from None


def build_component_ledger_metadata(public_key: str) -> str:
    _base64(public_key, 32)
    return _canonical({"schema": LEDGER_SCHEMA, "algorithm": "Ed25519", "public_key": public_key})


def parse_component_ledger_metadata(comment: object) -> str:
    try:
        if not isinstance(comment, str) or len(comment.encode("utf-8")) > 2048:
            raise ValueError
        value = json.loads(comment, object_pairs_hook=_unique)
        if not isinstance(value, dict) or set(value) != {"schema", "algorithm", "public_key"}:
            raise ValueError
        if value["schema"] != LEDGER_SCHEMA or value["algorithm"] != "Ed25519":
            raise ValueError
        if build_component_ledger_metadata(value["public_key"]) != comment:
            raise ValueError
        return value["public_key"]
    except (ValueError, TypeError, RuntimeError):
        raise ComponentLedgerError("component ledger metadata differs") from None


def signing_public_key(key: Ed25519PrivateKey) -> str:
    return base64.b64encode(key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)).decode("ascii")


def sign_component_manifest(manifest: dict[str, str], key: Ed25519PrivateKey) -> tuple[str, str]:
    encoded = _canonical(validate_component_release(manifest))
    signature = key.sign(SIGNATURE_DOMAIN + encoded.encode("utf-8"))
    return encoded, base64.b64encode(signature).decode("ascii")


def verify_signed_component_manifest(manifest_json: object, signature: object, public_key: str, expected_linux_sha: str) -> dict[str, str]:
    try:
        if not isinstance(manifest_json, str) or len(manifest_json.encode("utf-8")) > 16384:
            raise ValueError
        manifest = validate_component_release(json.loads(manifest_json, object_pairs_hook=_unique))
        if manifest["linux_build_sha"] != expected_linux_sha or _canonical(manifest) != manifest_json:
            raise ValueError
        Ed25519PublicKey.from_public_bytes(_base64(public_key, 32)).verify(
            _base64(signature, 64), SIGNATURE_DOMAIN + manifest_json.encode("utf-8"),
        )
        return manifest
    except (ValueError, TypeError, RuntimeError, InvalidSignature):
        raise ComponentLedgerError("component release signature differs") from None


def load_component_ledger_public_key(connection: Any) -> str:
    rows = connection.execute(text(
        "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, "
        "TABLE_TYPE AS table_type, ENGINE AS engine, TABLE_COMMENT AS table_comment, "
        "DATABASE() AS current_database FROM information_schema.TABLES "
        "WHERE TABLE_SCHEMA='probiga' AND TABLE_NAME=:table_name"
    ), {"table_name": TABLE_NAME}).mappings().all()
    if len(rows) != 1:
        raise ComponentLedgerError("component ledger metadata unavailable")
    row = rows[0]
    if (
        row.get("table_schema") != "probiga" or row.get("current_database") != "probiga"
        or row.get("table_name") != TABLE_NAME or row.get("table_type") != "BASE TABLE"
        or str(row.get("engine") or "").upper() != "INNODB"
    ):
        raise ComponentLedgerError("component ledger physical identity differs")
    columns = connection.execute(text(
        "SELECT COLUMN_NAME AS column_name, COLUMN_TYPE AS column_type, "
        "IS_NULLABLE AS is_nullable, COLUMN_DEFAULT AS column_default, EXTRA AS extra, "
        "COLLATION_NAME AS collation_name, ORDINAL_POSITION AS ordinal_position "
        "FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='probiga' "
        "AND TABLE_NAME=:table_name ORDER BY ORDINAL_POSITION"
    ), {"table_name": TABLE_NAME}).mappings().all()
    expected = (
        ("linux_build_sha", "char(40)", "ascii_bin"),
        ("manifest_json", "text", "utf8mb4_bin"),
        ("signature", "char(88)", "ascii_bin"),
    )
    if len(columns) != 3 or any(
        item.get("column_name") != name or item.get("column_type") != kind
        or item.get("is_nullable") != "NO" or item.get("column_default") is not None
        or item.get("extra") != "" or item.get("collation_name") != collation
        or item.get("ordinal_position") != index
        for index, (item, (name, kind, collation)) in enumerate(zip(columns, expected), 1)
    ):
        raise ComponentLedgerError("component ledger columns differ")
    indexes = connection.execute(text(
        "SELECT INDEX_NAME AS index_name, NON_UNIQUE AS non_unique, "
        "SEQ_IN_INDEX AS seq_in_index, COLUMN_NAME AS column_name, "
        "SUB_PART AS sub_part, EXPRESSION AS expression FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA='probiga' AND TABLE_NAME=:table_name"
    ), {"table_name": TABLE_NAME}).mappings().all()
    if len(indexes) != 1 or dict(indexes[0]) != {
        "index_name": "PRIMARY", "non_unique": 0, "seq_in_index": 1,
        "column_name": "linux_build_sha", "sub_part": None, "expression": None,
    }:
        raise ComponentLedgerError("component ledger primary key differs")
    return parse_component_ledger_metadata(row.get("table_comment"))


def load_component_release_row(connection: Any, linux_build_sha: str) -> dict[str, str]:
    import re
    if not isinstance(linux_build_sha, str) or re.fullmatch("[0-9a-f]{40}", linux_build_sha) is None or linux_build_sha == "0" * 40:
        raise ComponentLedgerError("component release build is invalid")
    public_key = load_component_ledger_public_key(connection)
    rows = connection.execute(text(
        "SELECT linux_build_sha, manifest_json, signature FROM probiga.st_component_release_manifest "
        "WHERE linux_build_sha=:linux_build_sha"
    ), {"linux_build_sha": linux_build_sha}).mappings().all()
    if len(rows) != 1 or set(rows[0]) != {"linux_build_sha", "manifest_json", "signature"} or rows[0].get("linux_build_sha") != linux_build_sha:
        raise ComponentLedgerError("component release row unavailable or ambiguous")
    return verify_signed_component_manifest(rows[0]["manifest_json"], rows[0]["signature"], public_key, linux_build_sha)


def load_signing_key(*, create: bool = False) -> Ed25519PrivateKey:
    """Read or create the single permanent root-only key; never rotate it."""
    if os.name != "posix" or not hasattr(os, "geteuid") or os.geteuid() != 0:
        raise ComponentLedgerError("component signing requires root")
    path = SIGNING_KEY_PATH
    try:
        for parent in path.parents:
            info = parent.lstat()
            if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
                raise ComponentLedgerError("component signing directory is unsafe")
        if stat.S_IMODE(path.parent.lstat().st_mode) != 0o755:
            raise ComponentLedgerError("component signing parent mode differs")
        if create and not os.path.lexists(path):
            key = Ed25519PrivateKey.generate().private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())
            descriptor, temporary = tempfile.mkstemp(prefix=".component-signing-", dir=path.parent)
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    stream.write(key)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.chmod(temporary, 0o600)
                try:
                    _atomic_install_noreplace(Path(temporary), path)
                except FileExistsError:
                    pass
                if os.path.lexists(temporary):
                    os.unlink(temporary)
                directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            finally:
                if os.path.lexists(temporary):
                    os.unlink(temporary)
        before = path.lstat()
        if not stat.S_ISREG(before.st_mode) or before.st_uid != 0 or before.st_nlink != 1 or stat.S_IMODE(before.st_mode) != 0o600:
            raise ComponentLedgerError("component signing key is unsafe")
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as stream:
            opened = os.fstat(stream.fileno())
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino) or opened.st_uid != 0 or opened.st_nlink != 1 or stat.S_IMODE(opened.st_mode) != 0o600:
                raise ComponentLedgerError("component signing key identity changed")
            encoded = stream.read(33)
        if len(encoded) != 32:
            raise ComponentLedgerError("component signing key format differs")
        return Ed25519PrivateKey.from_private_bytes(encoded)
    except OSError:
        raise ComponentLedgerError("component signing key unavailable") from None
