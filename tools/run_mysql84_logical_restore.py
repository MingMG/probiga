#!/usr/bin/env python3
"""Fail-closed logical seed restore into an empty Oracle MySQL 8.4.11 target.

This command is deliberately narrow.  It accepts only the output described by
one ``sanitize_mysql55_dump_for_mysql84.py`` manifest, verifies the complete
large file without loading it into memory, proves the identity and emptiness of
the target over TLS, and then feeds the dump to the Oracle 8.4.11 ``mysql``
client in the same session as ``SET SESSION sql_log_bin=0``.

The global binary log is never disabled.  Both before and after the seed the
server must report ``log_bin=ON``, ``binlog_format=ROW``, and
``log_bin_trust_function_creators=OFF``.  The session-only suppression avoids
duplicating a very large initial seed in the binary log.  A later incremental
or application write is therefore still protected by the server-wide binary
log policy.

``rehearsal`` can never target port 3306.  ``final-frozen`` additionally
requires an exact operator attestation; it may use 3306, although restoring on
a temporary local port before the service switch remains operationally safer.

Passwords are read from a protected MySQL ``[client]`` option file.  They are
never placed in argv, the environment, logs, exceptions, or JSON evidence.
The subprocess is launched directly (never through a shell), its real return
code is waited, and stdout/stderr are separate no-overwrite artifacts.
"""

from __future__ import annotations

import argparse
import configparser
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Iterable

import pymysql
from pymysql.cursors import DictCursor

# Keep both documented invocation styles working:
# ``python -m tools.run_mysql84_logical_restore`` and
# ``python tools/run_mysql84_logical_restore.py``.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tools.mysql_acceptance_tls import require_mysql_acceptance_ssl_ca
from tools.run_mysql55_consistent_dump import (
    DumpError as SourceDumpError,
    assert_protected_client_option_file as _assert_source_option_file_protected,
)
from tools.sanitize_mysql55_dump_for_mysql84 import (
    split_large_insert_line,
)

# MySQL 8.4.11 on Windows has reproduced parser access violations at the
# previous 64 KiB and 32 KiB physical INSERT boundaries.  Keep the sanitizer's
# default unchanged for compatibility, but use a smaller restore boundary by
# default.
RESTORE_DEFAULT_MAX_INSERT_BYTES = 16 * 1024
from tools.mysql84_bulk_transform import (
    BulkTransformError,
    BulkTransformStats,
    transform_dump_lines,
)


EXPECTED_SERVER_VERSION = "8.4.11"
EXPECTED_SCHEMAS = ("biga", "probiga", "probiga_qmt_history")
SYSTEM_SCHEMAS = frozenset(
    {"information_schema", "mysql", "performance_schema", "sys"}
)
MODES = ("rehearsal", "final-frozen")
FINAL_FROZEN_ATTESTATION = "I_CONFIRM_SOURCE_WRITES_ARE_FROZEN_AND_TARGET_IS_OFFLINE"
SESSION_BINLOG_OFF_MARKER = "PROBIGA_RESTORE_SESSION_BINLOG_OFF_OK"
EXPECTED_LOWER_CASE_TABLE_NAMES = 1
EXPECTED_CHARACTER_SET_SERVER = "utf8mb4"
EXPECTED_COLLATION_SERVER = "utf8mb4_general_ci"
EXPECTED_DEFAULT_COLLATION_FOR_UTF8MB4 = "utf8mb4_general_ci"
EXPECTED_GLOBAL_TIME_ZONE = "+08:00"
EXPECTED_SQL_MODE = frozenset(
    {
        "STRICT_TRANS_TABLES",
        "ERROR_FOR_DIVISION_BY_ZERO",
        "NO_ZERO_DATE",
        "NO_ZERO_IN_DATE",
        "NO_ENGINE_SUBSTITUTION",
        "ONLY_FULL_GROUP_BY",
    }
)
EXPECTED_MAX_ALLOWED_PACKET = 256 * 1024 * 1024

_ALLOWED_CLIENT_OPTIONS = frozenset(
    {"host", "port", "user", "password", "protocol"}
)
_VERSION_RE = re.compile(
    r"^8\.4\.11(?:[-+][0-9A-Za-z][0-9A-Za-z._-]*)?$"
)
_MYSQL_CLIENT_VERSION_RE = re.compile(
    r"\b(?:Ver|Distrib)\s+8\.4\.11"
    r"(?:[-+][0-9A-Za-z][0-9A-Za-z._-]*)?(?=$|[\s,()])",
    re.IGNORECASE,
)
_ORACLE_COMMENT_RE = re.compile(
    r"^MySQL (?:Community|Enterprise) Server\b", re.IGNORECASE
)
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_DISTRIBUTIONS = ("mariadb", "percona")
_MYSQL55_DUMP_HEADER_RE = re.compile(
    rb"(?im)^-- MySQL dump .*\bDistrib[ \t]+5\.5\.20"
    rb"(?:[-+][0-9A-Za-z][0-9A-Za-z._-]*)?(?=$|[\s,()])"
)
_STDERR_ERROR_RE = re.compile(rb"error", re.IGNORECASE)
_SET_GLOBAL_RE = re.compile(rb"\bset\s+(?:@@\s*)?global\b", re.IGNORECASE)
_RESET_CONNECTION_RE = re.compile(rb"\breset\s+connection\b", re.IGNORECASE)
_CLIENT_SESSION_COMMAND_RE = re.compile(
    rb"(?im)^[ \t]*(?:source|connect)(?:[ \t]|$)"
    rb"|^[ \t]*\\[.r](?:[ \t]|$)"
)
_DUMP_FORBIDDEN_LITERALS = (
    b"no_auto_create_user",
    b"sql_log_bin",
    b"log_bin_trust_function_creators",
)

_HASH_CHUNK_SIZE = 8 * 1024 * 1024
_SCAN_OVERLAP = 64 * 1024
_HEAD_BYTES = 128 * 1024
_TAIL_BYTES = 128 * 1024
_MAX_MANIFEST_BYTES = 4 * 1024 * 1024
_BELOW_NORMAL_PRIORITY_CLASS = 0x00004000


class RestoreError(RuntimeError):
    """A restore safety condition failed closed."""


class RestoreWriteError(OSError):
    """A mysql pipe failed while retaining streaming-transform evidence."""

    def __init__(self, cause: OSError, stats: BulkTransformStats | None) -> None:
        super().__init__(str(cause))
        self.transform_stats = stats


@dataclass(frozen=True, repr=False)
class AdminClientOptions:
    host: str
    port: int
    user: str
    password: str = field(repr=False)


@dataclass(frozen=True)
class MySQLClientIdentity:
    executable: str
    version_output: str


@dataclass(frozen=True)
class SanitizedDumpIdentity:
    path: str
    bytes: int
    sha256: str
    footer: str
    sanitizer_manifest: str
    sanitizer_manifest_sha256: str
    source_path: str
    source_bytes: int
    source_sha256: str
    changed_statements: int
    removed_tokens: int


@dataclass(frozen=True)
class TargetObservation:
    version: str
    version_comment: str
    server_uuid: str
    port: int
    datadir: str
    connection_id: int
    tls_cipher: str
    global_log_bin: bool
    global_binlog_format: str
    global_log_bin_trust_function_creators: bool
    business_schemas: tuple[str, ...]
    tables_by_schema: dict[str, int]
    observed_at_utc: str
    lower_case_table_names: int = EXPECTED_LOWER_CASE_TABLE_NAMES
    character_set_server: str = EXPECTED_CHARACTER_SET_SERVER
    collation_server: str = EXPECTED_COLLATION_SERVER
    default_collation_for_utf8mb4: str = EXPECTED_DEFAULT_COLLATION_FOR_UTF8MB4
    global_time_zone: str = EXPECTED_GLOBAL_TIME_ZONE
    global_sql_mode: str = ",".join(sorted(EXPECTED_SQL_MODE))
    global_require_secure_transport: bool = True
    global_local_infile: bool = False
    global_max_allowed_packet: int = EXPECTED_MAX_ALLOWED_PACKET


@dataclass(frozen=True)
class ArtifactPaths:
    stdout_log: Path
    stderr_log: Path
    evidence: Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _lexists(path: Path) -> bool:
    return os.path.lexists(os.fspath(path))


def _strip_option_value(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def _canonical_uuid(value: object, *, name: str) -> str:
    raw = str(value or "").strip().lower()
    try:
        parsed = uuid.UUID(raw)
    except (ValueError, AttributeError) as exc:
        raise RestoreError(f"{name} must be a canonical UUID") from exc
    canonical = str(parsed)
    if raw != canonical:
        raise RestoreError(f"{name} must be a canonical lowercase UUID")
    return canonical


def _resolve_existing_file(path: Path, *, name: str) -> Path:
    if not path.is_absolute():
        raise RestoreError(f"{name} path must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise RestoreError(f"{name} does not exist") from exc
    if not resolved.is_file():
        raise RestoreError(f"{name} must be a regular file")
    return resolved


def _resolve_existing_directory(path: Path, *, name: str) -> Path:
    if not path.is_absolute():
        raise RestoreError(f"{name} path must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except (OSError, RuntimeError) as exc:
        raise RestoreError(f"{name} does not exist") from exc
    if not resolved.is_dir():
        raise RestoreError(f"{name} must be a directory")
    return resolved


def _same_path(left: Path, right: Path) -> bool:
    try:
        return os.path.samefile(left, right)
    except OSError:
        return os.path.normcase(os.path.normpath(str(left))) == os.path.normcase(
            os.path.normpath(str(right))
        )


def assert_protected_client_option_file(path: Path) -> Path:
    """Require an absolute credential file with the existing ACL policy."""

    if not path.is_absolute():
        raise RestoreError("admin client option file path must be absolute")
    try:
        return _assert_source_option_file_protected(path)
    except (SourceDumpError, OSError, RuntimeError) as exc:
        raise RestoreError(str(exc)) from None


def read_admin_client_options(path: Path, *, expected_port: int) -> AdminClientOptions:
    """Read only minimal local TCP admin credentials; TLS stays independent."""

    protected = assert_protected_client_option_file(path)
    parser = configparser.RawConfigParser(interpolation=None, strict=True)
    try:
        with protected.open("r", encoding="utf-8-sig") as stream:
            parser.read_file(stream)
    except (OSError, configparser.Error) as exc:
        raise RestoreError("could not parse the protected admin option file") from exc
    if parser.sections() != ["client"]:
        raise RestoreError("admin option file must contain only one [client] section")
    option_names = {name.casefold() for name, _ in parser.items("client", raw=True)}
    unexpected = sorted(option_names - _ALLOWED_CLIENT_OPTIONS)
    if unexpected:
        raise RestoreError(
            "admin option file contains unsupported options: " + ", ".join(unexpected)
        )

    def option(name: str, default: str = "") -> str:
        return _strip_option_value(
            parser.get("client", name, fallback=default, raw=True)
        )

    host = option("host")
    user = option("user")
    password = option("password")
    protocol = option("protocol", "tcp").casefold()
    try:
        port = int(option("port"))
    except ValueError as exc:
        raise RestoreError("admin option file contains an invalid port") from exc
    if host != "127.0.0.1":
        raise RestoreError("target admin host must be exactly 127.0.0.1")
    if port != expected_port:
        raise RestoreError("target admin option port does not match expected port")
    if protocol != "tcp":
        raise RestoreError("target admin protocol must be TCP")
    if not user or not password:
        raise RestoreError("admin option file requires non-empty user and password")
    return AdminClientOptions(host=host, port=port, user=user, password=password)


def validate_ca_file(path: Path) -> Path:
    if not path.is_absolute():
        raise RestoreError("target SSL CA path must be absolute")
    try:
        validated = require_mysql_acceptance_ssl_ca(str(path))
    except ValueError as exc:
        raise RestoreError(str(exc)) from None
    return Path(validated.ssl_ca)


def _scrub_mysql_environment() -> dict[str, str]:
    environment = os.environ.copy()
    for name in tuple(environment):
        normalized = name.upper()
        if normalized.startswith("MYSQL_") or normalized.startswith("MARIADB_"):
            environment.pop(name, None)
    return environment


def inspect_mysql_client(executable: Path) -> MySQLClientIdentity:
    resolved = _resolve_existing_file(executable, name="mysql client executable")
    environment = _scrub_mysql_environment()
    try:
        try:
            completed = subprocess.run(
                [str(resolved), "--version"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=environment,
                shell=False,
                timeout=20,
                check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise RestoreError("could not inspect mysql client version") from exc
    finally:
        environment.clear()
    if completed.returncode != 0:
        raise RestoreError("mysql client --version failed")
    combined = (completed.stdout + b"\n" + completed.stderr).decode(
        "utf-8", errors="replace"
    ).strip()
    lowered = combined.casefold()
    if (
        not combined
        or any(token in lowered for token in _FORBIDDEN_DISTRIBUTIONS)
        or _MYSQL_CLIENT_VERSION_RE.search(combined) is None
        or "mysql" not in lowered
    ):
        raise RestoreError("mysql client must be exact Oracle MySQL 8.4.11")
    return MySQLClientIdentity(executable=str(resolved), version_output=combined)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(_HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _load_sanitizer_manifest(path: Path) -> tuple[Path, dict[str, Any], str]:
    resolved = _resolve_existing_file(path, name="sanitizer manifest")
    if resolved.stat().st_size > _MAX_MANIFEST_BYTES:
        raise RestoreError("sanitizer manifest is unreasonably large")
    manifest_sha256 = _sha256_file(resolved)
    try:
        with resolved.open("r", encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RestoreError("sanitizer manifest is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise RestoreError("sanitizer manifest root must be an object")
    return resolved, dict(value), manifest_sha256


def _scan_and_hash_sanitized_dump(path: Path) -> tuple[int, str, bytes, bytes]:
    """Hash and safety-scan a large dump with bounded memory."""

    digest = hashlib.sha256()
    total = 0
    head = bytearray()
    tail = b""
    overlap = b""
    with path.open("rb") as stream:
        while chunk := stream.read(_HASH_CHUNK_SIZE):
            total += len(chunk)
            digest.update(chunk)
            if len(head) < _HEAD_BYTES:
                head.extend(chunk[: _HEAD_BYTES - len(head)])
            tail = (tail + chunk)[-_TAIL_BYTES:]
            window = overlap + chunk
            lowered = window.lower()
            for forbidden in _DUMP_FORBIDDEN_LITERALS:
                if forbidden in lowered:
                    raise RestoreError(
                        "sanitized dump contains a forbidden restore-control token"
                    )
            if _SET_GLOBAL_RE.search(window) is not None:
                raise RestoreError("sanitized dump contains a forbidden SET GLOBAL statement")
            if _RESET_CONNECTION_RE.search(window) is not None:
                raise RestoreError("sanitized dump contains a forbidden RESET CONNECTION")
            if _CLIENT_SESSION_COMMAND_RE.search(window) is not None:
                raise RestoreError(
                    "sanitized dump contains a forbidden client session command"
                )
            overlap = window[-_SCAN_OVERLAP:]
    return total, digest.hexdigest(), bytes(head), tail


def _terminal_dump_footer(tail: bytes) -> str:
    stripped = tail.rstrip(b" \t\r\n")
    last_line = stripped.splitlines()[-1] if stripped else b""
    if not last_line.startswith(b"-- Dump completed on "):
        raise RestoreError("sanitized dump is missing its terminal Dump completed footer")
    return last_line.decode("utf-8", errors="replace")


def validate_sanitized_dump(
    dump_path: Path, sanitizer_manifest: Path
) -> SanitizedDumpIdentity:
    """Bind the exact sanitizer output artifact and reject the raw 5.5 dump."""

    dump_resolved = _resolve_existing_file(dump_path, name="sanitized dump")
    manifest_path, report, manifest_sha256 = _load_sanitizer_manifest(
        sanitizer_manifest
    )

    def required_string(name: str) -> str:
        value = report.get(name)
        if not isinstance(value, str) or not value.strip():
            raise RestoreError(f"sanitizer manifest field {name} is invalid")
        return value.strip()

    source_text = required_string("source")
    output_text = required_string("output")
    source_path = Path(source_text)
    output_path = Path(output_text)
    if not source_path.is_absolute() or not output_path.is_absolute():
        raise RestoreError("sanitizer manifest source/output paths must be absolute")
    output_resolved = _resolve_existing_file(output_path, name="manifest output")
    if not _same_path(dump_resolved, output_resolved):
        raise RestoreError("restore input is not the sanitizer manifest output path")
    if _same_path(dump_resolved, source_path):
        raise RestoreError("raw MySQL 5.5 dump input is forbidden")

    def required_nonnegative_int(name: str) -> int:
        value = report.get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise RestoreError(f"sanitizer manifest field {name} is invalid")
        return value

    output_bytes = required_nonnegative_int("output_bytes")
    source_bytes = required_nonnegative_int("source_bytes")
    changed = required_nonnegative_int("changed_statements")
    removed = required_nonnegative_int("removed_tokens")
    output_sha256 = required_string("output_sha256").lower()
    source_sha256 = required_string("source_sha256").lower()
    if not _SHA256_RE.fullmatch(output_sha256) or not _SHA256_RE.fullmatch(source_sha256):
        raise RestoreError("sanitizer manifest SHA-256 field is invalid")
    if changed != 2 or removed != 2:
        raise RestoreError(
            "sanitizer manifest must prove exactly two changed statements and tokens"
        )

    actual_bytes, actual_sha256, head, tail = _scan_and_hash_sanitized_dump(
        dump_resolved
    )
    if actual_bytes <= 0:
        raise RestoreError("sanitized dump is empty")
    if actual_bytes != output_bytes:
        raise RestoreError("sanitized dump byte count does not match its manifest")
    if actual_sha256 != output_sha256:
        raise RestoreError("sanitized dump SHA-256 does not match its manifest")
    lowered_head = head.lower()
    if (
        _MYSQL55_DUMP_HEADER_RE.search(head) is None
        or b"mariadb" in lowered_head
        or b"percona" in lowered_head
    ):
        raise RestoreError("restore input is not an Oracle MySQL 5.5.20 dump")
    footer = _terminal_dump_footer(tail)
    return SanitizedDumpIdentity(
        path=str(dump_resolved),
        bytes=actual_bytes,
        sha256=actual_sha256,
        footer=footer,
        sanitizer_manifest=str(manifest_path),
        sanitizer_manifest_sha256=manifest_sha256,
        source_path=str(source_path),
        source_bytes=source_bytes,
        source_sha256=source_sha256,
        changed_statements=changed,
        removed_tokens=removed,
    )


def _connect_target(
    options: AdminClientOptions, ca_file: Path
) -> pymysql.Connection:
    try:
        return pymysql.connect(
            host=options.host,
            port=options.port,
            user=options.user,
            password=options.password,
            charset="utf8mb4",
            autocommit=True,
            connect_timeout=10,
            read_timeout=60,
            write_timeout=60,
            cursorclass=DictCursor,
            ssl_ca=str(ca_file),
            ssl_verify_cert=True,
        )
    except pymysql.MySQLError:
        raise RestoreError("target TLS preflight connection failed") from None


def _fetch_mapping(cursor: Any, sql: str) -> Mapping[str, Any]:
    cursor.execute(sql)
    row = cursor.fetchone()
    if not isinstance(row, Mapping):
        raise RestoreError("target preflight query returned no mapping row")
    return row


def _mapping_value_casefold(row: Mapping[str, Any], name: str) -> Any:
    """Read INFORMATION_SCHEMA fields regardless of driver label casing."""

    if name in row:
        return row[name]
    expected = name.casefold()
    for key, value in row.items():
        if str(key).casefold() == expected:
            return value
    return None


def _as_server_bool(value: object, *, name: str) -> bool:
    if value in (0, "0", b"0", False):
        return False
    if value in (1, "1", b"1", True):
        return True
    raise RestoreError(f"target returned an invalid {name} value")


def _canonical_server_datadir(value: object) -> Path:
    text = str(value or "").strip()
    if not text:
        raise RestoreError("target returned an empty datadir")
    return _resolve_existing_directory(Path(text), name="target server datadir")


def inspect_target(
    options: AdminClientOptions,
    ca_file: Path,
    *,
    expected_server_uuid: str,
    expected_server_port: int,
    expected_datadir: Path,
    allow_log_bin_trust_function_creators: bool = False,
) -> TargetObservation:
    """Read and validate target identity, TLS, global policy, and catalogue.

    The normal restore/smoke path requires ``log_bin_trust_function_creators``
    to remain OFF.  The guarded trigger-migration window is the one explicit
    exception: its parent wrapper enables the setting only for the child
    migration process and independently verifies that it is OFF again before
    releasing the window.  Keeping this opt-in here prevents ordinary callers
    from silently accepting the relaxed setting.
    """

    connection = _connect_target(options, ca_file)
    try:
        with connection.cursor() as cursor:
            identity = _fetch_mapping(
                cursor,
                """
                SELECT
                    @@version AS version,
                    @@version_comment AS version_comment,
                    @@server_uuid AS server_uuid,
                    @@port AS port,
                    @@datadir AS datadir,
                    CONNECTION_ID() AS connection_id,
                    @@global.log_bin AS global_log_bin,
                    @@global.binlog_format AS global_binlog_format,
                    @@global.log_bin_trust_function_creators
                        AS global_log_bin_trust_function_creators,
                    @@lower_case_table_names AS lower_case_table_names,
                    @@global.character_set_server AS character_set_server,
                    @@global.collation_server AS collation_server,
                    @@global.default_collation_for_utf8mb4
                        AS default_collation_for_utf8mb4,
                    @@global.time_zone AS global_time_zone,
                    @@global.sql_mode AS global_sql_mode,
                    @@global.require_secure_transport
                        AS global_require_secure_transport,
                    @@global.local_infile AS global_local_infile,
                    @@global.max_allowed_packet AS global_max_allowed_packet
                """,
            )
            cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
            tls_row = cursor.fetchone()
            cursor.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "ORDER BY schema_name"
            )
            schema_rows = cursor.fetchall()
            cursor.execute(
                """
                SELECT table_schema, COUNT(*) AS table_count
                FROM information_schema.tables
                WHERE table_schema NOT IN
                    ('information_schema', 'mysql', 'performance_schema', 'sys')
                GROUP BY table_schema
                ORDER BY table_schema
                """
            )
            table_rows = cursor.fetchall()
    except pymysql.MySQLError:
        raise RestoreError("target identity/catalogue preflight query failed") from None
    finally:
        connection.close()

    version = str(identity.get("version") or "").strip()
    version_comment = str(identity.get("version_comment") or "").strip()
    combined = f"{version} {version_comment}".casefold()
    if (
        _VERSION_RE.fullmatch(version) is None
        or _ORACLE_COMMENT_RE.match(version_comment) is None
        or any(token in combined for token in _FORBIDDEN_DISTRIBUTIONS)
    ):
        raise RestoreError("target must be exact Oracle MySQL 8.4.11")
    try:
        port = int(identity.get("port"))
        connection_id = int(identity.get("connection_id"))
    except (TypeError, ValueError) as exc:
        raise RestoreError("target returned invalid numeric identity values") from exc
    if port != expected_server_port or port != options.port:
        raise RestoreError("connected target port does not match the expected port")
    observed_uuid = _canonical_uuid(identity.get("server_uuid"), name="server UUID")
    if observed_uuid != expected_server_uuid:
        raise RestoreError("connected target server UUID does not match expectation")
    observed_datadir = _canonical_server_datadir(identity.get("datadir"))
    if not _same_path(observed_datadir, expected_datadir):
        raise RestoreError("connected target datadir does not match expectation")

    tls_cipher = ""
    if isinstance(tls_row, Mapping):
        tls_cipher = str(tls_row.get("Value") or tls_row.get("value") or "").strip()
    elif isinstance(tls_row, (tuple, list)) and len(tls_row) >= 2:
        tls_cipher = str(tls_row[1] or "").strip()
    if not tls_cipher:
        raise RestoreError("target connection negotiated no TLS cipher")

    global_log_bin = _as_server_bool(
        identity.get("global_log_bin"), name="global log_bin"
    )
    binlog_format = str(identity.get("global_binlog_format") or "").strip().upper()
    trust = _as_server_bool(
        identity.get("global_log_bin_trust_function_creators"),
        name="global log_bin_trust_function_creators",
    )
    try:
        lower_case_table_names = int(identity.get("lower_case_table_names"))
        max_allowed_packet = int(identity.get("global_max_allowed_packet"))
    except (TypeError, ValueError) as exc:
        raise RestoreError("target returned invalid runtime policy values") from exc
    character_set_server = str(identity.get("character_set_server") or "").strip()
    collation_server = str(identity.get("collation_server") or "").strip()
    default_collation = str(
        identity.get("default_collation_for_utf8mb4") or ""
    ).strip()
    global_time_zone = str(identity.get("global_time_zone") or "").strip()
    global_sql_mode = str(identity.get("global_sql_mode") or "").strip()
    require_secure_transport = _as_server_bool(
        identity.get("global_require_secure_transport"),
        name="global require_secure_transport",
    )
    local_infile = _as_server_bool(
        identity.get("global_local_infile"), name="global local_infile"
    )
    observed_sql_mode = frozenset(
        token.strip().upper()
        for token in global_sql_mode.split(",")
        if token.strip()
    )
    if not global_log_bin:
        raise RestoreError("target global log_bin must remain ON")
    if binlog_format != "ROW":
        raise RestoreError("target global binlog_format must be ROW")
    if trust and not allow_log_bin_trust_function_creators:
        raise RestoreError(
            "target global log_bin_trust_function_creators must be OFF"
        )
    if lower_case_table_names != EXPECTED_LOWER_CASE_TABLE_NAMES:
        raise RestoreError("target lower_case_table_names must be 1")
    if character_set_server.casefold() != EXPECTED_CHARACTER_SET_SERVER:
        raise RestoreError("target character_set_server must be utf8mb4")
    if collation_server.casefold() != EXPECTED_COLLATION_SERVER:
        raise RestoreError("target collation_server must be utf8mb4_general_ci")
    if default_collation.casefold() != EXPECTED_DEFAULT_COLLATION_FOR_UTF8MB4:
        raise RestoreError(
            "target default_collation_for_utf8mb4 must be utf8mb4_general_ci"
        )
    if global_time_zone != EXPECTED_GLOBAL_TIME_ZONE:
        raise RestoreError("target global time_zone must be +08:00")
    if observed_sql_mode != EXPECTED_SQL_MODE:
        raise RestoreError("target global sql_mode is not the strict production policy")
    if not require_secure_transport:
        raise RestoreError("target require_secure_transport must be ON")
    if local_infile:
        raise RestoreError("target local_infile must be OFF")
    if max_allowed_packet < EXPECTED_MAX_ALLOWED_PACKET:
        raise RestoreError("target max_allowed_packet is below 256 MiB")

    schemas: list[str] = []
    for row in schema_rows:
        if not isinstance(row, Mapping):
            raise RestoreError("target schema query returned an invalid row")
        name = str(_mapping_value_casefold(row, "schema_name") or "")
        if name and name.casefold() not in SYSTEM_SCHEMAS:
            schemas.append(name)
    tables_by_schema: dict[str, int] = {}
    for row in table_rows:
        if not isinstance(row, Mapping):
            raise RestoreError("target table query returned an invalid row")
        name = str(_mapping_value_casefold(row, "table_schema") or "")
        try:
            count = int(_mapping_value_casefold(row, "table_count"))
        except (TypeError, ValueError) as exc:
            raise RestoreError("target table query returned an invalid count") from exc
        if not name:
            raise RestoreError("target table query returned an empty schema name")
        tables_by_schema[name] = count

    return TargetObservation(
        version=version,
        version_comment=version_comment,
        server_uuid=observed_uuid,
        port=port,
        datadir=str(observed_datadir),
        connection_id=connection_id,
        tls_cipher=tls_cipher,
        global_log_bin=global_log_bin,
        global_binlog_format=binlog_format,
        global_log_bin_trust_function_creators=trust,
        business_schemas=tuple(sorted(schemas)),
        tables_by_schema=dict(sorted(tables_by_schema.items())),
        observed_at_utc=_utc_now(),
        lower_case_table_names=lower_case_table_names,
        character_set_server=character_set_server,
        collation_server=collation_server,
        default_collation_for_utf8mb4=default_collation,
        global_time_zone=global_time_zone,
        global_sql_mode=global_sql_mode,
        global_require_secure_transport=require_secure_transport,
        global_local_infile=local_infile,
        global_max_allowed_packet=max_allowed_packet,
    )


def validate_empty_target(observation: TargetObservation) -> None:
    if observation.business_schemas:
        raise RestoreError(
            "target is not empty; non-system schemas exist: "
            + ", ".join(observation.business_schemas)
        )
    if observation.tables_by_schema:
        raise RestoreError("target contains non-system tables")


def validate_restored_target(observation: TargetObservation) -> None:
    if observation.business_schemas != tuple(sorted(EXPECTED_SCHEMAS)):
        raise RestoreError("restored target does not contain the exact business schema set")
    if set(observation.tables_by_schema) != set(EXPECTED_SCHEMAS):
        raise RestoreError("restored target table catalogue is missing a business schema")
    empty = sorted(
        schema for schema, count in observation.tables_by_schema.items() if count <= 0
    )
    if empty:
        raise RestoreError(
            "restored target has no tables in business schema(s): " + ", ".join(empty)
        )


def build_mysql_command(
    *,
    identity: MySQLClientIdentity,
    client_option_file: Path,
    ca_file: Path,
    expected_port: int,
) -> tuple[str, ...]:
    """Build a password-free argv; defaults-file must be the first option."""

    return (
        identity.executable,
        f"--defaults-file={client_option_file}",
        "--protocol=tcp",
        "--host=127.0.0.1",
        f"--port={expected_port}",
        "--ssl-mode=VERIFY_CA",
        f"--ssl-ca={ca_file}",
        "--default-character-set=utf8mb4",
        "--binary-mode=1",
        "--skip-reconnect",
        "--local-infile=0",
        "--batch",
        "--skip-column-names",
        "--max_allowed_packet=1G",
    )


def _write_restore_input(
    stdin: BinaryIO,
    dump_path: Path,
    *,
    split_insert_bytes: int | None,
    defer_secondary_indexes: Sequence[str] = (),
) -> BulkTransformStats | None:
    stats: BulkTransformStats | None = None

    def write_piece(piece: bytes) -> None:
        try:
            stdin.write(piece)
        except (BrokenPipeError, OSError) as exc:
            raise RestoreWriteError(exc, stats) from exc

    prelude = (
        "SET SESSION sql_log_bin=0;\n"
        "SELECT IF(@@SESSION.sql_log_bin=0, "
        f"'{SESSION_BINLOG_OFF_MARKER}', 'PROBIGA_RESTORE_SESSION_BINLOG_OFF_BAD');\n"
    ).encode("ascii")
    write_piece(prelude)
    with dump_path.open("rb") as source:
        if defer_secondary_indexes:
            try:
                transformed, stats = transform_dump_lines(
                    source, defer_secondary_indexes
                )
                lines: Iterable[bytes] = transformed
            except BulkTransformError as exc:
                raise RestoreError(str(exc)) from exc
        else:
            stats = None
            lines = source

        if split_insert_bytes is None and not defer_secondary_indexes:
            while chunk := source.read(_HASH_CHUNK_SIZE):
                stdin.write(chunk)
        else:
            try:
                for line in lines:
                    pieces = [line]
                    if split_insert_bytes is not None:
                        pieces, _ = split_large_insert_line(line, split_insert_bytes)
                    for piece in pieces:
                        write_piece(piece)
            except BulkTransformError as exc:
                raise RestoreError(str(exc)) from exc
    try:
        stdin.flush()
    except (BrokenPipeError, OSError) as exc:
        raise RestoreWriteError(exc, stats) from exc
    return stats


def _stream_contains(path: Path, pattern: re.Pattern[bytes]) -> bool:
    overlap = b""
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            window = overlap + chunk
            if pattern.search(window) is not None:
                return True
            overlap = window[-256:]
    return False


def _stream_contains_literal(path: Path, literal: bytes) -> bool:
    lowered = literal.lower()
    overlap = b""
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            window = overlap + chunk
            if lowered in window.lower():
                return True
            overlap = window[-max(1, len(lowered) - 1) :]
    return False


def _resolve_artifacts(
    *, evidence: Path, stdout_log: Path | None, stderr_log: Path | None
) -> ArtifactPaths:
    evidence = evidence.expanduser().resolve()
    stdout_log = (
        stdout_log.expanduser().resolve()
        if stdout_log is not None
        else Path(str(evidence) + ".stdout.log")
    )
    stderr_log = (
        stderr_log.expanduser().resolve()
        if stderr_log is not None
        else Path(str(evidence) + ".stderr.log")
    )
    paths = (evidence, stdout_log, stderr_log)
    if len({os.path.normcase(os.path.normpath(str(path))) for path in paths}) != 3:
        raise RestoreError("evidence, stdout, and stderr paths must be distinct")
    for path in paths:
        if _lexists(path):
            raise RestoreError(f"refusing to overwrite existing artifact: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
    return ArtifactPaths(stdout_log=stdout_log, stderr_log=stderr_log, evidence=evidence)


def _write_atomic_json(payload: Mapping[str, Any], destination: Path) -> None:
    if _lexists(destination):
        raise RestoreError(f"refusing to overwrite existing evidence: {destination}")
    temporary = destination.parent / f".{destination.name}.{uuid.uuid4().hex}.partial"
    try:
        with temporary.open("xb") as stream:
            encoded = (
                json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
                + "\n"
            ).encode("utf-8")
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        if os.name == "nt":
            os.rename(temporary, destination)
        else:
            os.link(temporary, destination)
            temporary.unlink()
    except FileExistsError as exc:
        raise RestoreError(f"refusing to overwrite existing evidence: {destination}") from exc
    except OSError as exc:
        raise RestoreError("could not atomically publish restore evidence") from exc
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _validate_mode(
    mode: str, expected_port: int, writes_frozen_attestation: str | None
) -> bool:
    if mode not in MODES:
        raise RestoreError("invalid restore mode")
    if not (1 <= expected_port <= 65535):
        raise RestoreError("expected server port is invalid")
    if mode == "rehearsal":
        if expected_port == 3306:
            raise RestoreError("rehearsal mode is forbidden from using port 3306")
        if writes_frozen_attestation:
            raise RestoreError(
                "writes-frozen attestation is valid only in final-frozen mode"
            )
        return False
    if writes_frozen_attestation != FINAL_FROZEN_ATTESTATION:
        raise RestoreError(
            "final-frozen mode requires the exact offline writes-frozen attestation"
        )
    return True


def run_logical_restore(
    *,
    mode: str,
    client_option_file: Path,
    ssl_ca: Path,
    mysql_executable: Path,
    dump_path: Path,
    sanitizer_manifest: Path,
    expected_server_uuid: str,
    expected_server_port: int,
    expected_datadir: Path,
    evidence: Path,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
    writes_frozen_attestation: str | None = None,
    split_insert_bytes: int | None = RESTORE_DEFAULT_MAX_INSERT_BYTES,
    defer_secondary_indexes: Sequence[str] = (),
) -> dict[str, Any]:
    """Validate, stream, wait, verify, and attest one initial seed restore."""

    if split_insert_bytes is not None and split_insert_bytes < 1024:
        raise RestoreError("split_insert_bytes must be at least 1024 or None")

    writes_frozen = _validate_mode(
        mode, expected_server_port, writes_frozen_attestation
    )
    expected_uuid = _canonical_uuid(
        expected_server_uuid, name="expected server UUID"
    )
    target_datadir = _resolve_existing_directory(
        expected_datadir, name="expected target datadir"
    )
    artifacts = _resolve_artifacts(
        evidence=evidence, stdout_log=stdout_log, stderr_log=stderr_log
    )
    protected_options = assert_protected_client_option_file(client_option_file)
    options = read_admin_client_options(
        protected_options, expected_port=expected_server_port
    )
    ca_file = validate_ca_file(ssl_ca)
    dump_identity = validate_sanitized_dump(dump_path, sanitizer_manifest)
    client_identity = inspect_mysql_client(mysql_executable)
    before = inspect_target(
        options,
        ca_file,
        expected_server_uuid=expected_uuid,
        expected_server_port=expected_server_port,
        expected_datadir=target_datadir,
    )
    validate_empty_target(before)

    command = build_mysql_command(
        identity=client_identity,
        client_option_file=protected_options,
        ca_file=ca_file,
        expected_port=expected_server_port,
    )
    if any(argument.casefold().startswith("--password") for argument in command):
        raise RestoreError("internal safety error: mysql argv contains a password option")

    started_at = _utc_now()
    started_monotonic = time.monotonic()
    environment = _scrub_mysql_environment()
    return_code: int | None = None
    write_error: str | None = None
    launched = False
    transform_stats: BulkTransformStats | None = None
    try:
        with artifacts.stdout_log.open("xb") as stdout_stream, artifacts.stderr_log.open(
            "xb"
        ) as stderr_stream:
            try:
                process = subprocess.Popen(
                    command,
                    stdin=subprocess.PIPE,
                    stdout=stdout_stream,
                    stderr=stderr_stream,
                    env=environment,
                    shell=False,
                    creationflags=(
                        _BELOW_NORMAL_PRIORITY_CLASS if os.name == "nt" else 0
                    ),
                )
                launched = True
            except OSError as exc:
                raise RestoreError("failed to launch Oracle MySQL 8.4 client") from exc
            try:
                if process.stdin is None:
                    raise RestoreError("mysql client stdin pipe was not created")
                try:
                    transform_stats = _write_restore_input(
                        process.stdin,
                        Path(dump_identity.path),
                        split_insert_bytes=split_insert_bytes,
                        defer_secondary_indexes=defer_secondary_indexes,
                    )
                except (BrokenPipeError, OSError) as exc:
                    transform_stats = getattr(exc, "transform_stats", transform_stats)
                    write_error = type(exc).__name__
                finally:
                    try:
                        process.stdin.close()
                    except (BrokenPipeError, OSError):
                        pass
                return_code = int(process.wait())
            except BaseException:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                raise
    finally:
        environment.clear()

    finished_at = _utc_now()
    duration_seconds = round(time.monotonic() - started_monotonic, 3)
    stdout_bytes = artifacts.stdout_log.stat().st_size
    stderr_bytes = artifacts.stderr_log.stat().st_size
    stdout_sha256 = _sha256_file(artifacts.stdout_log)
    stderr_sha256 = _sha256_file(artifacts.stderr_log)
    stderr_has_error = _stream_contains(artifacts.stderr_log, _STDERR_ERROR_RE)
    marker_seen = _stream_contains_literal(
        artifacts.stdout_log, SESSION_BINLOG_OFF_MARKER.encode("ascii")
    )

    base_evidence: dict[str, Any] = {
        "schema_version": 1,
        "mode": mode,
        "writes_frozen_attested": writes_frozen,
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "duration_seconds": duration_seconds,
        "input": asdict(dump_identity),
        "stream_transform": {
            "split_insert_bytes": split_insert_bytes,
            "split_large_multirow_inserts": split_insert_bytes is not None,
            "defer_secondary_indexes_requested": list(defer_secondary_indexes),
            "defer_secondary_indexes_matched": (
                []
                if transform_stats is None
                else [item.table for item in transform_stats.matched_tables]
            ),
            "deferred_secondary_index_statements_added": (
                0
                if transform_stats is None
                else transform_stats.added_index_statements
            ),
        },
        "mysql_client": asdict(client_identity),
        "target_before": asdict(before),
        "session_seed_policy": {
            "statement": "SET SESSION sql_log_bin=0",
            "verification_marker_seen": marker_seen,
            "global_log_bin_change_requested": False,
            "target_global_policy_verified_before": True,
        },
        "process": {
            "launched": launched,
            "return_code": return_code,
            "stdin_stream_error": write_error,
            "shell": False,
        },
        "artifacts": {
            "stdout_log": {
                "path": str(artifacts.stdout_log),
                "bytes": stdout_bytes,
                "sha256": stdout_sha256,
            },
            "stderr_log": {
                "path": str(artifacts.stderr_log),
                "bytes": stderr_bytes,
                "sha256": stderr_sha256,
                "contains_error_token": stderr_has_error,
            },
            "evidence": {"path": str(artifacts.evidence)},
        },
    }

    failure: str | None = None
    if return_code != 0:
        failure = f"mysql client failed with return code {return_code}"
    elif write_error is not None:
        failure = "mysql client input stream failed before completion"
    elif stderr_has_error:
        failure = "mysql client stderr contains an ERROR token"
    elif not marker_seen:
        failure = "mysql client did not prove session sql_log_bin was disabled"

    if failure is not None:
        failed = dict(base_evidence)
        failed.update({"status": "failed", "failure": failure})
        _write_atomic_json(failed, artifacts.evidence)
        raise RestoreError(failure)

    try:
        after = inspect_target(
            options,
            ca_file,
            expected_server_uuid=expected_uuid,
            expected_server_port=expected_server_port,
            expected_datadir=target_datadir,
        )
        validate_restored_target(after)
    except RestoreError as exc:
        failed = dict(base_evidence)
        failed.update({"status": "failed", "failure": str(exc)})
        _write_atomic_json(failed, artifacts.evidence)
        raise

    result = dict(base_evidence)
    result.update({"status": "success", "target_after": asdict(after)})
    _write_atomic_json(result, artifacts.evidence)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fail-closed Oracle MySQL 8.4.11 logical seed restore."
    )
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--client-options", type=Path, required=True)
    parser.add_argument("--ssl-ca", type=Path, required=True)
    parser.add_argument("--mysql", type=Path, required=True)
    parser.add_argument("--dump", type=Path, required=True)
    parser.add_argument("--sanitizer-manifest", type=Path, required=True)
    parser.add_argument("--expected-server-uuid", required=True)
    parser.add_argument("--expected-server-port", type=int, required=True)
    parser.add_argument("--expected-datadir", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--stdout-log", type=Path)
    parser.add_argument("--stderr-log", type=Path)
    parser.add_argument("--writes-frozen-attestation")
    parser.add_argument(
        "--split-insert-bytes",
        type=int,
        default=RESTORE_DEFAULT_MAX_INSERT_BYTES,
        help=(
            "Split multi-row INSERTs while streaming "
            f"(default: {RESTORE_DEFAULT_MAX_INSERT_BYTES}; use 0 to disable)."
        ),
    )
    parser.add_argument(
        "--defer-secondary-indexes",
        action="append",
        default=[],
        metavar="SCHEMA.TABLE",
        help=(
            "Defer secondary indexes for an explicitly named table during bulk "
            "load; may be repeated."
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_logical_restore(
            mode=args.mode,
            client_option_file=args.client_options,
            ssl_ca=args.ssl_ca,
            mysql_executable=args.mysql,
            dump_path=args.dump,
            sanitizer_manifest=args.sanitizer_manifest,
            expected_server_uuid=args.expected_server_uuid,
            expected_server_port=args.expected_server_port,
            expected_datadir=args.expected_datadir,
            evidence=args.evidence,
            stdout_log=args.stdout_log,
            stderr_log=args.stderr_log,
            writes_frozen_attestation=args.writes_frozen_attestation,
            split_insert_bytes=(None if args.split_insert_bytes == 0 else args.split_insert_bytes),
            defer_secondary_indexes=tuple(args.defer_secondary_indexes),
        )
    except (RestoreError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": result["status"],
                "mode": result["mode"],
                "evidence": result["artifacts"]["evidence"]["path"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
