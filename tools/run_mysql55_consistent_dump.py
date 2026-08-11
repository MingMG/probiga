#!/usr/bin/env python3
"""Run a fail-closed logical backup of the frozen Oracle MySQL 5.5 source.

This orchestrator is intentionally specific to the ProBigA 5.5.20 source.  It
does not stop MySQL, change port 3306, lock tables, or freeze application
writers.  A maintenance operator must do that outside this process before a
``final-frozen`` run.

Credentials are accepted only through a protected, minimal MySQL ``[client]``
option file.  The child process is launched and waited by this Python process,
so the recorded return code is the real ``mysqldump`` return code rather than
the empty ``ExitCode`` sometimes observed when a detached PowerShell process is
monitored later.
"""

from __future__ import annotations

import argparse
import base64
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
from typing import Any

import pymysql
from pymysql.cursors import DictCursor


EXPECTED_SERVER_VERSION = "5.5.20"
EXPECTED_SERVER_PORT = 3306
EXPECTED_SCHEMAS = ("biga", "probiga", "probiga_qmt_history")
LEGACY_EMPTY_SCHEMA = "test"
FINAL_FROZEN_ATTESTATION = "I_CONFIRM_ALL_WRITES_ARE_FROZEN"
MODES = ("online-rehearsal", "final-frozen")

_MYSQL_VERSION_RE = re.compile(r"^5\.5\.20(?:[-+][0-9A-Za-z][0-9A-Za-z._-]*)?$")
_MYSQLDUMP_VERSION_RE = re.compile(
    r"\bDistrib\s+5\.5\.20(?:[-+][0-9A-Za-z][0-9A-Za-z._-]*)?\b",
    re.IGNORECASE,
)
_ORACLE_COMMENT_RE = re.compile(
    r"^MySQL (?:Community|Enterprise) Server\b", re.IGNORECASE
)
_FORBIDDEN_DISTRIBUTIONS = ("mariadb", "percona")
_SYSTEM_SCHEMAS = ("information_schema", "mysql", "performance_schema")
_ALLOWED_CLIENT_OPTIONS = frozenset({"host", "port", "user", "password", "protocol"})
_UNSAFE_WINDOWS_SIDS = frozenset(
    {
        "S-1-1-0",  # Everyone
        "S-1-5-7",  # Anonymous Logon
        "S-1-5-11",  # Authenticated Users
        "S-1-5-32-545",  # BUILTIN\\Users
        "S-1-5-32-546",  # BUILTIN\\Guests
    }
)
_WINDOWS_READ_MASK = 0x0001 | 0x0008 | 0x0080 | 0x20000
_BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
_HASH_CHUNK_SIZE = 8 * 1024 * 1024
_FOOTER_READ_SIZE = 64 * 1024
_HEADER_READ_SIZE = 4 * 1024 * 1024
_BINLOG_COORDINATE_RE = re.compile(
    rb"(?m)^-- CHANGE MASTER TO MASTER_LOG_FILE='([^'\r\n]+)', "
    rb"MASTER_LOG_POS=([0-9]+);\r?$"
)
_BINLOG_FILE_RE = re.compile(r"^[0-9A-Za-z._-]+$")


class DumpError(RuntimeError):
    """A validation or dump condition failed closed."""


@dataclass(frozen=True, repr=False)
class ClientOptions:
    host: str
    port: int
    user: str
    password: str = field(repr=False)


@dataclass(frozen=True)
class SourceIdentity:
    version: str
    version_comment: str
    port: int
    hostname: str
    server_id: int
    datadir: str
    connection_id: int
    read_only: bool
    log_bin: bool
    binlog_format: str


@dataclass(frozen=True)
class TableInventory:
    total_tables: int
    tables_by_schema: dict[str, int]
    canonical_sha256: str


@dataclass(frozen=True)
class LegacySchemaObservation:
    schema: str
    present: bool
    object_counts: dict[str, int]


@dataclass(frozen=True)
class SessionObservation:
    client_session_count: int
    client_session_sample: tuple[dict[str, Any], ...]
    active_transaction_count: int
    active_transaction_sample: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class SourcePreflight:
    identity: SourceIdentity
    schemas: tuple[str, ...]
    legacy_empty_schema: LegacySchemaObservation
    tables: TableInventory
    sessions: SessionObservation
    observed_at_utc: str


@dataclass(frozen=True)
class MysqldumpIdentity:
    executable: str
    version_output: str


@dataclass(frozen=True)
class ArtifactPaths:
    output: Path
    stdout_log: Path
    stderr_log: Path
    manifest: Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _strip_option_value(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def _lexists(path: Path) -> bool:
    return os.path.lexists(os.fspath(path))


def _windows_acl_entries(path: Path) -> list[dict[str, Any]]:
    """Return Windows allow/deny ACEs with identities translated to SIDs."""

    script = r"""
$ErrorActionPreference = 'Stop'
$path = [Environment]::GetEnvironmentVariable('PROBIGA_CLIENT_OPTION_PATH')
$acl = Get-Acl -LiteralPath $path
$rows = @($acl.Access | ForEach-Object {
    [pscustomobject]@{
        sid = $_.IdentityReference.Translate(
            [System.Security.Principal.SecurityIdentifier]
        ).Value
        type = $_.AccessControlType.ToString()
        rights = [int64]$_.FileSystemRights
        inherited = [bool]$_.IsInherited
    }
})
ConvertTo-Json -Compress -InputObject $rows
""".strip()
    encoded = base64.b64encode(script.encode("utf-16le")).decode("ascii")
    env = os.environ.copy()
    env["PROBIGA_CLIENT_OPTION_PATH"] = str(path)
    try:
        completed = subprocess.run(
            [
                "powershell.exe",
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-EncodedCommand",
                encoded,
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise DumpError("could not inspect the Windows ACL of the client option file") from exc
    finally:
        env.pop("PROBIGA_CLIENT_OPTION_PATH", None)
    if completed.returncode != 0:
        raise DumpError("could not inspect the Windows ACL of the client option file")
    try:
        decoded = json.loads(completed.stdout.decode("utf-8-sig"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DumpError("Windows ACL inspection returned invalid data") from exc
    if decoded is None:
        return []
    if isinstance(decoded, Mapping):
        return [dict(decoded)]
    if not isinstance(decoded, list) or not all(isinstance(item, Mapping) for item in decoded):
        raise DumpError("Windows ACL inspection returned invalid entries")
    return [dict(item) for item in decoded]


def _windows_acl_is_safe(entries: Sequence[Mapping[str, Any]]) -> bool:
    for entry in entries:
        sid = str(entry.get("sid", "")).upper()
        ace_type = str(entry.get("type", "")).casefold()
        try:
            rights = int(entry.get("rights", 0))
        except (TypeError, ValueError):
            return False
        if sid in _UNSAFE_WINDOWS_SIDS and ace_type == "allow" and rights & _WINDOWS_READ_MASK:
            return False
    return True


def assert_protected_client_option_file(path: Path) -> Path:
    resolved = path.expanduser().resolve(strict=True)
    if not resolved.is_file():
        raise DumpError("client option path must be a regular file")
    if os.name == "nt":
        entries = _windows_acl_entries(resolved)
        if not entries:
            raise DumpError("client option file has no inspectable Windows ACL entries")
        if not _windows_acl_is_safe(entries):
            raise DumpError("client option file grants read access to a broad Windows principal")
    elif resolved.stat().st_mode & 0o077:
        raise DumpError("client option file must not be accessible by group or other users")
    return resolved


def read_client_options(path: Path) -> ClientOptions:
    """Read a minimal local source credential file without logging its secret."""

    protected = assert_protected_client_option_file(path)
    parser = configparser.RawConfigParser(interpolation=None, strict=False)
    try:
        with protected.open("r", encoding="utf-8-sig") as stream:
            parser.read_file(stream)
    except (OSError, configparser.Error) as exc:
        raise DumpError("could not parse the protected client option file") from exc
    if parser.sections() != ["client"]:
        raise DumpError("client option file must contain only one [client] section")
    option_names = {name.casefold() for name, _value in parser.items("client", raw=True)}
    unexpected = sorted(option_names - _ALLOWED_CLIENT_OPTIONS)
    if unexpected:
        raise DumpError("client option file contains unsupported options: " + ", ".join(unexpected))

    def option(name: str, default: str = "") -> str:
        return _strip_option_value(parser.get("client", name, fallback=default, raw=True))

    host = option("host")
    user = option("user")
    password = option("password")
    protocol = option("protocol", "tcp").casefold()
    try:
        port = int(option("port"))
    except ValueError as exc:
        raise DumpError("client option file contains an invalid port") from exc
    if host != "127.0.0.1":
        raise DumpError("source client option host must be exactly 127.0.0.1")
    if port != EXPECTED_SERVER_PORT:
        raise DumpError(f"source client option port must be exactly {EXPECTED_SERVER_PORT}")
    if protocol != "tcp":
        raise DumpError("source client option protocol must be TCP")
    if not user or not password:
        raise DumpError("client option file must contain non-empty user and password values")
    return ClientOptions(host=host, port=port, user=user, password=password)


def _connect_source(options: ClientOptions) -> pymysql.Connection:
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
        )
    except pymysql.MySQLError:
        raise DumpError("source preflight connection failed") from None


def _fetch_one(cursor: Any, sql: str) -> Mapping[str, Any]:
    cursor.execute(sql)
    row = cursor.fetchone()
    if not isinstance(row, Mapping):
        raise DumpError("source preflight query returned no mapping row")
    return row


def _safe_session_sample(rows: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    return tuple(
        {
            "id": int(row["id"]),
            "user": str(row.get("user") or ""),
            "host": str(row.get("host") or ""),
            "database": str(row.get("database_name") or ""),
            "command": str(row.get("command") or ""),
            "seconds": int(row.get("seconds") or 0),
            "state": str(row.get("state") or ""),
        }
        for row in rows
    )


def _safe_transaction_sample(rows: Sequence[Mapping[str, Any]]) -> tuple[dict[str, Any], ...]:
    return tuple(
        {
            "transaction_id": str(row.get("transaction_id") or ""),
            "thread_id": int(row.get("thread_id") or 0),
            "state": str(row.get("state") or ""),
            "started": str(row.get("started") or ""),
        }
        for row in rows
    )


def preflight_source(options: ClientOptions) -> SourcePreflight:
    """Inspect the source using SELECT-only queries before launching a dump."""

    connection = _connect_source(options)
    try:
        with connection.cursor() as cursor:
            identity_row = _fetch_one(
                cursor,
                "SELECT @@version AS version, @@version_comment AS version_comment, "
                "@@port AS port, @@hostname AS hostname, @@server_id AS server_id, "
                "@@datadir AS datadir, CONNECTION_ID() AS connection_id, "
                "@@global.read_only AS read_only, @@global.log_bin AS log_bin, "
                "@@global.binlog_format AS binlog_format",
            )
            version = str(identity_row.get("version") or "").strip()
            version_comment = str(identity_row.get("version_comment") or "").strip()
            combined = f"{version} {version_comment}".casefold()
            if (
                _MYSQL_VERSION_RE.fullmatch(version) is None
                or _ORACLE_COMMENT_RE.match(version_comment) is None
                or any(token in combined for token in _FORBIDDEN_DISTRIBUTIONS)
            ):
                raise DumpError("source must be the exact Oracle MySQL 5.5.20 build")
            try:
                source_port = int(identity_row["port"])
            except (KeyError, TypeError, ValueError) as exc:
                raise DumpError("source returned an invalid server port") from exc
            if source_port != EXPECTED_SERVER_PORT:
                raise DumpError(f"source server port must be exactly {EXPECTED_SERVER_PORT}")

            cursor.execute(
                "SELECT SCHEMA_NAME AS schema_name FROM information_schema.SCHEMATA "
                "WHERE LOWER(SCHEMA_NAME) NOT IN (%s, %s, %s) ORDER BY SCHEMA_NAME",
                _SYSTEM_SCHEMAS,
            )
            schema_rows = cursor.fetchall()
            schemas = tuple(str(row["schema_name"]) for row in schema_rows)
            schema_set = set(schemas)
            expected_schema_set = set(EXPECTED_SCHEMAS)
            missing_schemas = expected_schema_set - schema_set
            extra_schemas = schema_set - expected_schema_set
            if (
                missing_schemas
                or len(schemas) != len(schema_set)
                or not extra_schemas.issubset({LEGACY_EMPTY_SCHEMA})
            ):
                raise DumpError(
                    "source schemas must be the exact business set plus, at most, an empty "
                    f"legacy {LEGACY_EMPTY_SCHEMA} schema"
                )

            legacy_counts = {
                "tables": 0,
                "routines": 0,
                "events": 0,
                "triggers": 0,
            }
            legacy_present = LEGACY_EMPTY_SCHEMA in schema_set
            if legacy_present:
                cursor.execute(
                    "SELECT 'tables' AS object_type, COUNT(*) AS object_count "
                    "FROM information_schema.TABLES WHERE TABLE_SCHEMA = %s "
                    "UNION ALL "
                    "SELECT 'routines' AS object_type, COUNT(*) AS object_count "
                    "FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = %s "
                    "UNION ALL "
                    "SELECT 'events' AS object_type, COUNT(*) AS object_count "
                    "FROM information_schema.EVENTS WHERE EVENT_SCHEMA = %s "
                    "UNION ALL "
                    "SELECT 'triggers' AS object_type, COUNT(*) AS object_count "
                    "FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = %s",
                    (LEGACY_EMPTY_SCHEMA,) * 4,
                )
                count_rows = list(cursor.fetchall())
                observed_types: set[str] = set()
                for row in count_rows:
                    object_type = str(row.get("object_type") or "").casefold()
                    if object_type not in legacy_counts or object_type in observed_types:
                        raise DumpError("legacy test schema object audit returned invalid rows")
                    try:
                        count = int(row.get("object_count"))
                    except (TypeError, ValueError) as exc:
                        raise DumpError(
                            "legacy test schema object audit returned an invalid count"
                        ) from exc
                    if count < 0:
                        raise DumpError(
                            "legacy test schema object audit returned an invalid count"
                        )
                    legacy_counts[object_type] = count
                    observed_types.add(object_type)
                if observed_types != set(legacy_counts):
                    raise DumpError("legacy test schema object audit returned incomplete rows")
                nonempty_types = [
                    name for name, count in legacy_counts.items() if count != 0
                ]
                if nonempty_types:
                    raise DumpError(
                        "legacy test schema is allowed only when TABLES, ROUTINES, EVENTS, "
                        "and TRIGGERS are all empty; non-empty types: "
                        + ", ".join(nonempty_types)
                    )

            placeholders = ", ".join(["%s"] * len(EXPECTED_SCHEMAS))
            cursor.execute(
                "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, "
                "TABLE_TYPE AS table_type, ENGINE AS engine "
                "FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA IN ({placeholders}) "
                "ORDER BY TABLE_SCHEMA, TABLE_NAME",
                EXPECTED_SCHEMAS,
            )
            table_rows = list(cursor.fetchall())
            if not table_rows:
                raise DumpError("source business schemas contain no tables")
            invalid_tables = [
                row
                for row in table_rows
                if str(row.get("table_type") or "").upper() != "BASE TABLE"
                or str(row.get("engine") or "").casefold() != "innodb"
            ]
            if invalid_tables:
                sample = invalid_tables[0]
                raise DumpError(
                    "every business object must be an InnoDB base table; first mismatch is "
                    f"{sample.get('table_schema')}.{sample.get('table_name')}"
                )
            counts = {schema: 0 for schema in EXPECTED_SCHEMAS}
            table_digest = hashlib.sha256()
            for row in table_rows:
                schema = str(row["table_schema"])
                if schema not in counts:
                    raise DumpError("table inventory returned an unexpected schema")
                counts[schema] += 1
                canonical = "|".join(
                    (
                        schema,
                        str(row["table_name"]),
                        str(row["table_type"]),
                        str(row["engine"]),
                    )
                )
                table_digest.update(canonical.encode("utf-8"))
                table_digest.update(b"\n")
            if any(count == 0 for count in counts.values()):
                raise DumpError("each expected business schema must contain at least one InnoDB table")

            client_filter = (
                "ID <> CONNECTION_ID() AND "
                "LOWER(COALESCE(USER, '')) NOT IN ('system user', 'event_scheduler')"
            )
            client_count_row = _fetch_one(
                cursor,
                "SELECT COUNT(*) AS count FROM information_schema.PROCESSLIST WHERE "
                + client_filter,
            )
            cursor.execute(
                "SELECT ID AS id, USER AS user, HOST AS host, DB AS database_name, "
                "COMMAND AS command, TIME AS seconds, STATE AS state "
                "FROM information_schema.PROCESSLIST WHERE "
                + client_filter
                + " ORDER BY ID LIMIT 20"
            )
            client_sample = _safe_session_sample(list(cursor.fetchall()))

            transaction_count_row = _fetch_one(
                cursor,
                "SELECT COUNT(*) AS count FROM information_schema.INNODB_TRX "
                "WHERE trx_mysql_thread_id <> CONNECTION_ID()",
            )
            cursor.execute(
                "SELECT trx_id AS transaction_id, trx_mysql_thread_id AS thread_id, "
                "trx_state AS state, trx_started AS started "
                "FROM information_schema.INNODB_TRX "
                "WHERE trx_mysql_thread_id <> CONNECTION_ID() "
                "ORDER BY trx_started, trx_id LIMIT 20"
            )
            transaction_sample = _safe_transaction_sample(list(cursor.fetchall()))

            identity = SourceIdentity(
                version=version,
                version_comment=version_comment,
                port=source_port,
                hostname=str(identity_row.get("hostname") or ""),
                server_id=int(identity_row.get("server_id") or 0),
                datadir=str(identity_row.get("datadir") or ""),
                connection_id=int(identity_row.get("connection_id") or 0),
                read_only=bool(int(identity_row.get("read_only") or 0)),
                log_bin=bool(int(identity_row.get("log_bin") or 0)),
                binlog_format=str(identity_row.get("binlog_format") or ""),
            )
            inventory = TableInventory(
                total_tables=len(table_rows),
                tables_by_schema=counts,
                canonical_sha256=table_digest.hexdigest(),
            )
            sessions = SessionObservation(
                client_session_count=int(client_count_row.get("count") or 0),
                client_session_sample=client_sample,
                active_transaction_count=int(transaction_count_row.get("count") or 0),
                active_transaction_sample=transaction_sample,
            )
            return SourcePreflight(
                identity=identity,
                schemas=tuple(EXPECTED_SCHEMAS),
                legacy_empty_schema=LegacySchemaObservation(
                    schema=LEGACY_EMPTY_SCHEMA,
                    present=legacy_present,
                    object_counts=legacy_counts,
                ),
                tables=inventory,
                sessions=sessions,
                observed_at_utc=_utc_now(),
            )
    except pymysql.MySQLError:
        raise DumpError("source preflight query failed") from None
    finally:
        connection.close()


def inspect_mysqldump_version(executable: Path) -> MysqldumpIdentity:
    resolved = executable.expanduser().resolve(strict=True)
    if not resolved.is_file():
        raise DumpError("mysqldump executable path must be a regular file")
    try:
        completed = subprocess.run(
            [str(resolved), "--version"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise DumpError("could not inspect mysqldump version") from exc
    raw = (completed.stdout + b" " + completed.stderr)[:16_384]
    output = raw.decode("utf-8", errors="replace").strip()
    lowered = output.casefold()
    if (
        completed.returncode != 0
        or _MYSQLDUMP_VERSION_RE.search(output) is None
        or any(token in lowered for token in _FORBIDDEN_DISTRIBUTIONS)
    ):
        raise DumpError("mysqldump must be the exact Oracle MySQL 5.5.20 client")
    return MysqldumpIdentity(executable=str(resolved), version_output=output)


def _resolve_artifact_paths(
    output: Path,
    stdout_log: Path | None,
    stderr_log: Path | None,
    manifest: Path | None,
) -> ArtifactPaths:
    output = output.expanduser().resolve(strict=False)
    resolved = ArtifactPaths(
        output=output,
        stdout_log=(stdout_log or Path(str(output) + ".stdout.log"))
        .expanduser()
        .resolve(strict=False),
        stderr_log=(stderr_log or Path(str(output) + ".stderr.log"))
        .expanduser()
        .resolve(strict=False),
        manifest=(manifest or Path(str(output) + ".manifest.json"))
        .expanduser()
        .resolve(strict=False),
    )
    values = (resolved.output, resolved.stdout_log, resolved.stderr_log, resolved.manifest)
    if len(set(values)) != len(values):
        raise DumpError("output, logs, and manifest must use four different paths")
    for path in values:
        if not path.parent.is_dir():
            raise DumpError(f"artifact parent directory does not exist: {path.parent}")
        if _lexists(path):
            raise DumpError(f"refusing to overwrite an existing artifact: {path}")
    return resolved


def build_mysqldump_command(
    *,
    identity: MysqldumpIdentity,
    client_option_file: Path,
    result_file: Path,
    capture_binlog_coordinates: bool = False,
) -> tuple[str, ...]:
    """Build the fixed dump command; the defaults file must be option one."""

    command = [
        identity.executable,
        f"--defaults-file={client_option_file}",
        "--protocol=tcp",
        "--host=127.0.0.1",
        f"--port={EXPECTED_SERVER_PORT}",
        "--single-transaction",
        "--quick",
        "--skip-lock-tables",
    ]
    if capture_binlog_coordinates:
        command.append("--master-data=2")
    command.extend(
        [
        "--routines",
        "--events",
        "--triggers",
        "--hex-blob",
        "--default-character-set=utf8mb4",
        "--max_allowed_packet=256M",
        f"--result-file={result_file}",
        "--databases",
        *EXPECTED_SCHEMAS,
        ]
    )
    return tuple(command)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(_HASH_CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def _validate_dump_file(path: Path) -> tuple[int, str, str]:
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise DumpError("mysqldump did not create its result file") from exc
    if size <= 0:
        raise DumpError("mysqldump result file is empty")
    with path.open("rb") as stream:
        stream.seek(max(0, size - _FOOTER_READ_SIZE))
        tail = stream.read()
    stripped = tail.rstrip(b" \t\r\n")
    last_line = stripped.splitlines()[-1] if stripped else b""
    if not last_line.startswith(b"-- Dump completed on "):
        raise DumpError("mysqldump result is missing the terminal Dump completed footer")
    footer = last_line.decode("utf-8", errors="replace")
    return size, _sha256_file(path), footer


def _read_dump_binlog_coordinates(path: Path) -> dict[str, Any]:
    """Read and validate the single ``--master-data=2`` snapshot coordinate."""

    with path.open("rb") as stream:
        header = stream.read(_HEADER_READ_SIZE)
    matches = _BINLOG_COORDINATE_RE.findall(header)
    if len(matches) != 1:
        raise DumpError(
            "dump does not contain exactly one commented master binlog coordinate"
        )
    raw_file, raw_position = matches[0]
    try:
        binlog_file = raw_file.decode("ascii")
        binlog_position = int(raw_position)
    except (UnicodeDecodeError, ValueError) as exc:
        raise DumpError("dump contains an invalid master binlog coordinate") from exc
    if not _BINLOG_FILE_RE.fullmatch(binlog_file) or binlog_position < 4:
        raise DumpError("dump contains an unsafe master binlog coordinate")
    return {"file": binlog_file, "position": binlog_position}


def _publish_no_replace(source: Path, destination: Path) -> None:
    if _lexists(destination):
        raise DumpError(f"refusing to overwrite an existing artifact: {destination}")
    try:
        if os.name == "nt":
            os.rename(source, destination)
        else:
            os.link(source, destination)
            source.unlink()
    except FileExistsError as exc:
        raise DumpError(f"refusing to overwrite an existing artifact: {destination}") from exc
    except OSError as exc:
        raise DumpError(f"could not atomically publish artifact: {destination}") from exc


def _write_json_temp(payload: Mapping[str, Any], destination: Path) -> Path:
    temporary = destination.parent / f".{destination.name}.{uuid.uuid4().hex}.partial"
    encoded = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    try:
        with temporary.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
    except OSError as exc:
        try:
            temporary.unlink(missing_ok=True)
        except OSError:
            pass
        raise DumpError("could not prepare the atomic JSON manifest") from exc
    return temporary


def _scrub_mysql_environment() -> dict[str, str]:
    environment = os.environ.copy()
    for name in ("MYSQL_PWD", "MYSQL_HOST", "MYSQL_TCP_PORT", "MYSQL_UNIX_PORT"):
        environment.pop(name, None)
    return environment


def _validate_mode(mode: str, writes_frozen_attestation: str | None) -> bool:
    if mode not in MODES:
        raise DumpError("invalid dump mode")
    if mode == "final-frozen":
        if writes_frozen_attestation != FINAL_FROZEN_ATTESTATION:
            raise DumpError(
                "final-frozen mode requires the exact explicit writes-frozen attestation"
            )
        return True
    if writes_frozen_attestation:
        raise DumpError("writes-frozen attestation is valid only in final-frozen mode")
    return False


def run_consistent_dump(
    *,
    mode: str,
    client_option_file: Path,
    mysqldump_executable: Path,
    output: Path,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
    manifest: Path | None = None,
    writes_frozen_attestation: str | None = None,
    capture_binlog_coordinates: bool = False,
) -> dict[str, Any]:
    """Validate, launch, wait, verify, and publish one logical source dump."""

    writes_frozen = _validate_mode(mode, writes_frozen_attestation)
    artifacts = _resolve_artifact_paths(output, stdout_log, stderr_log, manifest)
    client_options = read_client_options(client_option_file)
    protected_options = client_option_file.expanduser().resolve(strict=True)
    dump_identity = inspect_mysqldump_version(mysqldump_executable)
    preflight = preflight_source(client_options)
    del client_options

    if capture_binlog_coordinates:
        if not preflight.identity.log_bin:
            raise DumpError("binlog coordinate capture requires source log_bin=ON")
        if preflight.identity.server_id <= 0:
            raise DumpError("binlog coordinate capture requires a positive source server_id")

    if mode == "final-frozen":
        if preflight.sessions.client_session_count:
            raise DumpError(
                "final-frozen preflight found other client sessions; writers are not proven frozen"
            )
        if preflight.sessions.active_transaction_count:
            raise DumpError(
                "final-frozen preflight found active transactions; writers are not proven frozen"
            )

    partial_output = artifacts.output.parent / (
        f".{artifacts.output.name}.{uuid.uuid4().hex}.partial"
    )
    if _lexists(partial_output):
        raise DumpError("generated partial output path already exists")
    command = build_mysqldump_command(
        identity=dump_identity,
        client_option_file=protected_options,
        result_file=partial_output,
        capture_binlog_coordinates=capture_binlog_coordinates,
    )
    creationflags = _BELOW_NORMAL_PRIORITY_CLASS if os.name == "nt" else 0
    started_at = _utc_now()
    started_monotonic = time.monotonic()
    environment = _scrub_mysql_environment()
    try:
        with artifacts.stdout_log.open("xb") as stdout_stream, artifacts.stderr_log.open(
            "xb"
        ) as stderr_stream:
            try:
                process = subprocess.Popen(
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=stdout_stream,
                    stderr=stderr_stream,
                    env=environment,
                    shell=False,
                    creationflags=creationflags,
                )
            except OSError as exc:
                raise DumpError("failed to launch mysqldump") from exc
            try:
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
    stderr_bytes = artifacts.stderr_log.stat().st_size
    stdout_bytes = artifacts.stdout_log.stat().st_size
    if return_code != 0:
        raise DumpError(f"mysqldump failed with return code {return_code}")
    if stderr_bytes != 0:
        raise DumpError("mysqldump wrote to stderr despite returning success")

    output_bytes, output_sha256, footer = _validate_dump_file(partial_output)
    binlog_coordinates = (
        _read_dump_binlog_coordinates(partial_output)
        if capture_binlog_coordinates
        else None
    )
    manifest_dump_options = [
        f"--defaults-file={protected_options}",
        *command[2:],
    ]
    manifest_dump_options = [
        f"--result-file={artifacts.output}"
        if option.startswith("--result-file=")
        else option
        for option in manifest_dump_options
    ]
    manifest_payload: dict[str, Any] = {
        "schema_version": 1,
        "status": "success",
        "mode": mode,
        "writes_frozen_attested": writes_frozen,
        "binlog_coordinates_captured": capture_binlog_coordinates,
        "snapshot_binlog_coordinates": binlog_coordinates,
        "started_at_utc": started_at,
        "finished_at_utc": finished_at,
        "duration_seconds": duration_seconds,
        "source_preflight": asdict(preflight),
        "mysqldump": {
            "executable": dump_identity.executable,
            "version_output": dump_identity.version_output,
            "return_code": return_code,
            "priority": "below-normal" if os.name == "nt" else "inherited",
            "fixed_options": manifest_dump_options,
            "schemas": list(EXPECTED_SCHEMAS),
        },
        "artifacts": {
            "dump": {
                "path": str(artifacts.output),
                "bytes": output_bytes,
                "sha256": output_sha256,
                "footer": footer,
            },
            "stdout_log": {
                "path": str(artifacts.stdout_log),
                "bytes": stdout_bytes,
                "sha256": _sha256_file(artifacts.stdout_log),
            },
            "stderr_log": {
                "path": str(artifacts.stderr_log),
                "bytes": stderr_bytes,
                "sha256": _sha256_file(artifacts.stderr_log),
            },
            "manifest": {"path": str(artifacts.manifest)},
        },
    }
    manifest_temp = _write_json_temp(manifest_payload, artifacts.manifest)
    try:
        _publish_no_replace(partial_output, artifacts.output)
        _publish_no_replace(manifest_temp, artifacts.manifest)
    finally:
        try:
            manifest_temp.unlink(missing_ok=True)
        except OSError:
            pass
    return manifest_payload


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fail-closed Oracle MySQL 5.5.20 consistent dump orchestrator."
    )
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--client-options", type=Path, required=True)
    parser.add_argument("--mysqldump", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--stdout-log", type=Path)
    parser.add_argument("--stderr-log", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument(
        "--capture-binlog-coordinates",
        action="store_true",
        help="Require log_bin=ON and record the --master-data=2 snapshot position.",
    )
    parser.add_argument(
        "--writes-frozen-attestation",
        help=(
            "Required only for final-frozen; exact value: "
            + FINAL_FROZEN_ATTESTATION
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = run_consistent_dump(
            mode=args.mode,
            client_option_file=args.client_options,
            mysqldump_executable=args.mysqldump,
            output=args.output,
            stdout_log=args.stdout_log,
            stderr_log=args.stderr_log,
            manifest=args.manifest,
            writes_frozen_attestation=args.writes_frozen_attestation,
            capture_binlog_coordinates=args.capture_binlog_coordinates,
        )
    except (DumpError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
