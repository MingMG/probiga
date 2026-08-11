#!/usr/bin/env python3
"""Fail-closed MySQL 5.5 STATEMENT-binlog catch-up for a restored 8.4 target.

The initial seed must come from ``run_mysql55_consistent_dump.py`` with
``--capture-binlog-coordinates``.  This command binds that coordinate to the
exact source, target UUID, local binlog inventory and an atomic checkpoint.
Each source binlog file is extracted, safety-scanned and applied separately so
a completed file is resumable.  A failed mysql apply is deliberately marked as
``target_may_be_tainted`` and cannot be resumed automatically.

This tool never changes port 3306, never deletes source binlogs, never uses
``--force``, and never places a password in argv, environment, logs or JSON.
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
from typing import Any, BinaryIO

import pymysql
from pymysql.cursors import DictCursor

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.run_mysql55_consistent_dump import (
    EXPECTED_SCHEMAS,
    ClientOptions,
    DumpError,
    assert_protected_client_option_file,
    read_client_options,
)
from tools.run_mysql84_logical_restore import (
    AdminClientOptions,
    RestoreError,
    inspect_mysql_client,
    inspect_target,
    read_admin_client_options,
    validate_ca_file,
)


EXPECTED_SOURCE_VERSION = "5.5.20-log"
EXPECTED_SOURCE_PORT = 3306
EXPECTED_SOURCE_SERVER_ID = 55
EXPECTED_SOURCE_BINLOG_FORMAT = "STATEMENT"
EXPECTED_TARGET_VERSION = "8.4.11"
FINAL_FROZEN_ACK = "I_CONFIRM_SOURCE_WRITES_ARE_FROZEN"
CUTOVER_FREEZE_NAMED_LOCK = "probiga:mysql55:cutover-freeze"
MODES = ("online", "final-frozen")
CHECKPOINT_FORMAT = "probiga.mysql55_to_mysql84.binlog_checkpoint"
EVIDENCE_FORMAT = "probiga.mysql55_to_mysql84.binlog_catchup"
ALLOWED_SCHEMAS = frozenset(EXPECTED_SCHEMAS)
SYSTEM_SCHEMAS = frozenset({"mysql", "sys", "performance_schema", "information_schema"})

_VERSION_RE = re.compile(r"\b(?:Ver|Distrib)\s+8\.4\.11(?:[-+][0-9A-Za-z._-]+)?\b", re.I)
_BINLOG_NAME_RE = re.compile(r"^[A-Za-z0-9._-]+\.[0-9]{6}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_USE_RE = re.compile(rb"(?im)^\s*use\s+`?([^`;\s]+)`?\s*/?\*!?[^\r\n]*")
_SYSTEM_QUALIFIER_RE = re.compile(
    rb"(?i)(?:^|[^A-Za-z0-9_])`?(mysql|sys|performance_schema|information_schema)`?\."
)
_FORBIDDEN_STATEMENT_RE = re.compile(
    rb"(?im)^\s*(?:/\*![0-9]{5,6}\s+)?(?:"
    rb"create\s+user|alter\s+user|drop\s+user|rename\s+user|"
    rb"grant\b|revoke\b|install\b|uninstall\b|"
    rb"set\s+(?:@@\s*)?global\b|reset\s+(?:master|replica|slave)\b|"
    rb"change\s+(?:master|replication\s+source)\b|"
    rb"drop\s+database\b"
    rb")"
)
_CREATE_DATABASE_RE = re.compile(
    rb"(?i)^(?:/\*![0-9]{5,6}\s+)?create\s+database\b"
)
_SAFE_CREATE_DATABASE_RE = re.compile(
    rb"(?i)^create\s+database\s+if\s+not\s+exists\s+"
    rb"`?([A-Za-z0-9_]+)`?\s+default\s+character\s+set\s+utf8mb4\s+"
    rb"collate\s+utf8mb4_(?:general|unicode)_ci\s*$"
)
_CLIENT_COMMAND_RE = re.compile(
    rb"(?im)^\s*\\[.r](?:\s|$)|"
    rb"^\s*(?:source|connect)\s+[^=(),;\r\n]+\s*$"
)
_SQL_LOG_BIN_ONE_RE = re.compile(
    rb"(?i)\b(?:@@(?:session\.)?)?sql_log_bin\s*=\s*1\b"
)
_ERROR_TOKEN_RE = re.compile(rb"error", re.I)
_MAX_JSON_BYTES = 8 * 1024 * 1024
_HASH_CHUNK = 8 * 1024 * 1024
_REGEX_SCAN_CHUNK = 4096
_REGEX_SCAN_OVERLAP = 256
_BELOW_NORMAL_PRIORITY_CLASS = 0x00004000


class CatchupError(RuntimeError):
    """A binlog catch-up safety or execution condition failed."""


@dataclass(frozen=True)
class Coordinate:
    file: str
    position: int


@dataclass(frozen=True)
class SourceObservation:
    version: str
    version_comment: str
    hostname: str
    port: int
    server_id: int
    log_bin: bool
    binlog_format: str
    read_only: bool
    connection_id: int
    master: Coordinate
    binary_logs: tuple[tuple[str, int], ...]
    active_non_sleep_sessions: int
    active_transactions: int
    observed_at_utc: str
    freeze_guardian_connection_id: int | None = None


@dataclass(frozen=True)
class SnapshotIdentity:
    manifest_path: str
    manifest_sha256: str
    dump_path: str
    dump_bytes: int
    dump_sha256: str
    source_hostname: str
    source_server_id: int
    source_version: str
    coordinate: Coordinate


@dataclass(frozen=True)
class SegmentPlan:
    file: str
    start_position: int
    stop_position: int
    cursor_after: Coordinate


@dataclass(frozen=True)
class SegmentEvidence:
    file: str
    start_position: int
    stop_position: int
    sql_path: str
    sql_bytes: int
    sql_sha256: str
    mysqlbinlog_return_code: int
    mysql_return_code: int
    cursor_after: Coordinate


@dataclass(frozen=True, repr=False)
class ParsedClientOptions:
    host: str
    port: int
    user: str
    password: str = field(repr=False)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(_HASH_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_uuid(value: object) -> str:
    raw = str(value or "").strip().lower()
    try:
        parsed = uuid.UUID(raw)
    except ValueError as exc:
        raise CatchupError("expected target UUID is invalid") from exc
    if str(parsed) != raw:
        raise CatchupError("expected target UUID must be canonical lowercase")
    return raw


def _mapping_value(row: Mapping[str, Any], name: str) -> Any:
    wanted = name.casefold()
    for key, value in row.items():
        if str(key).casefold() == wanted:
            return value
    return None


def _load_json(path: Path, *, label: str) -> tuple[Path, dict[str, Any], str]:
    if not path.is_absolute():
        raise CatchupError(f"{label} path must be absolute")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise CatchupError(f"{label} does not exist") from exc
    if not resolved.is_file() or resolved.stat().st_size > _MAX_JSON_BYTES:
        raise CatchupError(f"{label} must be a bounded regular file")
    digest = _sha256_file(resolved)
    try:
        value = json.loads(resolved.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CatchupError(f"{label} is not valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise CatchupError(f"{label} root must be an object")
    return resolved, dict(value), digest


def load_snapshot_identity(path: Path) -> SnapshotIdentity:
    resolved, report, manifest_sha256 = _load_json(path, label="dump manifest")
    if report.get("status") != "success":
        raise CatchupError("dump manifest does not report success")
    if report.get("binlog_coordinates_captured") is not True:
        raise CatchupError("dump manifest did not capture binlog coordinates")
    coordinate_value = report.get("snapshot_binlog_coordinates")
    artifacts = report.get("artifacts")
    preflight = report.get("source_preflight")
    if not isinstance(coordinate_value, Mapping):
        raise CatchupError("dump manifest snapshot coordinate is invalid")
    if not isinstance(artifacts, Mapping) or not isinstance(artifacts.get("dump"), Mapping):
        raise CatchupError("dump manifest artifact section is invalid")
    if not isinstance(preflight, Mapping) or not isinstance(preflight.get("identity"), Mapping):
        raise CatchupError("dump manifest source identity is invalid")
    dump = artifacts["dump"]
    source = preflight["identity"]
    file_name = str(coordinate_value.get("file") or "").strip()
    try:
        position = int(coordinate_value.get("position"))
        dump_bytes = int(dump.get("bytes"))
        server_id = int(source.get("server_id"))
    except (TypeError, ValueError) as exc:
        raise CatchupError("dump manifest contains a non-integer identity field") from exc
    dump_sha256 = str(dump.get("sha256") or "").strip().lower()
    dump_path = str(dump.get("path") or "").strip()
    source_version = str(source.get("version") or "").strip()
    source_hostname = str(source.get("hostname") or "").strip()
    if not _BINLOG_NAME_RE.fullmatch(file_name) or position < 4:
        raise CatchupError("dump manifest contains an unsafe binlog coordinate")
    if not Path(dump_path).is_absolute() or dump_bytes <= 0:
        raise CatchupError("dump manifest contains an invalid dump artifact")
    if _SHA256_RE.fullmatch(dump_sha256) is None:
        raise CatchupError("dump manifest contains an invalid dump SHA-256")
    if source_version != EXPECTED_SOURCE_VERSION:
        raise CatchupError("dump snapshot was not captured from exact MySQL 5.5.20-log")
    if server_id != EXPECTED_SOURCE_SERVER_ID or not source_hostname:
        raise CatchupError("dump snapshot source identity is not the configured source")
    return SnapshotIdentity(
        manifest_path=str(resolved),
        manifest_sha256=manifest_sha256,
        dump_path=dump_path,
        dump_bytes=dump_bytes,
        dump_sha256=dump_sha256,
        source_hostname=source_hostname,
        source_server_id=server_id,
        source_version=source_version,
        coordinate=Coordinate(file_name, position),
    )


def _connect_source(options: ClientOptions):
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
    except pymysql.MySQLError as exc:
        raise CatchupError("could not connect to the MySQL 5.5 source") from exc


def inspect_source(
    options: ClientOptions, *, freeze_guardian_lock_name: str | None = None
) -> SourceObservation:
    connection = _connect_source(options)
    freeze_guardian_connection_id: int | None = None
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT @@version AS version, @@version_comment AS version_comment, "
                "@@hostname AS hostname, @@port AS port, @@server_id AS server_id, "
                "@@global.log_bin AS log_bin, @@global.binlog_format AS binlog_format, "
                "@@global.read_only AS read_only, CONNECTION_ID() AS connection_id"
            )
            identity = cursor.fetchone()
            cursor.execute("SHOW MASTER STATUS")
            master_row = cursor.fetchone()
            cursor.execute("SHOW BINARY LOGS")
            log_rows = cursor.fetchall()
            if freeze_guardian_lock_name is not None:
                cursor.execute(
                    "SELECT IS_USED_LOCK(%s) AS connection_id",
                    (freeze_guardian_lock_name,),
                )
                freeze_guardian_row = cursor.fetchone()
                try:
                    freeze_guardian_connection_id = int(
                        _mapping_value(freeze_guardian_row or {}, "connection_id") or 0
                    )
                except (TypeError, ValueError) as exc:
                    raise CatchupError(
                        "source freeze guardian returned an invalid connection ID"
                    ) from exc
                if freeze_guardian_connection_id <= 0:
                    raise CatchupError(
                        "final-frozen source has no active cutover freeze guardian"
                    )
                cursor.execute(
                    "SELECT COUNT(*) AS count FROM information_schema.PROCESSLIST "
                    "WHERE ID <> CONNECTION_ID() AND ID <> %s AND COMMAND <> 'Sleep'",
                    (freeze_guardian_connection_id,),
                )
            else:
                cursor.execute(
                    "SELECT COUNT(*) AS count FROM information_schema.PROCESSLIST "
                    "WHERE ID <> CONNECTION_ID() AND COMMAND <> 'Sleep'"
                )
            active_row = cursor.fetchone()
            if freeze_guardian_connection_id is not None:
                cursor.execute(
                    "SELECT COUNT(*) AS count FROM information_schema.INNODB_TRX "
                    "WHERE trx_mysql_thread_id <> CONNECTION_ID() "
                    "AND trx_mysql_thread_id <> %s",
                    (freeze_guardian_connection_id,),
                )
            else:
                cursor.execute(
                    "SELECT COUNT(*) AS count FROM information_schema.INNODB_TRX "
                    "WHERE trx_mysql_thread_id <> CONNECTION_ID()"
                )
            transaction_row = cursor.fetchone()
    finally:
        connection.close()
    if not isinstance(identity, Mapping) or not isinstance(master_row, Mapping):
        raise CatchupError("source identity or master status returned no row")
    version = str(_mapping_value(identity, "version") or "")
    comment = str(_mapping_value(identity, "version_comment") or "")
    hostname = str(_mapping_value(identity, "hostname") or "")
    port = int(_mapping_value(identity, "port") or 0)
    server_id = int(_mapping_value(identity, "server_id") or 0)
    log_bin = bool(int(_mapping_value(identity, "log_bin") or 0))
    binlog_format = str(_mapping_value(identity, "binlog_format") or "").upper()
    if (
        version != EXPECTED_SOURCE_VERSION
        or "mysql" not in comment.casefold()
        or "mariadb" in comment.casefold()
        or "percona" in comment.casefold()
        or port != EXPECTED_SOURCE_PORT
        or server_id != EXPECTED_SOURCE_SERVER_ID
        or not log_bin
        or binlog_format != EXPECTED_SOURCE_BINLOG_FORMAT
    ):
        raise CatchupError("source is not the expected MySQL 5.5.20-log/3306/binlog server")
    master = Coordinate(
        str(_mapping_value(master_row, "File") or ""),
        int(_mapping_value(master_row, "Position") or 0),
    )
    logs: list[tuple[str, int]] = []
    for row in log_rows or ():
        name = str(_mapping_value(row, "Log_name") or "")
        size = int(_mapping_value(row, "File_size") or 0)
        if not _BINLOG_NAME_RE.fullmatch(name) or size < 4:
            raise CatchupError("source returned an unsafe binary log inventory")
        logs.append((name, size))
    if not logs or master.file not in {name for name, _size in logs} or master.position < 4:
        raise CatchupError("source master coordinate is absent from the binary log inventory")
    return SourceObservation(
        version=version,
        version_comment=comment,
        hostname=hostname,
        port=port,
        server_id=server_id,
        log_bin=log_bin,
        binlog_format=binlog_format,
        read_only=bool(int(_mapping_value(identity, "read_only") or 0)),
        connection_id=int(_mapping_value(identity, "connection_id") or 0),
        master=master,
        binary_logs=tuple(logs),
        active_non_sleep_sessions=int(_mapping_value(active_row or {}, "count") or 0),
        active_transactions=int(_mapping_value(transaction_row or {}, "count") or 0),
        observed_at_utc=_utc_now(),
        freeze_guardian_connection_id=freeze_guardian_connection_id,
    )


def compare_coordinates(
    left: Coordinate, right: Coordinate, inventory: Sequence[tuple[str, int]]
) -> int:
    order = {name: index for index, (name, _size) in enumerate(inventory)}
    if left.file not in order or right.file not in order:
        raise CatchupError("coordinate file is absent from the current binlog inventory")
    if order[left.file] != order[right.file]:
        return -1 if order[left.file] < order[right.file] else 1
    return (left.position > right.position) - (left.position < right.position)


def build_segment_plan(
    start: Coordinate,
    stop: Coordinate,
    inventory: Sequence[tuple[str, int]],
) -> tuple[SegmentPlan, ...]:
    if compare_coordinates(start, stop, inventory) > 0:
        raise CatchupError("catch-up start coordinate is after the requested stop")
    if start == stop:
        return ()
    order = {name: index for index, (name, _size) in enumerate(inventory)}
    sizes = dict(inventory)
    selected = list(inventory)[order[start.file] : order[stop.file] + 1]
    plans: list[SegmentPlan] = []
    for index, (name, size) in enumerate(selected):
        first = index == 0
        last = index == len(selected) - 1
        segment_start = start.position if first else 4
        segment_stop = stop.position if last else size
        if segment_start < 4 or segment_stop < segment_start or segment_stop > sizes[name]:
            raise CatchupError(f"invalid or unavailable binlog range for {name}")
        cursor_after = (
            stop
            if last
            else Coordinate(selected[index + 1][0], 4)
        )
        plans.append(
            SegmentPlan(
                file=name,
                start_position=segment_start,
                stop_position=segment_stop,
                cursor_after=cursor_after,
            )
        )
    return tuple(plans)


def audit_extracted_sql(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    total = 0
    used_schemas: set[str] = set()
    with path.open("rb") as stream:
        for line in stream:
            total += len(line)
            digest.update(line)
            statement_prefix = line.lstrip(b" \t")[:4096]
            use_match = _USE_RE.match(statement_prefix)
            if use_match is not None:
                used_schemas.add(
                    use_match.group(1).decode("utf-8", errors="strict").casefold()
                )
            if _bounded_regex_search(_SYSTEM_QUALIFIER_RE, line):
                raise CatchupError("binlog segment references a system schema")
            if _CREATE_DATABASE_RE.match(statement_prefix) is not None:
                safe_create = (
                    _SAFE_CREATE_DATABASE_RE.match(line.lstrip(b" \t"))
                    if len(line) <= 4096
                    else None
                )
                schema = (
                    safe_create.group(1).decode("ascii").casefold()
                    if safe_create is not None
                    else ""
                )
                if safe_create is None or schema not in ALLOWED_SCHEMAS:
                    raise CatchupError(
                        "binlog segment contains an unsafe CREATE DATABASE statement"
                    )
            if _FORBIDDEN_STATEMENT_RE.search(statement_prefix) is not None:
                raise CatchupError(
                    "binlog segment contains a forbidden administrative statement"
                )
            if _CLIENT_COMMAND_RE.search(statement_prefix) is not None:
                raise CatchupError(
                    "binlog segment contains a forbidden mysql client command"
                )
            if _bounded_regex_search(_SQL_LOG_BIN_ONE_RE, line):
                raise CatchupError(
                    "binlog segment attempts to enable session sql_log_bin"
                )
    if not total:
        raise CatchupError("mysqlbinlog produced an empty SQL segment")
    unexpected = sorted(used_schemas - ALLOWED_SCHEMAS)
    if unexpected:
        raise CatchupError("binlog segment uses a non-business schema: " + ", ".join(unexpected))
    return {
        "used_schemas": sorted(used_schemas),
        "bytes": total,
        "sha256": digest.hexdigest(),
    }


def _bounded_regex_search(pattern: re.Pattern[bytes], payload: bytes) -> bool:
    """Search large mysqlbinlog lines without exposing ``re`` to huge inputs."""
    if len(payload) <= _REGEX_SCAN_CHUNK:
        return pattern.search(payload) is not None
    start = 0
    while start < len(payload):
        stop = min(start + _REGEX_SCAN_CHUNK, len(payload))
        if pattern.search(payload[start:stop]) is not None:
            return True
        if stop == len(payload):
            break
        start = stop - _REGEX_SCAN_OVERLAP
    return False


def _atomic_json(path: Path, value: Mapping[str, Any], *, replace: bool) -> None:
    if not path.is_absolute():
        raise CatchupError("JSON artifact path must be absolute")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and not replace:
        raise CatchupError(f"refusing to overwrite existing artifact: {path}")
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    encoded = (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")
    try:
        with temporary.open("xb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _scrub_environment() -> dict[str, str]:
    result = os.environ.copy()
    for name in tuple(result):
        if name.upper().startswith(("MYSQL_", "MARIADB_")):
            result.pop(name, None)
    return result


def inspect_mysqlbinlog(executable: Path) -> str:
    resolved = executable.expanduser().resolve(strict=True)
    environment = _scrub_environment()
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
    finally:
        environment.clear()
    output = (completed.stdout + b"\n" + completed.stderr).decode("utf-8", errors="replace").strip()
    if completed.returncode != 0 or "mysqlbinlog" not in output.casefold() or _VERSION_RE.search(output) is None:
        raise CatchupError("mysqlbinlog must be exact Oracle MySQL 8.4.11")
    return output


def _apply_sql_stream(
    *, mysql_executable: str, option_file: Path, ca_file: Path, sql_path: Path,
    stdout_path: Path, stderr_path: Path,
) -> int:
    command = (
        mysql_executable,
        f"--defaults-file={option_file}",
        "--protocol=tcp",
        "--host=127.0.0.1",
        f"--port={read_admin_client_options(option_file, expected_port=_option_port(option_file)).port}",
        "--ssl-mode=VERIFY_CA",
        f"--ssl-ca={ca_file}",
        "--default-character-set=utf8mb4",
        "--binary-mode=1",
        "--skip-reconnect",
        "--local-infile=0",
        "--max_allowed_packet=1G",
    )
    environment = _scrub_environment()
    try:
        with sql_path.open("rb") as source, stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
            process = subprocess.Popen(
                command,
                stdin=subprocess.PIPE,
                stdout=stdout,
                stderr=stderr,
                env=environment,
                shell=False,
                creationflags=_BELOW_NORMAL_PRIORITY_CLASS if os.name == "nt" else 0,
            )
            if process.stdin is None:
                process.kill()
                raise CatchupError("mysql client stdin was not created")
            try:
                while chunk := source.read(_HASH_CHUNK):
                    process.stdin.write(chunk)
                process.stdin.close()
                return int(process.wait())
            except BaseException:
                process.kill()
                process.wait()
                raise
    finally:
        environment.clear()


def _option_port(path: Path) -> int:
    parser = configparser.RawConfigParser(interpolation=None)
    parser.read(path, encoding="utf-8-sig")
    try:
        return parser.getint("client", "port")
    except (configparser.Error, ValueError) as exc:
        raise CatchupError("target option file has no valid port") from exc


def _load_checkpoint(
    path: Path,
    *, snapshot: SnapshotIdentity,
    target_uuid: str,
) -> Coordinate:
    if not path.exists():
        return snapshot.coordinate
    _resolved, value, _digest = _load_json(path, label="binlog checkpoint")
    if value.get("format") != CHECKPOINT_FORMAT or value.get("status") != "success":
        raise CatchupError("binlog checkpoint is not a successful compatible checkpoint")
    if value.get("snapshot_manifest_sha256") != snapshot.manifest_sha256:
        raise CatchupError("binlog checkpoint belongs to a different dump snapshot")
    if value.get("target_server_uuid") != target_uuid:
        raise CatchupError("binlog checkpoint belongs to a different target server")
    cursor = value.get("cursor")
    if not isinstance(cursor, Mapping):
        raise CatchupError("binlog checkpoint cursor is invalid")
    result = Coordinate(str(cursor.get("file") or ""), int(cursor.get("position") or 0))
    if not _BINLOG_NAME_RE.fullmatch(result.file) or result.position < 4:
        raise CatchupError("binlog checkpoint coordinate is unsafe")
    return result


def _checkpoint_payload(
    *, snapshot: SnapshotIdentity, source: SourceObservation, target_uuid: str,
    cursor: Coordinate, segment: SegmentEvidence | None,
) -> dict[str, Any]:
    return {
        "format": CHECKPOINT_FORMAT,
        "schema_version": 1,
        "status": "success",
        "updated_at_utc": _utc_now(),
        "snapshot_manifest": snapshot.manifest_path,
        "snapshot_manifest_sha256": snapshot.manifest_sha256,
        "source_hostname": source.hostname,
        "source_server_id": source.server_id,
        "source_version": source.version,
        "target_server_uuid": target_uuid,
        "cursor": asdict(cursor),
        "last_segment": None if segment is None else asdict(segment),
    }


def _extract_segment(
    *, mysqlbinlog: Path, binlog_dir: Path, plan: SegmentPlan,
    sql_path: Path, stdout_path: Path, stderr_path: Path,
) -> tuple[int, dict[str, Any]]:
    source_file = (binlog_dir / plan.file).resolve(strict=True)
    if source_file.parent != binlog_dir or not source_file.is_file():
        raise CatchupError("resolved source binlog escaped the configured directory")
    for path in (sql_path, stdout_path, stderr_path):
        if path.exists():
            raise CatchupError(f"refusing to overwrite segment artifact: {path}")
    command = (
        str(mysqlbinlog),
        "--disable-log-bin",
        f"--start-position={plan.start_position}",
        f"--stop-position={plan.stop_position}",
        f"--result-file={sql_path}",
        str(source_file),
    )
    environment = _scrub_environment()
    try:
        with stdout_path.open("xb") as stdout, stderr_path.open("xb") as stderr:
            completed = subprocess.run(
                command,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                env=environment,
                shell=False,
                creationflags=_BELOW_NORMAL_PRIORITY_CLASS if os.name == "nt" else 0,
                check=False,
            )
    finally:
        environment.clear()
    return int(completed.returncode), audit_extracted_sql(sql_path) if completed.returncode == 0 else {}


def _validate_mode(mode: str, ack: str | None, source: SourceObservation) -> None:
    if mode not in MODES:
        raise CatchupError("invalid catch-up mode")
    if mode == "final-frozen":
        if ack != FINAL_FROZEN_ACK:
            raise CatchupError("final-frozen mode requires the exact writes-frozen acknowledgement")
        if not source.freeze_guardian_connection_id:
            raise CatchupError("final-frozen source has no active cutover freeze guardian")
        if source.active_non_sleep_sessions or source.active_transactions:
            raise CatchupError("final-frozen source still has active statements or transactions")
    elif ack:
        raise CatchupError("writes-frozen acknowledgement is valid only in final-frozen mode")


def run(args: argparse.Namespace) -> dict[str, Any]:
    started_at = _utc_now()
    if args.expected_target_port == 3306:
        raise CatchupError("catch-up must target the isolated pre-cutover port, never 3306")
    target_uuid = _canonical_uuid(args.expected_target_uuid)
    snapshot = load_snapshot_identity(args.dump_manifest)
    source_options = read_client_options(args.source_option_file)
    source = inspect_source(
        source_options,
        freeze_guardian_lock_name=(
            CUTOVER_FREEZE_NAMED_LOCK if args.mode == "final-frozen" else None
        ),
    )
    del source_options
    if (
        source.hostname != snapshot.source_hostname
        or source.server_id != snapshot.source_server_id
        or source.version != snapshot.source_version
    ):
        raise CatchupError("live source identity differs from the dump snapshot source")
    _validate_mode(args.mode, args.writes_frozen_ack, source)

    target_option_file = args.target_option_file.expanduser().resolve(strict=True)
    target_options = read_admin_client_options(
        target_option_file, expected_port=args.expected_target_port
    )
    ca_file = validate_ca_file(args.target_ssl_ca)
    target = inspect_target(
        target_options,
        ca_file,
        expected_server_uuid=target_uuid,
        expected_server_port=args.expected_target_port,
        expected_datadir=args.expected_target_datadir,
    )
    if tuple(sorted(target.business_schemas)) != tuple(sorted(EXPECTED_SCHEMAS)):
        raise CatchupError("target does not contain the exact restored business schemas")
    mysql_identity = inspect_mysql_client(args.mysql)
    mysqlbinlog_version = inspect_mysqlbinlog(args.mysqlbinlog)

    stop = (
        Coordinate(args.stop_file, args.stop_position)
        if args.stop_file is not None or args.stop_position is not None
        else source.master
    )
    if args.stop_file is None and args.stop_position is not None or args.stop_file is not None and args.stop_position is None:
        raise CatchupError("stop file and stop position must be supplied together")
    checkpoint = args.checkpoint.expanduser().resolve(strict=False)
    cursor = _load_checkpoint(
        checkpoint, snapshot=snapshot, target_uuid=target_uuid
    )
    plans = build_segment_plan(cursor, stop, source.binary_logs)
    segment_dir = args.segment_dir.expanduser().resolve(strict=False)
    segment_dir.mkdir(parents=True, exist_ok=True)
    evidence_path = args.evidence.expanduser().resolve(strict=False)
    if evidence_path.exists():
        raise CatchupError("refusing to overwrite existing catch-up evidence")

    completed_segments: list[SegmentEvidence] = []
    target_may_be_tainted = False
    try:
        for index, plan in enumerate(plans, start=1):
            stem = f"{index:04d}-{plan.file}-{plan.start_position}-{plan.stop_position}"
            sql_path = segment_dir / f"{stem}.sql"
            extract_stdout = segment_dir / f"{stem}.mysqlbinlog.stdout.log"
            extract_stderr = segment_dir / f"{stem}.mysqlbinlog.stderr.log"
            apply_stdout = segment_dir / f"{stem}.mysql.stdout.log"
            apply_stderr = segment_dir / f"{stem}.mysql.stderr.log"
            extract_code, audit = _extract_segment(
                mysqlbinlog=args.mysqlbinlog.expanduser().resolve(strict=True),
                binlog_dir=args.binlog_dir.expanduser().resolve(strict=True),
                plan=plan,
                sql_path=sql_path,
                stdout_path=extract_stdout,
                stderr_path=extract_stderr,
            )
            if extract_code != 0:
                raise CatchupError(f"mysqlbinlog extraction failed for {plan.file}")
            apply_code = _apply_sql_stream(
                mysql_executable=mysql_identity.executable,
                option_file=target_option_file,
                ca_file=ca_file,
                sql_path=sql_path,
                stdout_path=apply_stdout,
                stderr_path=apply_stderr,
            )
            target_may_be_tainted = apply_code != 0
            if apply_code != 0 or _ERROR_TOKEN_RE.search(apply_stderr.read_bytes()) is not None:
                target_may_be_tainted = True
                raise CatchupError(
                    f"mysql apply failed for {plan.file}; target may be partially advanced"
                )
            segment = SegmentEvidence(
                file=plan.file,
                start_position=plan.start_position,
                stop_position=plan.stop_position,
                sql_path=str(sql_path),
                sql_bytes=int(audit["bytes"]),
                sql_sha256=str(audit["sha256"]),
                mysqlbinlog_return_code=extract_code,
                mysql_return_code=apply_code,
                cursor_after=plan.cursor_after,
            )
            completed_segments.append(segment)
            cursor = plan.cursor_after
            _atomic_json(
                checkpoint,
                _checkpoint_payload(
                    snapshot=snapshot,
                    source=source,
                    target_uuid=target_uuid,
                    cursor=cursor,
                    segment=segment,
                ),
                replace=True,
            )
    except Exception as exc:
        failure = {
            "format": EVIDENCE_FORMAT,
            "schema_version": 1,
            "status": "failed",
            "failure": str(exc),
            "target_may_be_tainted": target_may_be_tainted,
            "started_at_utc": started_at,
            "finished_at_utc": _utc_now(),
            "snapshot": asdict(snapshot),
            "source": asdict(source),
            "target": asdict(target),
            "requested_stop": asdict(stop),
            "cursor_after": asdict(cursor),
            "completed_segments": [asdict(item) for item in completed_segments],
        }
        _atomic_json(evidence_path, failure, replace=False)
        raise

    after = inspect_target(
        target_options,
        ca_file,
        expected_server_uuid=target_uuid,
        expected_server_port=args.expected_target_port,
        expected_datadir=args.expected_target_datadir,
    )
    if not checkpoint.exists():
        _atomic_json(
            checkpoint,
            _checkpoint_payload(
                snapshot=snapshot,
                source=source,
                target_uuid=target_uuid,
                cursor=cursor,
                segment=None,
            ),
            replace=True,
        )
    result = {
        "format": EVIDENCE_FORMAT,
        "schema_version": 1,
        "status": "success",
        "mode": args.mode,
        "started_at_utc": started_at,
        "finished_at_utc": _utc_now(),
        "snapshot": asdict(snapshot),
        "source": asdict(source),
        "target_before": asdict(target),
        "target_after": asdict(after),
        "mysql_client": asdict(mysql_identity),
        "mysqlbinlog_version": mysqlbinlog_version,
        "requested_stop": asdict(stop),
        "cursor_after": asdict(cursor),
        "checkpoint": str(checkpoint),
        "segments": [asdict(item) for item in completed_segments],
        "target_may_be_tainted": False,
    }
    _atomic_json(evidence_path, result, replace=False)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fail-closed MySQL 5.5 statement-binlog catch-up into MySQL 8.4."
    )
    parser.add_argument("--mode", choices=MODES, required=True)
    parser.add_argument("--source-option-file", type=Path, required=True)
    parser.add_argument("--dump-manifest", type=Path, required=True)
    parser.add_argument("--binlog-dir", type=Path, required=True)
    parser.add_argument("--mysqlbinlog", type=Path, required=True)
    parser.add_argument("--mysql", type=Path, required=True)
    parser.add_argument("--target-option-file", type=Path, required=True)
    parser.add_argument("--target-ssl-ca", type=Path, required=True)
    parser.add_argument("--expected-target-uuid", required=True)
    parser.add_argument("--expected-target-port", type=int, required=True)
    parser.add_argument("--expected-target-datadir", type=Path, required=True)
    parser.add_argument("--checkpoint", type=Path, required=True)
    parser.add_argument("--segment-dir", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--stop-file")
    parser.add_argument("--stop-position", type=int)
    parser.add_argument("--writes-frozen-ack")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run(args)
    except (CatchupError, DumpError, RestoreError, OSError, ValueError, pymysql.MySQLError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
