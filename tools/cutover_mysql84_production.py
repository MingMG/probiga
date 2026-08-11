#!/usr/bin/env python3
"""Fail-closed Windows service and environment cutover to Oracle MySQL 8.4.

This command deliberately does not move database files and never deletes the
legacy MySQL service.  The cold data-layout transition is handled separately.
It promotes the already staged runtime environment only after the legacy
service is stopped, port 3306 is free, the formal local-disk configuration is
valid, and sealed final-acceptance/provisioning evidence identifies the exact
target UUID.  A failed apply stops and disables the new service and restores
the old environment, but leaves physical data recovery to the explicit
rollback workflow.
"""

from __future__ import annotations

import argparse
import configparser
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

import pymysql
from pymysql.cursors import DictCursor

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.run_mysql55_consistent_dump import (  # noqa: E402
    DumpError,
    assert_protected_client_option_file,
    read_client_options,
)
from tools.run_mysql84_logical_restore import validate_ca_file  # noqa: E402


APPLY_ACK = "I_CONFIRM_WRITES_FROZEN_AND_MYSQL84_ACCEPTED"
ROLLBACK_ACK = "I_CONFIRM_WRITES_FROZEN_AND_MYSQL55_PHYSICAL_DATA_RESTORED"
NEW_SERVICE = "ProBigA-MySQL84"
LEGACY_SERVICE = "MySQL"
EXPECTED_SCHEMAS = ("biga", "probiga", "probiga_qmt_history")
EXPECTED_VERSION = "8.4.11"
EXPECTED_DATADIR = Path(r"E:\MySQL84\Data")
EXPECTED_CONFIG = Path(r"D:\MySQL84\config\my.ini")
EXPECTED_CA = Path(r"D:\MySQL84\certs\ca.pem")
EXPECTED_MYSQLD = Path(
    r"D:\MySQL84\software\mysql-8.4.11-winx64\bin\mysqld.exe"
)
LEGACY_IBDATA = Path(r"E:\MySQL Datafiles\ibdata1")
LEGACY_CONFIG = Path(r"C:\Program Files\MySQL\MySQL Server 5.5\my.ini")
LEGACY_DATADIR = Path(r"C:\ProgramData\MySQL\MySQL Server 5.5\Data")
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_SERVICE_RE = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")
_DRIVE_REFERENCE_RE = re.compile(r"(?i)(?:^|[=\s\"'])([A-Z]):[/\\]")


class CutoverError(RuntimeError):
    """A public, credential-free cutover safety error."""


@dataclass(frozen=True, slots=True)
class ServiceState:
    exists: bool
    state: str | None
    start_type: str | None
    binary_path: str | None


@dataclass(frozen=True, slots=True)
class CutoverPaths:
    mysqld: Path
    formal_config: Path
    formal_datadir: Path
    formal_ca: Path
    runtime_option_file: Path
    active_env: Path
    staged_env: Path
    active_env_backup: Path
    provision_evidence: Path
    acceptance_evidence: Path
    evidence: Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical(path: Path, *, must_exist: bool = True, file: bool | None = None) -> Path:
    if not path.is_absolute():
        raise CutoverError(f"path must be absolute: {path}")
    try:
        resolved = path.resolve(strict=must_exist)
    except OSError as exc:
        raise CutoverError(f"required path does not exist: {path}") from exc
    if must_exist and file is True and not resolved.is_file():
        raise CutoverError(f"required path is not a file: {resolved}")
    if must_exist and file is False and not resolved.is_dir():
        raise CutoverError(f"required path is not a directory: {resolved}")
    return resolved


def _same_path(left: Path, right: Path) -> bool:
    return os.path.normcase(os.path.normpath(str(left))) == os.path.normcase(
        os.path.normpath(str(right))
    )


def secure_file_priv_is_disabled(value: object) -> bool:
    """Return whether MySQL reports file import/export as disabled.

    MySQL 8.4 on Windows exposes ``secure-file-priv=NULL`` through
    ``SELECT @@secure_file_priv`` as the four-character string ``NULL``
    rather than as SQL NULL. Accept both driver representations, while
    continuing to reject an empty value or a directory path.
    """

    return value is None or str(value).strip().upper() == "NULL"


def _require_fixed_path(
    actual: Path,
    expected: Path,
    *,
    label: str,
    file: bool,
    must_exist: bool = True,
) -> Path:
    resolved = _canonical(actual, must_exist=must_exist, file=file if must_exist else None)
    expected_resolved = _canonical(expected, must_exist=False)
    if not _same_path(resolved, expected_resolved):
        raise CutoverError(f"{label} must be exactly {expected_resolved}")
    return resolved


def _read_json(path: Path, *, label: str) -> dict[str, Any]:
    resolved = _canonical(path, file=True)
    try:
        value = json.loads(resolved.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CutoverError(f"{label} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise CutoverError(f"{label} must be a JSON object")
    return value


def _normalize_uuid(value: object) -> str:
    text = str(value or "").strip().lower()
    if _UUID_RE.fullmatch(text) is None:
        raise CutoverError("expected target UUID is invalid")
    return text


def _nested_get(value: Mapping[str, Any], *keys: str) -> object:
    current: object = value
    for key in keys:
        if not isinstance(current, Mapping) or key not in current:
            return None
        current = current[key]
    return current


def validate_acceptance_evidence(
    evidence: Mapping[str, Any], *, expected_uuid: str
) -> None:
    if evidence.get("tool") != "run_mysql84_final_acceptance":
        raise CutoverError("final acceptance evidence was produced by the wrong tool")
    if evidence.get("status") != "passed" or evidence.get("cutover_ready") is not True:
        raise CutoverError("final acceptance evidence is not cutover-ready")
    observed_uuid = _nested_get(evidence, "target", "server_uuid")
    if str(observed_uuid or "").strip().lower() != expected_uuid:
        raise CutoverError("final acceptance target UUID mismatch")
    steps = evidence.get("steps")
    if not isinstance(steps, list) or not steps:
        raise CutoverError("final acceptance evidence has no completed steps")
    if any(not isinstance(step, Mapping) or step.get("status") != "passed" for step in steps):
        raise CutoverError("final acceptance evidence contains an incomplete step")


def validate_provision_evidence(
    evidence: Mapping[str, Any],
    *, expected_uuid: str,
    staged_env: Path,
    runtime_option_file: Path,
    formal_ca: Path,
) -> None:
    if evidence.get("status") != "success" or evidence.get("secrets_in_evidence") is not False:
        raise CutoverError("runtime provisioning evidence is not successful")
    observed_uuid = _nested_get(evidence, "target", "server_uuid")
    if str(observed_uuid or "").strip().lower() != expected_uuid:
        raise CutoverError("runtime provisioning target UUID mismatch")
    expected_pairs = (
        (evidence.get("staged_env"), staged_env, "staged env"),
        (evidence.get("runtime_option_file"), runtime_option_file, "runtime option file"),
        (evidence.get("formal_ca"), formal_ca, "formal CA"),
    )
    for raw, expected, label in expected_pairs:
        if not raw or not _same_path(Path(str(raw)).resolve(), expected.resolve()):
            raise CutoverError(f"runtime provisioning {label} binding mismatch")
    if evidence.get("staged_env_sha256") != _sha256(staged_env):
        raise CutoverError("staged env hash differs from provisioning evidence")


def parse_mysql_option_file(path: Path) -> dict[str, str]:
    protected = assert_protected_client_option_file(path)
    parser = configparser.RawConfigParser(interpolation=None, strict=True)
    parser.read(protected, encoding="utf-8-sig")
    if parser.sections() != ["client"]:
        raise CutoverError("runtime option file must contain exactly [client]")
    result = {key: value.strip() for key, value in parser.items("client", raw=True)}
    expected = {"protocol": "tcp", "host": "127.0.0.1", "port": "3306"}
    for key, value in expected.items():
        if result.get(key, "").casefold() != value.casefold():
            raise CutoverError(f"runtime option file {key} is not production-safe")
    if not result.get("user") or not result.get("password"):
        raise CutoverError("runtime option file is missing credentials")
    return result


def _parse_my_ini(path: Path) -> dict[str, str]:
    parser = configparser.RawConfigParser(
        interpolation=None, strict=True, allow_no_value=True
    )
    parser.read(path, encoding="utf-8-sig")
    if "mysqld" not in parser:
        raise CutoverError("formal config has no [mysqld] section")
    return {
        key.replace("_", "-").casefold(): str(value or "").strip().strip('"')
        for key, value in parser.items("mysqld", raw=True)
    }


def validate_formal_config(path: Path, *, expected_datadir: Path) -> dict[str, str]:
    settings = _parse_my_ini(path)
    required = {
        "port": "3306",
        "bind-address": "127.0.0.1",
        "require-secure-transport": "ON",
        "binlog-format": "ROW",
        "sync-binlog": "1",
        "innodb-flush-log-at-trx-commit": "1",
        "local-infile": "OFF",
        "mysql-native-password": "OFF",
    }
    for key, expected in required.items():
        if settings.get(key, "").casefold() != expected.casefold():
            raise CutoverError(f"formal config {key} must be {expected}")
    configured_datadir = Path(settings.get("datadir", "").replace("/", os.sep))
    if not _same_path(configured_datadir, expected_datadir):
        raise CutoverError("formal config datadir does not match E:\\MySQL84\\Data")
    text = path.read_text(encoding="utf-8-sig")
    removable = sorted(
        {match.group(1).upper() for match in _DRIVE_REFERENCE_RE.finditer(text)}
    )
    if "F" in removable:
        raise CutoverError("formal config still references removable drive F")
    return settings


def read_datadir_uuid(datadir: Path) -> str:
    auto_cnf = _canonical(datadir / "auto.cnf", file=True)
    parser = configparser.RawConfigParser(interpolation=None, strict=True)
    parser.read(auto_cnf, encoding="utf-8-sig")
    return _normalize_uuid(parser.get("auto", "server-uuid", fallback="", raw=True))


def _run(
    command: Sequence[str],
    *,
    timeout: int = 60,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        list(command),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        shell=False,
        check=False,
    )
    if check and completed.returncode != 0:
        raise CutoverError(
            f"command failed without secret output: {Path(command[0]).name}"
        )
    return completed


def _is_admin() -> bool:
    if os.name != "nt":
        return False
    try:
        import ctypes

        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def query_service(name: str) -> ServiceState:
    if _SERVICE_RE.fullmatch(name) is None:
        raise CutoverError("unsafe Windows service name")
    query = _run(["sc.exe", "query", name], check=False)
    if query.returncode == 1060 or "1060" in (query.stdout + query.stderr):
        return ServiceState(False, None, None, None)
    if query.returncode != 0:
        raise CutoverError(f"could not query Windows service {name}")
    state_match = re.search(r"STATE\s*:\s*\d+\s+(\w+)", query.stdout)
    qc = _run(["sc.exe", "qc", name])
    start_match = re.search(r"START_TYPE\s*:\s*\d+\s+(\w+)", qc.stdout)
    path_match = re.search(r"BINARY_PATH_NAME\s*:\s*(.+)", qc.stdout)
    return ServiceState(
        True,
        state_match.group(1).upper() if state_match else "UNKNOWN",
        start_match.group(1).upper() if start_match else "UNKNOWN",
        path_match.group(1).strip() if path_match else None,
    )


def validate_new_service_registration(
    state: ServiceState, *, mysqld: Path, formal_config: Path
) -> None:
    if not state.exists:
        return
    binary_path = str(state.binary_path or "")
    normalized = os.path.normcase(binary_path.replace('"', "").replace("/", os.sep))
    expected_binary = os.path.normcase(str(mysqld))
    expected_config = os.path.normcase(str(formal_config))
    if expected_binary not in normalized or expected_config not in normalized:
        raise CutoverError(
            "existing MySQL 8.4 service registration does not use the formal binary/config"
        )


def port_is_free(port: int) -> bool:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        probe.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


def wait_service(name: str, expected: str, *, timeout: int = 90) -> ServiceState:
    deadline = time.monotonic() + timeout
    last = ServiceState(False, None, None, None)
    while time.monotonic() < deadline:
        last = query_service(name)
        if last.exists and last.state == expected:
            return last
        time.sleep(1)
    raise CutoverError(f"service {name} did not reach {expected}; last={last.state}")


def _atomic_copy_new(source: Path, destination: Path) -> None:
    if destination.exists():
        raise CutoverError(f"refusing to overwrite backup: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.partial")
    try:
        shutil.copy2(source, temporary)
        if _sha256(source) != _sha256(temporary):
            raise CutoverError("copied file hash verification failed")
        os.replace(temporary, destination)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _atomic_replace_from(source: Path, destination: Path) -> None:
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.partial")
    try:
        shutil.copy2(source, temporary)
        if _sha256(source) != _sha256(temporary):
            raise CutoverError("environment promotion hash verification failed")
        os.replace(temporary, destination)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _write_evidence(path: Path, payload: Mapping[str, Any]) -> None:
    if not path.is_absolute() or path.exists():
        raise CutoverError("cutover evidence path must be absolute and new")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _validate_paths(args: argparse.Namespace) -> CutoverPaths:
    return CutoverPaths(
        mysqld=_require_fixed_path(args.mysqld, EXPECTED_MYSQLD, label="mysqld", file=True),
        formal_config=_require_fixed_path(
            args.formal_config, EXPECTED_CONFIG, label="formal config", file=True
        ),
        formal_datadir=_require_fixed_path(
            args.formal_datadir,
            EXPECTED_DATADIR,
            label="formal datadir",
            file=False,
            must_exist=args.mode != "rollback",
        ),
        formal_ca=_require_fixed_path(args.formal_ca, EXPECTED_CA, label="formal CA", file=True),
        runtime_option_file=_canonical(args.runtime_option_file, file=True),
        active_env=_canonical(args.active_env, file=True),
        staged_env=_canonical(args.staged_env, file=True),
        active_env_backup=_canonical(
            args.active_env_backup, must_exist=args.mode == "rollback", file=True if args.mode == "rollback" else None
        ),
        provision_evidence=_canonical(args.provision_evidence, file=True),
        acceptance_evidence=_canonical(args.acceptance_evidence, file=True),
        evidence=_canonical(args.evidence, must_exist=False),
    )


def preflight(args: argparse.Namespace) -> tuple[CutoverPaths, dict[str, Any]]:
    if os.name != "nt" or not _is_admin():
        raise CutoverError("an elevated Windows administrator token is required")
    expected_uuid = _normalize_uuid(args.expected_target_uuid)
    if not _SERVICE_RE.fullmatch(args.new_service) or not _SERVICE_RE.fullmatch(
        args.legacy_service
    ):
        raise CutoverError("unsafe Windows service name")
    if args.new_service.casefold() == args.legacy_service.casefold():
        raise CutoverError("new and legacy service names must differ")
    paths = _validate_paths(args)
    validate_ca_file(paths.formal_ca)
    settings = validate_formal_config(
        paths.formal_config, expected_datadir=paths.formal_datadir
    )
    if read_datadir_uuid(paths.formal_datadir) != expected_uuid:
        raise CutoverError("formal datadir UUID mismatch")
    runtime = parse_mysql_option_file(paths.runtime_option_file)
    acceptance = _read_json(paths.acceptance_evidence, label="final acceptance evidence")
    provision = _read_json(paths.provision_evidence, label="runtime provision evidence")
    validate_acceptance_evidence(acceptance, expected_uuid=expected_uuid)
    validate_provision_evidence(
        provision,
        expected_uuid=expected_uuid,
        staged_env=paths.staged_env,
        runtime_option_file=paths.runtime_option_file,
        formal_ca=paths.formal_ca,
    )
    _run([str(paths.mysqld), f"--defaults-file={paths.formal_config}", "--validate-config"])
    legacy = query_service(args.legacy_service)
    if not legacy.exists:
        raise CutoverError("legacy MySQL service is missing")
    new = query_service(args.new_service)
    validate_new_service_registration(
        new, mysqld=paths.mysqld, formal_config=paths.formal_config
    )
    return paths, {
        "target_uuid": expected_uuid,
        "formal_config_sha256": _sha256(paths.formal_config),
        "formal_ca_sha256": _sha256(paths.formal_ca),
        "staged_env_sha256": _sha256(paths.staged_env),
        "runtime_user_sha256": hashlib.sha256(runtime["user"].encode()).hexdigest(),
        "formal_settings": {
            key: settings[key]
            for key in (
                "port",
                "datadir",
                "require-secure-transport",
                "binlog-format",
                "sync-binlog",
                "innodb-flush-log-at-trx-commit",
            )
        },
        "legacy_service_before": asdict(legacy),
        "new_service_before": asdict(new),
    }


def _verify_mysql84(paths: CutoverPaths, *, expected_uuid: str) -> dict[str, Any]:
    options = parse_mysql_option_file(paths.runtime_option_file)
    try:
        connection = pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user=options["user"],
            password=options["password"],
            database="probiga",
            charset="utf8mb4",
            autocommit=True,
            cursorclass=DictCursor,
            connect_timeout=15,
            read_timeout=60,
            write_timeout=60,
            ssl_ca=str(paths.formal_ca),
            ssl_verify_cert=True,
        )
    except pymysql.MySQLError as exc:
        raise CutoverError("production runtime TLS connection failed") from exc
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT @@version AS version, @@version_comment AS version_comment, "
                "@@server_uuid AS server_uuid, @@port AS port, @@datadir AS datadir, "
                "@@require_secure_transport AS require_secure_transport, "
                "@@global.log_bin AS log_bin, @@global.binlog_format AS binlog_format, "
                "@@global.sync_binlog AS sync_binlog, "
                "@@global.innodb_flush_log_at_trx_commit AS flush_at_commit, "
                "@@global.log_bin_trust_function_creators AS trust_creators, "
                "@@global.sql_mode AS sql_mode, @@global.time_zone AS time_zone, "
                "@@global.character_set_server AS character_set_server, "
                "@@global.collation_server AS collation_server, "
                "@@global.default_collation_for_utf8mb4 AS utf8mb4_default_collation, "
                "@@global.event_scheduler AS event_scheduler, "
                "@@global.local_infile AS local_infile, "
                "@@global.secure_file_priv AS secure_file_priv, "
                "@@basedir AS basedir, @@tmpdir AS tmpdir, "
                "@@global.log_bin_basename AS log_bin_basename, "
                "@@global.log_error AS log_error, @@global.pid_file AS pid_file, "
                "@@global.slow_query_log_file AS slow_query_log_file, "
                "@@global.ssl_ca AS ssl_ca, @@global.ssl_cert AS ssl_cert, "
                "@@global.ssl_key AS ssl_key"
            )
            row = cursor.fetchone() or {}
            cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
            tls = cursor.fetchone() or {}
            cursor.execute(
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name IN (%s,%s,%s) ORDER BY schema_name",
                EXPECTED_SCHEMAS,
            )
        schemas = tuple(
            next(
                value
                for key, value in item.items()
                if key.casefold() == "schema_name"
            )
            for item in cursor.fetchall()
        )
    finally:
        connection.close()
    observed_datadir = Path(str(row.get("datadir") or "").replace("/", os.sep))
    sql_modes = {
        item.strip().upper()
        for item in str(row.get("sql_mode") or "").split(",")
        if item.strip()
    }
    required_sql_modes = {
        "ONLY_FULL_GROUP_BY",
        "STRICT_TRANS_TABLES",
        "NO_ZERO_IN_DATE",
        "NO_ZERO_DATE",
        "ERROR_FOR_DIVISION_BY_ZERO",
        "NO_ENGINE_SUBSTITUTION",
    }
    runtime_paths = {
        key: str(row.get(key) or "")
        for key in (
            "basedir",
            "datadir",
            "tmpdir",
            "log_bin_basename",
            "log_error",
            "pid_file",
            "slow_query_log_file",
            "ssl_ca",
            "ssl_cert",
            "ssl_key",
        )
    }
    checks = {
        "version": str(row.get("version") or "") == EXPECTED_VERSION,
        "oracle_distribution": "mysql" in str(row.get("version_comment") or "").casefold(),
        "server_uuid": str(row.get("server_uuid") or "").lower() == expected_uuid,
        "port": int(row.get("port") or 0) == 3306,
        "datadir": _same_path(observed_datadir, paths.formal_datadir),
        "tls": bool(str(tls.get("Value") or "")),
        "require_secure_transport": int(row.get("require_secure_transport") or 0) == 1,
        "log_bin": int(row.get("log_bin") or 0) == 1,
        "binlog_format": str(row.get("binlog_format") or "").upper() == "ROW",
        "sync_binlog": int(row.get("sync_binlog") or 0) == 1,
        "flush_at_commit": int(row.get("flush_at_commit") or 0) == 1,
        "trust_creators_off": int(row.get("trust_creators") or 0) == 0,
        "strict_sql_mode": required_sql_modes.issubset(sql_modes),
        "time_zone": str(row.get("time_zone") or "") == "+08:00",
        "character_set_server": str(row.get("character_set_server") or "").lower()
        == "utf8mb4",
        "collation_server": str(row.get("collation_server") or "").lower()
        == "utf8mb4_general_ci",
        "utf8mb4_default_collation": str(
            row.get("utf8mb4_default_collation") or ""
        ).lower()
        == "utf8mb4_general_ci",
        "event_scheduler_off": str(row.get("event_scheduler") or "").upper()
        == "OFF",
        "local_infile_off": int(row.get("local_infile") or 0) == 0,
        "secure_file_priv_null": secure_file_priv_is_disabled(
            row.get("secure_file_priv")
        ),
        "no_removable_runtime_path": all(
            not value.strip().replace("/", "\\").upper().startswith("F:\\")
            for value in runtime_paths.values()
        ),
        "business_schemas": schemas == tuple(sorted(EXPECTED_SCHEMAS)),
    }
    if not all(checks.values()):
        failed = sorted(key for key, value in checks.items() if not value)
        raise CutoverError("production MySQL 8.4 verification failed: " + ", ".join(failed))
    return {
        "checks": checks,
        "server_uuid": expected_uuid,
        "version": EXPECTED_VERSION,
        "port": 3306,
        "datadir": str(paths.formal_datadir),
        "tls_cipher": str(tls.get("Value") or ""),
        "runtime_paths": runtime_paths,
        "sql_modes": sorted(sql_modes),
    }


def _stop_disable_new(name: str) -> None:
    state = query_service(name)
    if not state.exists:
        return
    if state.state not in {"STOPPED", "STOP_PENDING"}:
        _run(["sc.exe", "stop", name], check=False)
        try:
            wait_service(name, "STOPPED", timeout=90)
        except CutoverError:
            pass
    _run(["sc.exe", "config", name, "start=", "disabled"], check=False)


def apply(args: argparse.Namespace) -> dict[str, Any]:
    if args.ack != APPLY_ACK:
        raise CutoverError("exact MySQL 8.4 cutover acknowledgement is required")
    paths, facts = preflight(args)
    legacy = query_service(args.legacy_service)
    if legacy.state != "STOPPED":
        raise CutoverError("legacy MySQL service must already be STOPPED")
    new_before = query_service(args.new_service)
    if new_before.exists and new_before.state != "STOPPED":
        raise CutoverError("new MySQL 8.4 service must be absent or STOPPED")
    if not port_is_free(3306):
        raise CutoverError("TCP port 3306 is not free")
    if paths.active_env_backup.exists() and _sha256(paths.active_env_backup) != _sha256(
        paths.active_env
    ):
        raise CutoverError("existing active env backup does not match the current legacy env")

    started = _utc_now()
    installed_now = False
    env_promoted = False
    try:
        if not paths.active_env_backup.exists():
            _atomic_copy_new(paths.active_env, paths.active_env_backup)
        if not new_before.exists:
            # Oracle requires --install first, then service name, then
            # --defaults-file for a named Windows service.
            _run(
                [
                    str(paths.mysqld),
                    "--install",
                    args.new_service,
                    f"--defaults-file={paths.formal_config}",
                ]
            )
            installed_now = True
        _run(["sc.exe", "config", args.new_service, "start=", "auto"])
        _run(["sc.exe", "config", args.legacy_service, "start=", "disabled"])
        _atomic_replace_from(paths.staged_env, paths.active_env)
        env_promoted = True
        _run(["sc.exe", "start", args.new_service])
        wait_service(args.new_service, "RUNNING", timeout=args.start_timeout)
        deadline = time.monotonic() + args.start_timeout
        while time.monotonic() < deadline and port_is_free(3306):
            time.sleep(1)
        if port_is_free(3306):
            raise CutoverError("MySQL 8.4 service did not bind TCP port 3306")
        verification = _verify_mysql84(
            paths, expected_uuid=facts["target_uuid"]
        )
    except BaseException:
        _stop_disable_new(args.new_service)
        if env_promoted and paths.active_env_backup.exists():
            _atomic_replace_from(paths.active_env_backup, paths.active_env)
        legacy_start = "auto" if legacy.start_type == "AUTO_START" else "demand"
        _run(
            ["sc.exe", "config", args.legacy_service, "start=", legacy_start],
            check=False,
        )
        raise

    return {
        "schema_version": 1,
        "tool": "cutover_mysql84_production",
        "mode": "apply",
        "status": "passed",
        "started_at_utc": started,
        "finished_at_utc": _utc_now(),
        "facts": facts,
        "new_service_installed_by_run": installed_now,
        "new_service_after": asdict(query_service(args.new_service)),
        "legacy_service_after": asdict(query_service(args.legacy_service)),
        "active_env_sha256": _sha256(paths.active_env),
        "active_env_backup": str(paths.active_env_backup),
        "verification": verification,
        "production_trading_activation_changed": False,
        "legacy_service_deleted": False,
    }


def _verify_mysql55_source() -> dict[str, Any]:
    option_file = Path(r"D:\MySQL84\config\source-client.ini")
    try:
        options = read_client_options(option_file)
        connection = pymysql.connect(
            host=options.host,
            port=options.port,
            user=options.user,
            password=options.password,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=DictCursor,
            connect_timeout=15,
        )
    except (DumpError, KeyError, ValueError, pymysql.MySQLError) as exc:
        raise CutoverError("legacy MySQL verification connection failed") from exc
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT @@version AS version, @@port AS port, @@datadir AS datadir")
            row = cursor.fetchone() or {}
    finally:
        connection.close()
    if str(row.get("version") or "").split("-", 1)[0] != "5.5.20":
        raise CutoverError("rollback service is not MySQL 5.5.20")
    if int(row.get("port") or 0) != 3306:
        raise CutoverError("rollback service is not listening on 3306")
    observed = Path(str(row.get("datadir") or "").replace("/", os.sep))
    if not _same_path(observed, LEGACY_DATADIR):
        raise CutoverError("rollback service uses the wrong legacy datadir")
    return {"version": "5.5.20", "port": 3306, "datadir": str(observed)}


def rollback(args: argparse.Namespace) -> dict[str, Any]:
    if args.ack != ROLLBACK_ACK:
        raise CutoverError("exact MySQL 5.5 rollback acknowledgement is required")
    if os.name != "nt" or not _is_admin():
        raise CutoverError("an elevated Windows administrator token is required")
    paths = _validate_paths(args)
    for required in (LEGACY_IBDATA, LEGACY_CONFIG, LEGACY_DATADIR):
        _canonical(required, file=required != LEGACY_DATADIR)
    legacy_before = query_service(args.legacy_service)
    if legacy_before.state not in {"STOPPED", "RUNNING"}:
        raise CutoverError(
            "legacy service must be STOPPED or RUNNING before rollback activation"
        )
    started = _utc_now()
    if legacy_before.state == "RUNNING":
        new_before = query_service(args.new_service)
        if new_before.state != "STOPPED" or new_before.start_type != "DISABLED":
            raise CutoverError(
                "MySQL 8.4 service must remain stopped and disabled during rollback verification"
            )
        if _sha256(paths.active_env) != _sha256(paths.active_env_backup):
            raise CutoverError(
                "active environment does not match the preserved MySQL 5.5 environment"
            )
        verification = _verify_mysql55_source()
        return {
            "schema_version": 1,
            "tool": "cutover_mysql84_production",
            "mode": "rollback",
            "status": "passed",
            "started_at_utc": started,
            "finished_at_utc": _utc_now(),
            "legacy_service_after": asdict(query_service(args.legacy_service)),
            "new_service_after": asdict(query_service(args.new_service)),
            "active_env_sha256": _sha256(paths.active_env),
            "verification": verification,
            "activation_already_completed": True,
            "mysql84_service_deleted": False,
            "mysql84_data_deleted": False,
        }
    _stop_disable_new(args.new_service)
    if not port_is_free(3306):
        raise CutoverError("TCP port 3306 is not free after stopping MySQL 8.4")
    _atomic_replace_from(paths.active_env_backup, paths.active_env)
    _run(["sc.exe", "config", args.legacy_service, "start=", "auto"])
    _run(["sc.exe", "start", args.legacy_service])
    wait_service(args.legacy_service, "RUNNING", timeout=args.start_timeout)
    verification = _verify_mysql55_source()
    return {
        "schema_version": 1,
        "tool": "cutover_mysql84_production",
        "mode": "rollback",
        "status": "passed",
        "started_at_utc": started,
        "finished_at_utc": _utc_now(),
        "legacy_service_after": asdict(query_service(args.legacy_service)),
        "new_service_after": asdict(query_service(args.new_service)),
        "active_env_sha256": _sha256(paths.active_env),
        "verification": verification,
        "activation_already_completed": False,
        "mysql84_service_deleted": False,
        "mysql84_data_deleted": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("preflight", "apply", "rollback"), required=True)
    parser.add_argument("--mysqld", type=Path, default=EXPECTED_MYSQLD)
    parser.add_argument("--formal-config", type=Path, default=EXPECTED_CONFIG)
    parser.add_argument("--formal-datadir", type=Path, default=EXPECTED_DATADIR)
    parser.add_argument("--formal-ca", type=Path, default=EXPECTED_CA)
    parser.add_argument("--runtime-option-file", type=Path, required=True)
    parser.add_argument("--active-env", type=Path, required=True)
    parser.add_argument("--staged-env", type=Path, required=True)
    parser.add_argument("--active-env-backup", type=Path, required=True)
    parser.add_argument("--provision-evidence", type=Path, required=True)
    parser.add_argument("--acceptance-evidence", type=Path, required=True)
    parser.add_argument("--expected-target-uuid", required=True)
    parser.add_argument("--new-service", default=NEW_SERVICE)
    parser.add_argument("--legacy-service", default=LEGACY_SERVICE)
    parser.add_argument("--start-timeout", type=int, default=120)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--ack")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if not 30 <= args.start_timeout <= 600:
            raise CutoverError("start timeout must be in 30..600 seconds")
        if args.mode == "preflight":
            paths, facts = preflight(args)
            result = {
                "schema_version": 1,
                "tool": "cutover_mysql84_production",
                "mode": "preflight",
                "status": "passed",
                "finished_at_utc": _utc_now(),
                "facts": facts,
                "apply_ready_after_legacy_stop": True,
                "evidence_path": str(paths.evidence),
            }
        elif args.mode == "apply":
            paths = None
            result = apply(args)
        else:
            paths = None
            result = rollback(args)
        evidence_path = (paths.evidence if paths is not None else args.evidence.resolve())
        _write_evidence(evidence_path, result)
    except (CutoverError, OSError, ValueError, pymysql.MySQLError) as exc:
        try:
            failure_path = args.evidence.expanduser().resolve(strict=False)
            if failure_path.is_absolute() and not failure_path.exists():
                _write_evidence(
                    failure_path,
                    {
                        "schema_version": 1,
                        "tool": "cutover_mysql84_production",
                        "mode": args.mode,
                        "status": "failed",
                        "finished_at_utc": _utc_now(),
                        "failure_type": type(exc).__name__,
                        "failure": str(exc),
                        "legacy_service_deleted": False,
                        "data_deleted_by_service_tool": False,
                    },
                )
        except Exception:
            pass
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
