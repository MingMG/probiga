#!/usr/bin/env python3
"""Bootstrap a brand-new Oracle MySQL 8.4.11 instance without root login.

The command is deliberately unsuitable for an existing production instance:
it accepts only a new/freshly initialized data directory, requires a non-3306
port, builds every mysqld argument internally, and never uses
``--skip-grant-tables``.  The first start intentionally omits explicit TLS
certificate paths so MySQL can generate its CA/server certificate set.
"""

from __future__ import annotations

import argparse
import configparser
import hashlib
import json
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql
from pymysql.cursors import DictCursor

try:
    from tools.run_mysql55_consistent_dump import assert_protected_client_option_file
except ModuleNotFoundError:  # Support ``python tools/bootstrap_mysql84_instance.py``.
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from tools.run_mysql55_consistent_dump import assert_protected_client_option_file


EXPECTED_VERSION = "8.4.11"
DEPLOYMENT_MODES = ("formal", "rehearsal")
OPERATIONS = ("initialize-and-first-start", "first-start-only")
PREINITIALIZED_ATTESTATION = (
    "I_CONFIRM_DATADIR_WAS_JUST_INITIALIZED_AND_HAS_NO_BUSINESS_DATA"
)
SYSTEM_SCHEMAS = frozenset({"information_schema", "mysql", "performance_schema", "sys"})
CERTIFICATE_FILES = ("ca.pem", "server-cert.pem", "server-key.pem")
_ALLOWED_FRESH_DIRECTORIES = frozenset(
    {"mysql", "performance_schema", "sys", "#innodb_redo", "#innodb_temp"}
)
_VERSION_RE = re.compile(r"\bVer\s+8\.4\.11(?:[-+][0-9A-Za-z][0-9A-Za-z._-]*)?\b")
_SERVER_VERSION_RE = re.compile(r"^8\.4\.11(?:[-+][0-9A-Za-z][0-9A-Za-z._-]*)?$")
_ADMIN_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,31}$")
_FORBIDDEN_DISTRIBUTIONS = ("mariadb", "percona")
_ORACLE_RE = re.compile(r"MySQL (?:Community|Enterprise) Server", re.IGNORECASE)
_BELOW_NORMAL_PRIORITY_CLASS = 0x00004000
_HASH_CHUNK = 1024 * 1024


class BootstrapError(RuntimeError):
    """A bootstrap invariant failed closed."""


@dataclass(frozen=True)
class MysqldIdentity:
    executable: str
    basedir: str
    version_output: str


@dataclass(frozen=True)
class BootstrapPaths:
    datadir: Path
    cert_dir: Path
    state_dir: Path
    init_file: Path
    admin_options: Path
    restore_options: Path
    initialize_stdout: Path
    initialize_stderr: Path
    initialize_error: Path
    server_stdout: Path
    server_stderr: Path
    server_error: Path
    pid_file: Path
    evidence: Path


@dataclass(frozen=True)
class ServerObservation:
    version: str
    version_comment: str
    server_uuid: str
    port: int
    datadir: str
    current_user: str
    require_secure_transport: bool
    tls_cipher: str
    tls_version: str
    admin_plugin: str
    admin_ssl_type: str
    admin_account_locked: bool
    root_plugin: str
    root_account_locked: bool
    global_grant_verified: bool
    business_schemas: tuple[str, ...]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _lexists(path: Path) -> bool:
    return os.path.lexists(os.fspath(path))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(_HASH_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def _drive(path: Path) -> str:
    return path.drive.rstrip(":").upper()


def _same_path(left: Path | str, right: Path | str) -> bool:
    return os.path.normcase(os.path.abspath(os.fspath(left))) == os.path.normcase(
        os.path.abspath(os.fspath(right))
    )


def _is_nested(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _validate_path_policy(
    *,
    deployment_mode: str,
    mysqld: Path,
    datadir: Path,
    cert_dir: Path,
    state_dir: Path,
    allow_drive_f_for_rehearsal: bool,
) -> None:
    if deployment_mode not in DEPLOYMENT_MODES:
        raise BootstrapError("invalid deployment mode")
    paths = (mysqld, datadir, cert_dir, state_dir)
    uses_f = any(_drive(path) == "F" for path in paths)
    if deployment_mode == "formal":
        if allow_drive_f_for_rehearsal:
            raise BootstrapError("formal mode cannot accept the rehearsal F-drive override")
        if uses_f:
            raise BootstrapError("formal bootstrap paths must not use removable drive F")
        if _drive(cert_dir) != "D":
            raise BootstrapError("formal certificate directory must be on local drive D")
    elif uses_f and not allow_drive_f_for_rehearsal:
        raise BootstrapError("rehearsal use of drive F requires the explicit allow flag")
    if _is_nested(state_dir, datadir) or _is_nested(cert_dir, datadir):
        raise BootstrapError("state and certificate directories must be outside the data directory")
    if _is_nested(datadir, state_dir) or _is_nested(datadir, cert_dir):
        raise BootstrapError("data directory must be separate from state and certificates")
    if _is_nested(state_dir, cert_dir) or _is_nested(cert_dir, state_dir):
        raise BootstrapError("state and certificate directories must be separate")


def _run_icacls(path: Path, *, directory: bool) -> None:
    inheritance = "(OI)(CI)F" if directory else "F"
    current_user = str(os.environ.get("USERNAME") or "").strip()
    current_domain = str(os.environ.get("USERDOMAIN") or "").strip()
    current_principal = (
        f"{current_domain}\\{current_user}"
        if current_domain and current_user
        else current_user
    )
    command = [
        "icacls.exe",
        str(path),
        "/inheritance:r",
        "/grant:r",
        f"*S-1-5-18:{inheritance}",
        f"*S-1-5-32-544:{inheritance}",
    ]
    if current_principal:
        command.append(f"{current_principal}:{inheritance}")
    try:
        completed = subprocess.run(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise BootstrapError("could not restrict a bootstrap path ACL") from exc
    if completed.returncode != 0:
        raise BootstrapError("could not restrict a bootstrap path ACL")


def _protect_directory(path: Path) -> None:
    if os.name == "nt":
        _run_icacls(path, directory=True)
    else:
        path.chmod(0o700)


def _protect_file(path: Path) -> None:
    if os.name == "nt":
        _run_icacls(path, directory=False)
    else:
        path.chmod(0o600)
    try:
        assert_protected_client_option_file(path)
    except Exception as exc:
        raise BootstrapError("bootstrap secret file ACL verification failed") from exc


def _prepare_empty_directory(path: Path, *, label: str) -> None:
    if path.exists():
        if not path.is_dir() or any(path.iterdir()):
            raise BootstrapError(f"{label} must be a new or empty directory")
    else:
        if not path.parent.is_dir():
            raise BootstrapError(f"{label} parent directory does not exist")
        path.mkdir()
    _protect_directory(path)


def _write_restricted(path: Path, payload: bytes) -> None:
    try:
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as exc:
        raise BootstrapError(f"refusing to overwrite bootstrap artifact: {path}") from exc
    except OSError as exc:
        raise BootstrapError(f"could not create bootstrap artifact: {path}") from exc
    _protect_file(path)


def _generate_password() -> str:
    value = secrets.token_urlsafe(48)
    if len(value) < 60 or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise BootstrapError("secure password generation failed")
    return value


def _build_init_sql(admin_user: str, root_password: str, admin_password: str) -> bytes:
    if _ADMIN_RE.fullmatch(admin_user) is None:
        raise BootstrapError("admin user must be a safe 1-32 character MySQL account name")
    for password in (root_password, admin_password):
        if not re.fullmatch(r"[A-Za-z0-9_-]{60,}", password):
            raise BootstrapError("generated password failed the safe SQL alphabet gate")
    statements = (
        "ALTER USER 'root'@'localhost' IDENTIFIED WITH caching_sha2_password "
        f"BY '{root_password}';",
        f"CREATE USER '{admin_user}'@'127.0.0.1' IDENTIFIED WITH "
        f"caching_sha2_password BY '{admin_password}' REQUIRE SSL;",
        f"ALTER USER '{admin_user}'@'127.0.0.1' PASSWORD EXPIRE NEVER ACCOUNT UNLOCK;",
        f"GRANT ALL PRIVILEGES ON *.* TO '{admin_user}'@'127.0.0.1' WITH GRANT OPTION;",
        "ALTER USER 'root'@'localhost' ACCOUNT LOCK;",
    )
    return ("\n".join(statements) + "\n").encode("ascii")


def _build_admin_options(
    *, admin_user: str, admin_password: str, port: int, ca_file: Path
) -> bytes:
    if "\n" in admin_password or "\r" in admin_password:
        raise BootstrapError("invalid generated admin password")
    ca_value = ca_file.as_posix()
    return (
        "[client]\n"
        "protocol=tcp\n"
        "host=127.0.0.1\n"
        f"port={port}\n"
        f"user={admin_user}\n"
        f"password={admin_password}\n"
        "default-character-set=utf8mb4\n"
        "ssl-mode=VERIFY_CA\n"
        f"ssl-ca={ca_value}\n"
    ).encode("utf-8")


def _build_restore_options(
    *, admin_user: str, admin_password: str, port: int
) -> bytes:
    """Build the minimal option file accepted by the restore orchestrator."""

    if "\n" in admin_password or "\r" in admin_password:
        raise BootstrapError("invalid generated admin password")
    return (
        "[client]\n"
        "protocol=tcp\n"
        "host=127.0.0.1\n"
        f"port={port}\n"
        f"user={admin_user}\n"
        f"password={admin_password}\n"
    ).encode("ascii")


def inspect_mysqld(executable: Path) -> MysqldIdentity:
    resolved = executable.expanduser().resolve(strict=True)
    if not resolved.is_file() or resolved.name.casefold() not in {"mysqld", "mysqld.exe"}:
        raise BootstrapError("mysqld must be an existing executable named mysqld")
    command = [str(resolved), "--no-defaults", "--version"]
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
        )
        stdout, stderr = process.communicate(timeout=20)
    except (OSError, subprocess.SubprocessError) as exc:
        raise BootstrapError("could not inspect mysqld version") from exc
    output = (stdout + b" " + stderr)[:16384].decode("utf-8", errors="replace").strip()
    combined = output.casefold()
    if (
        process.returncode != 0
        or _VERSION_RE.search(output) is None
        or _ORACLE_RE.search(output) is None
        or any(token in combined for token in _FORBIDDEN_DISTRIBUTIONS)
    ):
        raise BootstrapError("mysqld must be the exact Oracle MySQL 8.4.11 build")
    if resolved.parent.name.casefold() != "bin":
        raise BootstrapError("mysqld must reside in the bin directory of its basedir")
    return MysqldIdentity(
        executable=str(resolved),
        basedir=str(resolved.parent.parent),
        version_output=output,
    )


def _fresh_uuid(datadir: Path) -> str:
    auto_cnf = datadir / "auto.cnf"
    if not auto_cnf.is_file() or auto_cnf.stat().st_size == 0:
        raise BootstrapError("fresh data directory is missing auto.cnf")
    parser = configparser.RawConfigParser(interpolation=None)
    try:
        parser.read(auto_cnf, encoding="utf-8-sig")
        raw = parser.get("auto", "server-uuid").strip().lower()
        parsed = uuid.UUID(raw)
    except (OSError, configparser.Error, ValueError) as exc:
        raise BootstrapError("fresh data directory has an invalid server UUID") from exc
    if str(parsed) != raw:
        raise BootstrapError("fresh data directory UUID must be canonical")
    return raw


def validate_fresh_datadir(
    datadir: Path, *, allow_initialize_generated_certificates: bool = False
) -> str:
    if not datadir.is_dir():
        raise BootstrapError("first-start-only requires an initialized data directory")
    for required in ("ibdata1", "mysql.ibd"):
        path = datadir / required
        if not path.is_file() or path.stat().st_size == 0:
            raise BootstrapError(f"fresh data directory is missing {required}")
    if not allow_initialize_generated_certificates:
        for name in CERTIFICATE_FILES:
            if _lexists(datadir / name):
                raise BootstrapError(
                    "data directory has already completed a TLS-generating first start"
                )
    unexpected_directories = sorted(
        child.name
        for child in datadir.iterdir()
        if child.is_dir() and child.name.casefold() not in _ALLOWED_FRESH_DIRECTORIES
    )
    if unexpected_directories:
        raise BootstrapError(
            "fresh data directory contains possible business schemas: "
            + ", ".join(unexpected_directories)
        )
    return _fresh_uuid(datadir)


def build_initialize_command(
    identity: MysqldIdentity, *, datadir: Path, error_log: Path
) -> tuple[str, ...]:
    return (
        identity.executable,
        "--no-defaults",
        "--initialize-insecure",
        f"--basedir={identity.basedir}",
        f"--datadir={datadir}",
        "--lower-case-table-names=1",
        "--character-set-server=utf8mb4",
        "--collation-server=utf8mb4_general_ci",
        f"--log-error={error_log}",
    )


def build_first_start_command(
    identity: MysqldIdentity,
    *,
    datadir: Path,
    port: int,
    init_file: Path,
    error_log: Path,
    pid_file: Path,
) -> tuple[str, ...]:
    if port == 3306 or not 1 <= port <= 65535:
        raise BootstrapError("bootstrap port must be an explicit non-3306 TCP port")
    command = (
        identity.executable,
        "--no-defaults",
        f"--basedir={identity.basedir}",
        f"--datadir={datadir}",
        f"--port={port}",
        "--bind-address=127.0.0.1",
        "--skip-networking=OFF",
        "--mysqlx=OFF",
        "--shared-memory=OFF",
        "--named-pipe=OFF",
        "--skip-name-resolve",
        "--require-secure-transport=ON",
        "--auto-generate-certs=ON",
        "--tls-version=TLSv1.2,TLSv1.3",
        "--mysql-native-password=OFF",
        "--local-infile=OFF",
        "--event-scheduler=OFF",
        "--secure-file-priv=NULL",
        "--skip-log-bin",
        "--lower-case-table-names=1",
        "--character-set-server=utf8mb4",
        "--collation-server=utf8mb4_general_ci",
        "--default-time-zone=+08:00",
        "--explicit-defaults-for-timestamp=ON",
        f"--init-file={init_file}",
        f"--log-error={error_log}",
        f"--pid-file={pid_file}",
    )
    lowered = tuple(item.casefold() for item in command)
    if any(item.startswith("--skip-grant-tables") for item in lowered):
        raise BootstrapError("skip-grant-tables is forbidden")
    if any(
        item.startswith(("--ssl-ca", "--ssl-cert", "--ssl-key")) for item in lowered
    ):
        raise BootstrapError("first start must not specify TLS certificate paths")
    return command


def _child_environment() -> dict[str, str]:
    result = os.environ.copy()
    for name in ("MYSQL_PWD", "MYSQL_HOST", "MYSQL_TCP_PORT", "MYSQL_UNIX_PORT"):
        result.pop(name, None)
    return result


def _popen_kwargs() -> dict[str, Any]:
    return {
        "stdin": subprocess.DEVNULL,
        "shell": False,
        "env": _child_environment(),
        "creationflags": _BELOW_NORMAL_PRIORITY_CLASS if os.name == "nt" else 0,
    }


def _initialize_datadir(
    identity: MysqldIdentity, paths: BootstrapPaths
) -> int:
    command = build_initialize_command(
        identity, datadir=paths.datadir, error_log=paths.initialize_error
    )
    with paths.initialize_stdout.open("xb") as stdout, paths.initialize_stderr.open(
        "xb"
    ) as stderr:
        process = subprocess.Popen(command, stdout=stdout, stderr=stderr, **_popen_kwargs())
        return_code = int(process.wait())
    if return_code != 0:
        raise BootstrapError(f"mysqld --initialize-insecure failed with code {return_code}")
    return return_code


def _start_server(identity: MysqldIdentity, paths: BootstrapPaths, port: int):
    command = build_first_start_command(
        identity,
        datadir=paths.datadir,
        port=port,
        init_file=paths.init_file,
        error_log=paths.server_error,
        pid_file=paths.pid_file,
    )
    with paths.server_stdout.open("xb") as stdout, paths.server_stderr.open("xb") as stderr:
        return subprocess.Popen(command, stdout=stdout, stderr=stderr, **_popen_kwargs())


def _assert_port_free(port: int) -> None:
    if port == 3306 or not 1 <= port <= 65535:
        raise BootstrapError("bootstrap port must be an explicit non-3306 TCP port")
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.settimeout(0.25)
        if probe.connect_ex(("127.0.0.1", port)) == 0:
            raise BootstrapError(f"bootstrap port {port} is already accepting connections")
    finally:
        probe.close()


def _connect_admin(*, user: str, password: str, port: int, ca_file: Path):
    try:
        return pymysql.connect(
            host="127.0.0.1",
            port=port,
            user=user,
            password=password,
            charset="utf8mb4",
            autocommit=True,
            connect_timeout=2,
            read_timeout=15,
            write_timeout=15,
            cursorclass=DictCursor,
            ssl={"ca": str(ca_file.resolve(strict=True)), "check_hostname": False},
        )
    except (pymysql.MySQLError, OSError):
        raise BootstrapError("TLS admin connection is not ready") from None


def wait_until_ready(
    process: Any,
    *,
    user: str,
    password: str,
    port: int,
    ca_file: Path,
    timeout_seconds: int,
):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        return_code = process.poll()
        if return_code is not None:
            raise BootstrapError(f"first-start mysqld exited before ready with code {return_code}")
        if ca_file.is_file():
            try:
                return _connect_admin(user=user, password=password, port=port, ca_file=ca_file)
            except BootstrapError:
                pass
        time.sleep(0.25)
    raise BootstrapError("first-start mysqld did not become TLS-ready before timeout")


def _canonical_uuid(value: object) -> str:
    raw = str(value or "").strip().lower()
    try:
        parsed = uuid.UUID(raw)
    except ValueError as exc:
        raise BootstrapError("server returned an invalid UUID") from exc
    if str(parsed) != raw:
        raise BootstrapError("server UUID is not canonical")
    return raw


def verify_instance(
    connection: Any,
    *,
    admin_user: str,
    expected_port: int,
    expected_datadir: Path,
    expected_uuid: str,
) -> ServerObservation:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT @@version AS version, @@version_comment AS version_comment, "
            "@@server_uuid AS server_uuid, @@port AS port, @@datadir AS datadir, "
            "@@require_secure_transport AS require_secure_transport, "
            "CURRENT_USER() AS current_user_name"
        )
        identity = cursor.fetchone()
        if not isinstance(identity, Mapping):
            raise BootstrapError("server identity query returned no row")
        version = str(identity.get("version") or "")
        comment = str(identity.get("version_comment") or "")
        combined = f"{version} {comment}".casefold()
        if (
            _SERVER_VERSION_RE.fullmatch(version) is None
            or _ORACLE_RE.match(comment) is None
            or any(token in combined for token in _FORBIDDEN_DISTRIBUTIONS)
        ):
            raise BootstrapError("connected server is not exact Oracle MySQL 8.4.11")
        server_uuid = _canonical_uuid(identity.get("server_uuid"))
        if server_uuid != expected_uuid:
            raise BootstrapError("connected server UUID differs from the fresh datadir UUID")
        if int(identity.get("port") or 0) != expected_port:
            raise BootstrapError("connected server port differs from the requested bootstrap port")
        if not _same_path(str(identity.get("datadir") or ""), expected_datadir):
            raise BootstrapError("connected server datadir differs from the requested new datadir")
        if int(identity.get("require_secure_transport") or 0) != 1:
            raise BootstrapError("require_secure_transport is not enabled")
        current_user = str(identity.get("current_user_name") or "")
        if current_user.casefold() != f"{admin_user}@127.0.0.1".casefold():
            raise BootstrapError("connection did not authenticate as the bootstrap admin account")

        cursor.execute(
            "SHOW SESSION STATUS WHERE Variable_name IN ('Ssl_cipher', 'Ssl_version')"
        )
        status_rows = cursor.fetchall()
        statuses = {
            str(row.get("Variable_name") or row.get("variable_name") or ""): str(
                row.get("Value") or row.get("value") or ""
            )
            for row in status_rows
        }
        cipher = statuses.get("Ssl_cipher", "")
        tls_version = statuses.get("Ssl_version", "")
        if not cipher or tls_version not in {"TLSv1.2", "TLSv1.3"}:
            raise BootstrapError("admin session is not using an accepted TLS transport")

        cursor.execute(
            "SELECT USER AS account_user, HOST AS account_host, plugin, ssl_type, "
            "account_locked FROM mysql.user WHERE USER = %s OR USER = 'root'",
            (admin_user,),
        )
        accounts = {
            (str(row["account_user"]), str(row["account_host"])): row
            for row in cursor.fetchall()
        }
        if set(accounts) != {(admin_user, "127.0.0.1"), ("root", "localhost")}:
            raise BootstrapError("bootstrap admin/root account inventory is not exact")
        admin = accounts[(admin_user, "127.0.0.1")]
        root = accounts[("root", "localhost")]
        if str(admin.get("plugin") or "") != "caching_sha2_password":
            raise BootstrapError("bootstrap admin authentication plugin is not caching_sha2_password")
        if str(admin.get("ssl_type") or "").upper() != "ANY":
            raise BootstrapError("bootstrap admin account does not REQUIRE SSL")
        if str(admin.get("account_locked") or "").upper() != "N":
            raise BootstrapError("bootstrap admin account is locked")
        if str(root.get("plugin") or "") != "caching_sha2_password":
            raise BootstrapError("root authentication plugin is not caching_sha2_password")
        if str(root.get("account_locked") or "").upper() != "Y":
            raise BootstrapError("root@localhost is not locked")

        cursor.execute(f"SHOW GRANTS FOR '{admin_user}'@'127.0.0.1'")
        grants = [str(next(iter(row.values()))) for row in cursor.fetchall()]
        normalized_grants = "\n".join(grants).upper()
        expanded_global_grant = (
            "CREATE USER" in normalized_grants
            and "TRIGGER" in normalized_grants
            and "SYSTEM_VARIABLES_ADMIN" in normalized_grants
        )
        grant_verified = (
            " ON *.* TO " in normalized_grants
            and "WITH GRANT OPTION" in normalized_grants
            and (
                "GRANT ALL PRIVILEGES" in normalized_grants
                or expanded_global_grant
            )
        )
        if not grant_verified:
            raise BootstrapError("bootstrap admin global grant is incomplete")

        cursor.execute(
            "SELECT SCHEMA_NAME AS schema_name FROM information_schema.SCHEMATA "
            "WHERE LOWER(SCHEMA_NAME) NOT IN ('information_schema','mysql',"
            "'performance_schema','sys') ORDER BY SCHEMA_NAME"
        )
        business_schemas = tuple(str(row["schema_name"]) for row in cursor.fetchall())
        if business_schemas:
            raise BootstrapError("fresh bootstrap target already contains business schemas")

    return ServerObservation(
        version=version,
        version_comment=comment,
        server_uuid=server_uuid,
        port=expected_port,
        datadir=str(expected_datadir),
        current_user=current_user,
        require_secure_transport=True,
        tls_cipher=cipher,
        tls_version=tls_version,
        admin_plugin=str(admin["plugin"]),
        admin_ssl_type=str(admin["ssl_type"]),
        admin_account_locked=False,
        root_plugin=str(root["plugin"]),
        root_account_locked=True,
        global_grant_verified=True,
        business_schemas=business_schemas,
    )


def configure_default_collation(connection: Any) -> dict[str, str]:
    """Pin utf8mb4's legacy-compatible default for this new instance.

    MySQL 8.4.11 accepts ``SET PERSIST`` for this variable but may defer the
    effective value until restart, so the bootstrap also applies ``SET GLOBAL``
    for the current process.  A mocked connection used by orchestration tests
    has no cursor and is intentionally left untouched.
    """

    cursor_factory = getattr(connection, "cursor", None)
    if not callable(cursor_factory):
        return {"status": "mock-orchestration-skipped"}
    with cursor_factory() as cursor:
        cursor.execute(
            "SET PERSIST default_collation_for_utf8mb4='utf8mb4_general_ci'"
        )
        cursor.execute(
            "SET GLOBAL default_collation_for_utf8mb4='utf8mb4_general_ci'"
        )
        cursor.execute(
            "SELECT @@GLOBAL.default_collation_for_utf8mb4 AS value, "
            "(SELECT VARIABLE_SOURCE FROM performance_schema.variables_info "
            "WHERE VARIABLE_NAME='default_collation_for_utf8mb4') AS source"
        )
        row = cursor.fetchone() or {}
    value = str(row.get("value", ""))
    if value != "utf8mb4_general_ci":
        raise BootstrapError("could not pin default_collation_for_utf8mb4")
    return {
        "value": value,
        "variable_source_after_bootstrap": str(row.get("source", "")),
        "persist_requested": "true",
    }


def _validate_generated_certificates(datadir: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for name in CERTIFICATE_FILES:
        path = datadir / name
        if not path.is_file() or path.stat().st_size < 128:
            raise BootstrapError(f"first start did not generate a usable {name}")
        head = path.read_bytes()[:256]
        if name.endswith("key.pem"):
            if b"PRIVATE KEY" not in head:
                raise BootstrapError("generated server private key is not PEM")
        elif b"BEGIN CERTIFICATE" not in head:
            raise BootstrapError(f"generated {name} is not a PEM certificate")
        item: dict[str, Any] = {"bytes": path.stat().st_size}
        if name != "server-key.pem":
            item["sha256"] = _sha256(path)
        result[name] = item
    return result


def _copy_certificates(datadir: Path, cert_dir: Path) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for name in CERTIFICATE_FILES:
        source = datadir / name
        destination = cert_dir / name
        if _lexists(destination):
            raise BootstrapError(f"refusing to overwrite certificate: {destination}")
        try:
            with source.open("rb") as reader, destination.open("xb") as writer:
                shutil.copyfileobj(reader, writer, length=_HASH_CHUNK)
                writer.flush()
                os.fsync(writer.fileno())
        except OSError as exc:
            raise BootstrapError(f"could not copy generated certificate {name}") from exc
        _protect_file(destination)
        item: dict[str, Any] = {
            "path": str(destination),
            "bytes": destination.stat().st_size,
            "acl_protected": True,
        }
        if name != "server-key.pem":
            item["sha256"] = _sha256(destination)
        result[name] = item
    return result


def shutdown_server(connection: Any, process: Any, *, timeout_seconds: int = 60) -> int:
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHUTDOWN")
    except pymysql.MySQLError as exc:
        if not exc.args or int(exc.args[0]) not in {2006, 2013}:
            raise BootstrapError("authenticated SHUTDOWN failed") from None
    finally:
        try:
            connection.close()
        except Exception:
            pass
    try:
        return_code = int(process.wait(timeout=timeout_seconds))
    except subprocess.TimeoutExpired as exc:
        raise BootstrapError("mysqld did not exit after authenticated SHUTDOWN") from exc
    if return_code != 0:
        raise BootstrapError(f"mysqld exited with code {return_code} after SHUTDOWN")
    return return_code


def _terminate_own_process(process: Any) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        process.terminate()
        process.wait(timeout=15)
    except Exception:
        try:
            process.kill()
            process.wait(timeout=15)
        except Exception:
            pass


def _delete_init_file(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        raise BootstrapError("could not delete the one-time credential init file") from exc
    if _lexists(path):
        raise BootstrapError("one-time credential init file still exists")


def _file_contains(path: Path, needles: Sequence[bytes]) -> bool:
    if not path.is_file():
        return False
    overlap = max((len(needle) for needle in needles), default=1) - 1
    previous = b""
    with path.open("rb") as stream:
        while chunk := stream.read(_HASH_CHUNK):
            data = previous + chunk
            if any(needle and needle in data for needle in needles):
                return True
            previous = data[-overlap:] if overlap else b""
    return False


def _assert_logs_secret_free(paths: BootstrapPaths, passwords: Sequence[str]) -> None:
    needles = tuple(password.encode("ascii") for password in passwords)
    logs = (
        paths.initialize_stdout,
        paths.initialize_stderr,
        paths.initialize_error,
        paths.server_stdout,
        paths.server_stderr,
        paths.server_error,
    )
    leaks = [path for path in logs if _file_contains(path, needles)]
    if leaks:
        for path in leaks:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
        raise BootstrapError("a server log contained credential material and was removed")


def _log_evidence(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"path": str(path), "present": False}
    return {
        "path": str(path),
        "present": True,
        "bytes": path.stat().st_size,
        "sha256": _sha256(path),
    }


def _write_evidence(path: Path, payload: Mapping[str, Any], passwords: Sequence[str]) -> None:
    encoded = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    lowered = encoded.lower()
    forbidden = (
        b"alter user",
        b"create user",
        b"grant all privileges",
        b"--skip-grant-tables",
        b'"argv"',
    )
    if any(password.encode("ascii") in encoded for password in passwords) or any(
        token in lowered for token in forbidden
    ):
        raise BootstrapError("evidence payload contains forbidden credential or command material")
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.partial"
    _write_restricted(temporary, encoded)
    try:
        if os.name == "nt":
            os.rename(temporary, path)
        else:
            os.link(temporary, path)
            temporary.unlink()
    except OSError as exc:
        raise BootstrapError("could not atomically publish bootstrap evidence") from exc
    finally:
        temporary.unlink(missing_ok=True)


def _make_paths(datadir: Path, cert_dir: Path, state_dir: Path) -> BootstrapPaths:
    return BootstrapPaths(
        datadir=datadir,
        cert_dir=cert_dir,
        state_dir=state_dir,
        init_file=state_dir / ".mysql84-first-start-init.sql",
        admin_options=state_dir / "mysql84-admin-client.ini",
        restore_options=state_dir / "mysql84-restore-client.ini",
        initialize_stdout=state_dir / "initialize.stdout.log",
        initialize_stderr=state_dir / "initialize.stderr.log",
        initialize_error=state_dir / "initialize.error.log",
        server_stdout=state_dir / "first-start.stdout.log",
        server_stderr=state_dir / "first-start.stderr.log",
        server_error=state_dir / "first-start.error.log",
        pid_file=state_dir / "first-start.pid",
        evidence=state_dir / "bootstrap-evidence.json",
    )


def bootstrap_instance(
    *,
    deployment_mode: str,
    operation: str,
    mysqld_executable: Path,
    datadir: Path,
    cert_dir: Path,
    state_dir: Path,
    port: int,
    admin_user: str = "probiga_admin",
    ready_timeout_seconds: int = 120,
    preinitialized_attestation: str | None = None,
    allow_drive_f_for_rehearsal: bool = False,
) -> dict[str, Any]:
    if operation not in OPERATIONS:
        raise BootstrapError("invalid bootstrap operation")
    if _ADMIN_RE.fullmatch(admin_user) is None:
        raise BootstrapError("admin user must be a safe 1-32 character MySQL account name")
    if not 10 <= ready_timeout_seconds <= 600:
        raise BootstrapError("ready timeout must be between 10 and 600 seconds")
    if operation == "first-start-only":
        if preinitialized_attestation != PREINITIALIZED_ATTESTATION:
            raise BootstrapError("first-start-only requires the exact fresh-datadir attestation")
    elif preinitialized_attestation:
        raise BootstrapError("preinitialized attestation is valid only for first-start-only")
    _assert_port_free(port)

    mysqld_path = mysqld_executable.expanduser().resolve(strict=True)
    datadir = datadir.expanduser().resolve(strict=False)
    cert_dir = cert_dir.expanduser().resolve(strict=False)
    state_dir = state_dir.expanduser().resolve(strict=False)
    _validate_path_policy(
        deployment_mode=deployment_mode,
        mysqld=mysqld_path,
        datadir=datadir,
        cert_dir=cert_dir,
        state_dir=state_dir,
        allow_drive_f_for_rehearsal=allow_drive_f_for_rehearsal,
    )
    identity = inspect_mysqld(mysqld_path)
    paths = _make_paths(datadir, cert_dir, state_dir)

    initialize_return_code: int | None = None
    if operation == "initialize-and-first-start":
        _prepare_empty_directory(state_dir, label="bootstrap state directory")
        _prepare_empty_directory(cert_dir, label="certificate directory")
        _prepare_empty_directory(datadir, label="new data directory")
        initialize_return_code = _initialize_datadir(identity, paths)
        fresh_uuid = validate_fresh_datadir(
            datadir, allow_initialize_generated_certificates=True
        )
    else:
        fresh_uuid = validate_fresh_datadir(datadir)
        _prepare_empty_directory(state_dir, label="bootstrap state directory")
        _prepare_empty_directory(cert_dir, label="certificate directory")
        _protect_directory(datadir)

    root_password = _generate_password()
    admin_password = _generate_password()
    if root_password == admin_password:
        raise BootstrapError("independent root/admin password generation collided")
    process = None
    connection = None
    shutdown_return_code: int | None = None
    success = False
    started_at = _utc_now()
    try:
        _write_restricted(
            paths.admin_options,
            _build_admin_options(
                admin_user=admin_user,
                admin_password=admin_password,
                port=port,
                ca_file=cert_dir / "ca.pem",
            ),
        )
        _write_restricted(
            paths.init_file,
            _build_init_sql(admin_user, root_password, admin_password),
        )
        process = _start_server(identity, paths, port)
        connection = wait_until_ready(
            process,
            user=admin_user,
            password=admin_password,
            port=port,
            ca_file=datadir / "ca.pem",
            timeout_seconds=ready_timeout_seconds,
        )
        default_collation = configure_default_collation(connection)
        observation = verify_instance(
            connection,
            admin_user=admin_user,
            expected_port=port,
            expected_datadir=datadir,
            expected_uuid=fresh_uuid,
        )
        generated_certificates = _validate_generated_certificates(datadir)
        shutdown_return_code = shutdown_server(connection, process)
        connection = None
        process = None
        deployed_certificates = _copy_certificates(datadir, cert_dir)
        _write_restricted(
            paths.restore_options,
            _build_restore_options(
                admin_user=admin_user,
                admin_password=admin_password,
                port=port,
            ),
        )
        _protect_file(paths.admin_options)
        _delete_init_file(paths.init_file)
        _assert_logs_secret_free(paths, (root_password, admin_password))

        evidence: dict[str, Any] = {
            "schema_version": 1,
            "status": "success",
            "deployment_mode": deployment_mode,
            "operation": operation,
            "started_at_utc": started_at,
            "finished_at_utc": _utc_now(),
            "mysqld": asdict(identity),
            "requested_port": port,
            "new_datadir": str(datadir),
            "fresh_datadir_uuid": fresh_uuid,
            "preinitialized_empty_attested": operation == "first-start-only",
            "server_observation": asdict(observation),
            "security": {
                "skip_grant_tables_used": False,
                "one_time_init_file_deleted": True,
                "admin_option_file": str(paths.admin_options),
                "admin_option_acl_protected": True,
                "restore_option_file": str(paths.restore_options),
                "restore_option_acl_protected": True,
                "root_password_retained": False,
                "admin_password_only_in_protected_option_file": True,
            },
            "certificates": {
                "auto_generated_without_explicit_ssl_paths": True,
                "source": generated_certificates,
                "deployed": deployed_certificates,
            },
            "compatibility": {
                "default_collation_for_utf8mb4": default_collation,
            },
            "processes": {
                "initialize_return_code": initialize_return_code,
                "shutdown_return_code": shutdown_return_code,
            },
            "logs": {
                "initialize_stdout": _log_evidence(paths.initialize_stdout),
                "initialize_stderr": _log_evidence(paths.initialize_stderr),
                "initialize_error": _log_evidence(paths.initialize_error),
                "first_start_stdout": _log_evidence(paths.server_stdout),
                "first_start_stderr": _log_evidence(paths.server_stderr),
                "first_start_error": _log_evidence(paths.server_error),
            },
        }
        _write_evidence(paths.evidence, evidence, (root_password, admin_password))
        success = True
        return evidence
    finally:
        if connection is not None and process is not None and process.poll() is None:
            try:
                shutdown_server(connection, process)
                connection = None
                process = None
            except Exception:
                pass
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass
        _terminate_own_process(process)
        _delete_init_file(paths.init_file)
        if not success:
            paths.restore_options.unlink(missing_ok=True)
        if not success:
            _assert_logs_secret_free(paths, (root_password, admin_password))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Fail-closed bootstrap for a brand-new Oracle MySQL 8.4.11 instance."
    )
    parser.add_argument("--deployment-mode", choices=DEPLOYMENT_MODES, required=True)
    parser.add_argument("--operation", choices=OPERATIONS, required=True)
    parser.add_argument("--mysqld", type=Path, required=True)
    parser.add_argument("--datadir", type=Path, required=True)
    parser.add_argument("--cert-dir", type=Path, required=True)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--admin-user", default="probiga_admin")
    parser.add_argument("--ready-timeout-seconds", type=int, default=120)
    parser.add_argument("--preinitialized-attestation")
    parser.add_argument("--allow-drive-f-for-rehearsal", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = bootstrap_instance(
            deployment_mode=args.deployment_mode,
            operation=args.operation,
            mysqld_executable=args.mysqld,
            datadir=args.datadir,
            cert_dir=args.cert_dir,
            state_dir=args.state_dir,
            port=args.port,
            admin_user=args.admin_user,
            ready_timeout_seconds=args.ready_timeout_seconds,
            preinitialized_attestation=args.preinitialized_attestation,
            allow_drive_f_for_rehearsal=args.allow_drive_f_for_rehearsal,
        )
    except (BootstrapError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": "success",
                "server_uuid": report["server_observation"]["server_uuid"],
                "evidence": str(Path(args.state_dir) / "bootstrap-evidence.json"),
                "admin_options": str(Path(args.state_dir) / "mysql84-admin-client.ini"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
