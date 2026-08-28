#!/usr/bin/env python3
from __future__ import annotations

import argparse
import configparser
import json
import os
import re
import socket
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any, Mapping

import pymysql
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from integrations.qmt.local_history import (  # noqa: E402
    LocalHistoryProvenanceSchemaError,
    get_local_history_engine,
    migrate_local_history_provenance_schema,
    validate_local_history_provenance_schema,
)
from server.common.batch_db import create_batch_engine  # noqa: E402
from server.common.mysql_version_policy import (  # noqa: E402
    MYSQL_84_ISOLATED_ACCEPTANCE,
    is_oracle_mysql_distribution,
    isolated_acceptance_version,
)
from tools.env_config import load_project_env  # noqa: E402


WINDOWS_LOCAL_OPTION_FILE = Path(
    r"D:\MySQL84\config\mysql84-runtime-client.ini"
)
WINDOWS_LOCAL_HISTORY_DATABASE = "probiga_qmt_history"
WINDOWS_LOCAL_MYSQL_HOST = "127.0.0.1"
WINDOWS_LOCAL_MYSQL_PORT = 3306
WINDOWS_LOCAL_MYSQL_HOSTNAME = "WIN-20260322RGF"
WINDOWS_LOCAL_MYSQL_SERVER_UUID = "f40c3202-9260-11f1-86ae-74d4dd7f8500"
WINDOWS_LOCAL_MYSQL_RUNTIME_IDENTITY = "probiga_runtime@127.0.0.1"
_OPTION_FILE_KEYS = {"protocol", "host", "port", "user", "password"}
_OPTION_USER_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")
_OPTION_PASSWORD_RE = re.compile(r"^[A-Za-z0-9_-]{48,160}$")
_WINDOWS_SYSTEM_SID = "S-1-5-18"
_WINDOWS_ADMINISTRATORS_SID = "S-1-5-32-544"
_WINDOWS_READ_DATA = 0x0001
_WINDOWS_ACL_TARGET_ENV = "PROBIGA_QMT_OPTION_FILE_ACL_TARGET"
_WINDOWS_REPARSE_POINT = getattr(
    stat,
    "FILE_ATTRIBUTE_REPARSE_POINT",
    0x0400,
)
_WINDOWS_ACL_SCRIPT = r"""
$ErrorActionPreference = 'Stop'
$target = [System.IO.Path]::GetFullPath(
    $env:PROBIGA_QMT_OPTION_FILE_ACL_TARGET
)
$acl = [System.IO.File]::GetAccessControl($target)
$ownerSid = ([System.Security.Principal.NTAccount]$acl.Owner).Translate(
    [System.Security.Principal.SecurityIdentifier]
).Value
$currentSid = [System.Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$rules = @(
    $acl.GetAccessRules(
        $true,
        $true,
        [System.Security.Principal.SecurityIdentifier]
    ) | ForEach-Object {
        [pscustomobject]@{
            sid = $_.IdentityReference.Value
            access_type = $_.AccessControlType.ToString()
            inherited = [bool]$_.IsInherited
            rights = [int64]$_.FileSystemRights
        }
    }
)
[pscustomobject]@{
    owner_sid = $ownerSid
    current_user_sid = $currentSid
    protected = [bool]$acl.AreAccessRulesProtected
    rules = $rules
} | ConvertTo-Json -Compress -Depth 4
""".strip()


class WindowsLocalHistoryBoundaryError(RuntimeError):
    """The fixed Windows option-file route is not safe to use."""


def _running_on_windows() -> bool:
    return os.name == "nt"


def _windows_acl_snapshot(path: Path) -> dict[str, Any]:
    inherited_environment_names = {
        "comspec",
        "path",
        "pathext",
        "systemdrive",
        "systemroot",
        "temp",
        "tmp",
        "windir",
    }
    process_environment = {
        key: value
        for key, value in os.environ.items()
        if key.casefold() in inherited_environment_names
    }
    process_environment[_WINDOWS_ACL_TARGET_ENV] = str(path)
    try:
        completed = subprocess.run(
            [
                "powershell.exe",
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                _WINDOWS_ACL_SCRIPT,
            ],
            check=False,
            capture_output=True,
            timeout=10,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            env=process_environment,
        )
    except (OSError, subprocess.SubprocessError, UnicodeError):
        raise WindowsLocalHistoryBoundaryError(
            "Windows MySQL option file ACL cannot be verified"
        ) from None
    finally:
        process_environment.pop(_WINDOWS_ACL_TARGET_ENV, None)
    try:
        output = completed.stdout.decode("ascii", errors="strict").strip()
    except UnicodeError:
        raise WindowsLocalHistoryBoundaryError(
            "Windows MySQL option file ACL cannot be verified"
        ) from None
    if completed.returncode != 0 or not output or len(output) > 65536:
        raise WindowsLocalHistoryBoundaryError(
            "Windows MySQL option file ACL cannot be verified"
        )
    try:
        snapshot = json.loads(output)
    except (TypeError, ValueError):
        raise WindowsLocalHistoryBoundaryError(
            "Windows MySQL option file ACL cannot be verified"
        ) from None
    if not isinstance(snapshot, dict):
        raise WindowsLocalHistoryBoundaryError(
            "Windows MySQL option file ACL cannot be verified"
        )
    return snapshot


def _validate_windows_acl_snapshot(snapshot: Mapping[str, Any]) -> None:
    owner_sid = str(snapshot.get("owner_sid") or "")
    current_sid = str(snapshot.get("current_user_sid") or "")
    if (
        not bool(snapshot.get("protected"))
        or not owner_sid
        or not current_sid
        or owner_sid not in {current_sid, _WINDOWS_ADMINISTRATORS_SID}
    ):
        raise WindowsLocalHistoryBoundaryError(
            "Windows MySQL option file DACL is not private"
        )
    rules = snapshot.get("rules")
    if not isinstance(rules, list) or not rules:
        raise WindowsLocalHistoryBoundaryError(
            "Windows MySQL option file DACL is not private"
        )
    allowed_sids = {
        current_sid,
        _WINDOWS_SYSTEM_SID,
        _WINDOWS_ADMINISTRATORS_SID,
    }
    observed_sids: set[str] = set()
    for rule in rules:
        if not isinstance(rule, Mapping):
            raise WindowsLocalHistoryBoundaryError(
                "Windows MySQL option file DACL is not private"
            )
        sid = str(rule.get("sid") or "")
        try:
            rights = int(rule.get("rights"))
        except (TypeError, ValueError):
            raise WindowsLocalHistoryBoundaryError(
                "Windows MySQL option file DACL is not private"
            ) from None
        if (
            sid not in allowed_sids
            or str(rule.get("access_type") or "") != "Allow"
            or bool(rule.get("inherited"))
            or not rights & _WINDOWS_READ_DATA
        ):
            raise WindowsLocalHistoryBoundaryError(
                "Windows MySQL option file DACL is not private"
            )
        observed_sids.add(sid)
    if observed_sids != allowed_sids:
        raise WindowsLocalHistoryBoundaryError(
            "Windows MySQL option file DACL is not private"
        )


def _protected_windows_option_file(path: Path) -> Path:
    if not _running_on_windows() or not path.is_absolute():
        raise WindowsLocalHistoryBoundaryError(
            "fixed Windows MySQL option file is unavailable"
        )
    try:
        if not os.path.lexists(path) or path.is_symlink():
            raise OSError("unsafe option file")
        resolved = path.resolve(strict=True)
        state = resolved.stat()
    except OSError:
        raise WindowsLocalHistoryBoundaryError(
            "fixed Windows MySQL option file is unavailable"
        ) from None
    if (
        not stat.S_ISREG(state.st_mode)
        or int(getattr(state, "st_nlink", 1)) != 1
        or int(getattr(state, "st_file_attributes", 0))
        & _WINDOWS_REPARSE_POINT
    ):
        raise WindowsLocalHistoryBoundaryError(
            "fixed Windows MySQL option file is not a regular private file"
        )
    _validate_windows_acl_snapshot(_windows_acl_snapshot(resolved))
    return resolved


def _validate_windows_option_file_shape(path: Path) -> None:
    parser = configparser.RawConfigParser(interpolation=None, strict=True)
    try:
        with path.open("r", encoding="utf-8-sig") as stream:
            parser.read_file(stream)
        if (
            parser.sections() != ["client"]
            or set(parser.options("client")) != _OPTION_FILE_KEYS
        ):
            raise ValueError("unexpected option file shape")
        values = {
            key: parser.get("client", key, raw=True).strip()
            for key in _OPTION_FILE_KEYS
        }
        if (
            values["protocol"].casefold() != "tcp"
            or values["host"] != WINDOWS_LOCAL_MYSQL_HOST
            or values["port"] != str(WINDOWS_LOCAL_MYSQL_PORT)
            or _OPTION_USER_RE.fullmatch(values["user"]) is None
            or _OPTION_PASSWORD_RE.fullmatch(values["password"]) is None
        ):
            raise ValueError("unexpected option file target")
    except (OSError, UnicodeError, configparser.Error, ValueError):
        raise WindowsLocalHistoryBoundaryError(
            "Windows MySQL option file shape or target differs"
        ) from None
    finally:
        parser.clear()
        if "values" in locals():
            values.clear()


def _connect_from_windows_option_file(path: Path):
    # Validate on every new DBAPI connection.  The driver receives only the
    # protected file path; its password is never copied into a command argument
    # or SQLAlchemy URL/connect mapping.
    resolved = _protected_windows_option_file(path)
    _validate_windows_option_file_shape(resolved)
    return pymysql.connect(
        read_default_file=str(resolved),
        read_default_group="client",
        database=WINDOWS_LOCAL_HISTORY_DATABASE,
        charset="utf8mb4",
        autocommit=False,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
        ssl={"check_hostname": False},
    )


def _create_windows_local_history_engine(
    path: Path = WINDOWS_LOCAL_OPTION_FILE,
) -> Engine:
    resolved = _protected_windows_option_file(path)
    _validate_windows_option_file_shape(resolved)
    safe_url = URL.create(
        "mysql+pymysql",
        database=WINDOWS_LOCAL_HISTORY_DATABASE,
    )
    return create_engine(
        safe_url,
        creator=lambda: _connect_from_windows_option_file(resolved),
        pool_pre_ping=True,
        future=True,
    )


def _validate_windows_local_mysql84_boundary(engine: Engine) -> dict[str, Any]:
    try:
        with engine.connect() as connection:
            state = dict(connection.execute(text(
                "SELECT VERSION() AS mysql_version, "
                "@@version_comment AS version_comment, "
                "DATABASE() AS database_name, "
                "CURRENT_USER() AS authenticated_user, "
                "@@server_uuid AS server_uuid, "
                "@@hostname AS server_hostname, "
                "@@port AS server_port"
            )).mappings().one())
            tls_row = connection.execute(text(
                "SHOW SESSION STATUS LIKE 'Ssl_cipher'"
            )).one()
            tls_cipher = str(tls_row[1] or "").strip()
    except Exception:
        raise WindowsLocalHistoryBoundaryError(
            "fixed Windows MySQL 8.4 boundary cannot be verified"
        ) from None

    mysql_version = str(state.get("mysql_version") or "").strip()
    version_comment = str(state.get("version_comment") or "").strip()
    database_name = str(state.get("database_name") or "").strip()
    authenticated_user = str(state.get("authenticated_user") or "").strip()
    server_uuid = str(state.get("server_uuid") or "").strip().casefold()
    server_hostname = str(state.get("server_hostname") or "").strip()
    try:
        server_port = int(state.get("server_port"))
    except (TypeError, ValueError):
        server_port = 0
    local_hostname = socket.gethostname().strip()
    if (
        isolated_acceptance_version(mysql_version)
        != MYSQL_84_ISOLATED_ACCEPTANCE
        or not is_oracle_mysql_distribution(mysql_version, version_comment)
        or database_name != WINDOWS_LOCAL_HISTORY_DATABASE
        or authenticated_user.casefold()
        != WINDOWS_LOCAL_MYSQL_RUNTIME_IDENTITY.casefold()
        or server_uuid != WINDOWS_LOCAL_MYSQL_SERVER_UUID
        or server_hostname.casefold()
        != WINDOWS_LOCAL_MYSQL_HOSTNAME.casefold()
        or server_hostname.casefold() != local_hostname.casefold()
        or server_port != WINDOWS_LOCAL_MYSQL_PORT
        or not tls_cipher
    ):
        raise WindowsLocalHistoryBoundaryError(
            "fixed Windows MySQL 8.4 identity or TLS boundary differs"
        )
    return {
        "ready": True,
        "mysql_version": mysql_version,
        "database": database_name,
        "server_uuid": server_uuid,
        "server_hostname": server_hostname,
        "server_port": server_port,
        "tls": True,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Validate or explicitly add the fail-closed QMT daily provenance "
            "column. Existing rows remain UNVERIFIED_LEGACY."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Apply the one-column ALGORITHM=INSTANT migration. Without this "
            "flag the command is strictly read-only."
        ),
    )
    parser.add_argument(
        "--check-via-primary",
        action="store_true",
        help=(
            "Validate the qualified history table through the primary runtime "
            "connection. This remains SELECT-only and is intended for the "
            "pre-stop production release gate."
        ),
    )
    parser.add_argument(
        "--windows-local-option-file",
        action="store_true",
        help=(
            "Use the fixed protected Windows client option file and verify "
            "the exact local Oracle MySQL 8.4 runtime identity before the "
            "read-only history schema check."
        ),
    )
    args = parser.parse_args(argv)
    if args.apply and args.check_via_primary:
        parser.error("--apply and --check-via-primary are mutually exclusive")
    if args.apply and args.windows_local_option_file:
        parser.error(
            "--apply requires a dedicated privileged database connection; "
            "--windows-local-option-file is the read-only runtime identity"
        )
    if args.windows_local_option_file and args.check_via_primary:
        parser.error(
            "--windows-local-option-file and --check-via-primary are "
            "mutually exclusive"
        )
    history_engine: Engine | None = None
    engine: Engine | None = None
    try:
        if args.windows_local_option_file:
            history_engine = _create_windows_local_history_engine()
            _validate_windows_local_mysql84_boundary(history_engine)
        else:
            load_project_env()
            history_engine = get_local_history_engine()
        engine = (
            create_batch_engine(future=True)
            if args.check_via_primary
            else history_engine
        )
        history_database = str(history_engine.url.database or "")
        try:
            if args.apply:
                result = migrate_local_history_provenance_schema(
                    engine,
                    apply=True,
                    database=history_database,
                )
            else:
                result = validate_local_history_provenance_schema(
                    engine,
                    database=history_database,
                )
                result = {**result, "status": "ok", "applied": False}
        except (
            LocalHistoryProvenanceSchemaError,
            WindowsLocalHistoryBoundaryError,
        ) as exc:
            result = {
                "status": "blocked",
                "reason": str(exc),
                "applied": False,
                "legacy_rows_default_to": "UNVERIFIED_LEGACY",
                "native_qmt_inferred": False,
            }
            print(json.dumps(result, ensure_ascii=True, sort_keys=True))
            return 2
        print(json.dumps(
            {
                **result,
                "native_qmt_inferred": False,
            },
            ensure_ascii=True,
            sort_keys=True,
            default=str,
        ))
        return 0
    except WindowsLocalHistoryBoundaryError as exc:
        result = {
            "status": "blocked",
            "reason": str(exc),
            "applied": False,
            "legacy_rows_default_to": "UNVERIFIED_LEGACY",
            "native_qmt_inferred": False,
        }
        print(json.dumps(result, ensure_ascii=True, sort_keys=True))
        return 2
    finally:
        if engine is not None and engine is not history_engine:
            engine.dispose()
        if history_engine is not None:
            history_engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
