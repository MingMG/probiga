#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Prepare strategy-governance schema through a fenced MySQL 8.4 window.

Production's MySQL server is remote from the Linux application host. The
release broker therefore uses two root-owned TLS option files instead of a
local socket:

* ``probiga_trigger_admin`` owns ``SYSTEM_VARIABLES_ADMIN`` plus read-only
  ``SHOW_ROUTINE`` inventory authority; it may mutate only the trust setting;
* ``probiga_migrator`` owns only ``probiga`` schema privileges and performs all
  schema DDL, preserving the established trigger DEFINER.

Preflight is read-only. Cutover is accepted only after the deploy script has
persistently disabled and drained every writer. The global trust variable is
restored to OFF in every catchable exit path and then verified through a fresh
administrator connection and the independent runtime identity. A SIGKILL or
host loss cannot run Python cleanup, so the deploy script keeps every writer
disabled until this command has returned success; ``recover`` safely forces the
one global variable back OFF before a later retry.
"""
from __future__ import annotations

import argparse
import configparser
import json
import os
import re
import signal
import stat
import sys
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

import pymysql
from pymysql.cursors import DictCursor
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.pool import NullPool

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.mysql_version_policy import (  # noqa: E402
    MYSQL_84_ISOLATED_ACCEPTANCE,
    PRODUCTION_DATABASE_ACTIVATION_ALLOWED,
    is_oracle_mysql_distribution,
    isolated_acceptance_version,
)
from tools.env_config import create_tool_engine, load_project_env  # noqa: E402


DATABASE_NAME = "probiga"
EXPECTED_SERVER_UUID = "f40c3202-9260-11f1-86ae-74d4dd7f8500"
EXPECTED_SERVER_PORT = 3306
EXPECTED_SERVER_HOSTNAME = "WIN-20260322RGF"
EXPECTED_RUNTIME_USER = "probiga_runtime@127.0.0.1"
EXPECTED_MIGRATOR_USER = "probiga_migrator@127.0.0.1"
EXPECTED_ADMIN_USER = "probiga_trigger_admin@127.0.0.1"
ADMIN_OPTION_FILE = Path("/etc/probiga/mysql-trigger-admin.ini")
MIGRATOR_OPTION_FILE = Path("/etc/probiga/mysql-migrator.ini")
FIXED_TLS_CA_FILE = Path("/etc/probiga/mysql84-ca.pem")
WINDOW_LOCK_NAME = "probiga:mysql84:strategy-governance-trigger-window"
EXPECTED_SQL_MODE = (
    "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,"
    "ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
)
EXPECTED_CHARACTER_SET_CLIENT = "utf8mb4"
EXPECTED_COLLATION_CONNECTION = "utf8mb4_general_ci"
EXPECTED_DATABASE_COLLATION = "utf8mb4_unicode_ci"
EXPECTED_ADMIN_GLOBAL_PRIVILEGES = frozenset({
    "SYSTEM_VARIABLES_ADMIN",
    "SHOW_ROUTINE",
})
ADMIN_IO_TIMEOUT_SECONDS = 60
MIGRATOR_IO_TIMEOUT_SECONDS = 900
MIGRATOR_LOCK_WAIT_TIMEOUT_SECONDS = 120
EXPECTED_RUNTIME_SCHEMA_PRIVILEGES = {
    "BIGA.*": frozenset({"SELECT"}),
    "PROBIGA.*": frozenset({
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "ALTER",
        "DROP",
        "INDEX",
        "REFERENCES",
        "CREATE TEMPORARY TABLES",
    }),
    "PROBIGA_QMT_HISTORY.*": frozenset({"SELECT"}),
}
EXPECTED_RUNTIME_SCHEMA_SCOPES = frozenset(
    EXPECTED_RUNTIME_SCHEMA_PRIVILEGES
)
LEGACY_TRIGGER_REHOME_METADATA = {
    "trg_trade_account_v2_real_disabled_bi": (
        "root@localhost",
        "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
    ),
    "trg_trade_account_v2_real_disabled_bu": (
        "root@localhost",
        "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION",
    ),
}
EXPECTED_INITIAL_PENDING_V3 = frozenset({
    "20260804_000_shadow_intelligence_runtime",
    "20260817_000_horizon_protocol_v2_governance",
    "20260817_001_horizon_candidate_ledger_registration",
    "20260822_001_freeze_forward_strategy_version",
    "20260822_002_freeze_v2_fill_cash_ledgers",
    "20260822_003_forward_exit_allocation_ledger",
})

_CREATE_TRIGGER_RE = re.compile(
    r"^\s*CREATE\s+TRIGGER\s+`?([a-z0-9_]+)`?\s+"
    r"(BEFORE|AFTER)\s+(INSERT|UPDATE|DELETE)\s+ON\s+"
    r"`?([a-z0-9_]+)`?\s+FOR\s+EACH\s+ROW\s+(.*)\s*$",
    re.IGNORECASE | re.DOTALL,
)
_DROP_TRIGGER_RE = re.compile(
    r"^\s*DROP\s+TRIGGER\s+IF\s+EXISTS\s+`?([a-z0-9_]+)`?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_SAFE_NAME_RE = re.compile(r"^[a-z0-9_]{1,64}$", re.IGNORECASE)
_OPTION_PASSWORD_RE = re.compile(r"^[A-Za-z0-9_-]{48,160}$")


class PrivilegedSchemaPreparationError(RuntimeError):
    """Fail-closed preparation error; message is never emitted verbatim."""

    def __init__(
        self,
        message: str,
        *,
        safety_evidence: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.safety_evidence = dict(safety_evidence or {})


@dataclass(frozen=True, slots=True)
class OptionCredential:
    path: Path
    host: str
    port: int
    user: str
    password: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class TargetState:
    mysql_version: str
    version_comment: str
    database_name: str | None
    authenticated_user: str
    active_roles: str
    server_uuid: str
    server_port: int
    server_hostname: str
    log_bin: int
    binlog_format: str
    trust_creators: int
    session_sql_mode: str
    character_set_client: str
    collation_connection: str
    database_collation: str
    tls_cipher: str


@dataclass(frozen=True, slots=True)
class TriggerContract:
    name: str
    timing: str
    event: str
    table: str
    body: str
    normalizer: str
    owner: str


@dataclass(slots=True)
class DatabaseBoundary:
    runtime_engine: Engine
    migrator_engine: Engine | None
    admin_credential: OptionCredential
    migrator_credential: OptionCredential | None
    ssl_ca: Path
    runtime_state: TargetState
    admin_state: TargetState
    migrator_state: TargetState | None


@dataclass(frozen=True, slots=True)
class RecoveryBoundary:
    admin_credential: OptionCredential
    ssl_ca: Path


def _require_root_execution() -> None:
    if os.name != "posix" or not hasattr(os, "geteuid") or os.geteuid() != 0:
        raise PrivilegedSchemaPreparationError(
            "schema preparation must run as root through the production broker"
        )


def _protected_option_file(path: Path) -> Path:
    if not path.is_absolute() or not os.path.lexists(path):
        raise PrivilegedSchemaPreparationError(
            "database option file is missing or not absolute"
        )
    if path.is_symlink():
        raise PrivilegedSchemaPreparationError(
            "database option file must not be a symlink"
        )
    try:
        link_state = path.lstat()
        resolved = path.resolve(strict=True)
        file_state = resolved.stat()
        parent_state = resolved.parent.stat()
    except OSError as exc:
        raise PrivilegedSchemaPreparationError(
            "database option file metadata is unavailable"
        ) from exc
    if (
        not stat.S_ISREG(file_state.st_mode)
        or link_state.st_uid != 0
        or file_state.st_uid != 0
        or stat.S_IMODE(link_state.st_mode) != 0o600
        or stat.S_IMODE(file_state.st_mode) != 0o600
        or parent_state.st_uid != 0
        or stat.S_IMODE(parent_state.st_mode) & 0o022
    ):
        raise PrivilegedSchemaPreparationError(
            "database option file ownership or mode is unsafe"
        )
    return resolved


def _read_option_credential(
    path: Path,
    *,
    expected_user: str,
) -> OptionCredential:
    resolved = _protected_option_file(path)
    parser = configparser.RawConfigParser(interpolation=None, strict=True)
    try:
        with resolved.open("r", encoding="utf-8-sig") as stream:
            parser.read_file(stream)
    except (OSError, configparser.Error, UnicodeError) as exc:
        raise PrivilegedSchemaPreparationError(
            "database option file cannot be parsed"
        ) from exc
    expected_keys = {"protocol", "host", "port", "user", "password"}
    if parser.sections() != ["client"] or set(parser.options("client")) != expected_keys:
        raise PrivilegedSchemaPreparationError(
            "database option file has an unexpected shape"
        )
    values = {
        key: parser.get("client", key, raw=True).strip()
        for key in expected_keys
    }
    if (
        values["protocol"].casefold() != "tcp"
        or values["host"] != "127.0.0.1"
        or values["port"] != str(EXPECTED_SERVER_PORT)
        or values["user"] != expected_user
        or _OPTION_PASSWORD_RE.fullmatch(values["password"]) is None
    ):
        raise PrivilegedSchemaPreparationError(
            "database option file target or credential policy differs"
        )
    return OptionCredential(
        path=resolved,
        host=values["host"],
        port=int(values["port"]),
        user=values["user"],
        password=values["password"],
    )


def _fixed_ssl_ca() -> Path:
    candidate = FIXED_TLS_CA_FILE
    if not candidate.is_absolute() or not os.path.lexists(candidate):
        raise PrivilegedSchemaPreparationError(
            "fixed production MySQL CA is missing or not absolute"
        )
    if candidate.is_symlink():
        raise PrivilegedSchemaPreparationError(
            "fixed production MySQL CA must not be a symlink"
        )
    try:
        link_state = candidate.lstat()
        resolved = candidate.resolve(strict=True)
        state = resolved.stat()
        ancestor_states = [parent.stat() for parent in resolved.parents]
    except OSError as exc:
        raise PrivilegedSchemaPreparationError(
            "fixed production MySQL CA metadata is unavailable"
        ) from exc
    if (
        not stat.S_ISREG(link_state.st_mode)
        or not stat.S_ISREG(state.st_mode)
        or link_state.st_uid != 0
        or state.st_uid != 0
        or stat.S_IMODE(link_state.st_mode) & 0o022
        or stat.S_IMODE(state.st_mode) & 0o022
        or any(
            ancestor.st_uid != 0
            or stat.S_IMODE(ancestor.st_mode) & 0o022
            for ancestor in ancestor_states
        )
    ):
        raise PrivilegedSchemaPreparationError(
            "fixed production MySQL CA ownership boundary is unsafe"
        )
    return resolved


def _runtime_ssl_ca() -> Path:
    from server.common.config import get_mysql_tls_runtime_config

    fixed = _fixed_ssl_ca()
    config = get_mysql_tls_runtime_config()
    raw = str(config.get("ssl_ca") or "").strip()
    if not bool(config.get("required")) or not raw:
        raise PrivilegedSchemaPreparationError(
            "production MySQL TLS policy is not mandatory"
        )
    candidate = Path(raw)
    if not candidate.is_absolute():
        raise PrivilegedSchemaPreparationError("production MySQL CA is not absolute")
    try:
        resolved = candidate.resolve(strict=True)
        state = resolved.stat()
    except OSError as exc:
        raise PrivilegedSchemaPreparationError(
            "production MySQL CA is unavailable"
        ) from exc
    if (
        not resolved.is_file()
        or stat.S_IMODE(state.st_mode) & 0o022
        or not resolved.samefile(fixed)
    ):
        raise PrivilegedSchemaPreparationError(
            "production MySQL CA differs from the fixed recovery boundary"
        )
    return fixed


def _connect_option(
    credential: OptionCredential,
    ssl_ca: Path,
    *,
    database: str | None,
    configure_trigger_session: bool,
    autocommit: bool,
    io_timeout_seconds: int = ADMIN_IO_TIMEOUT_SECONDS,
) -> pymysql.Connection:
    if type(io_timeout_seconds) is not int or io_timeout_seconds <= 0:
        raise PrivilegedSchemaPreparationError(
            "database I/O timeout policy is invalid"
        )
    connection = pymysql.connect(
        host=credential.host,
        port=credential.port,
        user=credential.user,
        password=credential.password,
        database=database,
        charset="utf8mb4",
        autocommit=autocommit,
        cursorclass=DictCursor,
        connect_timeout=10,
        read_timeout=io_timeout_seconds,
        write_timeout=io_timeout_seconds,
        local_infile=False,
        ssl_ca=str(ssl_ca),
        ssl_verify_cert=True,
        ssl_verify_identity=False,
        program_name="probiga-strategy-governance-schema-window",
    )
    if configure_trigger_session:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET SESSION sql_mode=%s", (EXPECTED_SQL_MODE,))
                cursor.execute("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
                cursor.execute(
                    "SET SESSION lock_wait_timeout=%s",
                    (MIGRATOR_LOCK_WAIT_TIMEOUT_SECONDS,),
                )
                cursor.execute(
                    "SET SESSION innodb_lock_wait_timeout=%s",
                    (MIGRATOR_LOCK_WAIT_TIMEOUT_SECONDS,),
                )
        except BaseException:
            connection.close()
            raise
    return connection


def _create_migrator_engine(
    credential: OptionCredential,
    ssl_ca: Path,
) -> Engine:
    def creator() -> pymysql.Connection:
        return _connect_option(
            credential,
            ssl_ca,
            database=DATABASE_NAME,
            configure_trigger_session=True,
            autocommit=False,
            io_timeout_seconds=MIGRATOR_IO_TIMEOUT_SECONDS,
        )

    return create_engine(
        "mysql+pymysql://",
        creator=creator,
        poolclass=NullPool,
        future=True,
    )


def _binary_setting(value: object, *, name: str) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int) and value in {0, 1}:
        return value
    normalized = str(value or "").strip().upper()
    if normalized in {"0", "OFF"}:
        return 0
    if normalized in {"1", "ON"}:
        return 1
    raise PrivilegedSchemaPreparationError(f"invalid binary target state: {name}")


_STATE_SQL = (
    "SELECT VERSION() AS mysql_version, "
    "@@version_comment AS version_comment_value, "
    "DATABASE() AS database_name, "
    "CURRENT_USER() AS authenticated_user, "
    "CURRENT_ROLE() AS active_roles, "
    "@@server_uuid AS server_uuid_value, @@port AS server_port, "
    "@@hostname AS server_hostname, "
    "@@GLOBAL.log_bin AS log_bin, @@GLOBAL.binlog_format AS binlog_format, "
    "@@GLOBAL.log_bin_trust_function_creators AS trust_creators, "
    "@@SESSION.sql_mode AS session_sql_mode, "
    "@@SESSION.character_set_client AS character_set_client, "
    "@@SESSION.collation_connection AS collation_connection, "
    "@@collation_database AS database_collation"
)


def _state_from_row(row: Mapping[str, Any], tls_cipher: object) -> TargetState:
    return TargetState(
        mysql_version=str(row.get("mysql_version") or "").strip(),
        version_comment=str(row.get("version_comment_value") or "").strip(),
        database_name=(
            str(row.get("database_name"))
            if row.get("database_name") is not None
            else None
        ),
        authenticated_user=str(row.get("authenticated_user") or "").strip(),
        active_roles=str(row.get("active_roles") or "").strip(),
        server_uuid=str(row.get("server_uuid_value") or "").strip().casefold(),
        server_port=int(row.get("server_port") or 0),
        server_hostname=str(row.get("server_hostname") or "").strip(),
        log_bin=_binary_setting(row.get("log_bin"), name="log_bin"),
        binlog_format=str(row.get("binlog_format") or "").strip().upper(),
        trust_creators=_binary_setting(
            row.get("trust_creators"), name="log_bin_trust_function_creators"
        ),
        session_sql_mode=str(row.get("session_sql_mode") or "").strip(),
        character_set_client=str(row.get("character_set_client") or "").strip(),
        collation_connection=str(row.get("collation_connection") or "").strip(),
        database_collation=str(row.get("database_collation") or "").strip(),
        tls_cipher=str(tls_cipher or "").strip(),
    )


def _read_dbapi_state(connection: pymysql.Connection) -> TargetState:
    with connection.cursor() as cursor:
        cursor.execute(_STATE_SQL)
        row = cursor.fetchone()
        cursor.execute("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
        ssl_row = cursor.fetchone()
    if not isinstance(row, Mapping):
        raise PrivilegedSchemaPreparationError("target identity query was incomplete")
    cipher = ""
    if isinstance(ssl_row, Mapping):
        cipher = ssl_row.get("Value") or ssl_row.get("VALUE") or ""
    return _state_from_row(row, cipher)


def _read_sa_state(connection: Connection) -> TargetState:
    row = connection.execute(text(_STATE_SQL)).mappings().one()
    ssl_row = connection.execute(
        text("SHOW SESSION STATUS LIKE 'Ssl_cipher'")
    ).mappings().first()
    cipher = "" if ssl_row is None else (
        ssl_row.get("Value") or ssl_row.get("VALUE") or ""
    )
    return _state_from_row(row, cipher)


def _validate_target_state(
    state: TargetState,
    *,
    expected_user: str,
    require_database: bool,
    expected_trust: int | None,
    require_trigger_session: bool,
) -> None:
    if PRODUCTION_DATABASE_ACTIVATION_ALLOWED is not False:
        raise PrivilegedSchemaPreparationError(
            "schema maintenance must not authorize production trading activation"
        )
    if (
        isolated_acceptance_version(state.mysql_version)
        != MYSQL_84_ISOLATED_ACCEPTANCE
        or not is_oracle_mysql_distribution(
            state.mysql_version, state.version_comment
        )
        or state.server_uuid != EXPECTED_SERVER_UUID
        or state.server_port != EXPECTED_SERVER_PORT
        or state.server_hostname != EXPECTED_SERVER_HOSTNAME
        or state.authenticated_user != expected_user
        or state.active_roles.upper() != "NONE"
        or state.log_bin != 1
        or state.binlog_format != "ROW"
        or not state.tls_cipher
        or (expected_trust is not None and state.trust_creators != expected_trust)
    ):
        raise PrivilegedSchemaPreparationError(
            "database target, identity, TLS, or binary-log state differs"
        )
    if require_database and state.database_name != DATABASE_NAME:
        raise PrivilegedSchemaPreparationError("database schema target differs")
    if require_trigger_session and (
        state.session_sql_mode != EXPECTED_SQL_MODE
        or state.character_set_client != EXPECTED_CHARACTER_SET_CLIENT
        or state.collation_connection != EXPECTED_COLLATION_CONNECTION
        or state.database_collation != EXPECTED_DATABASE_COLLATION
    ):
        raise PrivilegedSchemaPreparationError(
            "trigger creation session metadata differs"
        )


def _dbapi_grants(connection: pymysql.Connection) -> tuple[str, ...]:
    with connection.cursor() as cursor:
        cursor.execute("SHOW GRANTS FOR CURRENT_USER()")
        rows = cursor.fetchall()
    grants: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping) or len(row) != 1:
            raise PrivilegedSchemaPreparationError("database grants were malformed")
        grants.append(str(next(iter(row.values())) or ""))
    return tuple(grants)


def _dbapi_trigger_exists(
    connection: pymysql.Connection,
    trigger_name: str,
) -> bool:
    if _SAFE_NAME_RE.fullmatch(trigger_name) is None:
        raise PrivilegedSchemaPreparationError("trigger identifier is unsafe")
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) AS trigger_count "
            "FROM information_schema.TRIGGERS "
            "WHERE TRIGGER_SCHEMA=%s AND BINARY TRIGGER_NAME=BINARY %s",
            (DATABASE_NAME, trigger_name),
        )
        row = cursor.fetchone()
    if not isinstance(row, Mapping):
        raise PrivilegedSchemaPreparationError(
            "trigger absence proof was incomplete"
        )
    return int(row.get("trigger_count") or 0) != 0


def _sa_grants(connection: Connection) -> tuple[str, ...]:
    return tuple(
        str(row[0] or "")
        for row in connection.execute(text("SHOW GRANTS FOR CURRENT_USER()"))
    )


def _normalized_grants(grants: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(" ".join(str(item).upper().split()) for item in grants)
    if not normalized or any(not item.startswith("GRANT ") for item in normalized):
        raise PrivilegedSchemaPreparationError("database grants were incomplete")
    if any(item.startswith("GRANT ") and " ON " not in item for item in normalized):
        raise PrivilegedSchemaPreparationError("assignable database roles are forbidden")
    if any(
        " WITH GRANT OPTION" in item or item.startswith("GRANT PROXY ")
        for item in normalized
    ):
        raise PrivilegedSchemaPreparationError("grant delegation is forbidden")
    if not any(" REQUIRE SSL" in item for item in normalized):
        raise PrivilegedSchemaPreparationError("database account does not require TLS")
    return normalized


def _global_privileges(grants: Iterable[str]) -> set[str]:
    privileges: set[str] = set()
    for grant in grants:
        match = re.match(r"^GRANT (.+?) ON \*\.\* TO ", grant)
        if match is None:
            continue
        privileges.update(item.strip() for item in match.group(1).split(","))
    return privileges


def _grant_scope_entries(
    grants: Iterable[str],
) -> tuple[tuple[set[str], str], ...]:
    entries: list[tuple[set[str], str]] = []
    for grant in grants:
        match = re.match(r"^GRANT (.+?) ON (.+?) TO ", grant)
        if match is None:
            raise PrivilegedSchemaPreparationError("database grant syntax differs")
        privileges = {
            item.strip() for item in match.group(1).split(",") if item.strip()
        }
        scope = match.group(2).replace("`", "").upper()
        if not privileges or not scope:
            raise PrivilegedSchemaPreparationError("database grant syntax differs")
        entries.append((privileges, scope))
    return tuple(entries)


def _validate_admin_grants(grants: Iterable[str]) -> None:
    normalized = _normalized_grants(grants)
    entries = _grant_scope_entries(normalized)
    global_privileges = set().union(
        *(privileges for privileges, scope in entries if scope == "*.*")
    )
    if (
        any(scope != "*.*" for _privileges, scope in entries)
        or global_privileges - {"USAGE"}
        != set(EXPECTED_ADMIN_GLOBAL_PRIVILEGES)
        or global_privileges - set(EXPECTED_ADMIN_GLOBAL_PRIVILEGES) not in (
            set(),
            {"USAGE"},
        )
    ):
        raise PrivilegedSchemaPreparationError(
            "trigger administrator global privileges are not exact"
        )


def _validate_migrator_grants(grants: Iterable[str]) -> None:
    normalized = _normalized_grants(grants)
    entries = _grant_scope_entries(normalized)
    global_entries = tuple(
        privileges for privileges, scope in entries if scope == "*.*"
    )
    schema_entries = tuple(
        privileges for privileges, scope in entries if scope == "PROBIGA.*"
    )
    if (
        any(scope not in {"*.*", "PROBIGA.*"} for _privileges, scope in entries)
        or len(global_entries) != 1
        or global_entries[0] != {"USAGE"}
        or len(schema_entries) != 1
        or schema_entries[0] != {"ALL PRIVILEGES"}
    ):
        raise PrivilegedSchemaPreparationError(
            "migration account privileges are not exact"
        )


def _validate_runtime_grants(grants: Iterable[str]) -> None:
    normalized = _normalized_grants(grants)
    entries = _grant_scope_entries(normalized)
    global_entries = tuple(
        privileges for privileges, scope in entries if scope == "*.*"
    )
    schema_entries = {
        scope: privileges
        for privileges, scope in entries
        if scope in EXPECTED_RUNTIME_SCHEMA_SCOPES
    }
    schema_entry_count = sum(
        1 for _privileges, scope in entries
        if scope in EXPECTED_RUNTIME_SCHEMA_SCOPES
    )
    if (
        any(
            scope not in {"*.*", *EXPECTED_RUNTIME_SCHEMA_SCOPES}
            for _privileges, scope in entries
        )
        or len(global_entries) != 1
        or global_entries[0] != {"USAGE"}
        or set(schema_entries) != set(EXPECTED_RUNTIME_SCHEMA_SCOPES)
        or schema_entry_count != len(EXPECTED_RUNTIME_SCHEMA_SCOPES)
        or any(
            privileges
            != set(EXPECTED_RUNTIME_SCHEMA_PRIVILEGES[scope])
            for scope, privileges in schema_entries.items()
        )
    ):
        raise PrivilegedSchemaPreparationError(
            "runtime identity privileges differ from the audited boundary"
        )


def _runtime_grant_summary(grants: Iterable[str]) -> dict[str, Any]:
    normalized = _normalized_grants(grants)
    _validate_runtime_grants(normalized)
    entries = _grant_scope_entries(normalized)
    return {
        "global_privileges": sorted(set().union(*(
            privileges for privileges, scope in entries if scope == "*.*"
        ))),
        "schema_privileges": {
            scope: sorted(privileges)
            for privileges, scope in sorted(entries, key=lambda item: item[1])
            if scope != "*.*"
        },
        "require_ssl": True,
        "roles": [],
        "grant_option": False,
    }


def _definer_routine_inventory_sql(*, self_only: bool) -> str:
    self_clause = "AND BINARY DEFINER=BINARY CURRENT_USER() " if self_only else ""
    return "".join((
        "SELECT ROUTINE_SCHEMA AS routine_schema, "
        "ROUTINE_NAME AS routine_name, ROUTINE_TYPE AS routine_type, "
        "DEFINER AS definer "
        "FROM information_schema.ROUTINES "
        "WHERE UPPER(ROUTINE_SCHEMA) IN "
        "('BIGA', 'PROBIGA', 'PROBIGA_QMT_HISTORY') "
        "AND UPPER(SECURITY_TYPE)='DEFINER' ",
        self_clause,
        "ORDER BY BINARY ROUTINE_SCHEMA, BINARY ROUTINE_NAME, ROUTINE_TYPE"
    ))


def _validate_no_runtime_definer_routines(
    connection: Connection,
) -> dict[str, Any]:
    """Full inventory helper; callers must supply an audit-visible identity."""

    rows = connection.execute(text(
        _definer_routine_inventory_sql(self_only=False)
    )).mappings().all()
    if rows:
        raise PrivilegedSchemaPreparationError(
            "runtime schema contains SQL SECURITY DEFINER routines"
        )
    return {
        "runtime_definer_routine_count": 0,
        "runtime_definer_routine_inventory_verified": True,
    }


def _validate_no_self_definer_routines(
    connection: Connection,
    *,
    identity: str,
) -> dict[str, Any]:
    rows = connection.execute(text(
        _definer_routine_inventory_sql(self_only=True)
    )).mappings().all()
    if rows:
        raise PrivilegedSchemaPreparationError(
            f"{identity} identity owns SQL SECURITY DEFINER routines"
        )
    return {f"{identity}_self_definer_routine_count": 0}


def _validate_complete_routine_inventory_dbapi(
    connection: pymysql.Connection,
) -> dict[str, Any]:
    with connection.cursor() as cursor:
        cursor.execute(_definer_routine_inventory_sql(self_only=False))
        rows = list(cursor.fetchall())
    if rows:
        raise PrivilegedSchemaPreparationError(
            "runtime schema contains SQL SECURITY DEFINER routines"
        )
    return {
        "runtime_definer_routine_count": 0,
        "runtime_definer_routine_inventory_verified": True,
        "runtime_definer_routine_inventory_complete": True,
        "runtime_definer_routine_inventory_authority": EXPECTED_ADMIN_USER,
        "runtime_definer_routine_inventory_schemas": [
            "biga", "probiga", "probiga_qmt_history",
        ],
    }


def _runtime_least_privilege_evidence(
    boundary: DatabaseBoundary,
) -> dict[str, Any]:
    if boundary.migrator_engine is None:
        raise PrivilegedSchemaPreparationError("migration engine is unavailable")
    boundary.runtime_engine.dispose()
    with boundary.runtime_engine.connect() as runtime_connection:
        runtime_state = _read_sa_state(runtime_connection)
        _validate_target_state(
            runtime_state,
            expected_user=EXPECTED_RUNTIME_USER,
            require_database=True,
            expected_trust=0,
            require_trigger_session=True,
        )
        grant_summary = _runtime_grant_summary(
            _sa_grants(runtime_connection)
        )
        runtime_self = _validate_no_self_definer_routines(
            runtime_connection,
            identity="runtime",
        )
    with boundary.migrator_engine.connect() as migrator_connection:
        migrator_self = _validate_no_self_definer_routines(
            migrator_connection,
            identity="migrator",
        )
    admin = _connect_admin(boundary)
    try:
        admin_state = _read_dbapi_state(admin)
        _validate_target_state(
            admin_state,
            expected_user=EXPECTED_ADMIN_USER,
            require_database=False,
            expected_trust=0,
            require_trigger_session=False,
        )
        _validate_admin_grants(_dbapi_grants(admin))
        routine_detail = _validate_complete_routine_inventory_dbapi(admin)
    finally:
        _close_quietly(admin)
    return {
        "runtime_least_privilege_verified": True,
        "runtime_grant_summary": grant_summary,
        **runtime_self,
        **migrator_self,
        **routine_detail,
    }


def _open_boundary(
    *,
    include_migrator: bool,
    expected_trust: int | None,
) -> DatabaseBoundary:
    _require_root_execution()
    admin_credential = _read_option_credential(
        ADMIN_OPTION_FILE,
        expected_user=EXPECTED_ADMIN_USER.split("@", 1)[0],
    )
    migrator_credential = None
    if include_migrator:
        migrator_credential = _read_option_credential(
            MIGRATOR_OPTION_FILE,
            expected_user=EXPECTED_MIGRATOR_USER.split("@", 1)[0],
        )
        if migrator_credential.path.samefile(admin_credential.path):
            raise PrivilegedSchemaPreparationError(
                "administrator and migration option files overlap"
            )
    ssl_ca = _runtime_ssl_ca()
    if ssl_ca in {
        admin_credential.path,
        migrator_credential.path if migrator_credential else Path("/"),
    }:
        raise PrivilegedSchemaPreparationError(
            "database credential file aliases the TLS CA"
        )
    runtime_engine = create_tool_engine(future=True, poolclass=NullPool)
    migrator_engine = (
        _create_migrator_engine(migrator_credential, ssl_ca)
        if migrator_credential is not None
        else None
    )
    admin: pymysql.Connection | None = None
    try:
        with runtime_engine.connect() as connection:
            runtime_state = _read_sa_state(connection)
            _validate_target_state(
                runtime_state,
                expected_user=EXPECTED_RUNTIME_USER,
                require_database=True,
                expected_trust=expected_trust,
                require_trigger_session=True,
            )
            _validate_runtime_grants(_sa_grants(connection))
        admin = _connect_option(
            admin_credential,
            ssl_ca,
            database=None,
            configure_trigger_session=False,
            autocommit=True,
        )
        admin_state = _read_dbapi_state(admin)
        _validate_target_state(
            admin_state,
            expected_user=EXPECTED_ADMIN_USER,
            require_database=False,
            expected_trust=expected_trust,
            require_trigger_session=False,
        )
        _validate_admin_grants(_dbapi_grants(admin))
        migrator_state = None
        if migrator_engine is not None:
            with migrator_engine.connect() as connection:
                migrator_state = _read_sa_state(connection)
                _validate_target_state(
                    migrator_state,
                    expected_user=EXPECTED_MIGRATOR_USER,
                    require_database=True,
                    expected_trust=expected_trust,
                    require_trigger_session=True,
                )
                _validate_migrator_grants(_sa_grants(connection))
        identities = {
            runtime_state.authenticated_user,
            admin_state.authenticated_user,
            migrator_state.authenticated_user if migrator_state else "",
        }
        expected_identity_count = 3 if migrator_state else 2
        if len(identities - {""}) != expected_identity_count:
            raise PrivilegedSchemaPreparationError(
                "database duty-separation identities overlap"
            )
        return DatabaseBoundary(
            runtime_engine=runtime_engine,
            migrator_engine=migrator_engine,
            admin_credential=admin_credential,
            migrator_credential=migrator_credential,
            ssl_ca=ssl_ca,
            runtime_state=runtime_state,
            admin_state=admin_state,
            migrator_state=migrator_state,
        )
    except BaseException:
        runtime_engine.dispose()
        if migrator_engine is not None:
            migrator_engine.dispose()
        raise
    finally:
        if admin is not None:
            admin.close()


def _open_recovery_boundary() -> RecoveryBoundary:
    """Open only immutable credentials; emergency OFF must be admin-first."""

    _require_root_execution()
    admin_credential = _read_option_credential(
        ADMIN_OPTION_FILE,
        expected_user=EXPECTED_ADMIN_USER.split("@", 1)[0],
    )
    ssl_ca = _fixed_ssl_ca()
    if ssl_ca == admin_credential.path:
        raise PrivilegedSchemaPreparationError(
            "database credential file aliases the TLS CA"
        )
    return RecoveryBoundary(
        admin_credential=admin_credential,
        ssl_ca=ssl_ca,
    )


def _parse_create_trigger(
    statement: str,
    *,
    normalizer: str,
    owner: str,
) -> TriggerContract:
    match = _CREATE_TRIGGER_RE.match(str(statement or "").strip())
    if match is None:
        raise PrivilegedSchemaPreparationError(
            "release trigger statement cannot be parsed"
        )
    name, timing, event, table_name, body = match.groups()
    if any(
        _SAFE_NAME_RE.fullmatch(item) is None for item in (name, table_name)
    ):
        raise PrivilegedSchemaPreparationError("release trigger identifier is unsafe")
    return TriggerContract(
        name=name,
        timing=timing.upper(),
        event=event.upper(),
        table=table_name,
        body=body,
        normalizer=normalizer,
        owner=owner,
    )


def _v3_trigger_states(
    migration_results: Iterable[Any],
) -> tuple[dict[str, TriggerContract], dict[str, TriggerContract]]:
    from server.db.migrations_v3 import MIGRATIONS

    results = tuple(migration_results)
    if len(results) != len(MIGRATIONS):
        raise PrivilegedSchemaPreparationError("V3 migration plan is incomplete")
    applied: dict[str, TriggerContract] = {}
    final: dict[str, TriggerContract] = {}
    for migration, result in zip(MIGRATIONS, results):
        version = str(migration["version"])
        if (
            str(result.version) != version
            or result.status not in {"exists", "would_apply", "applied"}
        ):
            raise PrivilegedSchemaPreparationError("V3 migration plan ordering differs")
        apply_now = result.status in {"exists", "applied"}
        for raw in tuple(migration["statements"]):
            statement = str(raw).strip()
            drop = _DROP_TRIGGER_RE.match(statement)
            create = _CREATE_TRIGGER_RE.match(statement)
            if drop is not None:
                final.pop(drop.group(1), None)
                if apply_now:
                    applied.pop(drop.group(1), None)
            elif create is not None:
                contract = _parse_create_trigger(
                    statement,
                    normalizer="v3",
                    owner=version,
                )
                final[contract.name] = contract
                if apply_now:
                    applied[contract.name] = contract
    return applied, final


def _all_v3_trigger_contracts() -> tuple[TriggerContract, ...]:
    """Return every frozen CREATE, including intermediate replacements."""

    from server.db.migrations_v3 import MIGRATIONS

    contracts: list[TriggerContract] = []
    for migration in MIGRATIONS:
        version = str(migration["version"])
        for raw in tuple(migration["statements"]):
            statement = str(raw).strip()
            if _CREATE_TRIGGER_RE.match(statement) is None:
                continue
            contracts.append(_parse_create_trigger(
                statement,
                normalizer="v3",
                owner=version,
            ))
    return tuple(contracts)


def _final_v3_trigger_contracts() -> dict[str, TriggerContract]:
    from server.db.migrations_v3 import MIGRATIONS

    final: dict[str, TriggerContract] = {}
    for migration in MIGRATIONS:
        version = str(migration["version"])
        for raw in tuple(migration["statements"]):
            statement = str(raw).strip()
            drop = _DROP_TRIGGER_RE.match(statement)
            if drop is not None:
                final.pop(drop.group(1), None)
            elif _CREATE_TRIGGER_RE.match(statement) is not None:
                contract = _parse_create_trigger(
                    statement,
                    normalizer="v3",
                    owner=version,
                )
                final[contract.name] = contract
    return final


def _non_v3_trigger_contracts() -> dict[str, TriggerContract]:
    from server.engine.strategy_governance import (
        GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS,
        METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS,
    )
    from tools.attest_qmt_daily_kline import ATTESTATION_TRIGGER_STATEMENTS

    contracts: dict[str, TriggerContract] = {}
    for statement in ATTESTATION_TRIGGER_STATEMENTS.values():
        contract = _parse_create_trigger(
            statement,
            normalizer="qmt",
            owner="qmt_attestation",
        )
        contracts[contract.name] = contract
    for statement in GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS.values():
        contract = _parse_create_trigger(
            statement,
            normalizer="governance",
            owner="strategy_governance",
        )
        if contract.name in contracts:
            raise PrivilegedSchemaPreparationError("duplicate release trigger name")
        contracts[contract.name] = contract
    for trigger_name, raw in METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS.items():
        statement = (
            f"CREATE TRIGGER {trigger_name} {raw['timing']} {raw['event']} "
            f"ON {raw['table']} FOR EACH ROW {raw['body']}"
        )
        contract = _parse_create_trigger(
            statement,
            normalizer="metric",
            owner="strategy_governance",
        )
        if contract.name in contracts:
            raise PrivilegedSchemaPreparationError("duplicate release trigger name")
        contracts[contract.name] = contract
    return contracts


def _normalized_trigger_body(contract: TriggerContract, value: object) -> str:
    if contract.normalizer == "v3":
        from server.db.migrations_v3 import _normalized_trigger_sql

        return _normalized_trigger_sql(value)
    if contract.normalizer == "qmt":
        from tools.attest_qmt_daily_kline import _normalized_trigger_body

        return _normalized_trigger_body(value)
    if contract.normalizer == "metric":
        from server.engine.strategy_governance import (
            _normalized_metric_input_trigger_body,
        )

        return _normalized_metric_input_trigger_body(value)
    from server.engine.strategy_governance import _normalized_governance_trigger_body

    return _normalized_governance_trigger_body(value)


def _trigger_inventory(
    connection: Connection,
    *,
    names: Iterable[str],
    controlled_tables: Iterable[str],
) -> dict[str, dict[str, Any]]:
    safe_names = sorted(set(names))
    safe_tables = sorted(set(controlled_tables))
    if any(
        _SAFE_NAME_RE.fullmatch(item) is None
        for item in (*safe_names, *safe_tables)
    ):
        raise PrivilegedSchemaPreparationError("trigger inventory identifier is unsafe")
    clauses: list[str] = []
    params: dict[str, str] = {}
    if safe_names:
        placeholders = []
        for index, name in enumerate(safe_names):
            key = f"trigger_name_{index}"
            placeholders.append(f":{key}")
            params[key] = name
        clauses.append("TRIGGER_NAME IN (" + ", ".join(placeholders) + ")")
    if safe_tables:
        placeholders = []
        for index, table_name in enumerate(safe_tables):
            key = f"trigger_table_{index}"
            placeholders.append(f":{key}")
            params[key] = table_name
        clauses.append("EVENT_OBJECT_TABLE IN (" + ", ".join(placeholders) + ")")
    if not clauses:
        return {}
    rows = connection.execute(
        text(
            "SELECT TRIGGER_NAME AS trigger_name, DEFINER AS definer, "
            "ACTION_TIMING AS action_timing, "
            "EVENT_MANIPULATION AS event_manipulation, "
            "EVENT_OBJECT_TABLE AS event_object_table, "
            "ACTION_ORIENTATION AS action_orientation, "
            "ACTION_STATEMENT AS action_statement, SQL_MODE AS sql_mode, "
            "CHARACTER_SET_CLIENT AS character_set_client, "
            "COLLATION_CONNECTION AS collation_connection, "
            "DATABASE_COLLATION AS database_collation "
            "FROM information_schema.TRIGGERS "
            "WHERE TRIGGER_SCHEMA=DATABASE() AND ("
            + " OR ".join(clauses)
            + ") ORDER BY BINARY TRIGGER_NAME"
        ),
        params,
    ).mappings().all()
    observed: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("trigger_name") or "")
        if not name or name in observed:
            raise PrivilegedSchemaPreparationError("trigger inventory is malformed")
        observed[name] = dict(row)
    return observed


def validate_release_trigger_contracts(
    connection: Connection,
    *,
    required: Mapping[str, TriggerContract],
    optional: Mapping[str, TriggerContract],
    forbidden_names: Iterable[str] = (),
    allow_legacy_rehome: bool = False,
    controlled_contracts: Mapping[str, TriggerContract] | None = None,
) -> dict[str, Any]:
    expected = dict(required)
    for name, contract in optional.items():
        if name in expected and expected[name] != contract:
            raise PrivilegedSchemaPreparationError("release trigger contracts overlap")
        expected[name] = contract
    forbidden = set(forbidden_names)
    inventory_contracts = dict(expected)
    for name, contract in (controlled_contracts or {}).items():
        existing = inventory_contracts.get(name)
        if existing is not None and existing != contract:
            raise PrivilegedSchemaPreparationError(
                "controlled release trigger contracts overlap"
            )
        inventory_contracts[name] = contract
    controlled_tables = {
        contract.table
        for contract in inventory_contracts.values()
    }
    observed = _trigger_inventory(
        connection,
        names=set(expected) | forbidden,
        controlled_tables=controlled_tables,
    )
    missing = set(required) - set(observed)
    unexpected = set(observed) - set(expected)
    if missing or unexpected or (set(observed) & forbidden):
        raise PrivilegedSchemaPreparationError(
            "release trigger inventory is incomplete or unexpected"
        )
    legacy_rehome: list[str] = []
    for name, row in observed.items():
        contract = expected[name]
        observed_definer = str(row.get("definer") or "")
        observed_sql_mode = str(row.get("sql_mode") or "")
        legacy_metadata = LEGACY_TRIGGER_REHOME_METADATA.get(name)
        legacy_match = bool(
            allow_legacy_rehome
            and legacy_metadata is not None
            and (observed_definer, observed_sql_mode) == legacy_metadata
        )
        if legacy_match:
            legacy_rehome.append(name)
        if (
            (
                not legacy_match
                and observed_definer != EXPECTED_MIGRATOR_USER
            )
            or str(row.get("action_timing") or "").upper() != contract.timing
            or str(row.get("event_manipulation") or "").upper() != contract.event
            or str(row.get("event_object_table") or "") != contract.table
            or str(row.get("action_orientation") or "").upper() != "ROW"
            or _normalized_trigger_body(contract, row.get("action_statement"))
            != _normalized_trigger_body(contract, contract.body)
            or (not legacy_match and observed_sql_mode != EXPECTED_SQL_MODE)
            or str(row.get("character_set_client") or "")
            != EXPECTED_CHARACTER_SET_CLIENT
            or str(row.get("collation_connection") or "")
            != EXPECTED_COLLATION_CONNECTION
            or str(row.get("database_collation") or "")
            != EXPECTED_DATABASE_COLLATION
        ):
            raise PrivilegedSchemaPreparationError(
                "release trigger physical metadata differs"
            )
    return {
        "required_count": len(required),
        "optional_count": len(optional),
        "observed_count": len(observed),
        "definer": EXPECTED_MIGRATOR_USER,
        "metadata_frozen": True,
        "legacy_rehome_names": sorted(legacy_rehome),
    }


def _rehome_legacy_triggers(
    engine: Engine,
    contracts: Mapping[str, TriggerContract],
    *,
    trigger_ddl_executor,
) -> tuple[str, ...]:
    selected = {
        name: contracts[name]
        for name in sorted(LEGACY_TRIGGER_REHOME_METADATA)
        if name in contracts
    }
    if len(selected) != len(LEGACY_TRIGGER_REHOME_METADATA):
        raise PrivilegedSchemaPreparationError(
            "legacy trigger rehome contract is missing"
        )
    with engine.connect() as connection:
        observed = _trigger_inventory(
            connection,
            names=selected,
            controlled_tables=(),
        )
        if observed:
            validate_release_trigger_contracts(
                connection,
                required={name: selected[name] for name in observed},
                optional={},
                allow_legacy_rehome=True,
            )

    changed = [
        name
        for name in selected
        if observed.get(name) is None
        or (
            str(observed[name].get("definer") or "")
            != EXPECTED_MIGRATOR_USER
            or str(observed[name].get("sql_mode") or "")
            != EXPECTED_SQL_MODE
        )
    ]
    if changed and not callable(trigger_ddl_executor):
        raise PrivilegedSchemaPreparationError(
            "legacy trigger rehome requires the explicit trigger DDL executor"
        )
    for name in changed:
        contract = selected[name]
        if observed.get(name) is not None:
            # DROP TRIGGER is permitted while global trust remains OFF.  It is
            # committed before the narrowly brokered replacement CREATE.
            with engine.begin() as connection:
                connection.execute(text(f"DROP TRIGGER IF EXISTS `{name}`"))
        trigger_ddl_executor(
            f"CREATE TRIGGER `{name}` {contract.timing} {contract.event} "
            f"ON `{contract.table}` FOR EACH ROW {contract.body}"
        )
    return tuple(changed)


def _repair_interrupted_legacy_rehome(
    engine: Engine,
    contracts: Mapping[str, TriggerContract],
    *,
    trigger_ddl_executor: Callable[[str], None],
) -> dict[str, Any]:
    """Repair only a fully absent legacy guard during fenced resume."""

    selected = {
        name: contracts[name]
        for name in sorted(LEGACY_TRIGGER_REHOME_METADATA)
        if name in contracts
    }
    if len(selected) != len(LEGACY_TRIGGER_REHOME_METADATA):
        raise PrivilegedSchemaPreparationError(
            "legacy trigger resume contract is incomplete"
        )
    with engine.connect() as connection:
        observed = _trigger_inventory(
            connection,
            names=selected,
            controlled_tables={contract.table for contract in selected.values()},
        )
        unexpected = set(observed) - set(selected)
        if unexpected:
            raise PrivilegedSchemaPreparationError(
                "release trigger inventory is incomplete or unexpected"
            )
        if observed:
            # Existing rows may be either the one exact audited root metadata
            # pair or the final migrator metadata.  Any body/timing/renamed
            # trigger on either controlled table remains a hard failure.
            validate_release_trigger_contracts(
                connection,
                required={name: selected[name] for name in observed},
                optional={
                    name: selected[name]
                    for name in selected if name not in observed
                },
                allow_legacy_rehome=True,
                controlled_contracts=selected,
            )
    candidates = sorted(set(selected) - set(observed))
    repaired: list[str] = []
    for name in candidates:
        contract = selected[name]
        trigger_ddl_executor(
            f"CREATE TRIGGER `{name}` {contract.timing} {contract.event} "
            f"ON `{contract.table}` FOR EACH ROW {contract.body}"
        )
        repaired.append(name)
    if repaired:
        with engine.connect() as connection:
            validate_release_trigger_contracts(
                connection,
                required=selected,
                optional={},
                allow_legacy_rehome=True,
                controlled_contracts=selected,
            )
    return {
        "candidate_names": candidates,
        "repaired_names": repaired,
        "post_validation_verified": True,
    }


def _table_inventory(connection: Connection, names: Iterable[str]) -> set[str]:
    expected = sorted(set(names))
    placeholders = []
    params: dict[str, str] = {}
    for index, name in enumerate(expected):
        if _SAFE_NAME_RE.fullmatch(name) is None:
            raise PrivilegedSchemaPreparationError("table inventory identifier is unsafe")
        key = f"table_name_{index}"
        placeholders.append(f":{key}")
        params[key] = name
    if not placeholders:
        return set()
    return {
        str(row[0])
        for row in connection.execute(
            text(
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ("
                + ", ".join(placeholders)
                + ")"
            ),
            params,
        )
    }


def _preflight_schema(boundary: DatabaseBoundary) -> dict[str, Any]:
    if boundary.migrator_engine is None:
        raise PrivilegedSchemaPreparationError("migration engine is unavailable")
    from server.db.migrations_v3 import run_v3_migrations
    from server.engine.strategy_governance import GOVERNANCE_TABLE_NAMES
    from tools.attest_qmt_daily_kline import (
        ATTESTATION_TABLE_NAMES,
        validate_attestation_schema,
    )
    from tools.prepare_strategy_governance_qmt_history import (
        plan_legacy_completed_run_binding,
    )

    runtime_security = _runtime_least_privilege_evidence(boundary)
    plan = run_v3_migrations(boundary.migrator_engine, dry_run=True)
    pending_versions = {
        str(item.version) for item in plan if item.status == "would_apply"
    }
    if not pending_versions <= EXPECTED_INITIAL_PENDING_V3:
        raise PrivilegedSchemaPreparationError(
            "V3 migration ledger differs from the audited production boundary"
        )
    applied_v3, final_v3 = _v3_trigger_states(plan)
    non_v3 = _non_v3_trigger_contracts()
    legacy_binding_plan = {
        "legacy_run_count": 0,
        "legacy_binding_plan_hash": "",
        "legacy_binding_marker_present": False,
        "legacy_binding_pending": False,
    }
    with boundary.migrator_engine.connect() as connection:
        qmt_tables = _table_inventory(connection, ATTESTATION_TABLE_NAMES)
        if qmt_tables and qmt_tables != set(ATTESTATION_TABLE_NAMES):
            raise PrivilegedSchemaPreparationError(
                "QMT attestation table inventory is partial"
            )
        if qmt_tables:
            qmt_detail = validate_attestation_schema(
                connection,
                require_triggers=False,
                require_current_manifests=False,
            )
            legacy_binding_plan = plan_legacy_completed_run_binding(
                connection
            )
            if (
                int(qmt_detail.get("legacy_ineligible_run_count") or 0)
                != int(legacy_binding_plan["legacy_run_count"])
                or str(qmt_detail.get("legacy_binding_plan_hash") or "")
                != str(legacy_binding_plan["legacy_binding_plan_hash"])
            ):
                raise PrivilegedSchemaPreparationError(
                    "QMT legacy binding validators disagree"
                )
        governance_tables = _table_inventory(connection, GOVERNANCE_TABLE_NAMES)
        if governance_tables and governance_tables != set(GOVERNANCE_TABLE_NAMES):
            raise PrivilegedSchemaPreparationError(
                "strategy governance table inventory is partial"
            )
        trigger_detail = validate_release_trigger_contracts(
            connection,
            required=applied_v3,
            optional=non_v3,
            forbidden_names=set(final_v3) - set(applied_v3),
            allow_legacy_rehome=True,
            controlled_contracts={**final_v3, **non_v3},
        )
    return {
        **runtime_security,
        "v3_migrations": [
            {
                "version": item.version,
                "status": item.status,
                "statement_count": item.statement_count,
            }
            for item in plan
        ],
        "pending_v3_versions": sorted(pending_versions),
        "qmt_table_count": len(qmt_tables),
        "governance_table_count": len(governance_tables),
        "legacy_binding_plan": {
            key: value for key, value in legacy_binding_plan.items()
            if key != "legacy_bindings"
        },
        "trigger_contract": trigger_detail,
    }


def _connect_admin(
    boundary: DatabaseBoundary | RecoveryBoundary,
) -> pymysql.Connection:
    return _connect_option(
        boundary.admin_credential,
        boundary.ssl_ca,
        database=None,
        configure_trigger_session=False,
        autocommit=True,
    )


def _connect_migrator(boundary: DatabaseBoundary) -> pymysql.Connection:
    credential = boundary.migrator_credential
    if credential is None:
        raise PrivilegedSchemaPreparationError(
            "migration credential is unavailable"
        )
    return _connect_option(
        credential,
        boundary.ssl_ca,
        database=DATABASE_NAME,
        configure_trigger_session=True,
        autocommit=True,
        io_timeout_seconds=MIGRATOR_IO_TIMEOUT_SECONDS,
    )


def _set_trust(connection: pymysql.Connection, *, enabled: bool) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            "SET GLOBAL log_bin_trust_function_creators = ON"
            if enabled
            else "SET GLOBAL log_bin_trust_function_creators = OFF"
        )


def _acquire_lock(connection: pymysql.Connection) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (WINDOW_LOCK_NAME,))
        row = cursor.fetchone()
    return bool(
        isinstance(row, Mapping) and int(row.get("acquired") or 0) == 1
    )


def _owns_window_lock(connection: pymysql.Connection) -> bool:
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT CONNECTION_ID() AS connection_id, "
                "IS_USED_LOCK(%s) AS owner_id",
                (WINDOW_LOCK_NAME,),
            )
            row = cursor.fetchone()
        return bool(
            isinstance(row, Mapping)
            and row.get("owner_id") is not None
            and int(row.get("owner_id") or 0)
            == int(row.get("connection_id") or -1)
        )
    except Exception:
        return False


def _release_lock(connection: pymysql.Connection) -> bool:
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT RELEASE_LOCK(%s) AS released", (WINDOW_LOCK_NAME,)
            )
            row = cursor.fetchone()
        return bool(
            isinstance(row, Mapping) and int(row.get("released") or 0) == 1
        )
    except Exception:
        return False


def _close_quietly(connection: pymysql.Connection | None) -> None:
    if connection is None:
        return
    try:
        connection.close()
    except Exception:
        return


def _restore_and_double_verify(
    boundary: DatabaseBoundary,
    primary: pymysql.Connection | None,
) -> dict[str, bool]:
    admin_verification = _restore_and_verify_admin(boundary, primary)
    runtime_verified = _verify_runtime_trust_off(boundary.runtime_engine)
    return {
        **admin_verification,
        "runtime_trust_off_verified": runtime_verified,
    }


def _restore_and_verify_admin(
    boundary: DatabaseBoundary | RecoveryBoundary,
    primary: pymysql.Connection | None,
) -> dict[str, bool]:
    primary_verified = False
    restore_connection = primary
    owns_restore = False
    try:
        if restore_connection is None or not getattr(restore_connection, "open", False):
            restore_connection = _connect_admin(boundary)
            owns_restore = True
        # This must be the first explicit SQL issued after a trust window.  Do
        # not inspect identity, grants, or server state until the global value
        # has been forced OFF.  The option-file boundary and the already-open
        # primary session establish the administrator identity beforehand.
        _set_trust(restore_connection, enabled=False)
        after = _read_dbapi_state(restore_connection)
        _validate_target_state(
            after,
            expected_user=EXPECTED_ADMIN_USER,
            require_database=False,
            expected_trust=0,
            require_trigger_session=False,
        )
        primary_verified = True
    except Exception:
        primary_verified = False
    finally:
        if owns_restore:
            _close_quietly(restore_connection)

    # The original TCP session can disappear after SET GLOBAL ON. Recover the
    # setting through a fresh exact-identity connection before the independent
    # verification connection below.
    if not primary_verified:
        recovery: pymysql.Connection | None = None
        try:
            recovery = _connect_admin(boundary)
            # If the original TCP session disappeared after SET GLOBAL ON,
            # the first explicit statement on the fresh exact-option-file
            # connection must still be SET ... OFF.
            _set_trust(recovery, enabled=False)
            recovered_state = _read_dbapi_state(recovery)
            _validate_target_state(
                recovered_state,
                expected_user=EXPECTED_ADMIN_USER,
                require_database=False,
                expected_trust=0,
                require_trigger_session=False,
            )
            _validate_admin_grants(_dbapi_grants(recovery))
            primary_verified = True
        except Exception:
            primary_verified = False
        finally:
            _close_quietly(recovery)

    secondary: pymysql.Connection | None = None
    secondary_verified = False
    try:
        secondary = _connect_admin(boundary)
        # Treat every newly opened verifier as if the previous connection may
        # have died before its SET GLOBAL reached the server.  Its first SQL
        # must therefore also force the trust flag OFF before any read.
        _set_trust(secondary, enabled=False)
        state = _read_dbapi_state(secondary)
        _validate_target_state(
            state,
            expected_user=EXPECTED_ADMIN_USER,
            require_database=False,
            expected_trust=0,
            require_trigger_session=False,
        )
        _validate_admin_grants(_dbapi_grants(secondary))
        secondary_verified = True
    except Exception:
        secondary_verified = False
    finally:
        _close_quietly(secondary)

    return {
        "restore_primary_verified": primary_verified,
        "restore_secondary_verified": secondary_verified,
    }


def _verify_runtime_trust_off(runtime_engine: Engine) -> bool:
    try:
        runtime_engine.dispose()
        with runtime_engine.connect() as connection:
            state = _read_sa_state(connection)
            _validate_target_state(
                state,
                expected_user=EXPECTED_RUNTIME_USER,
                require_database=True,
                expected_trust=0,
                require_trigger_session=True,
            )
            _validate_runtime_grants(_sa_grants(connection))
            return True
    except Exception:
        return False


@contextmanager
def _catch_termination_signals():
    previous: dict[int, Any] = {}

    def interrupted(signum, _frame):
        raise PrivilegedSchemaPreparationError(
            f"database schema preparation interrupted by signal {signum}"
        )

    for name in ("SIGINT", "SIGTERM", "SIGHUP", "SIGQUIT"):
        signum = getattr(signal, name, None)
        if signum is not None:
            previous[signum] = signal.getsignal(signum)
            signal.signal(signum, interrupted)
    try:
        yield
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


def _frozen_trigger_contract_for_statement(
    statement: str,
    contracts: Iterable[TriggerContract],
) -> TriggerContract:
    preliminary = _parse_create_trigger(
        statement,
        normalizer="governance",
        owner="release_callback",
    )
    candidates = tuple(
        contract for contract in contracts
        if contract.name == preliminary.name
    )
    if not candidates:
        raise PrivilegedSchemaPreparationError(
            "trigger DDL is absent from the frozen release contract"
        )
    for expected in candidates:
        parsed = _parse_create_trigger(
            statement,
            normalizer=expected.normalizer,
            owner=expected.owner,
        )
        if (
            parsed.name == expected.name
            and parsed.timing == expected.timing
            and parsed.event == expected.event
            and parsed.table == expected.table
            and _normalized_trigger_body(parsed, parsed.body)
            == _normalized_trigger_body(expected, expected.body)
        ):
            return expected
    raise PrivilegedSchemaPreparationError(
        "trigger DDL differs from the frozen release contract"
    )


def _build_trigger_ddl_executor(
    boundary: DatabaseBoundary,
    admin: pymysql.Connection,
    contracts: Iterable[TriggerContract],
    evidence: dict[str, Any],
) -> Callable[[str], None]:
    """Broker one independently restored global-trust window per CREATE."""

    if boundary.migrator_engine is None:
        raise PrivilegedSchemaPreparationError("migration engine is unavailable")
    frozen_contracts = tuple(contracts)

    def execute(statement: str) -> None:
        contract = _frozen_trigger_contract_for_statement(
            statement,
            frozen_contracts,
        )

        # Prove OFF, exact migrator identity and actual absence before opening
        # the global setting at all.  The outer administrator connection must
        # still own the release lock; a dropped TCP session loses that lock.
        admin_off = _read_dbapi_state(admin)
        _validate_target_state(
            admin_off,
            expected_user=EXPECTED_ADMIN_USER,
            require_database=False,
            expected_trust=0,
            require_trigger_session=False,
        )
        if not _owns_window_lock(admin):
            raise PrivilegedSchemaPreparationError(
                "database trigger maintenance lock ownership was lost"
            )
        migrator: pymysql.Connection | None = None
        try:
            # Establish and fully authenticate the dedicated DDL connection
            # while trust is OFF.  Once ON is read back, this already-open
            # connection needs only one state read and the single CREATE.
            migrator = _connect_migrator(boundary)
            migrator_off = _read_dbapi_state(migrator)
            _validate_target_state(
                migrator_off,
                expected_user=EXPECTED_MIGRATOR_USER,
                require_database=True,
                expected_trust=0,
                require_trigger_session=True,
            )
            _validate_migrator_grants(_dbapi_grants(migrator))
            if _dbapi_trigger_exists(migrator, contract.name):
                raise PrivilegedSchemaPreparationError(
                    "trigger DDL executor received a CREATE for an existing trigger"
                )
        except BaseException:
            _close_quietly(migrator)
            raise
        assert migrator is not None

        operation_error: BaseException | None = None
        trust_attempted = False
        restoration = {
            "restore_primary_verified": False,
            "restore_secondary_verified": False,
            "runtime_trust_off_verified": False,
        }
        try:
            trust_attempted = True
            evidence["trigger_trust_window_count"] = int(
                evidence.get("trigger_trust_window_count") or 0
            ) + 1
            evidence.setdefault("trigger_trust_window_names", []).append(
                contract.name
            )
            _set_trust(admin, enabled=True)
            with migrator.cursor() as cursor:
                cursor.execute(statement)
        except BaseException as exc:
            operation_error = exc
        finally:
            if trust_attempted:
                # Keep the global window to the only operation that needs it:
                # SET ON -> frozen CREATE TRIGGER -> SET OFF.  Verification
                # and connection shutdown happen only after OFF is attempted.
                restoration = _restore_and_double_verify(boundary, admin)
            _close_quietly(migrator)

        restoration_ok = all(restoration.values())
        safety = {
            "global_trust_changed": trust_attempted,
            "trust_restoration_verified": restoration_ok,
            "trigger_name": contract.name,
            **restoration,
        }
        evidence["last_trigger_window_restoration"] = dict(safety)
        if trust_attempted and not restoration_ok:
            raise PrivilegedSchemaPreparationError(
                "could not prove global trigger trust is OFF",
                safety_evidence=safety,
            ) from operation_error
        if operation_error is not None:
            if isinstance(operation_error, PrivilegedSchemaPreparationError):
                operation_error.safety_evidence.update(safety)
                raise operation_error
            raise PrivilegedSchemaPreparationError(
                "frozen trigger creation failed",
                safety_evidence=safety,
            ) from operation_error

    return execute


def _cutover_schema(
    boundary: DatabaseBoundary,
    *,
    repair_interrupted_legacy: bool = False,
) -> dict[str, Any]:
    if boundary.migrator_engine is None:
        raise PrivilegedSchemaPreparationError("migration engine is unavailable")
    admin: pymysql.Connection | None = None
    lock_acquired = False
    operation_error: BaseException | None = None
    detail: dict[str, Any] = {}
    runtime_security: dict[str, Any] = {}
    window_evidence: dict[str, Any] = {
        "trigger_trust_window_count": 0,
        "trigger_trust_window_names": [],
    }
    restoration = {
        "restore_primary_verified": False,
        "restore_secondary_verified": False,
        "runtime_trust_off_verified": False,
    }
    try:
        admin = _connect_admin(boundary)
        state = _read_dbapi_state(admin)
        _validate_target_state(
            state,
            expected_user=EXPECTED_ADMIN_USER,
            require_database=False,
            expected_trust=0,
            require_trigger_session=False,
        )
        _validate_admin_grants(_dbapi_grants(admin))
        lock_acquired = _acquire_lock(admin)
        if not lock_acquired:
            raise PrivilegedSchemaPreparationError(
                "database trigger maintenance lock is busy"
            )
        runtime_security = _runtime_least_privilege_evidence(boundary)
        from server.db.migrations_v3 import run_v3_migrations
        from server.engine.strategy_governance import (
            ensure_strategy_governance_tables,
        )
        from tools.attest_qmt_daily_kline import (
            ensure_attestation_tables,
            validate_attestation_schema,
        )
        from tools.prepare_strategy_governance_qmt_history import (
            apply_legacy_completed_run_binding,
        )

        final_contracts = {
            **_final_v3_trigger_contracts(),
            **_non_v3_trigger_contracts(),
        }
        trigger_create_allowlist = (
            *_all_v3_trigger_contracts(),
            *_non_v3_trigger_contracts().values(),
        )
        trigger_ddl_executor = _build_trigger_ddl_executor(
            boundary,
            admin,
            trigger_create_allowlist,
            window_evidence,
        )
        legacy_trigger_repair = {
            "candidate_names": [],
            "repaired_names": [],
            "post_validation_verified": True,
        }
        if repair_interrupted_legacy:
            legacy_trigger_repair = _repair_interrupted_legacy_rehome(
                boundary.migrator_engine,
                _final_v3_trigger_contracts(),
                trigger_ddl_executor=trigger_ddl_executor,
            )

        # Build the complete immutable allow-list while trust is still OFF.
        # All long-running tables, columns, indexes and data backfills then run
        # under OFF; only a genuinely missing CREATE enters the callback.
        migration_plan = run_v3_migrations(
            boundary.migrator_engine,
            dry_run=True,
        )
        _applied_plan, final_v3_plan = _v3_trigger_states(migration_plan)
        planned_final_contracts = {
            **final_v3_plan,
            **_non_v3_trigger_contracts(),
        }
        if planned_final_contracts != final_contracts:
            raise PrivilegedSchemaPreparationError(
                "release trigger plan differs from frozen final contracts"
            )

        migrations = run_v3_migrations(
            boundary.migrator_engine,
            trigger_ddl_executor=trigger_ddl_executor,
        )
        ensure_attestation_tables(
            boundary.migrator_engine,
            trigger_ddl_executor=trigger_ddl_executor,
            allow_legacy_manifest_candidates=True,
        )
        legacy_binding = apply_legacy_completed_run_binding(
            boundary.migrator_engine
        )
        ensure_strategy_governance_tables(
            engine=boundary.migrator_engine,
            trigger_ddl_executor=trigger_ddl_executor,
        )
        validate_attestation_schema(
            boundary.migrator_engine,
        )
        _applied, final_v3 = _v3_trigger_states(migrations)
        rehomed = _rehome_legacy_triggers(
            boundary.migrator_engine,
            final_v3,
            trigger_ddl_executor=trigger_ddl_executor,
        )
        if final_contracts != {
            **final_v3,
            **_non_v3_trigger_contracts(),
        }:
            raise PrivilegedSchemaPreparationError(
                "release trigger contract changed during cutover"
            )
        with boundary.migrator_engine.connect() as connection:
            trigger_detail = validate_release_trigger_contracts(
                connection,
                required=final_contracts,
                optional={},
                controlled_contracts=final_contracts,
            )
        final_runtime_security = _runtime_least_privilege_evidence(boundary)
        if final_runtime_security != runtime_security:
            raise PrivilegedSchemaPreparationError(
                "runtime least-privilege evidence changed during cutover"
            )
        detail = {
            **final_runtime_security,
            "v3_migrations": [
                {
                    "version": item.version,
                    "status": item.status,
                    "statement_count": item.statement_count,
                }
                for item in migrations
            ],
            "trigger_contract": trigger_detail,
            "rehomed_legacy_triggers": list(rehomed),
            "legacy_binding_plan": {
                key: value for key, value in legacy_binding.items()
                if key != "legacy_bindings"
            },
            "legacy_trigger_repair": legacy_trigger_repair,
            **window_evidence,
        }
    except BaseException as exc:
        operation_error = exc
    finally:
        if admin is not None:
            # Even a fully migrated/no-delta cutover ends with the same three
            # independent OFF proofs.  This never issues SET ... ON.
            restoration = _restore_and_double_verify(boundary, admin)
        if lock_acquired and admin is not None:
            _release_lock(admin)
        _close_quietly(admin)

    restoration_ok = all(restoration.values())
    trust_changed = bool(
        int(window_evidence.get("trigger_trust_window_count") or 0)
    )
    safety = {
        "global_trust_changed": trust_changed,
        "trust_restoration_verified": restoration_ok,
        **restoration,
    }
    if not restoration_ok:
        raise PrivilegedSchemaPreparationError(
            "could not prove global trigger trust is OFF",
            safety_evidence=safety,
        ) from operation_error
    if operation_error is not None:
        if isinstance(operation_error, PrivilegedSchemaPreparationError):
            operation_error.safety_evidence.update(safety)
            raise operation_error
        raise PrivilegedSchemaPreparationError(
            "database schema cutover failed",
            safety_evidence=safety,
        ) from operation_error

    from server.api.routers._engine import dispose_engine, get_engine
    from server.engine.strategy_governance import (
        seed_governance_registry,
        validate_default_governance_seed_contract,
        validate_governance_append_only_triggers,
        validate_governance_table_schema,
        validate_metric_input_review_triggers,
    )

    try:
        seed_governance_registry()
        api_engine = get_engine()
        with api_engine.connect() as connection:
            metric = validate_metric_input_review_triggers(connection)
            governance_schema = validate_governance_table_schema(connection)
        append_only = validate_governance_append_only_triggers(api_engine)
        seed_contract = validate_default_governance_seed_contract(
            api_engine,
            require_initial_shadow=True,
        )
        detail.update(
            {
                **governance_schema,
                **seed_contract,
                "governance_trigger_count": int(metric["trigger_count"])
                + int(append_only["trigger_count"]),
            }
        )
    except BaseException as exc:
        if isinstance(exc, PrivilegedSchemaPreparationError):
            exc.safety_evidence.update(safety)
            raise
        raise PrivilegedSchemaPreparationError(
            "strategy governance seed validation failed",
            safety_evidence=safety,
        ) from exc
    finally:
        dispose_engine()
    detail.update(safety)
    return detail


def _recover_trust(boundary: RecoveryBoundary) -> dict[str, Any]:
    admin: pymysql.Connection | None = None
    runtime_engine: Engine | None = None
    lock_acquired = False
    # Recovery deliberately treats the global value as potentially ON.  It
    # cannot safely read the old value first, because that would extend a
    # crash-left trust window.  `global_trust_changed` therefore records the
    # conservative OFF-forcing operation rather than an unsafe prior read.
    trust_off_forced = False
    restoration = {
        "restore_primary_verified": False,
        "restore_secondary_verified": False,
        "runtime_trust_off_verified": False,
    }
    try:
        admin = _connect_admin(boundary)
        trust_off_forced = True
        restoration.update(_restore_and_verify_admin(boundary, admin))
        if not (
            restoration["restore_primary_verified"]
            and restoration["restore_secondary_verified"]
        ):
            raise PrivilegedSchemaPreparationError(
                "could not recover global trigger trust to OFF",
                safety_evidence={
                    "global_trust_changed": trust_off_forced,
                    "trust_restoration_verified": False,
                    **restoration,
                },
            )
        initial = _read_dbapi_state(admin)
        _validate_target_state(
            initial,
            expected_user=EXPECTED_ADMIN_USER,
            require_database=False,
            expected_trust=0,
            require_trigger_session=False,
        )
        _validate_admin_grants(_dbapi_grants(admin))
        lock_acquired = _acquire_lock(admin)
        if not lock_acquired:
            raise PrivilegedSchemaPreparationError(
                "database trigger maintenance lock is busy"
            )
        try:
            # Project configuration is intentionally loaded only after two
            # administrator-side OFF proofs.  A broken .env therefore keeps
            # the release guard engaged but cannot prevent emergency OFF.
            load_project_env()
            runtime_engine = create_tool_engine(future=True, poolclass=NullPool)
            restoration["runtime_trust_off_verified"] = (
                _verify_runtime_trust_off(runtime_engine)
            )
        except Exception:
            restoration["runtime_trust_off_verified"] = False
    finally:
        if lock_acquired and admin is not None:
            _release_lock(admin)
        _close_quietly(admin)
        if runtime_engine is not None:
            runtime_engine.dispose()
    if not all(restoration.values()):
        raise PrivilegedSchemaPreparationError(
            "could not recover global trigger trust to OFF",
            safety_evidence={
                "global_trust_changed": trust_off_forced,
                "trust_restoration_verified": False,
                **restoration,
            },
        )
    return {
        "global_trust_changed": trust_off_forced,
        "trust_restoration_verified": True,
        **restoration,
    }


def prepare_schema(*, phase: str, writers_fenced: bool) -> dict[str, Any]:
    fenced_phases = {"cutover", "resume"}
    if phase not in {"recover", "preflight", *fenced_phases}:
        raise ValueError("phase must be recover, preflight, cutover, or resume")
    if phase not in fenced_phases and writers_fenced:
        raise ValueError(
            "only cutover or resume accepts the writer-fence assertion"
        )
    if phase in fenced_phases and not writers_fenced:
        raise PrivilegedSchemaPreparationError(
            "cutover trigger replacement requires the verified writer fence"
        )
    if phase == "recover":
        recovery_boundary = _open_recovery_boundary()
        detail = _recover_trust(recovery_boundary)
        return {
            "status": "ok",
            "phase": phase,
            "server_uuid": EXPECTED_SERVER_UUID,
            "server_port": EXPECTED_SERVER_PORT,
            "runtime_user": EXPECTED_RUNTIME_USER,
            "migrator_user": None,
            "admin_user": EXPECTED_ADMIN_USER,
            "runtime_privileges_changed": False,
            "automatic_real_order_submission": False,
            **detail,
        }
    if phase in fenced_phases:
        # A resume is deliberately the same idempotent cutover after an
        # admin-first emergency OFF.  In particular, it can restore either
        # exact legacy trigger that was dropped immediately before an earlier
        # process interruption.
        _recover_trust(_open_recovery_boundary())
    load_project_env()
    boundary = _open_boundary(
        include_migrator=True,
        expected_trust=0,
    )
    try:
        if phase == "preflight":
            detail = _preflight_schema(boundary)
            detail.update(
                {
                    "global_trust_changed": False,
                    "trust_restoration_verified": True,
                }
            )
        else:
            with _catch_termination_signals():
                detail = _cutover_schema(
                    boundary,
                    repair_interrupted_legacy=phase == "resume",
                )
        return {
            "status": "ok",
            "phase": phase,
            "server_uuid": EXPECTED_SERVER_UUID,
            "server_port": EXPECTED_SERVER_PORT,
            "runtime_user": EXPECTED_RUNTIME_USER,
            "migrator_user": (
                EXPECTED_MIGRATOR_USER
            ),
            "admin_user": EXPECTED_ADMIN_USER,
            "runtime_privileges_changed": False,
            "automatic_real_order_submission": False,
            **detail,
        }
    finally:
        boundary.runtime_engine.dispose()
        if boundary.migrator_engine is not None:
            boundary.migrator_engine.dispose()


def _public_failure_payload(exc: BaseException, *, phase: str) -> dict[str, Any]:
    evidence = (
        dict(exc.safety_evidence)
        if isinstance(exc, PrivilegedSchemaPreparationError)
        else {}
    )
    return {
        "status": "blocked",
        "phase": phase,
        "reason": (
            f"{type(exc).__name__}: database schema preparation failed closed"
        ),
        "global_trust_changed": bool(evidence.get("global_trust_changed")),
        "trust_restoration_verified": bool(
            evidence.get("trust_restoration_verified")
        ),
        "restore_primary_verified": bool(
            evidence.get("restore_primary_verified")
        ),
        "restore_secondary_verified": bool(
            evidence.get("restore_secondary_verified")
        ),
        "restore_fresh_admin_verified": bool(
            evidence.get("restore_secondary_verified")
        ),
        "runtime_trust_off_verified": bool(
            evidence.get("runtime_trust_off_verified")
        ),
        "runtime_privileges_changed": False,
        "automatic_real_order_submission": False,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase",
        choices=("recover", "preflight", "cutover", "resume"),
        required=True,
    )
    parser.add_argument(
        "--writers-fenced",
        action="store_true",
        help="required only after deploy has disabled and drained every writer",
    )
    args = parser.parse_args(argv)
    try:
        result = prepare_schema(
            phase=args.phase,
            writers_fenced=bool(args.writers_fenced),
        )
    except BaseException as exc:
        print(
            json.dumps(
                _public_failure_payload(exc, phase=args.phase),
                ensure_ascii=False,
            )
        )
        return 2
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
