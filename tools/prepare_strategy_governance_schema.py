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
import hashlib
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
from pymysql.cursors import Cursor, DictCursor
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
from server.engine.strategy_funding_checkpoint import (  # noqa: E402
    FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH,
    FUNDING_CHECKPOINT_TABLE_NAME,
    FUNDING_DAILY_FACT_TABLE_NAME,
    validate_strategy_funding_checkpoint_schema,
)
from tools.env_config import create_tool_engine, load_project_env  # noqa: E402


DATABASE_NAME = "probiga"
EXPECTED_SERVER_UUID = "f40c3202-9260-11f1-86ae-74d4dd7f8500"
EXPECTED_CLIENT_ENDPOINT_HOST = "127.0.0.1"
EXPECTED_CLIENT_ENDPOINT_PORT = 13306
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
EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES = frozenset({
    "trg_dynamic_shadow_trial_chain_immutable_bd",
    "trg_dynamic_shadow_trial_chain_immutable_bu",
    "trg_dynamic_shadow_trial_exit_binding_immutable_bd",
    "trg_dynamic_shadow_trial_exit_binding_immutable_bu",
    "trg_dynamic_shadow_trial_plan_immutable_bd",
    "trg_dynamic_shadow_trial_plan_immutable_bu",
    "trg_strategy_adapter_candidate_fact_immutable_bd",
    "trg_strategy_adapter_candidate_fact_immutable_bu",
    "trg_strategy_adapter_run_receipt_immutable_bd",
    "trg_strategy_adapter_run_receipt_immutable_bu",
    "trg_strategy_allocation_snapshot_immutable_bd",
    "trg_strategy_allocation_snapshot_immutable_bu",
    "trg_strategy_combination_health_snapshot_immutable_bd",
    "trg_strategy_combination_health_snapshot_immutable_bu",
    "trg_strategy_combination_immutable_bd",
    "trg_strategy_combination_version_immutable_bd",
    "trg_strategy_combination_version_immutable_bu",
    "trg_strategy_funding_checkpoint_immutable_bd",
    "trg_strategy_funding_checkpoint_immutable_bu",
    "trg_strategy_funding_daily_fact_immutable_bd",
    "trg_strategy_funding_daily_fact_immutable_bu",
    "trg_strategy_governance_audit_immutable_bd",
    "trg_strategy_governance_audit_immutable_bu",
    "trg_strategy_governance_run_frozen_bu",
    "trg_strategy_governance_run_immutable_bd",
    "trg_strategy_governance_schema_migration_immutable_bd",
    "trg_strategy_governance_schema_migration_immutable_bu",
    "trg_strategy_health_snapshot_immutable_bd",
    "trg_strategy_health_snapshot_immutable_bu",
    "trg_strategy_industry_history_immutable_bd",
    "trg_strategy_industry_history_immutable_bu",
    "trg_strategy_lifecycle_event_immutable_bd",
    "trg_strategy_lifecycle_event_immutable_bu",
    "trg_strategy_pool_snapshot_immutable_bd",
    "trg_strategy_pool_snapshot_immutable_bu",
    "trg_strategy_registry_immutable_bd",
    "trg_strategy_version_immutable_bd",
    "trg_strategy_version_immutable_bu",
})
EXPECTED_METRIC_INPUT_REVIEW_TRIGGER_NAMES = frozenset({
    "trg_strategy_metric_input_immutable_bd",
    "trg_strategy_metric_input_review_bu",
})
EXPECTED_GOVERNANCE_TRIGGER_NAMES = frozenset(
    EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES
    | EXPECTED_METRIC_INPUT_REVIEW_TRIGGER_NAMES
)
EXPECTED_GOVERNANCE_RELEASE_TRIGGER_SOURCE_HASH = (
    "5a1a19e0664c715ae0cac7cfa8dd87c47da1b63b1d2df869561cecf3c995f01f"
)
EXPECTED_GOVERNANCE_APPEND_ONLY_PHYSICAL_CONTRACT_HASH = (
    "bf537f9ed5fb1d31195092ae6a24262511de6f45bf9addacefebc88e25b6b9d8"
)
EXPECTED_METRIC_REVIEW_PHYSICAL_CONTRACT_HASH = (
    "c217a42eb6c2a5f7bed592bb7c7e724499546f997061c4daad1db957317bdf28"
)
EXPECTED_CORE_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH = (
    "1fcde61ce5a5ea0cc16f1910d94da431d044c667383fafd2224217709f555943"
)
EXPECTED_CORE_METRIC_INPUT_REVIEW_CONTRACT_HASH = (
    "0dbaa644427139c472bab0c3f719d78bd292bb6a7726a0f0ef195adc2e37fa84"
)
EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH = (
    "47b44f4c1e5201b4ea7cd51f61073fdb4229c245214685c338e24809435a7bde"
)
EXPECTED_NON_V3_RELEASE_TRIGGER_COUNT = 82
EXPECTED_NON_V3_RELEASE_TRIGGER_SOURCE_HASH = (
    "7c261eaff759e562b883d19880ef345c6733cacf911218437adc72ba864934e2"
)
EXPECTED_MANAGED_RELEASE_TRIGGER_COUNT = 102
EXPECTED_FULL_RELEASE_TRIGGER_COUNT = 143
EXPECTED_FULL_RELEASE_TRIGGER_NAMESET_HASH = (
    "6df9585376ec190a8d78c996336ff9f2c68bf1a4860e88809561a55df7cbfde5"
)
EXPECTED_OPTIONAL_V4_TRIGGER_COUNT = 32
EXPECTED_OPTIONAL_V4_TRIGGER_NAMESET_HASH = (
    "ca55fb3f2722ae7dfe05a8f12071b07929160ffba39dc42c9b19f29e2139b095"
)
EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_COUNT = 175
EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_NAMESET_HASH = (
    "a1d2a23569adc5318b5806e3040487cedcb9e31a60da3dae7756ed7bdf7044d7"
)
EXPECTED_V2_RELEASE_TRIGGER_SOURCE_HASH = (
    "5167f36ee731c2544be73590e4e00716f334c58b5746f776e610254904cf8883"
)
EXPECTED_MANAGED_RELEASE_TRIGGER_SOURCE_HASH = (
    "7e154c081f807ce3d88311dc6d7db74170951abe890130a02343010466dc2f75"
)
LEGACY_ACTIVATION_TRIGGER_UPGRADE_PREVIOUS_BUILD_SHA = (
    "dee1a1a7f4acee704c2e38ce23164f83e569ab3b"
)
LEGACY_ACTIVATION_TRIGGER_UPGRADE_COMPATIBILITY_HASH = (
    "8c68a3065b39e7111628330632d5a12efc3d0e76d307a9536a56d5a8b713fb3c"
)
LEGACY_ACTIVATION_TRIGGER_UPGRADE_CONTRACT_HASH = (
    "c8eb6623b252ce14b3b6bcc20d33d3196364b0fd65851918fda0e2f5870499b9"
)
LEGACY_ACTIVATION_TRIGGER_UPGRADE_SUPPORTING_COUNT = 81
LEGACY_ACTIVATION_TRIGGER_UPGRADE_SUPPORTING_SOURCE_HASH = (
    "076a2b84c15b9dbb54901c63f980c2f85ab17f7652d9334ab661d89ad990d0bc"
)
LEGACY_ACTIVATION_TRIGGER_UPGRADE_SUPPORTING_NAMESET_HASH = (
    "9f22808ad42bbc7df65f1aa1cbbf1c761664ca20865497a6174c4f5fa5372ff1"
)
LEGACY_ACTIVATION_TRIGGER_UPGRADE_MANAGED_COUNT = 101
LEGACY_ACTIVATION_TRIGGER_UPGRADE_MANAGED_SOURCE_HASH = (
    "7e42c91e534dd3d61d212f0c16fa7297c29b8f4756812de2e072874179537423"
)
LEGACY_ACTIVATION_TRIGGER_UPGRADE_MANAGED_NAMESET_HASH = (
    "aa40fd09c6afbe3186d3037e43c8854285aad80046641c2e468eb435200eb8ba"
)
LEGACY_ACTIVATION_TRIGGER_UPGRADE_BASE_COUNT = 142
LEGACY_ACTIVATION_TRIGGER_UPGRADE_BASE_NAMESET_HASH = (
    "a1c6aa0e9f241a419bbb87c101fbac7d8dd1404aa9f95493afbd604370644a87"
)
LEGACY_ACTIVATION_TRIGGER_UPGRADE_FULL_COUNT = 174
LEGACY_ACTIVATION_TRIGGER_UPGRADE_FULL_NAMESET_HASH = (
    "6cb393a3b7e8471d2e9a382dea51dded58de3662eb87f944886574831567eec0"
)
EXPECTED_SCHEMA_RECOVERY_EVIDENCE_TRIGGER_SOURCE_HASH = (
    "c6f0b347b0f9b1f9d4e78ab53469ffbefbdceed4c2e2184e0b0b3dfd00db22b5"
)
ADMIN_IO_TIMEOUT_SECONDS = 60
MIGRATOR_IO_TIMEOUT_SECONDS = 900
MIGRATOR_LOCK_WAIT_TIMEOUT_SECONDS = 120
TARGET_RUNTIME_PRIVILEGE_CONTRACT = "TARGET_LEAST_PRIVILEGE"
LEGACY_RUNTIME_PRIVILEGE_CONTRACT = "LEGACY_DDL_COMPATIBILITY"
PERMISSION_AUDIT_STATUS = "SKIPPED_BY_USER_AUTHORIZATION"
RUNTIME_PERSISTENT_DDL_PRIVILEGES = frozenset({
    "ALTER",
    "CREATE",
    "DROP",
    "INDEX",
    "REFERENCES",
})
TARGET_RUNTIME_SCHEMA_PRIVILEGES = {
    "BIGA.*": frozenset({"SELECT"}),
    # Persistent DDL belongs only to the fenced migrator.  Runtime retains the
    # established application DML surface and session-local temporary tables;
    # mutations of append-only governance
    # rows (including both funding ledgers) are denied by the frozen 40-trigger
    # contract.  With DROP, ALTER and TRIGGER absent, runtime cannot TRUNCATE
    # protected tables, remove the guards or structurally bypass them.
    "PROBIGA.*": frozenset({
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE TEMPORARY TABLES",
    }),
    "PROBIGA_QMT_HISTORY.*": frozenset({"SELECT"}),
}
LEGACY_RUNTIME_SCHEMA_PRIVILEGES = {
    **TARGET_RUNTIME_SCHEMA_PRIVILEGES,
    "PROBIGA.*": frozenset({
        *TARGET_RUNTIME_SCHEMA_PRIVILEGES["PROBIGA.*"],
        *RUNTIME_PERSISTENT_DDL_PRIVILEGES,
    }),
}
# Keep the target name as the canonical end-state contract for callers that
# publish policy metadata.  Grant validation below deliberately accepts only
# this target or the one frozen compatibility contract above.
EXPECTED_RUNTIME_SCHEMA_PRIVILEGES = TARGET_RUNTIME_SCHEMA_PRIVILEGES
EXPECTED_RUNTIME_SCHEMA_SCOPES = frozenset(
    EXPECTED_RUNTIME_SCHEMA_PRIVILEGES
)
RUNTIME_FUNDING_APPEND_ONLY_TABLES = frozenset({
    FUNDING_DAILY_FACT_TABLE_NAME,
    FUNDING_CHECKPOINT_TABLE_NAME,
})
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

PREFLIGHT_DIAGNOSTIC_SCHEMA = (
    "probiga.strategy-governance-preflight-diagnostic.v1"
)
PREFLIGHT_STAGE_REASON_CODES = {
    "project_environment": "PREFLIGHT_PROJECT_ENVIRONMENT_BLOCKED",
    "database_boundary": "PREFLIGHT_DATABASE_BOUNDARY_BLOCKED",
    "database_root_execution": "PREFLIGHT_DATABASE_ROOT_EXECUTION_BLOCKED",
    "database_admin_credential": "PREFLIGHT_DATABASE_ADMIN_CREDENTIAL_BLOCKED",
    "database_migrator_credential": (
        "PREFLIGHT_DATABASE_MIGRATOR_CREDENTIAL_BLOCKED"
    ),
    "database_credential_separation": (
        "PREFLIGHT_DATABASE_CREDENTIAL_SEPARATION_BLOCKED"
    ),
    "database_tls_ca": "PREFLIGHT_DATABASE_TLS_CA_BLOCKED",
    "database_engine_construction": (
        "PREFLIGHT_DATABASE_ENGINE_CONSTRUCTION_BLOCKED"
    ),
    "database_runtime_connection": (
        "PREFLIGHT_DATABASE_RUNTIME_CONNECTION_BLOCKED"
    ),
    "database_runtime_state": "PREFLIGHT_DATABASE_RUNTIME_STATE_BLOCKED",
    "database_admin_connection": (
        "PREFLIGHT_DATABASE_ADMIN_CONNECTION_BLOCKED"
    ),
    "database_admin_state": "PREFLIGHT_DATABASE_ADMIN_STATE_BLOCKED",
    "database_migrator_connection": (
        "PREFLIGHT_DATABASE_MIGRATOR_CONNECTION_BLOCKED"
    ),
    "database_migrator_state": (
        "PREFLIGHT_DATABASE_MIGRATOR_STATE_BLOCKED"
    ),
    "database_duty_separation": (
        "PREFLIGHT_DATABASE_DUTY_SEPARATION_BLOCKED"
    ),
    "dependency_imports": "PREFLIGHT_DEPENDENCY_IMPORTS_BLOCKED",
    "runtime_identity_transport_boundary": (
        "PREFLIGHT_RUNTIME_IDENTITY_TRANSPORT_BOUNDARY_BLOCKED"
    ),
    "runtime_schema_bundle": "PREFLIGHT_RUNTIME_SCHEMA_BUNDLE_BLOCKED",
    "scheduler_runtime_schema": (
        "PREFLIGHT_SCHEDULER_RUNTIME_SCHEMA_BLOCKED"
    ),
    "scheduler_task_history_schema": (
        "PREFLIGHT_SCHEDULER_TASK_HISTORY_SCHEMA_BLOCKED"
    ),
    "qmt_reference_schema": "PREFLIGHT_QMT_REFERENCE_SCHEMA_BLOCKED",
    "v3_migration_plan": "PREFLIGHT_V3_MIGRATION_PLAN_BLOCKED",
    "qmt_attestation_schema": "PREFLIGHT_QMT_ATTESTATION_SCHEMA_BLOCKED",
    "qmt_history_coverage_schema": (
        "PREFLIGHT_QMT_HISTORY_COVERAGE_SCHEMA_BLOCKED"
    ),
    "strategy_governance_schema": (
        "PREFLIGHT_STRATEGY_GOVERNANCE_SCHEMA_BLOCKED"
    ),
    "dynamic_shadow_schema": "PREFLIGHT_DYNAMIC_SHADOW_SCHEMA_BLOCKED",
    "pit_fact_schema": "PREFLIGHT_PIT_FACT_SCHEMA_BLOCKED",
    "release_trigger_contract": (
        "PREFLIGHT_RELEASE_TRIGGER_CONTRACT_BLOCKED"
    ),
}
PREFLIGHT_UNCLASSIFIED_STAGE = "unclassified"
PREFLIGHT_UNCLASSIFIED_REASON_CODE = "PREFLIGHT_UNCLASSIFIED_BLOCKED"


class PrivilegedSchemaPreparationError(RuntimeError):
    """Fail-closed preparation error; message is never emitted verbatim."""

    def __init__(
        self,
        message: str,
        *,
        safety_evidence: Mapping[str, Any] | None = None,
        preflight_substage: str | None = None,
        reason_code: str | None = None,
    ) -> None:
        super().__init__(message)
        self.safety_evidence = dict(safety_evidence or {})
        expected_code = PREFLIGHT_STAGE_REASON_CODES.get(
            str(preflight_substage or "")
        )
        if expected_code is not None and reason_code == expected_code:
            self.preflight_substage = str(preflight_substage)
            self.reason_code = str(reason_code)
        else:
            self.preflight_substage = None
            self.reason_code = None


@contextmanager
def _preflight_diagnostic_scope(substage: str):
    """Attach one allow-listed diagnostic identity without exposing errors."""

    reason_code = PREFLIGHT_STAGE_REASON_CODES.get(substage)
    if reason_code is None:
        raise ValueError("preflight diagnostic substage is not allow-listed")
    try:
        yield
    except BaseException as exc:
        if isinstance(exc, PrivilegedSchemaPreparationError):
            if exc.preflight_substage is None or exc.reason_code is None:
                exc.preflight_substage = substage
                exc.reason_code = reason_code
            raise
        raise PrivilegedSchemaPreparationError(
            "preflight diagnostic substage failed closed",
            preflight_substage=substage,
            reason_code=reason_code,
        ) from exc


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
    runtime_current_user: str
    runtime_session_user: str
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
        or values["host"] != EXPECTED_CLIENT_ENDPOINT_HOST
        or values["port"] != str(EXPECTED_CLIENT_ENDPOINT_PORT)
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
    cursorclass: type[Cursor] = DictCursor,
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
        cursorclass=cursorclass,
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
            # SQLAlchemy's MySQL dialect reads its initial server-version
            # result positionally.  DictCursor is reserved for the direct
            # DB-API administrator/lock paths below.
            cursorclass=Cursor,
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
        cursor.execute("SHOW CREATE USER CURRENT_USER()")
        create_user_row = cursor.fetchone()
    grants: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping) or len(row) != 1:
            raise PrivilegedSchemaPreparationError("database grants were malformed")
        grants.append(str(next(iter(row.values())) or ""))
    if not isinstance(create_user_row, Mapping) or len(create_user_row) < 1:
        raise PrivilegedSchemaPreparationError("database account metadata was malformed")
    create_user = str(tuple(create_user_row.values())[-1] or "")
    return _with_account_tls_clause(grants, create_user)


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
    grants = tuple(
        str(row[0] or "")
        for row in connection.execute(text("SHOW GRANTS FOR CURRENT_USER()"))
    )
    create_user_row = connection.execute(
        text("SHOW CREATE USER CURRENT_USER()")
    ).one()
    if len(create_user_row) < 1:
        raise PrivilegedSchemaPreparationError("database account metadata was malformed")
    return _with_account_tls_clause(grants, str(create_user_row[-1] or ""))


def _with_account_tls_clause(
    grants: Iterable[str],
    create_user: str,
) -> tuple[str, ...]:
    resolved = tuple(str(item or "") for item in grants)
    normalized_create_user = " ".join(str(create_user).upper().split())
    if not normalized_create_user.startswith("CREATE USER "):
        raise PrivilegedSchemaPreparationError("database account metadata was malformed")
    if " REQUIRE SSL" not in normalized_create_user:
        return resolved
    if any(" REQUIRE SSL" in " ".join(item.upper().split()) for item in resolved):
        return resolved
    for index, item in enumerate(resolved):
        normalized = " ".join(item.upper().split())
        if normalized.startswith("GRANT USAGE ON *.* TO "):
            return resolved[:index] + (item.rstrip() + " REQUIRE SSL",) + resolved[index + 1 :]
    raise PrivilegedSchemaPreparationError("database grants were incomplete")


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
    *,
    expected_identity: str,
) -> tuple[tuple[set[str], str], ...]:
    entries: list[tuple[set[str], str]] = []
    for grant in grants:
        match = re.fullmatch(
            r"GRANT (.+?) ON (.+?) TO "
            r"`([^`]+)`@`([^`]+)`(.*)",
            grant,
        )
        if match is None:
            raise PrivilegedSchemaPreparationError("database grant syntax differs")
        privileges = {
            item.strip() for item in match.group(1).split(",") if item.strip()
        }
        scope = match.group(2).replace("`", "").upper()
        grantee = f"{match.group(3)}@{match.group(4)}".upper()
        tail = " ".join(match.group(5).strip().split()).upper()
        allowed_tails = {"", "REQUIRE SSL"} if scope == "*.*" else {""}
        if (
            not privileges
            or not scope
            or grantee != expected_identity.upper()
            or tail not in allowed_tails
        ):
            raise PrivilegedSchemaPreparationError("database grant syntax differs")
        entries.append((privileges, scope))
    return tuple(entries)


def _validate_admin_grants(grants: Iterable[str]) -> None:
    normalized = _normalized_grants(grants)
    entries = _grant_scope_entries(
        normalized,
        expected_identity=EXPECTED_ADMIN_USER,
    )
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
    entries = _grant_scope_entries(
        normalized,
        expected_identity=EXPECTED_MIGRATOR_USER,
    )
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


def _classify_runtime_grants(grants: Iterable[str]) -> str:
    normalized = _normalized_grants(grants)
    entries = _grant_scope_entries(
        normalized,
        expected_identity=EXPECTED_RUNTIME_USER,
    )
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
    common_boundary_invalid = (
        any(
            scope not in {"*.*", *EXPECTED_RUNTIME_SCHEMA_SCOPES}
            for _privileges, scope in entries
        )
        or len(global_entries) != 1
        or global_entries[0] != {"USAGE"}
        or set(schema_entries) != set(EXPECTED_RUNTIME_SCHEMA_SCOPES)
        or schema_entry_count != len(EXPECTED_RUNTIME_SCHEMA_SCOPES)
    )
    observed_schema = {
        scope: frozenset(privileges)
        for scope, privileges in schema_entries.items()
    }
    if (
        not common_boundary_invalid
        and observed_schema == TARGET_RUNTIME_SCHEMA_PRIVILEGES
    ):
        return TARGET_RUNTIME_PRIVILEGE_CONTRACT
    if (
        not common_boundary_invalid
        and observed_schema == LEGACY_RUNTIME_SCHEMA_PRIVILEGES
    ):
        return LEGACY_RUNTIME_PRIVILEGE_CONTRACT
    raise PrivilegedSchemaPreparationError(
        "runtime identity privileges differ from the audited boundary"
    )


def _validate_runtime_grants(grants: Iterable[str]) -> None:
    try:
        _classify_runtime_grants(grants)
    except PrivilegedSchemaPreparationError:
        raise PrivilegedSchemaPreparationError(
            "runtime identity privileges differ from the audited boundary"
        ) from None


def _runtime_grant_summary(grants: Iterable[str]) -> dict[str, Any]:
    normalized = _normalized_grants(grants)
    observed_contract = _classify_runtime_grants(normalized)
    entries = _grant_scope_entries(
        normalized,
        expected_identity=EXPECTED_RUNTIME_USER,
    )
    schema_privileges = {
        scope: sorted(privileges)
        for privileges, scope in sorted(entries, key=lambda item: item[1])
        if scope != "*.*"
    }
    persistent_ddl_privileges = sorted(
        set(schema_privileges["PROBIGA.*"])
        & RUNTIME_PERSISTENT_DDL_PRIVILEGES
    )
    return {
        "observed_contract": observed_contract,
        "persistent_ddl_privileges": persistent_ddl_privileges,
        "global_privileges": sorted(set().union(*(
            privileges for privileges, scope in entries if scope == "*.*"
        ))),
        "schema_privileges": schema_privileges,
        "funding_append_only_tables": sorted(
            RUNTIME_FUNDING_APPEND_ONLY_TABLES
        ),
        "funding_append_only_verified": True,
        "funding_row_mutation_denied_by_triggers": ["DELETE", "UPDATE"],
        "funding_structural_bypass_privileges": persistent_ddl_privileges,
        "truncate_denied_by_absent_drop_privilege": (
            "DROP" not in persistent_ddl_privileges
        ),
        "trigger_drop_denied_by_absent_trigger_privilege": True,
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
    if boundary.migrator_state is None:
        raise PrivilegedSchemaPreparationError("migration engine is unavailable")
    runtime_current_user = boundary.runtime_current_user
    runtime_session_user = boundary.runtime_session_user
    if (
        runtime_current_user != EXPECTED_RUNTIME_USER.lower()
        or runtime_session_user != EXPECTED_RUNTIME_USER.lower()
    ):
        raise PrivilegedSchemaPreparationError(
            "runtime database identity attestation differs"
        )

    return {
        "permission_audit_status": PERMISSION_AUDIT_STATUS,
        "permission_audit_verified": False,
        "runtime_privilege_boundary_verified": False,
        "runtime_least_privilege_verified": False,
        "runtime_legacy_ddl_compatibility": False,
        "runtime_grant_summary": {
            "permission_audit_status": PERMISSION_AUDIT_STATUS,
            "permission_audit_verified": False,
            "runtime_grant_count": None,
            "runtime_grant_contract_hash": "",
        },
        "runtime_current_user": runtime_current_user,
        "runtime_session_user": runtime_session_user,
        "runtime_tls_verified": True,
        "runtime_grant_count": None,
        "runtime_grant_contract_hash": "",
        "routine_inventory_audit_status": PERMISSION_AUDIT_STATUS,
        "runtime_self_definer_routine_count": None,
        "migrator_self_definer_routine_count": None,
        "runtime_definer_routine_count": None,
        "runtime_definer_routine_inventory_verified": False,
        "runtime_definer_routine_inventory_complete": False,
        "runtime_definer_routine_inventory_authority": "",
        "runtime_definer_routine_inventory_schemas": [],
    }


def _open_boundary(
    *,
    include_migrator: bool,
    expected_trust: int | None,
) -> DatabaseBoundary:
    with _preflight_diagnostic_scope("database_root_execution"):
        _require_root_execution()
    with _preflight_diagnostic_scope("database_admin_credential"):
        admin_credential = _read_option_credential(
            ADMIN_OPTION_FILE,
            expected_user=EXPECTED_ADMIN_USER.split("@", 1)[0],
        )
    migrator_credential = None
    if include_migrator:
        with _preflight_diagnostic_scope("database_migrator_credential"):
            migrator_credential = _read_option_credential(
                MIGRATOR_OPTION_FILE,
                expected_user=EXPECTED_MIGRATOR_USER.split("@", 1)[0],
            )
        with _preflight_diagnostic_scope("database_credential_separation"):
            if migrator_credential.path.samefile(admin_credential.path):
                raise PrivilegedSchemaPreparationError(
                    "administrator and migration option files overlap"
                )
    with _preflight_diagnostic_scope("database_tls_ca"):
        ssl_ca = _runtime_ssl_ca()
        if ssl_ca in {
            admin_credential.path,
            migrator_credential.path if migrator_credential else Path("/"),
        }:
            raise PrivilegedSchemaPreparationError(
                "database credential file aliases the TLS CA"
            )
    with _preflight_diagnostic_scope("database_engine_construction"):
        runtime_engine = create_tool_engine(future=True, poolclass=NullPool)
        migrator_engine = (
            _create_migrator_engine(migrator_credential, ssl_ca)
            if migrator_credential is not None
            else None
        )
    admin: pymysql.Connection | None = None
    try:
        with _preflight_diagnostic_scope("database_runtime_connection"):
            with runtime_engine.connect() as connection:
                with _preflight_diagnostic_scope("database_runtime_state"):
                    runtime_state = _read_sa_state(connection)
                    _validate_target_state(
                        runtime_state,
                        expected_user=EXPECTED_RUNTIME_USER,
                        require_database=True,
                        expected_trust=expected_trust,
                        require_trigger_session=True,
                    )
                    identity_rows = connection.execute(text(
                        "SELECT CURRENT_USER() AS runtime_current_identity, "
                        "USER() AS runtime_session_identity"
                    )).mappings().all()
                    if len(identity_rows) != 1:
                        raise PrivilegedSchemaPreparationError(
                            "runtime database identity attestation is unavailable"
                        )
                    runtime_current_user = str(
                        identity_rows[0].get("runtime_current_identity") or ""
                    ).lower()
                    runtime_session_user = str(
                        identity_rows[0].get("runtime_session_identity") or ""
                    ).lower()
                    if (
                        runtime_current_user != EXPECTED_RUNTIME_USER.lower()
                        or runtime_session_user != EXPECTED_RUNTIME_USER.lower()
                    ):
                        raise PrivilegedSchemaPreparationError(
                            "runtime database identity attestation differs"
                        )
        with _preflight_diagnostic_scope("database_admin_connection"):
            admin = _connect_option(
                admin_credential,
                ssl_ca,
                database=None,
                configure_trigger_session=False,
                autocommit=True,
            )
        with _preflight_diagnostic_scope("database_admin_state"):
            admin_state = _read_dbapi_state(admin)
            _validate_target_state(
                admin_state,
                expected_user=EXPECTED_ADMIN_USER,
                require_database=False,
                expected_trust=expected_trust,
                require_trigger_session=False,
            )
        migrator_state = None
        if migrator_engine is not None:
            with _preflight_diagnostic_scope("database_migrator_connection"):
                with migrator_engine.connect() as connection:
                    with _preflight_diagnostic_scope("database_migrator_state"):
                        migrator_state = _read_sa_state(connection)
                        _validate_target_state(
                            migrator_state,
                            expected_user=EXPECTED_MIGRATOR_USER,
                            require_database=True,
                            expected_trust=expected_trust,
                            require_trigger_session=True,
                        )
        with _preflight_diagnostic_scope("database_duty_separation"):
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
            runtime_current_user=runtime_current_user,
            runtime_session_user=runtime_session_user,
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
    from server.common.auxiliary_runtime_schema import (
        qmt_membership_trigger_ddl_statements,
    )
    from server.common.turnover_snapshot_schema import (
        market_field_capture_trigger_ddl_statements,
    )
    from server.common.pit_facts import PIT_FACT_TRIGGER_STATEMENTS
    from server.common.qmt_history_coverage import (
        coverage_trigger_ddl_statements,
    )
    from server.common.scheduler_task_history_schema import (
        scheduler_task_history_trigger_ddl_statements,
    )
    from server.common.schema_recovery_evidence import TRIGGER_STATEMENTS
    from server.engine.strategy_governance import (
        GOVERNANCE_APPEND_ONLY_TRIGGER_STATEMENTS,
        METRIC_INPUT_REVIEW_TRIGGER_CONTRACTS,
    )
    from tools.attest_qmt_daily_kline import ATTESTATION_TRIGGER_STATEMENTS
    from tools.sync_guojin_qmt_reference_data import (
        reference_trigger_ddl_contracts,
    )

    contracts: dict[str, TriggerContract] = {}
    for statement in TRIGGER_STATEMENTS.values():
        contract = _parse_create_trigger(
            statement,
            normalizer="qmt",
            owner="schema_recovery_evidence",
        )
        contracts[contract.name] = contract
    for statement in ATTESTATION_TRIGGER_STATEMENTS.values():
        contract = _parse_create_trigger(
            statement,
            normalizer="qmt",
            owner="qmt_attestation",
        )
        contracts[contract.name] = contract
    for raw_statement in reference_trigger_ddl_contracts():
        statement = re.sub(
            r"^\s*CREATE\s+TRIGGER\s+IF\s+NOT\s+EXISTS\s+",
            "CREATE TRIGGER ",
            str(raw_statement),
            count=1,
            flags=re.IGNORECASE,
        )
        contract = _parse_create_trigger(
            statement,
            normalizer="qmt",
            owner="qmt_reference",
        )
        if contract.name in contracts:
            raise PrivilegedSchemaPreparationError(
                "duplicate release trigger name"
            )
        contracts[contract.name] = contract
    for raw_statement in coverage_trigger_ddl_statements():
        statement = re.sub(
            r"^\s*CREATE\s+TRIGGER\s+IF\s+NOT\s+EXISTS\s+",
            "CREATE TRIGGER ",
            str(raw_statement),
            count=1,
            flags=re.IGNORECASE,
        )
        contract = _parse_create_trigger(
            statement,
            normalizer="qmt",
            owner="qmt_history_coverage",
        )
        if contract.name in contracts:
            raise PrivilegedSchemaPreparationError(
                "duplicate release trigger name"
            )
        contracts[contract.name] = contract
    for raw_statement in scheduler_task_history_trigger_ddl_statements():
        statement = re.sub(
            r"^\s*CREATE\s+TRIGGER\s+IF\s+NOT\s+EXISTS\s+",
            "CREATE TRIGGER ",
            str(raw_statement),
            count=1,
            flags=re.IGNORECASE,
        )
        contract = _parse_create_trigger(
            statement,
            normalizer="qmt",
            owner="scheduler_task_history",
        )
        if contract.name in contracts:
            raise PrivilegedSchemaPreparationError(
                "duplicate release trigger name"
            )
        contracts[contract.name] = contract
    for owner, statements in (
        (
            "market_field_capture",
            market_field_capture_trigger_ddl_statements(),
        ),
        (
            "qmt_membership",
            qmt_membership_trigger_ddl_statements(),
        ),
    ):
        for statement in statements:
            contract = _parse_create_trigger(
                str(statement),
                normalizer="qmt",
                owner=owner,
            )
            if contract.name in contracts:
                raise PrivilegedSchemaPreparationError(
                    "duplicate release trigger name"
                )
            contracts[contract.name] = contract
    for statement in PIT_FACT_TRIGGER_STATEMENTS.values():
        contract = _parse_create_trigger(
            statement,
            normalizer="governance",
            owner="pit_facts",
        )
        if contract.name in contracts:
            raise PrivilegedSchemaPreparationError("duplicate release trigger name")
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


def _release_trigger_source_contract_hash(
    contracts: Mapping[str, TriggerContract],
) -> str:
    """Hash the exact parsed source contract before any privileged CREATE."""

    members = [
        {
            "name": name,
            "timing": contract.timing,
            "event": contract.event,
            "table": contract.table,
            "body": _normalized_trigger_body(contract, contract.body),
            "normalizer": contract.normalizer,
            "owner": contract.owner,
        }
        for name, contract in sorted(contracts.items())
    ]
    payload = {
        "schema": "probiga.release-trigger-source-contract.v1",
        "members": members,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _v2_release_trigger_contract() -> tuple[
    dict[str, tuple[str, str]],
    dict[str, str],
    dict[str, int],
]:
    """Load the final canonical V2 trigger shapes, bodies and action order."""

    from server.trading_v2.execution_evidence_schema_gate import (
        _all_trigger_bodies,
        _all_trigger_contracts,
        _trigger_action_order_contracts,
    )

    contracts = dict(_all_trigger_contracts())
    bodies = dict(_all_trigger_bodies())
    action_orders, _references = _trigger_action_order_contracts(contracts)
    if (
        len(contracts) != 41
        or set(contracts) != set(bodies)
        or set(contracts) != set(action_orders)
    ):
        raise PrivilegedSchemaPreparationError(
            "V2 release trigger source contract differs"
        )
    return contracts, bodies, action_orders


def _v2_release_trigger_source_contract_hash() -> str:
    contracts, bodies, action_orders = _v2_release_trigger_contract()
    payload = {
        "schema": "probiga.v2-release-trigger-source-contract.v1",
        "members": [
            {
                "name": name,
                "event": contracts[name][0],
                "table": contracts[name][1],
                "body": bodies[name],
                "action_order": action_orders[name],
            }
            for name in sorted(contracts)
        ],
    }
    return hashlib.sha256(json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")).hexdigest()


def _full_release_trigger_nameset_hash(names: Iterable[str]) -> str:
    payload = {
        "schema": "probiga.full-release-trigger-names.v1",
        "names": sorted(set(str(name) for name in names)),
    }
    return hashlib.sha256(json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")).hexdigest()


def _frozen_full_release_trigger_names(
    managed_contracts: Mapping[str, TriggerContract],
) -> frozenset[str]:
    v2_contracts, _bodies, _orders = _v2_release_trigger_contract()
    overlap = set(v2_contracts) & set(managed_contracts)
    expected = frozenset({*v2_contracts, *managed_contracts})
    if (
        overlap
        or len(expected) != EXPECTED_FULL_RELEASE_TRIGGER_COUNT
        or _release_trigger_source_contract_hash(managed_contracts)
        != EXPECTED_MANAGED_RELEASE_TRIGGER_SOURCE_HASH
        or _full_release_trigger_nameset_hash(expected)
        != EXPECTED_FULL_RELEASE_TRIGGER_NAMESET_HASH
        or _v2_release_trigger_source_contract_hash()
        != EXPECTED_V2_RELEASE_TRIGGER_SOURCE_HASH
    ):
        raise PrivilegedSchemaPreparationError(
            "full release trigger source contract differs"
        )
    return expected


def _validated_applied_v4_trigger_names(engine: Engine) -> frozenset[str]:
    """Return the complete optional V4 trigger group after read-only attestation."""

    from server.db.migrations_v4 import MIGRATIONS, run_v4_migrations

    results = tuple(run_v4_migrations(engine, dry_run=True))
    migrations = tuple(MIGRATIONS)
    if len(results) != len(migrations):
        raise PrivilegedSchemaPreparationError(
            "optional V4 migration plan is incomplete"
        )
    statuses: list[str] = []
    names: list[str] = []
    for migration, result in zip(migrations, results):
        version = str(migration["version"])
        status = str(result.status)
        if str(result.version) != version or status not in {
            "exists", "would_apply",
        }:
            raise PrivilegedSchemaPreparationError(
                "optional V4 migration plan differs"
            )
        statuses.append(status)
        for raw in tuple(migration["statements"]):
            matched = _CREATE_TRIGGER_RE.match(str(raw).strip())
            if matched is not None:
                names.append(matched.group(1))
    if len(names) != len(set(names)):
        raise PrivilegedSchemaPreparationError(
            "optional V4 trigger source inventory is duplicated"
        )
    frozen_names = frozenset(names)
    if (
        len(frozen_names) != EXPECTED_OPTIONAL_V4_TRIGGER_COUNT
        or _full_release_trigger_nameset_hash(frozen_names)
        != EXPECTED_OPTIONAL_V4_TRIGGER_NAMESET_HASH
    ):
        raise PrivilegedSchemaPreparationError(
            "optional V4 trigger source contract differs"
        )
    if all(status == "exists" for status in statuses):
        return frozen_names
    if all(status == "would_apply" for status in statuses):
        return frozenset()
    raise PrivilegedSchemaPreparationError(
        "optional V4 migration ledger is partial"
    )


def _release_trigger_owner_counts(
    contracts: Mapping[str, TriggerContract],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for contract in contracts.values():
        counts[contract.owner] = counts.get(contract.owner, 0) + 1
    return dict(sorted(counts.items()))


def _frozen_non_v3_release_trigger_contracts(
    contracts: Mapping[str, TriggerContract],
) -> dict[str, TriggerContract]:
    frozen = dict(contracts)
    owner_counts = _release_trigger_owner_counts(frozen)
    if (
        len(frozen) != EXPECTED_NON_V3_RELEASE_TRIGGER_COUNT
        or owner_counts != {
            "market_field_capture": 5,
            "pit_facts": 6,
            "qmt_attestation": 6,
            "qmt_history_coverage": 4,
            "qmt_membership": 6,
            "qmt_reference": 10,
            "scheduler_task_history": 3,
            "schema_recovery_evidence": 2,
            "strategy_governance": 40,
        }
        or _release_trigger_source_contract_hash(frozen)
        != EXPECTED_NON_V3_RELEASE_TRIGGER_SOURCE_HASH
    ):
        raise PrivilegedSchemaPreparationError(
            "non-V3 release trigger source contract differs"
        )
    return frozen


def _frozen_governance_release_trigger_contracts(
    release_contracts: Mapping[str, TriggerContract],
) -> dict[str, TriggerContract]:
    """Bind core exports to this release's literal 40-trigger contract."""

    from server.engine.strategy_governance import (
        EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES as core_append_names,
        EXPECTED_METRIC_INPUT_REVIEW_TRIGGER_NAMES as core_metric_names,
        GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACT_HASH as core_contract_hash,
        METRIC_INPUT_REVIEW_TRIGGER_CONTRACT_HASH as core_metric_contract_hash,
    )

    if (
        core_append_names != EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES
        or core_metric_names != EXPECTED_METRIC_INPUT_REVIEW_TRIGGER_NAMES
        or core_contract_hash
        != EXPECTED_CORE_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
        or core_metric_contract_hash
        != EXPECTED_CORE_METRIC_INPUT_REVIEW_CONTRACT_HASH
        or FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH
        != EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH
    ):
        raise PrivilegedSchemaPreparationError(
            "core governance trigger or funding schema contract differs"
        )
    selected = {
        name: release_contracts[name]
        for name in EXPECTED_GOVERNANCE_TRIGGER_NAMES
        if name in release_contracts
    }
    if (
        set(selected) != EXPECTED_GOVERNANCE_TRIGGER_NAMES
        or _release_trigger_source_contract_hash(selected)
        != EXPECTED_GOVERNANCE_RELEASE_TRIGGER_SOURCE_HASH
    ):
        raise PrivilegedSchemaPreparationError(
            "release governance trigger source contract differs"
        )
    return selected


def _ensure_frozen_release_triggers(
    engine: Engine,
    contracts: Mapping[str, TriggerContract],
    *,
    expected_names: Iterable[str],
    expected_source_contract_hash: str,
    trigger_ddl_executor: Callable[[str], None],
    trusted_verified_trigger_seal_present: bool = False,
) -> dict[str, Any]:
    """Create only absent frozen triggers, then require exact metadata.

    This deliberately never drops or replaces an observed trigger.  Any
    unknown member on a controlled table, wrong body or metadata drift is a
    hard failure before the narrowly brokered CREATE callback can run.
    """

    from server.common.scheduler_task_history_schema import (
        QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_NAME,
    )

    frozen_names = frozenset(str(name) for name in expected_names)
    source_hash = _release_trigger_source_contract_hash(contracts)
    if (
        not callable(trigger_ddl_executor)
        or not frozen_names
        or set(contracts) != set(frozen_names)
        or re.fullmatch(r"[0-9a-f]{64}", expected_source_contract_hash)
        is None
        or source_hash != expected_source_contract_hash
    ):
        raise PrivilegedSchemaPreparationError(
            "release trigger source contract differs before creation"
        )
    controlled_tables = {contract.table for contract in contracts.values()}
    with engine.connect() as connection:
        observed = _trigger_inventory(
            connection,
            names=frozen_names,
            controlled_tables=controlled_tables,
        )
        unexpected = set(observed) - set(contracts)
        if unexpected:
            raise PrivilegedSchemaPreparationError(
                "release trigger inventory is incomplete or unexpected"
            )
        existing = {
            name: contracts[name] for name in observed
        }
        absent = {
            name: contracts[name]
            for name in contracts
            if name not in observed
        }
        activation_trigger_was_absent = (
            QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_NAME in absent
        )
        activation_history_must_be_empty = (
            QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_NAME in contracts
            and (
                activation_trigger_was_absent
                or trusted_verified_trigger_seal_present is not True
            )
        )
        validate_release_trigger_contracts(
            connection,
            required=existing,
            optional=absent,
            controlled_contracts=contracts,
        )
        if activation_history_must_be_empty:
            _assert_no_preexisting_qmt_release_activation_rows(connection)

    created: list[str] = []
    creation_order = sorted(
        absent,
        key=lambda name: (
            name != QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_NAME,
            name,
        ),
    )
    for name in creation_order:
        contract = absent[name]
        trigger_ddl_executor(
            f"CREATE TRIGGER `{name}` {contract.timing} {contract.event} "
            f"ON `{contract.table}` FOR EACH ROW {contract.body}"
        )
        created.append(name)
        if name == QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_NAME:
            with engine.connect() as connection:
                _assert_no_preexisting_qmt_release_activation_rows(
                    connection
                )

    with engine.connect() as connection:
        metadata = validate_release_trigger_contracts(
            connection,
            required=contracts,
            optional={},
            controlled_contracts=contracts,
        )
    release_binding = {}
    if (
        frozen_names == EXPECTED_GOVERNANCE_TRIGGER_NAMES
        and source_hash == EXPECTED_GOVERNANCE_RELEASE_TRIGGER_SOURCE_HASH
    ):
        release_binding = {
            "append_only_physical_contract_hash": (
                EXPECTED_GOVERNANCE_APPEND_ONLY_PHYSICAL_CONTRACT_HASH
            ),
            "metric_review_physical_contract_hash": (
                EXPECTED_METRIC_REVIEW_PHYSICAL_CONTRACT_HASH
            ),
            "core_append_only_contract_hash": (
                EXPECTED_CORE_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
            ),
            "core_metric_review_contract_hash": (
                EXPECTED_CORE_METRIC_INPUT_REVIEW_CONTRACT_HASH
            ),
            "funding_schema_contract_hash": (
                EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH
            ),
        }
    return {
        **metadata,
        "source_contract_hash": source_hash,
        **release_binding,
        "expected_names": sorted(frozen_names),
        "created_names": created,
        "created_count": len(created),
    }


def _assert_no_preexisting_qmt_release_activation_rows(
    connection: Connection,
) -> None:
    """Reject a forged activation row across the first trigger install gap."""

    from server.common.qmt_edge_release_receipt import (
        QMT_EDGE_RELEASE_REQUEST_TASK_TYPE,
        QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_SOURCE,
    )

    rows = connection.execute(
        text(
            "SELECT id FROM st_scheduled_task_history "
            "WHERE BINARY task_type=BINARY :task_type "
            "AND BINARY trigger_source=BINARY :trigger_source LIMIT 1"
        ),
        {
            "task_type": QMT_EDGE_RELEASE_REQUEST_TASK_TYPE,
            "trigger_source": (
                QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_SOURCE
            )
        },
    ).mappings().all()
    if rows:
        raise PrivilegedSchemaPreparationError(
            "preexisting QMT release activation rows are forbidden before "
            "activation trigger installation"
        )


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


def _all_database_trigger_inventory(
    connection: Connection,
) -> dict[str, dict[str, Any]]:
    rows = connection.execute(text(
        "SELECT TRIGGER_SCHEMA AS trigger_schema, "
        "TRIGGER_NAME AS trigger_name, DEFINER AS definer, "
        "EVENT_OBJECT_SCHEMA AS event_object_schema, "
        "ACTION_TIMING AS action_timing, "
        "EVENT_MANIPULATION AS event_manipulation, "
        "EVENT_OBJECT_TABLE AS event_object_table, "
        "ACTION_ORIENTATION AS action_orientation, "
        "ACTION_STATEMENT AS action_statement, ACTION_ORDER AS action_order, "
        "SQL_MODE AS sql_mode, CHARACTER_SET_CLIENT AS character_set_client, "
        "COLLATION_CONNECTION AS collation_connection, "
        "DATABASE_COLLATION AS database_collation "
        "FROM information_schema.TRIGGERS "
        "WHERE TRIGGER_SCHEMA=DATABASE() ORDER BY BINARY TRIGGER_NAME"
    )).mappings().all()
    observed: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        name = str(
            row.get("trigger_name") or row.get("TRIGGER_NAME") or ""
        )
        if _SAFE_NAME_RE.fullmatch(name) is None or name in observed:
            raise PrivilegedSchemaPreparationError(
                "full database trigger inventory is malformed"
            )
        observed[name] = row
    return observed


def _validate_full_database_trigger_inventory_exact(
    connection: Connection,
    *,
    managed_contracts: Mapping[str, TriggerContract],
    include_applied_v4: bool = False,
    expected_base_count: int,
    expected_base_nameset_hash: str,
    expected_full_count: int,
    expected_full_nameset_hash: str,
    expected_managed_source_contract_hash: str,
) -> dict[str, Any]:
    """Attest every production trigger, including the canonical V2 guards."""

    managed = dict(managed_contracts)
    v2_contracts, v2_bodies, v2_action_orders = (
        _v2_release_trigger_contract()
    )
    if set(v2_contracts) & set(managed):
        raise PrivilegedSchemaPreparationError(
            "full release trigger source contract overlaps"
        )
    base_expected_names = frozenset({*v2_contracts, *managed})
    if (
        len(base_expected_names) != expected_base_count
        or _release_trigger_source_contract_hash(managed)
        != expected_managed_source_contract_hash
        or _full_release_trigger_nameset_hash(base_expected_names)
        != expected_base_nameset_hash
        or _v2_release_trigger_source_contract_hash()
        != EXPECTED_V2_RELEASE_TRIGGER_SOURCE_HASH
    ):
        raise PrivilegedSchemaPreparationError(
            "full release trigger source contract differs"
        )
    optional_v4_names = (
        _validated_applied_v4_trigger_names(connection.engine)
        if include_applied_v4 else frozenset()
    )
    if set(base_expected_names) & set(optional_v4_names):
        raise PrivilegedSchemaPreparationError(
            "optional V4 trigger inventory overlaps the release contract"
        )
    expected_names = frozenset({*base_expected_names, *optional_v4_names})
    expected_nameset_hash = (
        expected_full_nameset_hash
        if optional_v4_names else expected_base_nameset_hash
    )
    expected_count = (
        expected_full_count if optional_v4_names else expected_base_count
    )
    if (
        len(expected_names) != expected_count
        or _full_release_trigger_nameset_hash(expected_names)
        != expected_nameset_hash
    ):
        raise PrivilegedSchemaPreparationError(
            "full release trigger nameset differs"
        )
    managed_detail = validate_release_trigger_contracts(
        connection,
        required=managed,
        optional={},
        controlled_contracts=managed,
    )
    observed = _all_database_trigger_inventory(connection)
    if set(observed) != set(expected_names):
        raise PrivilegedSchemaPreparationError(
            "full database trigger inventory is incomplete or unexpected"
        )

    from server.trading_v2.execution_evidence_schema_gate import (
        _trigger_row_matches_contract,
    )

    canonical_rows: list[dict[str, Any]] = []
    for name in sorted(observed):
        row = observed[name]
        trigger_schema = str(
            row.get("trigger_schema") or row.get("TRIGGER_SCHEMA") or ""
        )
        event_object_schema = str(
            row.get("event_object_schema")
            or row.get("EVENT_OBJECT_SCHEMA")
            or ""
        )
        action_order_raw = (
            row.get("action_order")
            if "action_order" in row
            else row.get("ACTION_ORDER")
        )
        try:
            action_order = int(action_order_raw)
        except (TypeError, ValueError) as exc:
            raise PrivilegedSchemaPreparationError(
                "full database trigger physical metadata differs"
            ) from exc
        if (
            trigger_schema != DATABASE_NAME
            or event_object_schema != DATABASE_NAME
            or str(row.get("definer") or row.get("DEFINER") or "")
            != EXPECTED_MIGRATOR_USER
            or str(
                row.get("action_orientation")
                or row.get("ACTION_ORIENTATION")
                or ""
            ).upper()
            != "ROW"
            or str(row.get("sql_mode") or row.get("SQL_MODE") or "")
            != EXPECTED_SQL_MODE
            or str(
                row.get("character_set_client")
                or row.get("CHARACTER_SET_CLIENT")
                or ""
            )
            != EXPECTED_CHARACTER_SET_CLIENT
            or str(
                row.get("collation_connection")
                or row.get("COLLATION_CONNECTION")
                or ""
            )
            != EXPECTED_COLLATION_CONNECTION
            or str(
                row.get("database_collation")
                or row.get("DATABASE_COLLATION")
                or ""
            )
            != EXPECTED_DATABASE_COLLATION
        ):
            raise PrivilegedSchemaPreparationError(
                "full database trigger physical metadata differs"
            )

        if name in v2_contracts:
            if not _trigger_row_matches_contract(
                row,
                trigger_name=name,
                contracts=v2_contracts,
                bodies=v2_bodies,
                action_orders=v2_action_orders,
            ):
                raise PrivilegedSchemaPreparationError(
                    "V2 release trigger physical contract differs"
                )
            normalized_body = v2_bodies[name]
        elif name in managed:
            contract = managed[name]
            if action_order != 1:
                raise PrivilegedSchemaPreparationError(
                    "managed release trigger action order differs"
                )
            normalized_body = _normalized_trigger_body(
                contract,
                row.get("action_statement")
                or row.get("ACTION_STATEMENT")
                or "",
            )
        else:
            if name not in optional_v4_names:
                raise PrivilegedSchemaPreparationError(
                    "optional V4 trigger physical metadata differs"
                )
            from server.db.migrations_v4 import _normalize_trigger_body

            normalized_body = _normalize_trigger_body(
                row.get("action_statement")
                or row.get("ACTION_STATEMENT")
                or ""
            )
        canonical_rows.append({
            "name": name,
            "definer": EXPECTED_MIGRATOR_USER,
            "timing": str(
                row.get("action_timing")
                or row.get("ACTION_TIMING")
                or ""
            ).upper(),
            "event": str(
                row.get("event_manipulation")
                or row.get("EVENT_MANIPULATION")
                or ""
            ).upper(),
            "table": str(
                row.get("event_object_table")
                or row.get("EVENT_OBJECT_TABLE")
                or ""
            ),
            "body": normalized_body,
            "action_order": action_order,
            "sql_mode": EXPECTED_SQL_MODE,
            "character_set_client": EXPECTED_CHARACTER_SET_CLIENT,
            "collation_connection": EXPECTED_COLLATION_CONNECTION,
            "database_collation": EXPECTED_DATABASE_COLLATION,
        })
    observed_metadata_sha256 = hashlib.sha256(json.dumps(
        {
            "schema": "probiga.full-release-trigger-physical-inventory.v1",
            "members": canonical_rows,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")).hexdigest()
    return {
        "expected_count": len(expected_names),
        "observed_count": len(observed),
        "v2_count": len(v2_contracts),
        "managed_count": len(managed),
        "optional_v4_count": len(optional_v4_names),
        "expected_names": sorted(expected_names),
        "nameset_sha256": expected_nameset_hash,
        "base_nameset_sha256": expected_base_nameset_hash,
        "v2_source_contract_sha256": (
            EXPECTED_V2_RELEASE_TRIGGER_SOURCE_HASH
        ),
        "managed_source_contract_sha256": (
            expected_managed_source_contract_hash
        ),
        "observed_metadata_sha256": observed_metadata_sha256,
        "managed_contract": managed_detail,
        "metadata_frozen": True,
        "read_only": True,
    }


def validate_full_database_trigger_inventory(
    connection: Connection,
    *,
    managed_contracts: Mapping[str, TriggerContract],
    include_applied_v4: bool = False,
) -> dict[str, Any]:
    return _validate_full_database_trigger_inventory_exact(
        connection,
        managed_contracts=managed_contracts,
        include_applied_v4=include_applied_v4,
        expected_base_count=EXPECTED_FULL_RELEASE_TRIGGER_COUNT,
        expected_base_nameset_hash=(
            EXPECTED_FULL_RELEASE_TRIGGER_NAMESET_HASH
        ),
        expected_full_count=EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_COUNT,
        expected_full_nameset_hash=(
            EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_NAMESET_HASH
        ),
        expected_managed_source_contract_hash=(
            EXPECTED_MANAGED_RELEASE_TRIGGER_SOURCE_HASH
        ),
    )


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


def _prepare_qmt_reference_schema_tables(engine: Engine) -> dict[str, Any]:
    """Apply only the frozen QMT truth table/migration DDL under the fence.

    Trigger creation is deliberately excluded here.  All ten append-only
    triggers are installed later through the narrowly allow-listed broker.
    """

    from tools.sync_guojin_qmt_reference_data import (
        REFERENCE_SCHEMA_CONTRACT_HASH,
        REFERENCE_TABLE_NAMES,
        REFERENCE_TRIGGER_NAMES,
        execute_reference_ddl_contracts,
        reference_migration_ddl_contracts,
        reference_table_ddl_contracts,
    )

    table_statements = tuple(reference_table_ddl_contracts())
    migration_statements = tuple(reference_migration_ddl_contracts())
    with engine.begin() as connection:
        execute_reference_ddl_contracts(
            connection,
            (*table_statements, *migration_statements),
        )
    return {
        "contract_hash": REFERENCE_SCHEMA_CONTRACT_HASH,
        "table_names": list(REFERENCE_TABLE_NAMES),
        "trigger_names": list(REFERENCE_TRIGGER_NAMES),
        "table_ddl_count": len(table_statements),
        "migration_ddl_count": len(migration_statements),
        "runtime_ddl_required": False,
    }


def _prepare_qmt_history_coverage_schema_tables(
    engine: Engine,
) -> dict[str, Any]:
    """Install coverage tables through the fenced privileged migrator only."""

    from server.common.qmt_history_coverage import (
        COVERAGE_TABLE_NAMES,
        COVERAGE_TRIGGER_NAMES,
        coverage_table_ddl_statements,
    )

    table_statements = tuple(coverage_table_ddl_statements())
    with engine.begin() as connection:
        for statement in table_statements:
            connection.execute(text(statement))
    return {
        "database": DATABASE_NAME,
        "table_names": list(COVERAGE_TABLE_NAMES),
        "trigger_names": list(COVERAGE_TRIGGER_NAMES),
        "table_ddl_count": len(table_statements),
        "trigger_ddl_count": len(COVERAGE_TRIGGER_NAMES),
        "runtime_ddl_required": False,
    }


def _preflight_governance_cutover_recovery(
    connection: Connection,
    *,
    governance_tables_present: bool,
) -> dict[str, Any]:
    """Classify the one forward-only governance state that needs ``resume``.

    A failed release can commit the full funding migration marker and then
    restore the pre-cutover trigger snapshot while rolling the application
    back.  Deleting that marker would rewrite migration history, while treating
    the database as an ordinary deferred schema would falsely attest that a
    completed trigger boundary still exists.  Report the state read-only so
    the release broker can reinstall the frozen triggers after fencing every
    writer.
    """

    if not governance_tables_present:
        return {
            "schema": "probiga.strategy-governance-cutover-recovery.v1",
            "status": "CUTOVER_READY",
            "read_only": True,
            "full_migration_marker_present": False,
            "full_migration_marker_hash_verified": False,
            "expected_trigger_count": 0,
            "installed_trigger_count": 0,
            "missing_trigger_count": 0,
            "resume_required": False,
        }

    from server.engine.strategy_funding_checkpoint import (
        FUNDING_CHECKPOINT_MIGRATION_HASH,
        FUNDING_CHECKPOINT_MIGRATION_KEY,
    )
    from server.engine.strategy_governance import (
        validate_deferred_governance_trigger_inventory,
    )

    trigger_detail = validate_deferred_governance_trigger_inventory(connection)
    marker_rows = connection.execute(text(
        "SELECT migration_key, migration_hash "
        "FROM st_strategy_governance_schema_migration "
        "WHERE migration_key=:migration_key"
    ), {
        "migration_key": FUNDING_CHECKPOINT_MIGRATION_KEY,
    }).mappings().all()
    if len(marker_rows) > 1:
        raise PrivilegedSchemaPreparationError(
            "full governance migration marker is not unique"
        )
    marker_present = bool(marker_rows)
    marker_verified = False
    if marker_present:
        marker_verified = (
            str(marker_rows[0].get("migration_hash") or "")
            == FUNDING_CHECKPOINT_MIGRATION_HASH
        )
        if not marker_verified:
            raise PrivilegedSchemaPreparationError(
                "full governance migration marker differs"
            )

    expected_count = int(trigger_detail["expected_trigger_count"])
    installed_count = int(trigger_detail["installed_trigger_count"])
    missing_count = int(trigger_detail["missing_trigger_count"])
    if installed_count + missing_count != expected_count:
        raise PrivilegedSchemaPreparationError(
            "governance trigger recovery inventory is incomplete"
        )
    resume_required = marker_verified and missing_count > 0
    if resume_required:
        status = "RESUME_REQUIRED"
    elif marker_verified:
        status = "SEALED"
    else:
        status = "CUTOVER_READY"
    return {
        "schema": "probiga.strategy-governance-cutover-recovery.v1",
        "status": status,
        "read_only": True,
        "full_migration_marker_present": marker_present,
        "full_migration_marker_hash_verified": marker_verified,
        "expected_trigger_count": expected_count,
        "installed_trigger_count": installed_count,
        "missing_trigger_count": missing_count,
        "resume_required": resume_required,
    }


def _preflight_schema(boundary: DatabaseBoundary) -> dict[str, Any]:
    if boundary.migrator_engine is None:
        raise PrivilegedSchemaPreparationError("migration engine is unavailable")
    with _preflight_diagnostic_scope("dependency_imports"):
        from server.db.migrations_v3 import run_v3_migrations
        from server.engine.strategy_governance import GOVERNANCE_TABLE_NAMES
        from server.engine.dynamic_shadow_ledger_schema import (
            preflight_dynamic_shadow_ledger_schema_upgrade,
        )
        from server.common.pit_facts import preflight_pit_fact_schema
        from server.common.scheduler_runtime_schema import (
            preflight_scheduler_runtime_heartbeat_schema,
        )
        from server.common.scheduler_task_history_schema import (
            validate_scheduler_task_history_schema,
        )
        from server.common.production_runtime_schema_bundle import (
            preflight_runtime_schema_bundle,
        )
        from server.common.qmt_history_coverage import (
            COVERAGE_TABLE_NAMES,
            COVERAGE_TRIGGER_NAMES,
            validate_coverage_schema,
        )
        from tools.attest_qmt_daily_kline import (
            ATTESTATION_TABLE_NAMES,
            validate_attestation_schema,
        )
        from tools.prepare_strategy_governance_qmt_history import (
            plan_legacy_completed_run_binding,
        )
        from tools.sync_guojin_qmt_reference_data import (
            preflight_reference_tables,
        )

    with _preflight_diagnostic_scope("runtime_identity_transport_boundary"):
        runtime_security = _runtime_least_privilege_evidence(boundary)
    with _preflight_diagnostic_scope("runtime_schema_bundle"):
        runtime_schema_bundle = preflight_runtime_schema_bundle(
            boundary.migrator_engine
        )
    with _preflight_diagnostic_scope("scheduler_runtime_schema"):
        scheduler_runtime_schema = preflight_scheduler_runtime_heartbeat_schema(
            boundary.migrator_engine
        )
    try:
        with _preflight_diagnostic_scope("scheduler_task_history_schema"):
            scheduler_task_history_schema = {
                **validate_scheduler_task_history_schema(
                    boundary.migrator_engine
                ),
                "status": "READY",
            }
    except Exception as exc:
        scheduler_task_history_schema = {
            "table": "st_scheduled_task_history",
            "status": "MIGRATION_REQUIRED",
            "runtime_ddl_required": False,
            "read_only": True,
            "preflight_error_type": type(exc).__name__,
        }
    with _preflight_diagnostic_scope("qmt_reference_schema"):
        qmt_reference_preflight = preflight_reference_tables(
            boundary.migrator_engine
        )
    with _preflight_diagnostic_scope("v3_migration_plan"):
        plan = run_v3_migrations(boundary.migrator_engine, dry_run=True)
        pending_versions = {
            str(item.version) for item in plan if item.status == "would_apply"
        }
        if not pending_versions <= EXPECTED_INITIAL_PENDING_V3:
            raise PrivilegedSchemaPreparationError(
                "V3 migration ledger differs from the audited production boundary"
            )
        applied_v3, final_v3 = _v3_trigger_states(plan)
        non_v3 = _frozen_non_v3_release_trigger_contracts(
            _non_v3_trigger_contracts()
        )
        governance_release_contracts = (
            _frozen_governance_release_trigger_contracts(non_v3)
        )
    legacy_binding_plan = {
        "legacy_run_count": 0,
        "legacy_binding_plan_hash": "",
        "legacy_binding_marker_present": False,
        "legacy_binding_pending": False,
    }
    with boundary.migrator_engine.connect() as connection:
        with _preflight_diagnostic_scope("qmt_attestation_schema"):
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
        with _preflight_diagnostic_scope("qmt_history_coverage_schema"):
            coverage_tables = _table_inventory(
                connection,
                COVERAGE_TABLE_NAMES,
            )
            if coverage_tables and coverage_tables != set(COVERAGE_TABLE_NAMES):
                raise PrivilegedSchemaPreparationError(
                    "QMT history coverage table inventory is partial"
                )
            coverage_schema: dict[str, Any] = {
                "status": "EMPTY",
                "database": DATABASE_NAME,
                "table_names": list(COVERAGE_TABLE_NAMES),
                "table_count": 0,
                "trigger_names": list(COVERAGE_TRIGGER_NAMES),
                "expected_trigger_count": len(COVERAGE_TRIGGER_NAMES),
                "runtime_ddl_required": False,
                "physical_schema_verified": False,
                "physical_seal_verified": False,
                "read_only": True,
            }
            if coverage_tables:
                coverage_schema = {
                    **validate_coverage_schema(
                        connection,
                        require_triggers=False,
                    ),
                    "status": "READY_FOR_TRIGGER_CUTOVER",
                    "read_only": True,
                }
        with _preflight_diagnostic_scope("strategy_governance_schema"):
            governance_tables = _table_inventory(
                connection, GOVERNANCE_TABLE_NAMES
            )
            if (
                governance_tables
                and governance_tables != set(GOVERNANCE_TABLE_NAMES)
            ):
                raise PrivilegedSchemaPreparationError(
                    "strategy governance table inventory is partial"
                )
            governance_cutover_recovery = (
                _preflight_governance_cutover_recovery(
                    connection,
                    governance_tables_present=bool(governance_tables),
                )
            )
        with _preflight_diagnostic_scope("dynamic_shadow_schema"):
            dynamic_shadow_schema = (
                preflight_dynamic_shadow_ledger_schema_upgrade(connection)
            )
        with _preflight_diagnostic_scope("pit_fact_schema"):
            pit_fact_schema = preflight_pit_fact_schema(connection)
        with _preflight_diagnostic_scope("release_trigger_contract"):
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
        "governance_cutover_recovery": governance_cutover_recovery,
        "dynamic_shadow_schema": dynamic_shadow_schema,
        "pit_fact_schema": pit_fact_schema,
        "qmt_reference_schema": qmt_reference_preflight,
        "qmt_history_coverage_schema": coverage_schema,
        "scheduler_runtime_heartbeat_schema": scheduler_runtime_schema,
        "scheduler_task_history_schema": scheduler_task_history_schema,
        "runtime_schema_bundle": runtime_schema_bundle,
        "legacy_binding_plan": {
            key: value for key, value in legacy_binding_plan.items()
            if key != "legacy_bindings"
        },
        "trigger_contract": trigger_detail,
        "supporting_trigger_source_contract": {
            "trigger_count": len(non_v3),
            "trigger_names": sorted(non_v3),
            "source_contract_hash": (
                EXPECTED_NON_V3_RELEASE_TRIGGER_SOURCE_HASH
            ),
            "owner_counts": _release_trigger_owner_counts(non_v3),
        },
        "governance_trigger_source_contract": {
            "trigger_count": len(governance_release_contracts),
            "trigger_names": sorted(governance_release_contracts),
            "source_contract_hash": (
                EXPECTED_GOVERNANCE_RELEASE_TRIGGER_SOURCE_HASH
            ),
            "append_only_physical_contract_hash": (
                EXPECTED_GOVERNANCE_APPEND_ONLY_PHYSICAL_CONTRACT_HASH
            ),
            "metric_review_physical_contract_hash": (
                EXPECTED_METRIC_REVIEW_PHYSICAL_CONTRACT_HASH
            ),
            "core_append_only_contract_hash": (
                EXPECTED_CORE_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
            ),
            "core_metric_review_contract_hash": (
                EXPECTED_CORE_METRIC_INPUT_REVIEW_CONTRACT_HASH
            ),
            "funding_schema_contract_hash": (
                EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH
            ),
        },
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


def _legacy_activation_trigger_upgrade_managed_contracts(
) -> dict[str, TriggerContract]:
    """Reconstruct the one exact 174-trigger predecessor contract."""

    from server.common.scheduler_task_history_schema import (
        QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_NAME,
    )

    supporting = dict(
        _frozen_non_v3_release_trigger_contracts(
            _non_v3_trigger_contracts()
        )
    )
    removed = supporting.pop(
        QMT_EDGE_RELEASE_ACTIVATION_TRIGGER_NAME,
        None,
    )
    if (
        removed is None
        or len(supporting)
        != LEGACY_ACTIVATION_TRIGGER_UPGRADE_SUPPORTING_COUNT
        or _release_trigger_source_contract_hash(supporting)
        != LEGACY_ACTIVATION_TRIGGER_UPGRADE_SUPPORTING_SOURCE_HASH
        or hashlib.sha256(json.dumps(
            sorted(supporting),
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        != LEGACY_ACTIVATION_TRIGGER_UPGRADE_SUPPORTING_NAMESET_HASH
    ):
        raise PrivilegedSchemaPreparationError(
            "legacy activation-trigger supporting contract differs"
        )
    managed = {
        **_final_v3_trigger_contracts(),
        **supporting,
    }
    if (
        len(managed) != LEGACY_ACTIVATION_TRIGGER_UPGRADE_MANAGED_COUNT
        or _release_trigger_source_contract_hash(managed)
        != LEGACY_ACTIVATION_TRIGGER_UPGRADE_MANAGED_SOURCE_HASH
        or _full_release_trigger_nameset_hash(managed)
        != LEGACY_ACTIVATION_TRIGGER_UPGRADE_MANAGED_NAMESET_HASH
    ):
        raise PrivilegedSchemaPreparationError(
            "legacy activation-trigger managed contract differs"
        )
    return managed


def _validate_legacy_activation_trigger_upgrade_inventory(
    connection: Connection,
) -> dict[str, Any]:
    legacy_managed = _legacy_activation_trigger_upgrade_managed_contracts()
    current_managed = {
        **_final_v3_trigger_contracts(),
        **_frozen_non_v3_release_trigger_contracts(
            _non_v3_trigger_contracts()
        ),
    }
    v2_contracts, _v2_bodies, _v2_orders = _v2_release_trigger_contract()
    optional_v4_names = _validated_applied_v4_trigger_names(
        connection.engine
    )
    legacy_names = frozenset({
        *v2_contracts,
        *legacy_managed,
        *optional_v4_names,
    })
    current_names = frozenset({
        *_frozen_full_release_trigger_names(current_managed),
        *optional_v4_names,
    })
    if (
        len(optional_v4_names) != EXPECTED_OPTIONAL_V4_TRIGGER_COUNT
        or _full_release_trigger_nameset_hash(optional_v4_names)
        != EXPECTED_OPTIONAL_V4_TRIGGER_NAMESET_HASH
        or len(legacy_names)
        != LEGACY_ACTIVATION_TRIGGER_UPGRADE_FULL_COUNT
        or _full_release_trigger_nameset_hash(legacy_names)
        != LEGACY_ACTIVATION_TRIGGER_UPGRADE_FULL_NAMESET_HASH
        or len(current_names) != EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_COUNT
        or _full_release_trigger_nameset_hash(current_names)
        != EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_NAMESET_HASH
    ):
        raise PrivilegedSchemaPreparationError(
            "legacy activation-trigger expected inventory differs"
        )
    observed_names = frozenset(_all_database_trigger_inventory(connection))
    if observed_names == legacy_names:
        live_state = "LEGACY_174"
        detail = _validate_full_database_trigger_inventory_exact(
            connection,
            managed_contracts=legacy_managed,
            include_applied_v4=True,
            expected_base_count=(
                LEGACY_ACTIVATION_TRIGGER_UPGRADE_BASE_COUNT
            ),
            expected_base_nameset_hash=(
                LEGACY_ACTIVATION_TRIGGER_UPGRADE_BASE_NAMESET_HASH
            ),
            expected_full_count=LEGACY_ACTIVATION_TRIGGER_UPGRADE_FULL_COUNT,
            expected_full_nameset_hash=(
                LEGACY_ACTIVATION_TRIGGER_UPGRADE_FULL_NAMESET_HASH
            ),
            expected_managed_source_contract_hash=(
                LEGACY_ACTIVATION_TRIGGER_UPGRADE_MANAGED_SOURCE_HASH
            ),
        )
    elif observed_names == current_names:
        live_state = "CURRENT_175_RETRY"
        detail = validate_full_database_trigger_inventory(
            connection,
            managed_contracts=current_managed,
            include_applied_v4=True,
        )
    else:
        raise PrivilegedSchemaPreparationError(
            "legacy activation-trigger live inventory differs"
        )
    _assert_no_preexisting_qmt_release_activation_rows(connection)
    expected_live_count = (
        LEGACY_ACTIVATION_TRIGGER_UPGRADE_FULL_COUNT
        if live_state == "LEGACY_174"
        else EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_COUNT
    )
    expected_managed_count = (
        LEGACY_ACTIVATION_TRIGGER_UPGRADE_MANAGED_COUNT
        if live_state == "LEGACY_174"
        else EXPECTED_MANAGED_RELEASE_TRIGGER_COUNT
    )
    expected_nameset_hash = (
        LEGACY_ACTIVATION_TRIGGER_UPGRADE_FULL_NAMESET_HASH
        if live_state == "LEGACY_174"
        else EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_NAMESET_HASH
    )
    expected_managed_source_hash = (
        LEGACY_ACTIVATION_TRIGGER_UPGRADE_MANAGED_SOURCE_HASH
        if live_state == "LEGACY_174"
        else EXPECTED_MANAGED_RELEASE_TRIGGER_SOURCE_HASH
    )
    if (
        detail.get("expected_count") != expected_live_count
        or detail.get("observed_count") != expected_live_count
        or detail.get("managed_count") != expected_managed_count
        or detail.get("optional_v4_count")
        != EXPECTED_OPTIONAL_V4_TRIGGER_COUNT
        or detail.get("nameset_sha256") != expected_nameset_hash
        or detail.get("managed_source_contract_sha256")
        != expected_managed_source_hash
        or detail.get("metadata_frozen") is not True
        or detail.get("read_only") is not True
    ):
        raise PrivilegedSchemaPreparationError(
            "legacy activation-trigger live inventory differs"
        )
    return {**detail, "upgrade_live_state": live_state}


def _legacy_activation_trigger_upgrade_seal_state(
    envelope: Mapping[str, Any],
    *,
    identity: Mapping[str, Any],
) -> str:
    """Classify the two exact seal states allowed for the dee1 upgrade.

    The predecessor used the older 174-trigger contract.  After the new trigger
    inventory has been committed, a client timeout may leave the current target
    seal durably PENDING (or already VERIFIED) while the runtime still reports
    dee1 as its predecessor.  Only that exact same-target journal is a valid
    retry; it must never turn this one-build compatibility bridge into a general
    lineage bypass.
    """

    build_sha = str(identity.get("build_sha") or "").strip().lower()
    if (
        re.fullmatch(r"[0-9a-f]{40}", build_sha) is None
        or build_sha == LEGACY_ACTIVATION_TRIGGER_UPGRADE_PREVIOUS_BUILD_SHA
    ):
        raise PrivilegedSchemaPreparationError(
            "legacy activation-trigger seal differs"
        )
    legacy_entry = {
        "build_sha": LEGACY_ACTIVATION_TRIGGER_UPGRADE_PREVIOUS_BUILD_SHA,
        "compatibility_hash": LEGACY_ACTIVATION_TRIGGER_UPGRADE_COMPATIBILITY_HASH,
        "contract_hash": LEGACY_ACTIVATION_TRIGGER_UPGRADE_CONTRACT_HASH,
        "status": "VERIFIED",
    }
    if (
        envelope.get("candidate_build_sha")
        == LEGACY_ACTIVATION_TRIGGER_UPGRADE_PREVIOUS_BUILD_SHA
        and envelope.get("rollback_build_sha") == ""
        and envelope.get("entries") == [legacy_entry]
    ):
        return "LEGACY_PREDECESSOR"
    current_entry = dict(identity.get("entry") or {})
    for status in ("PENDING", "VERIFIED"):
        if (
            envelope.get("candidate_build_sha") == build_sha
            and envelope.get("rollback_build_sha") == ""
            and envelope.get("entries")
            == [{**current_entry, "status": status}]
        ):
            return f"CURRENT_TARGET_{status}"
    raise PrivilegedSchemaPreparationError(
        "legacy activation-trigger seal differs"
    )


def _privileged_trigger_inventory_lineage_preflight(
    boundary: DatabaseBoundary,
    *,
    build_sha: object,
    previous_build_sha: object,
) -> dict[str, Any]:
    """Read and validate the rollback-compatible seal before any trigger DDL."""

    from server.engine.strategy_governance import (
        PRIVILEGED_TRIGGER_SEAL_BOOTSTRAP_PREVIOUS_BUILD_SHA,
        compose_privileged_trigger_inventory_seal_comment,
        parse_privileged_trigger_inventory_seal_comment,
        privileged_trigger_inventory_seal_identity,
    )

    if boundary.migrator_engine is None:
        raise PrivilegedSchemaPreparationError("migration engine is unavailable")
    previous = str(previous_build_sha or "").strip().lower()
    if re.fullmatch(r"[0-9a-f]{40}", previous) is None:
        raise PrivilegedSchemaPreparationError(
            "previous production build identity is unavailable"
        )
    with boundary.migrator_engine.connect() as connection:
        identity_rows = connection.execute(text(
            "SELECT @@server_uuid AS server_uuid, DATABASE() AS database_name"
        )).mappings().all()
        metadata_rows = connection.execute(text(
            "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, "
            "TABLE_TYPE AS table_type, ENGINE AS engine, "
            "TABLE_COMMENT AS table_comment "
            "FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=:table_schema AND TABLE_NAME=:table_name"
        ), {
            "table_schema": "probiga",
            "table_name": "st_privileged_schema_recovery_evidence",
        }).mappings().all()
    if (
        len(identity_rows) != 1
        or len(metadata_rows) != 1
        or str(metadata_rows[0].get("table_schema") or "").lower()
        != "probiga"
        or str(metadata_rows[0].get("table_name") or "")
        != "st_privileged_schema_recovery_evidence"
        or str(metadata_rows[0].get("table_type") or "").upper()
        != "BASE TABLE"
        or str(metadata_rows[0].get("engine") or "").upper() != "INNODB"
    ):
        raise PrivilegedSchemaPreparationError(
            "privileged trigger seal metadata preflight is unavailable"
        )
    try:
        identity = privileged_trigger_inventory_seal_identity(
            build_sha,
            server_uuid=identity_rows[0].get("server_uuid"),
            database_name=identity_rows[0].get("database_name"),
        )
    except (TypeError, ValueError) as exc:
        raise PrivilegedSchemaPreparationError(
            "privileged trigger seal database identity differs"
        ) from exc
    old_comment = str(metadata_rows[0].get("table_comment") or "")
    lineage_previous = previous
    bootstrap = previous == PRIVILEGED_TRIGGER_SEAL_BOOTSTRAP_PREVIOUS_BUILD_SHA
    legacy_activation_trigger_upgrade = (
        previous == LEGACY_ACTIVATION_TRIGGER_UPGRADE_PREVIOUS_BUILD_SHA
    )
    legacy_activation_trigger_upgrade_seal_state = "NOT_APPLICABLE"
    legacy_activation_trigger_upgrade_inventory: dict[str, Any] = {}
    if legacy_activation_trigger_upgrade:
        try:
            old_envelope = parse_privileged_trigger_inventory_seal_comment(
                old_comment,
                expected_server_uuid=identity["server_uuid"],
            )
        except (TypeError, ValueError) as exc:
            raise PrivilegedSchemaPreparationError(
                "legacy activation-trigger seal differs"
            ) from exc
        legacy_activation_trigger_upgrade_seal_state = (
            _legacy_activation_trigger_upgrade_seal_state(
                old_envelope,
                identity=identity,
            )
        )
        with boundary.migrator_engine.connect() as connection:
            legacy_activation_trigger_upgrade_inventory = (
                _validate_legacy_activation_trigger_upgrade_inventory(
                    connection
                )
            )
        if (
            legacy_activation_trigger_upgrade_seal_state
            != "LEGACY_PREDECESSOR"
            and legacy_activation_trigger_upgrade_inventory.get(
                "upgrade_live_state"
            )
            != "CURRENT_175_RETRY"
        ):
            raise PrivilegedSchemaPreparationError(
                "legacy activation-trigger retry seal precedes trigger inventory"
            )
        # The predecessor contract did not include the activation INSERT
        # guard.  It is accepted only as migration provenance, never as
        # authority for any preexisting QMT activation grant.
        lineage_previous = ""
    elif bootstrap:
        # The exact 8e production baseline predates this seal contract.  It
        # does not consume TABLE_COMMENT, so a failed first rollout can safely
        # return to it.  This exception is intentionally one-build-only.
        lineage_previous = ""
    try:
        pending = compose_privileged_trigger_inventory_seal_comment(
            identity,
            previous_comment=old_comment,
            previous_build_sha=lineage_previous,
            status="PENDING",
        )
        verified = compose_privileged_trigger_inventory_seal_comment(
            identity,
            previous_comment=old_comment,
            previous_build_sha=lineage_previous,
            status="VERIFIED",
        )
    except (TypeError, ValueError) as exc:
        raise PrivilegedSchemaPreparationError(
            "previous privileged trigger seal is not rollback-compatible"
        ) from exc
    trusted_verified_trigger_seal_present = False
    if not legacy_activation_trigger_upgrade:
        try:
            old_envelope = parse_privileged_trigger_inventory_seal_comment(
                old_comment,
                expected_server_uuid=identity["server_uuid"],
            )
            old_entries_are_compatible = True
            for entry in old_envelope["entries"]:
                entry_identity = privileged_trigger_inventory_seal_identity(
                    entry["build_sha"],
                    server_uuid=identity["server_uuid"],
                    database_name=identity["database_name"],
                )
                if (
                    entry["compatibility_hash"]
                    != entry_identity["compatibility_hash"]
                    or entry["contract_hash"]
                    != entry_identity["contract_hash"]
                ):
                    old_entries_are_compatible = False
                    break
            trusted_verified_trigger_seal_present = (
                old_entries_are_compatible
                and any(
                    entry["status"] == "VERIFIED"
                    for entry in old_envelope["entries"]
                )
            )
        except (RuntimeError, TypeError, ValueError):
            # The one frozen legacy bootstrap is allowed to start without a
            # seal, but never authorizes preexisting activation rows.
            trusted_verified_trigger_seal_present = False
    return {
        "schema": "probiga.privileged-trigger-inventory-lineage-preflight.v1",
        "build_sha": identity["build_sha"],
        "previous_build_sha": previous,
        "legacy_bootstrap": bootstrap,
        "legacy_activation_trigger_upgrade": (
            legacy_activation_trigger_upgrade
        ),
        "legacy_activation_trigger_upgrade_seal_state": (
            legacy_activation_trigger_upgrade_seal_state
        ),
        "legacy_activation_trigger_upgrade_inventory": (
            legacy_activation_trigger_upgrade_inventory
        ),
        "trusted_verified_trigger_seal_present": (
            trusted_verified_trigger_seal_present
        ),
        "server_uuid": identity["server_uuid"],
        "database_name": identity["database_name"],
        "seal_table": identity["seal_table"],
        "compatibility_hash": identity["compatibility_hash"],
        "contract_hash": identity["contract_hash"],
        "old_table_comment": old_comment,
        "old_table_comment_sha256": hashlib.sha256(
            old_comment.encode("utf-8")
        ).hexdigest(),
        "pending_table_comment": pending,
        "pending_table_comment_sha256": hashlib.sha256(
            pending.encode("utf-8")
        ).hexdigest(),
        "verified_table_comment": verified,
        "verified_table_comment_sha256": hashlib.sha256(
            verified.encode("utf-8")
        ).hexdigest(),
        "read_only": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


def _persist_privileged_trigger_inventory_seal(
    boundary: DatabaseBoundary,
    evidence: Mapping[str, Any],
    *,
    build_sha: object,
) -> dict[str, Any]:
    """Seal the verified inventory in runtime-readable permanent metadata.

    The runtime account can insert rows and create temporary tables in
    ``probiga``.  A row in an application table is therefore not a privileged
    proof.  The migrator instead writes a build/server-bound comment to an
    existing permanent InnoDB table while the administrator maintenance lock
    is held and global trigger trust is proven OFF.  Runtime can read that
    permanent metadata through ``information_schema`` but has no ALTER
    authority to forge it; a same-name temporary table cannot shadow it.
    """

    from server.engine.strategy_governance import (
        PRIVILEGED_FULL_TRIGGER_NAMESET_HASH,
        PRIVILEGED_MANAGED_TRIGGER_NAMESET_HASH,
        PRIVILEGED_MANAGED_TRIGGER_SOURCE_CONTRACT_HASH,
        PRIVILEGED_PIT_FACT_SCHEMA_CONTRACT_HASH,
        PRIVILEGED_SUPPORTING_TRIGGER_NAMESET_HASH,
        PRIVILEGED_TRIGGER_SEAL_BOOTSTRAP_PREVIOUS_BUILD_SHA,
        PRIVILEGED_V2_TRIGGER_SOURCE_CONTRACT_HASH,
        compose_privileged_trigger_inventory_seal_comment,
        parse_privileged_trigger_inventory_seal_comment,
        privileged_trigger_inventory_seal_identity,
    )

    if boundary.migrator_engine is None:
        raise PrivilegedSchemaPreparationError("migration engine is unavailable")
    supporting = evidence.get("supporting_trigger_source_contract")
    full = evidence.get("full_trigger_inventory")
    pit = evidence.get("pit_fact_schema")
    trigger = evidence.get("trigger_contract")
    supporting_names = (
        supporting.get("expected_names")
        if isinstance(supporting, Mapping) else None
    )
    supporting_names_hash = (
        hashlib.sha256(json.dumps(
            supporting_names,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        if isinstance(supporting_names, list) else ""
    )
    managed_contracts = {
        **_final_v3_trigger_contracts(),
        **_frozen_non_v3_release_trigger_contracts(
            _non_v3_trigger_contracts()
        ),
    }
    managed_names = sorted(managed_contracts)
    full_names = (
        full.get("expected_names") if isinstance(full, Mapping) else None
    )
    if (
        evidence.get("permission_audit_status") != PERMISSION_AUDIT_STATUS
        or evidence.get("permission_audit_verified") is not False
        or evidence.get("runtime_privilege_boundary_verified") is not False
        or evidence.get("runtime_least_privilege_verified") is not False
        or evidence.get("runtime_legacy_ddl_compatibility") is not False
        or evidence.get("runtime_current_user")
        != EXPECTED_RUNTIME_USER.lower()
        or evidence.get("runtime_session_user")
        != EXPECTED_RUNTIME_USER.lower()
        or evidence.get("runtime_tls_verified") is not True
        or evidence.get("runtime_grant_count") is not None
        or evidence.get("runtime_grant_contract_hash") != ""
        or evidence.get("routine_inventory_audit_status")
        != PERMISSION_AUDIT_STATUS
        or evidence.get("runtime_self_definer_routine_count") is not None
        or evidence.get("migrator_self_definer_routine_count") is not None
        or evidence.get("runtime_definer_routine_count") is not None
        or evidence.get("runtime_definer_routine_inventory_verified") is not False
        or evidence.get("runtime_definer_routine_inventory_complete") is not False
        or evidence.get("runtime_definer_routine_inventory_authority") != ""
        or evidence.get("runtime_definer_routine_inventory_schemas") != []
        or not isinstance(supporting, Mapping)
        or supporting.get("required_count")
        != EXPECTED_NON_V3_RELEASE_TRIGGER_COUNT
        or supporting.get("optional_count") != 0
        or supporting.get("observed_count")
        != EXPECTED_NON_V3_RELEASE_TRIGGER_COUNT
        or supporting.get("source_contract_hash")
        != EXPECTED_NON_V3_RELEASE_TRIGGER_SOURCE_HASH
        or supporting.get("owner_counts")
        != _release_trigger_owner_counts(_non_v3_trigger_contracts())
        or supporting_names != sorted(set(supporting_names or []))
        or len(supporting_names or [])
        != EXPECTED_NON_V3_RELEASE_TRIGGER_COUNT
        or supporting_names_hash
        != PRIVILEGED_SUPPORTING_TRIGGER_NAMESET_HASH
        or not isinstance(trigger, Mapping)
        or trigger.get("required_count")
        != EXPECTED_MANAGED_RELEASE_TRIGGER_COUNT
        or trigger.get("optional_count") != 0
        or trigger.get("observed_count")
        != EXPECTED_MANAGED_RELEASE_TRIGGER_COUNT
        or len(managed_names) != EXPECTED_MANAGED_RELEASE_TRIGGER_COUNT
        or _release_trigger_source_contract_hash(managed_contracts)
        != PRIVILEGED_MANAGED_TRIGGER_SOURCE_CONTRACT_HASH
        or _full_release_trigger_nameset_hash(managed_names)
        != PRIVILEGED_MANAGED_TRIGGER_NAMESET_HASH
        or not isinstance(full, Mapping)
        or full.get("expected_count")
        != EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_COUNT
        or full.get("observed_count")
        != EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_COUNT
        or full.get("managed_count")
        != EXPECTED_MANAGED_RELEASE_TRIGGER_COUNT
        or full.get("managed_source_contract_sha256")
        != PRIVILEGED_MANAGED_TRIGGER_SOURCE_CONTRACT_HASH
        or full.get("v2_source_contract_sha256")
        != PRIVILEGED_V2_TRIGGER_SOURCE_CONTRACT_HASH
        or full_names != sorted(set(full_names or []))
        or len(full_names or [])
        != EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_COUNT
        or not set(managed_names) <= set(full_names or [])
        or _full_release_trigger_nameset_hash(full_names or [])
        != PRIVILEGED_FULL_TRIGGER_NAMESET_HASH
        or full.get("optional_v4_count") != EXPECTED_OPTIONAL_V4_TRIGGER_COUNT
        or full.get("nameset_sha256")
        != PRIVILEGED_FULL_TRIGGER_NAMESET_HASH
        or full.get("metadata_frozen") is not True
        or full.get("read_only") is not True
        or not isinstance(pit, Mapping)
        or pit.get("schema") != "probiga.pit-fact-schema-health.v1"
        or pit.get("status") != "HEALTHY"
        or pit.get("valid") is not True
        or pit.get("table_count") != 3
        or pit.get("trigger_count") != 6
        or pit.get("missing_tables") != []
        or pit.get("missing_columns") != {}
        or pit.get("missing_triggers") != []
        or pit.get("contract_hash")
        != PRIVILEGED_PIT_FACT_SCHEMA_CONTRACT_HASH
        or evidence.get("trust_restoration_verified") is not True
        or evidence.get("runtime_trust_off_verified") is not True
    ):
        raise PrivilegedSchemaPreparationError(
            "privileged trigger inventory cannot be sealed"
        )
    admin: pymysql.Connection | None = None
    lock_acquired = False
    operation_error: BaseException | None = None
    result: dict[str, Any] = {}
    restoration = {
        "restore_primary_verified": False,
        "restore_secondary_verified": False,
        "runtime_trust_off_verified": False,
    }
    lineage = evidence.get("privileged_trigger_inventory_lineage_preflight")
    if not isinstance(lineage, Mapping):
        raise PrivilegedSchemaPreparationError(
            "privileged trigger inventory lineage preflight is unavailable"
        )
    old_comment = str(lineage.get("old_table_comment") or "")
    pending_comment = str(lineage.get("pending_table_comment") or "")
    verified_comment = str(lineage.get("verified_table_comment") or "")
    expected_lineage = {
        "schema": "probiga.privileged-trigger-inventory-lineage-preflight.v1",
        "build_sha": str(build_sha or "").strip().lower(),
        "server_uuid": str(lineage.get("server_uuid") or "").strip().lower(),
        "database_name": "probiga",
        "seal_table": "st_privileged_schema_recovery_evidence",
        "compatibility_hash": str(
            lineage.get("compatibility_hash") or ""
        ).strip().lower(),
        "contract_hash": str(lineage.get("contract_hash") or "").strip().lower(),
        "read_only": True,
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }
    if any(lineage.get(key) != value for key, value in expected_lineage.items()):
        raise PrivilegedSchemaPreparationError(
            "privileged trigger inventory lineage preflight drifted"
        )
    previous_build = str(
        lineage.get("previous_build_sha") or ""
    ).strip().lower()
    legacy_bootstrap = (
        previous_build == PRIVILEGED_TRIGGER_SEAL_BOOTSTRAP_PREVIOUS_BUILD_SHA
    )
    legacy_activation_trigger_upgrade = (
        previous_build
        == LEGACY_ACTIVATION_TRIGGER_UPGRADE_PREVIOUS_BUILD_SHA
    )
    legacy_upgrade_seal_state = lineage.get(
        "legacy_activation_trigger_upgrade_seal_state"
    )
    legacy_upgrade_inventory = lineage.get(
        "legacy_activation_trigger_upgrade_inventory"
    )
    legacy_upgrade_live_state = (
        str(legacy_upgrade_inventory.get("upgrade_live_state") or "")
        if isinstance(legacy_upgrade_inventory, Mapping)
        else ""
    )
    legacy_upgrade_expected_values = {
        "LEGACY_174": (
            LEGACY_ACTIVATION_TRIGGER_UPGRADE_FULL_COUNT,
            LEGACY_ACTIVATION_TRIGGER_UPGRADE_MANAGED_COUNT,
            LEGACY_ACTIVATION_TRIGGER_UPGRADE_FULL_NAMESET_HASH,
            LEGACY_ACTIVATION_TRIGGER_UPGRADE_MANAGED_SOURCE_HASH,
        ),
        "CURRENT_175_RETRY": (
            EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_COUNT,
            EXPECTED_MANAGED_RELEASE_TRIGGER_COUNT,
            EXPECTED_FULL_RELEASE_WITH_V4_TRIGGER_NAMESET_HASH,
            EXPECTED_MANAGED_RELEASE_TRIGGER_SOURCE_HASH,
        ),
    }.get(legacy_upgrade_live_state)
    legacy_upgrade_inventory_is_exact = bool(
        isinstance(legacy_upgrade_inventory, Mapping)
        and legacy_upgrade_expected_values is not None
        and legacy_upgrade_inventory.get("expected_count")
        == legacy_upgrade_expected_values[0]
        and legacy_upgrade_inventory.get("observed_count")
        == legacy_upgrade_expected_values[0]
        and legacy_upgrade_inventory.get("managed_count")
        == legacy_upgrade_expected_values[1]
        and legacy_upgrade_inventory.get("optional_v4_count")
        == EXPECTED_OPTIONAL_V4_TRIGGER_COUNT
        and legacy_upgrade_inventory.get("nameset_sha256")
        == legacy_upgrade_expected_values[2]
        and legacy_upgrade_inventory.get("managed_source_contract_sha256")
        == legacy_upgrade_expected_values[3]
        and legacy_upgrade_inventory.get("metadata_frozen") is True
        and legacy_upgrade_inventory.get("read_only") is True
    )
    if (
        re.fullmatch(r"[0-9a-f]{40}", previous_build) is None
        or lineage.get("legacy_bootstrap") is not legacy_bootstrap
        or lineage.get("legacy_activation_trigger_upgrade")
        is not legacy_activation_trigger_upgrade
        or (
            legacy_activation_trigger_upgrade
            and (
                lineage.get("trusted_verified_trigger_seal_present")
                is not False
                or not legacy_upgrade_inventory_is_exact
                or legacy_upgrade_seal_state not in {
                    "LEGACY_PREDECESSOR",
                    "CURRENT_TARGET_PENDING",
                    "CURRENT_TARGET_VERIFIED",
                }
                or (
                    legacy_upgrade_seal_state
                    != "LEGACY_PREDECESSOR"
                    and legacy_upgrade_live_state != "CURRENT_175_RETRY"
                )
            )
        )
        or (
            not legacy_activation_trigger_upgrade
            and (
                legacy_upgrade_inventory != {}
                or legacy_upgrade_seal_state != "NOT_APPLICABLE"
            )
        )
    ):
        raise PrivilegedSchemaPreparationError(
            "privileged trigger inventory lineage predecessor drifted"
        )
    for field, comment in (
        ("old_table_comment_sha256", old_comment),
        ("pending_table_comment_sha256", pending_comment),
        ("verified_table_comment_sha256", verified_comment),
    ):
        if lineage.get(field) != hashlib.sha256(
            comment.encode("utf-8")
        ).hexdigest():
            raise PrivilegedSchemaPreparationError(
                "privileged trigger inventory lineage hash drifted"
            )
    comment_write_attempted = False
    pending_comment_write_attempted = False
    verified_comment_write_attempted = False
    observed_failure_state = "NOT_APPLICABLE"

    def read_metadata(connection: Connection) -> dict[str, Any]:
        rows = connection.execute(text(
            "SELECT TABLE_SCHEMA AS table_schema, TABLE_NAME AS table_name, "
            "TABLE_TYPE AS table_type, ENGINE AS engine, "
            "TABLE_COMMENT AS table_comment "
            "FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=:table_schema AND TABLE_NAME=:table_name"
        ), {
            "table_schema": "probiga",
            "table_name": "st_privileged_schema_recovery_evidence",
        }).mappings().all()
        if (
            len(rows) != 1
            or str(rows[0].get("table_schema") or "").lower() != "probiga"
            or str(rows[0].get("table_name") or "")
            != "st_privileged_schema_recovery_evidence"
            or str(rows[0].get("table_type") or "").upper() != "BASE TABLE"
            or str(rows[0].get("engine") or "").upper() != "INNODB"
        ):
            raise PrivilegedSchemaPreparationError(
                "privileged trigger seal table metadata is unavailable"
            )
        return dict(rows[0])

    try:
        admin = _connect_admin(boundary)
        admin_state = _read_dbapi_state(admin)
        _validate_target_state(
            admin_state,
            expected_user=EXPECTED_ADMIN_USER,
            require_database=False,
            expected_trust=0,
            require_trigger_session=False,
        )
        lock_acquired = _acquire_lock(admin)
        if not lock_acquired or not _owns_window_lock(admin):
            raise PrivilegedSchemaPreparationError(
                "database trigger maintenance lock is unavailable for sealing"
            )
        current_security = _runtime_least_privilege_evidence(boundary)
        if any(
            evidence.get(key) != value
            for key, value in current_security.items()
        ):
            raise PrivilegedSchemaPreparationError(
                "runtime identity and schema evidence changed before sealing"
            )

        from server.common.pit_facts import pit_fact_schema_health

        with boundary.migrator_engine.connect() as connection:
            identity_rows = connection.execute(text(
                "SELECT @@server_uuid AS server_uuid, "
                "DATABASE() AS database_name"
            )).mappings().all()
            if len(identity_rows) != 1:
                raise PrivilegedSchemaPreparationError(
                    "privileged trigger database identity is unavailable"
                )
            seal = privileged_trigger_inventory_seal_identity(
                build_sha,
                server_uuid=identity_rows[0].get("server_uuid"),
                database_name=identity_rows[0].get("database_name"),
            )
            metadata = read_metadata(connection)
            observed_old_comment = str(metadata.get("table_comment") or "")
            observed_legacy_upgrade_seal_state = "NOT_APPLICABLE"
            if legacy_activation_trigger_upgrade:
                try:
                    observed_legacy_upgrade_seal_state = (
                        _legacy_activation_trigger_upgrade_seal_state(
                            parse_privileged_trigger_inventory_seal_comment(
                                old_comment,
                                expected_server_uuid=seal["server_uuid"],
                            ),
                            identity=seal,
                        )
                    )
                except (
                    PrivilegedSchemaPreparationError,
                    TypeError,
                    ValueError,
                ) as exc:
                    raise PrivilegedSchemaPreparationError(
                        "legacy activation-trigger lineage cannot be reproduced"
                    ) from exc
            lineage_previous = (
                ""
                if (
                    legacy_bootstrap
                    or legacy_activation_trigger_upgrade
                )
                else previous_build
            )
            try:
                recomputed_pending_comment = (
                    compose_privileged_trigger_inventory_seal_comment(
                        seal,
                        previous_comment=old_comment,
                        previous_build_sha=lineage_previous,
                        status="PENDING",
                    )
                )
                recomputed_verified_comment = (
                    compose_privileged_trigger_inventory_seal_comment(
                        seal,
                        previous_comment=old_comment,
                        previous_build_sha=lineage_previous,
                        status="VERIFIED",
                    )
                )
            except (TypeError, ValueError) as exc:
                raise PrivilegedSchemaPreparationError(
                    "privileged trigger inventory lineage cannot be reproduced"
                ) from exc
            live_trigger = validate_release_trigger_contracts(
                connection,
                required=managed_contracts,
                optional={},
                controlled_contracts=managed_contracts,
            )
            live_full = validate_full_database_trigger_inventory(
                connection,
                managed_contracts=managed_contracts,
                include_applied_v4=True,
            )
            live_pit = pit_fact_schema_health(connection)
            if (
                observed_old_comment != old_comment
                or observed_legacy_upgrade_seal_state
                != legacy_upgrade_seal_state
                or seal["build_sha"] != lineage.get("build_sha")
                or seal["server_uuid"] != lineage.get("server_uuid")
                or seal["database_name"] != lineage.get("database_name")
                or seal["seal_table"] != lineage.get("seal_table")
                or seal["compatibility_hash"]
                != lineage.get("compatibility_hash")
                or seal["contract_hash"] != lineage.get("contract_hash")
                or pending_comment != recomputed_pending_comment
                or verified_comment != recomputed_verified_comment
                or live_trigger != trigger
                or live_full != full
                or live_pit != pit
                or not _owns_window_lock(admin)
            ):
                raise PrivilegedSchemaPreparationError(
                    "privileged trigger inventory changed before sealing"
                )
            intermediate_comment = verified_comment
            if observed_old_comment != verified_comment:
                # Mark the attempt before ALTER.  MySQL DDL may commit on the
                # server and still surface a client timeout; PENDING is a
                # durable fail-closed journal state in that case.
                comment_write_attempted = True
                pending_comment_write_attempted = True
                connection.execute(text(
                    "ALTER TABLE "
                    "`probiga`.`st_privileged_schema_recovery_evidence` "
                    "COMMENT=:table_comment"
                ), {"table_comment": pending_comment})
                intermediate_comment = pending_comment
            sealed_metadata = read_metadata(connection)
            post_trigger = validate_release_trigger_contracts(
                connection,
                required=managed_contracts,
                optional={},
                controlled_contracts=managed_contracts,
            )
            post_full = validate_full_database_trigger_inventory(
                connection,
                managed_contracts=managed_contracts,
                include_applied_v4=True,
            )
            post_pit = pit_fact_schema_health(connection)
            if (
                str(sealed_metadata.get("table_comment") or "")
                != intermediate_comment
                or post_trigger != trigger
                or post_full != full
                or post_pit != pit
                or not _owns_window_lock(admin)
            ):
                raise PrivilegedSchemaPreparationError(
                    "privileged trigger inventory changed while sealing"
                )
            final_security = _runtime_least_privilege_evidence(boundary)
            if (
                final_security != current_security
                or not _owns_window_lock(admin)
            ):
                raise PrivilegedSchemaPreparationError(
                    "runtime identity and schema evidence changed while sealing"
                )
            if intermediate_comment != verified_comment:
                verified_comment_write_attempted = True
                comment_write_attempted = True
                connection.execute(text(
                    "ALTER TABLE "
                    "`probiga`.`st_privileged_schema_recovery_evidence` "
                    "COMMENT=:table_comment"
                ), {"table_comment": verified_comment})
            verified_metadata = read_metadata(connection)
            if (
                str(verified_metadata.get("table_comment") or "")
                != verified_comment
                or not _owns_window_lock(admin)
            ):
                raise PrivilegedSchemaPreparationError(
                    "privileged trigger inventory verification seal drifted"
                )
            seal_envelope = parse_privileged_trigger_inventory_seal_comment(
                verified_comment,
                expected_server_uuid=seal["server_uuid"],
            )
            result = {
                "schema": seal["schema"],
                "authority": "PRIVILEGED_CUTOVER_TABLE_METADATA_SEAL",
                "attested_build_sha": seal["build_sha"],
                "trigger_inventory_seal_database": seal["database_name"],
                "trigger_inventory_seal_table": seal["seal_table"],
                "trigger_inventory_server_uuid": seal["server_uuid"],
                "trigger_inventory_contract_hash": seal["contract_hash"],
                "trigger_inventory_table_comment": verified_comment,
                "trigger_inventory_candidate_build_sha": (
                    seal_envelope["candidate_build_sha"]
                ),
                "trigger_inventory_rollback_build_sha": (
                    seal_envelope["rollback_build_sha"]
                ),
                "trigger_inventory_entry_count": len(seal_envelope["entries"]),
                "supporting_trigger_count": 82,
                "managed_trigger_count": 102,
                "managed_trigger_source_contract_hash": (
                    PRIVILEGED_MANAGED_TRIGGER_SOURCE_CONTRACT_HASH
                ),
                "managed_trigger_nameset_hash": (
                    PRIVILEGED_MANAGED_TRIGGER_NAMESET_HASH
                ),
                "v2_trigger_source_contract_hash": (
                    PRIVILEGED_V2_TRIGGER_SOURCE_CONTRACT_HASH
                ),
                "full_trigger_count": 175,
                "full_trigger_nameset_hash": (
                    PRIVILEGED_FULL_TRIGGER_NAMESET_HASH
                ),
                "pit_fact_trigger_count": 6,
                "pit_fact_schema_contract_hash": (
                    PRIVILEGED_PIT_FACT_SCHEMA_CONTRACT_HASH
                ),
                "permission_audit_status": PERMISSION_AUDIT_STATUS,
                "permission_audit_verified": False,
                "runtime_least_privilege_verified": False,
                "runtime_current_user": EXPECTED_RUNTIME_USER.lower(),
                "runtime_session_user": EXPECTED_RUNTIME_USER.lower(),
                "runtime_tls_verified": True,
                "runtime_grant_count": None,
                "runtime_grant_contract_hash": "",
                "routine_inventory_audit_status": PERMISSION_AUDIT_STATUS,
                "runtime_self_definer_routine_count": None,
                "migrator_self_definer_routine_count": None,
                "runtime_definer_routine_count": None,
                "runtime_definer_routine_inventory_verified": False,
                "runtime_definer_routine_inventory_complete": False,
                "runtime_definer_routine_inventory_authority": "",
                "runtime_definer_routine_inventory_schemas": [],
                "maintenance_lock_verified": True,
                "trust_off_verified": True,
                "automatic_real_order_submission": False,
                "real_order_authority": False,
            }
    except BaseException as exc:
        operation_error = exc
        if comment_write_attempted:
            if admin is None or not _owns_window_lock(admin):
                observed_failure_state = "LOCK_LOST_UNOBSERVED"
            else:
                try:
                    with boundary.migrator_engine.connect() as observe_connection:
                        observed = str(
                            read_metadata(observe_connection).get(
                                "table_comment"
                            ) or ""
                        )
                    if observed == old_comment:
                        observed_failure_state = "OLD"
                    elif observed == pending_comment:
                        observed_failure_state = "PENDING"
                    elif observed == verified_comment:
                        observed_failure_state = "VERIFIED"
                    else:
                        observed_failure_state = "THIRD_PARTY_OR_DRIFT"
                        operation_error = PrivilegedSchemaPreparationError(
                            "privileged trigger inventory failure state drifted"
                        )
                except Exception:
                    observed_failure_state = "OBSERVATION_FAILED"
    finally:
        if admin is not None:
            try:
                restoration = _restore_and_double_verify(boundary, admin)
            except Exception:
                restoration = {
                    "restore_primary_verified": False,
                    "restore_secondary_verified": False,
                    "runtime_trust_off_verified": False,
                }
        if lock_acquired and admin is not None:
            _release_lock(admin)
        _close_quietly(admin)

    safety = {
        **restoration,
        "trust_restoration_verified": all(restoration.values()),
        "maintenance_lock_acquired": lock_acquired,
        "metadata_comment_changed": comment_write_attempted,
        "metadata_comment_write_attempted": comment_write_attempted,
        "metadata_pending_write_attempted": pending_comment_write_attempted,
        "metadata_verified_write_attempted": verified_comment_write_attempted,
        "metadata_failure_observed_state": observed_failure_state,
        "metadata_rollback_ddl_attempted": False,
    }
    if not all(restoration.values()):
        raise PrivilegedSchemaPreparationError(
            "could not prove global trigger trust is OFF after sealing",
            safety_evidence=safety,
        ) from operation_error
    if operation_error is not None:
        if isinstance(operation_error, PrivilegedSchemaPreparationError):
            operation_error.safety_evidence.update(safety)
            raise operation_error
        raise PrivilegedSchemaPreparationError(
            "privileged trigger inventory sealing failed",
            safety_evidence=safety,
        ) from operation_error
    return {
        **result,
        **safety,
    }


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
        lock_acquired = _acquire_lock(admin)
        if not lock_acquired:
            raise PrivilegedSchemaPreparationError(
                "database trigger maintenance lock is busy"
            )
        runtime_security = _runtime_least_privilege_evidence(boundary)
        trigger_inventory_lineage = (
            _privileged_trigger_inventory_lineage_preflight(
                boundary,
                build_sha=os.environ.get("PROBIGA_EXPECTED_GIT_SHA", ""),
                previous_build_sha=os.environ.get(
                    "PROBIGA_PREVIOUS_GIT_SHA", ""
                ),
            )
        )
        from server.db.migrations_v3 import run_v3_migrations
        from server.engine.strategy_governance import (
            ensure_strategy_governance_tables,
        )
        from server.engine.dynamic_shadow_ledger_schema import (
            validate_dynamic_shadow_ledger_schema,
        )
        from server.common.pit_facts import ensure_pit_fact_schema
        from server.common.scheduler_runtime_schema import (
            migrate_scheduler_runtime_heartbeat,
            validate_scheduler_runtime_heartbeat_schema,
        )
        from server.common.scheduler_task_history_schema import (
            migrate_scheduler_task_history,
            validate_scheduler_task_history_schema,
        )
        from server.common.schema_recovery_evidence import (
            ensure_evidence_table,
            validate_recovery_evidence_schema,
        )
        from server.common.production_runtime_schema_bundle import (
            privileged_migrate_runtime_schema_bundle,
            validate_runtime_schema_bundle,
        )
        from server.common.qmt_history_coverage import (
            validate_coverage_schema,
        )
        from tools.attest_qmt_daily_kline import (
            privileged_migrate_attestation_tables,
            validate_attestation_schema,
        )
        from tools.prepare_strategy_governance_qmt_history import (
            apply_legacy_completed_run_binding,
        )
        from tools.sync_guojin_qmt_reference_data import (
            attest_prepared_reference_schema,
            validate_reference_tables,
        )

        non_v3_contracts = _frozen_non_v3_release_trigger_contracts(
            _non_v3_trigger_contracts()
        )
        governance_release_contracts = (
            _frozen_governance_release_trigger_contracts(
                non_v3_contracts
            )
        )
        final_contracts = {
            **_final_v3_trigger_contracts(),
            **non_v3_contracts,
        }
        trigger_create_allowlist = (
            *_all_v3_trigger_contracts(),
            *non_v3_contracts.values(),
        )
        trigger_ddl_executor = _build_trigger_ddl_executor(
            boundary,
            admin,
            trigger_create_allowlist,
            window_evidence,
        )
        evidence_trigger_contracts = {
            name: contract for name, contract in non_v3_contracts.items()
            if contract.owner == "schema_recovery_evidence"
        }
        with boundary.migrator_engine.begin() as connection:
            ensure_evidence_table(connection, require_triggers=False)
        evidence_trigger_source_detail = _ensure_frozen_release_triggers(
            boundary.migrator_engine,
            evidence_trigger_contracts,
            expected_names=frozenset(evidence_trigger_contracts),
            expected_source_contract_hash=(
                EXPECTED_SCHEMA_RECOVERY_EVIDENCE_TRIGGER_SOURCE_HASH
            ),
            trigger_ddl_executor=trigger_ddl_executor,
        )
        evidence_schema_detail = validate_recovery_evidence_schema(
            boundary.migrator_engine
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
            **non_v3_contracts,
        }
        if planned_final_contracts != final_contracts:
            raise PrivilegedSchemaPreparationError(
                "release trigger plan differs from frozen final contracts"
            )

        migrations = run_v3_migrations(
            boundary.migrator_engine,
            trigger_ddl_executor=trigger_ddl_executor,
        )
        scheduler_runtime_schema_migration = (
            migrate_scheduler_runtime_heartbeat(boundary.migrator_engine)
        )
        scheduler_runtime_schema_validation = (
            validate_scheduler_runtime_heartbeat_schema(
                boundary.migrator_engine
            )
        )
        scheduler_task_history_schema_migration = (
            migrate_scheduler_task_history(boundary.migrator_engine)
        )
        scheduler_task_history_schema_validation = (
            validate_scheduler_task_history_schema(
                boundary.migrator_engine
            )
        )
        runtime_schema_bundle = privileged_migrate_runtime_schema_bundle(
            boundary.migrator_engine,
            defer_trigger_validation=True,
        )
        qmt_reference_schema = _prepare_qmt_reference_schema_tables(
            boundary.migrator_engine
        )
        qmt_history_coverage_schema = (
            _prepare_qmt_history_coverage_schema_tables(
                boundary.migrator_engine
            )
        )
        privileged_migrate_attestation_tables(
            boundary.migrator_engine,
            trigger_ddl_executor=trigger_ddl_executor,
            allow_legacy_manifest_candidates=True,
        )
        pit_fact_schema = ensure_pit_fact_schema(
            boundary.migrator_engine,
            trigger_ddl_executor=trigger_ddl_executor,
        )
        legacy_binding = apply_legacy_completed_run_binding(
            boundary.migrator_engine
        )
        # Commit every governance table/data migration before opening any
        # CREATE TRIGGER window.  The trigger executor uses a separate,
        # narrowly authenticated connection; invoking it from inside the
        # governance transaction would make that connection wait on our own
        # metadata locks until MySQL's lock timeout expires.
        ensure_strategy_governance_tables(
            engine=boundary.migrator_engine,
            writers_fenced=True,
            base_schema_only=True,
        )
        supporting_trigger_source_detail = (
            _ensure_frozen_release_triggers(
                boundary.migrator_engine,
                non_v3_contracts,
                expected_names=frozenset(non_v3_contracts),
                expected_source_contract_hash=(
                    EXPECTED_NON_V3_RELEASE_TRIGGER_SOURCE_HASH
                ),
                trigger_ddl_executor=trigger_ddl_executor,
                trusted_verified_trigger_seal_present=(
                    trigger_inventory_lineage.get(
                        "trusted_verified_trigger_seal_present"
                    )
                    is True
                ),
            )
        )
        supporting_trigger_source_detail = {
            **supporting_trigger_source_detail,
            "owner_counts": _release_trigger_owner_counts(non_v3_contracts),
        }
        runtime_schema_bundle = {
            **runtime_schema_bundle,
            "runtime_validation": validate_runtime_schema_bundle(
                boundary.migrator_engine
            ),
            "trigger_validation_deferred": False,
        }
        governance_trigger_source_detail = (
            _ensure_frozen_release_triggers(
                boundary.migrator_engine,
                governance_release_contracts,
                expected_names=EXPECTED_GOVERNANCE_TRIGGER_NAMES,
                expected_source_contract_hash=(
                    EXPECTED_GOVERNANCE_RELEASE_TRIGGER_SOURCE_HASH
                ),
                trigger_ddl_executor=trigger_ddl_executor,
            )
        )
        # The frozen release pass above has installed and validated every
        # governance trigger outside the base-schema transaction.  Re-enter
        # the idempotent full path only to seal the full migration markers and
        # validate the already-present trigger contract; no trigger DDL is
        # allowed or required in this transaction.
        ensure_strategy_governance_tables(
            engine=boundary.migrator_engine,
            writers_fenced=True,
        )
        qmt_reference_seal = attest_prepared_reference_schema(
            boundary.migrator_engine
        )
        validate_reference_tables(
            boundary.migrator_engine,
            verify_triggers=True,
        )
        with boundary.migrator_engine.connect() as connection:
            qmt_history_coverage_seal = validate_coverage_schema(
                connection,
                require_triggers=True,
            )
            dynamic_shadow_schema = validate_dynamic_shadow_ledger_schema(
                connection
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
            **_frozen_non_v3_release_trigger_contracts(
                _non_v3_trigger_contracts()
            ),
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
            full_trigger_inventory = (
                validate_full_database_trigger_inventory(
                    connection,
                    managed_contracts=final_contracts,
                    include_applied_v4=True,
                )
            )
        final_runtime_security = _runtime_least_privilege_evidence(boundary)
        if final_runtime_security != runtime_security:
            raise PrivilegedSchemaPreparationError(
                "runtime identity and schema evidence changed during cutover"
            )
        detail = {
            **final_runtime_security,
            "privileged_trigger_inventory_lineage_preflight": (
                trigger_inventory_lineage
            ),
            "v3_migrations": [
                {
                    "version": item.version,
                    "status": item.status,
                    "statement_count": item.statement_count,
                }
                for item in migrations
            ],
            "trigger_contract": trigger_detail,
            "full_trigger_inventory": full_trigger_inventory,
            "governance_trigger_source_contract": (
                governance_trigger_source_detail
            ),
            "supporting_trigger_source_contract": (
                supporting_trigger_source_detail
            ),
            "schema_recovery_evidence": {
                **evidence_schema_detail,
                "trigger_source_contract": evidence_trigger_source_detail,
            },
            "rehomed_legacy_triggers": list(rehomed),
            "legacy_binding_plan": {
                key: value for key, value in legacy_binding.items()
                if key != "legacy_bindings"
            },
            "legacy_trigger_repair": legacy_trigger_repair,
            "dynamic_shadow_schema": dynamic_shadow_schema,
            "pit_fact_schema": pit_fact_schema,
            "qmt_reference_schema": {
                **qmt_reference_schema,
                **qmt_reference_seal,
            },
            "qmt_history_coverage_schema": {
                **qmt_history_coverage_schema,
                **qmt_history_coverage_seal,
            },
            "scheduler_runtime_heartbeat_schema": {
                **scheduler_runtime_schema_migration,
                "runtime_validation": scheduler_runtime_schema_validation,
            },
            "scheduler_task_history_schema": {
                **scheduler_task_history_schema_migration,
                "runtime_validation": scheduler_task_history_schema_validation,
            },
            "runtime_schema_bundle": runtime_schema_bundle,
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
    from server.common.pit_facts import pit_fact_schema_health
    from server.common.qmt_history_coverage import validate_coverage_schema
    from server.common.scheduler_task_history_schema import (
        validate_scheduler_task_history_schema,
    )
    from server.common.production_runtime_schema_bundle import (
        validate_runtime_schema_bundle,
    )
    from server.engine.strategy_governance import (
        EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES as core_append_names,
        EXPECTED_METRIC_INPUT_REVIEW_TRIGGER_NAMES as core_metric_names,
        GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACT_HASH as core_contract_hash,
        METRIC_INPUT_REVIEW_TRIGGER_CONTRACT_HASH as core_metric_contract_hash,
        seed_governance_registry,
        validate_default_governance_seed_contract,
        validate_governance_append_only_triggers,
        validate_governance_table_schema,
        validate_metric_input_review_triggers,
    )
    from tools.sync_guojin_qmt_reference_data import (
        REFERENCE_SCHEMA_CONTRACT_HASH,
        validate_reference_tables,
    )

    try:
        seed_governance_registry()
        api_engine = get_engine()
        metadata_engine = boundary.migrator_engine
        pit_runtime_schema = pit_fact_schema_health(metadata_engine)
        if not bool(pit_runtime_schema.get("valid")):
            raise PrivilegedSchemaPreparationError(
                "PIT fact schema runtime validation failed"
            )
        validate_reference_tables(api_engine, verify_triggers=False)
        validate_reference_tables(metadata_engine, verify_triggers=True)
        scheduler_task_history_runtime_schema = (
            validate_scheduler_task_history_schema(api_engine)
        )
        runtime_schema_bundle_validation = validate_runtime_schema_bundle(
            metadata_engine
        )
        with api_engine.connect() as runtime_connection:
            governance_schema = validate_governance_table_schema(
                runtime_connection
            )
        with metadata_engine.connect() as metadata_connection:
            qmt_history_coverage_runtime_schema = validate_coverage_schema(
                metadata_connection,
                require_triggers=True,
            )
            metric = validate_metric_input_review_triggers(
                metadata_connection
            )
            funding_schema = validate_strategy_funding_checkpoint_schema(
                metadata_connection
            )
        append_only = validate_governance_append_only_triggers(
            metadata_engine
        )
        seed_contract = validate_default_governance_seed_contract(
            api_engine,
            require_initial_shadow=True,
        )
        metric_trigger_count = int(metric.get("trigger_count") or 0)
        append_only_trigger_count = int(
            append_only.get("trigger_count") or 0
        )
        funding_trigger_count = int(
            funding_schema.get("trigger_count") or 0
        )
        if (
            metric_trigger_count != 2
            or append_only_trigger_count != 38
            or funding_trigger_count != 4
            or set(metric.get("trigger_names") or ())
            != EXPECTED_METRIC_INPUT_REVIEW_TRIGGER_NAMES
            or set(append_only.get("trigger_names") or ())
            != EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES
            or core_metric_names
            != EXPECTED_METRIC_INPUT_REVIEW_TRIGGER_NAMES
            or core_append_names
            != EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES
            or core_contract_hash
            != EXPECTED_CORE_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
            or core_metric_contract_hash
            != EXPECTED_CORE_METRIC_INPUT_REVIEW_CONTRACT_HASH
            or str(append_only.get("contract_hash") or "")
            != EXPECTED_CORE_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
            or str(metric.get("contract_hash") or "")
            != EXPECTED_CORE_METRIC_INPUT_REVIEW_CONTRACT_HASH
            or str(funding_schema.get("contract_hash") or "")
            != EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH
        ):
            raise PrivilegedSchemaPreparationError(
                "strategy governance exact 40-trigger contract differs"
            )
        detail.update(
            {
                **governance_schema,
                **seed_contract,
                "funding_checkpoint_schema": funding_schema,
                "funding_checkpoint_contract_hash": str(
                    funding_schema["contract_hash"]
                ),
                "funding_checkpoint_table_count": int(
                    funding_schema["table_count"]
                ),
                "funding_checkpoint_trigger_count": int(
                    funding_schema["trigger_count"]
                ),
                "governance_append_only_trigger_count": (
                    append_only_trigger_count
                ),
                "governance_metric_review_trigger_count": (
                    metric_trigger_count
                ),
                "governance_trigger_count": (
                    metric_trigger_count + append_only_trigger_count
                ),
                "governance_trigger_source_contract_hash": (
                    EXPECTED_GOVERNANCE_RELEASE_TRIGGER_SOURCE_HASH
                ),
                "governance_append_only_physical_contract_hash": (
                    EXPECTED_GOVERNANCE_APPEND_ONLY_PHYSICAL_CONTRACT_HASH
                ),
                "governance_metric_review_physical_contract_hash": (
                    EXPECTED_METRIC_REVIEW_PHYSICAL_CONTRACT_HASH
                ),
                "governance_append_only_core_contract_hash": (
                    EXPECTED_CORE_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
                ),
                "governance_metric_review_core_contract_hash": (
                    EXPECTED_CORE_METRIC_INPUT_REVIEW_CONTRACT_HASH
                ),
                "pit_fact_schema": pit_runtime_schema,
                "qmt_reference_contract_hash": (
                    REFERENCE_SCHEMA_CONTRACT_HASH
                ),
                "qmt_reference_runtime_valid": True,
                "qmt_history_coverage_runtime_schema": (
                    qmt_history_coverage_runtime_schema
                ),
                "scheduler_task_history_runtime_schema": (
                    scheduler_task_history_runtime_schema
                ),
                "runtime_schema_bundle_validation": (
                    runtime_schema_bundle_validation
                ),
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
    detail["privileged_trigger_inventory_seal"] = (
        _persist_privileged_trigger_inventory_seal(
            boundary,
            detail,
            build_sha=os.environ.get("PROBIGA_EXPECTED_GIT_SHA", ""),
        )
    )
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
    if phase == "preflight":
        with _preflight_diagnostic_scope("project_environment"):
            load_project_env()
        with _preflight_diagnostic_scope("database_boundary"):
            boundary = _open_boundary(
                include_migrator=True,
                expected_trust=0,
            )
    else:
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
    substage = PREFLIGHT_UNCLASSIFIED_STAGE
    reason_code = PREFLIGHT_UNCLASSIFIED_REASON_CODE
    if isinstance(exc, PrivilegedSchemaPreparationError):
        expected_code = PREFLIGHT_STAGE_REASON_CODES.get(
            str(exc.preflight_substage or "")
        )
        if expected_code is not None and exc.reason_code == expected_code:
            substage = str(exc.preflight_substage)
            reason_code = str(exc.reason_code)
    return {
        "status": "blocked",
        "phase": phase,
        "reason": "database schema preparation failed closed",
        "diagnostic_schema": PREFLIGHT_DIAGNOSTIC_SCHEMA,
        "preflight_substage": substage,
        "reason_code": reason_code,
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
