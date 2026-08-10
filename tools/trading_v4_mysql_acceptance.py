"""Opt-in MySQL acceptance runner for the isolated V4 control-plane schema.

This command deliberately refuses the normal ``MYSQL_URL`` and
``DATABASE_URL``.  An operator must provide an empty, dedicated database
through ``V4_TEST_MYSQL_URL`` (or another explicitly named V4 test/CI
environment variable), and the database name must clearly be a V4 test/CI
database.  The runner never drops tables or data.  Its default mode performs
exactly one initial migration before checking serial and concurrent replays;
an explicit concurrent-initial mode uses a separate empty database to test
first-writer serialization without ever cleaning or reusing a schema.
"""
from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from decimal import Decimal
import sys
from threading import Barrier, Event
import time
from typing import Callable, TypeVar
import uuid


# ``python tools/trading_v4_mysql_acceptance.py`` sets sys.path[0] to the
# tools directory.  Add the repository root so the documented direct command
# resolves the local ``server`` package without relying on an ambient
# PYTHONPATH.  This does not connect to a database or weaken any URL guard.
_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(_REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPOSITORY_ROOT))

from sqlalchemy import event, text
from sqlalchemy.engine import Connection, Engine, make_url
from sqlalchemy.exc import ArgumentError, DBAPIError, OperationalError

from server.common.mysql_version_policy import (
    is_isolated_acceptance_version,
    is_oracle_mysql_distribution,
    isolated_acceptance_versions_label,
)
from server.db.migrations_v4 import (
    MIGRATIONS,
    MIGRATION_TABLE_DDL,
    V4MigrationResult,
    _mysql_regexp_compatible_statement,
    _trigger_body_from_ddl,
    run_v4_migrations,
)
from tools.mysql_acceptance_tls import (
    MySQLAcceptanceTLSConfig,
    create_mysql_acceptance_engine as create_tool_engine,
    resolve_mysql_acceptance_tls_config,
)
from server.trading_v4.domain import (
    DataManifest,
    DecisionClock,
    DecisionContext,
    QualityStatus,
    SourceWatermark,
)
from server.trading_v4.infrastructure.repository import (
    HeadPublishConflictError,
    TradingV4Repository,
)
from server.trading_v4.infrastructure.job_store import (
    EXHAUSTED_LEASE_ERROR_CODE,
    JOB_FAILED,
    JOB_LEASE_MAX_DURATION_SECONDS,
    JOB_SUCCEEDED,
    JobAlreadyTerminalError,
    JobConflictError,
    JobStoreRepository,
)


DEFAULT_URL_ENV = "V4_TEST_MYSQL_URL"
DEFAULT_SERVER_UUID_ENV = "V4_TEST_MYSQL_SERVER_UUID"
DEFAULT_SSL_CA_ENV = "V4_TEST_MYSQL_SSL_CA"
_SAFE_URL_ENV_RE = re.compile(
    r"^V4_(?:TEST|CI)(?:_[A-Z0-9]+)*_MYSQL_URL$",
)
_SAFE_SERVER_UUID_ENV_RE = re.compile(
    r"^V4_(?:TEST|CI)(?:_[A-Z0-9]+)*_MYSQL_SERVER_UUID$",
)
_FORBIDDEN_URL_ENVS = frozenset({"MYSQL_URL", "DATABASE_URL"})
_SAFE_DATABASE_RE = re.compile(
    r"^[a-z0-9]+(?:_[a-z0-9]+)*_v4_(?:test|ci)(?:_[a-z0-9]+)*$",
    re.IGNORECASE,
)
_CANONICAL_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)
_GRANT_RE = re.compile(
    r"^GRANT\s+(?P<privileges>.+?)\s+ON\s+(?P<target>.+?)\s+TO\s+",
    re.IGNORECASE,
)
_REQUIRED_SCHEMA_PRIVILEGES = frozenset(
    {
        "ALTER",
        "SELECT",
        "INSERT",
        "UPDATE",
        "CREATE",
        "REFERENCES",
        "TRIGGER",
    }
)
_CREATE_TABLE_RE = re.compile(
    r"\bCREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+`?([a-z0-9_]+)`?",
    re.IGNORECASE,
)
_CREATE_TRIGGER_RE = re.compile(
    r"\bCREATE\s+TRIGGER\s+`?(?P<name>[a-z0-9_]+)`?\s+"
    r"BEFORE\s+(?P<event>INSERT|UPDATE|DELETE)\s+ON\s+"
    r"`?(?P<table>[a-z0-9_]+)`?\s+FOR\s+EACH\s+ROW\b",
    re.IGNORECASE,
)
V4_CONTROL_PLANE_TABLES = frozenset(
    {
        "schema_migration_v4",
        "st_decision_context_v4",
        "st_source_watermark_v4",
        "st_decision_run_v4",
        "st_job_run_v4",
        "st_job_claim_token_v4",
        "st_decision_channel_head_v4",
        "st_runtime_control_v4",
        "st_runtime_control_transition_v4",
        "st_data_source_certification_v4",
        "st_factor_definition_v4",
        "st_entity_feature_snapshot_v4",
    }
)
JOB_LEASE_ACCEPTANCE_MAX_TRANSIENT_ATTEMPTS = 4
_JOB_LEASE_TRANSIENT_CODES = frozenset({1205, 1213})
_T = TypeVar("_T")


def _statement_checksum(statements: tuple[str, ...]) -> str:
    payload = "\n".join(item.strip() for item in statements).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


_DECLARED_MIGRATIONS = tuple(
    (
        str(item["version"]),
        _statement_checksum(tuple(item["statements"])),
        len(tuple(item["statements"])),
    )
    for item in MIGRATIONS
)
FROZEN_EXPECTED_MIGRATIONS = (
    (
        "20260803_001_trading_v4_control_plane",
        "49887b8222632a4770fc53f84d4104d425852bb9ba40267f3fd2f4f12863a0ec",
        7,
    ),
    (
        "20260804_002_v4_job_lease_repair",
        "d8affcd4f94a14709133b61fc0c87275c55c8067d985412c495f95d466c008a2",
        6,
    ),
    (
        "20260804_003_v4_claim_token_registry",
        "1ef8d2e44ef17c8b419e737d737e575dad736b04c0b9e7a9ebbf3b0840902c8e",
        4,
    ),
    (
        "20260804_004_v4_control_plane_guards",
        "3b554ef9a8b637706c6d641bfc2b07c329498fe7a743e38967aa3510a6c79b02",
        16,
    ),
    (
        "20260804_005_v4_pit_factor_registry",
        "14b5b8b2eba30739c897b7c4bb9ba33ab44e132604bcda589f4636c63b5c74db",
        3,
    ),
    (
        "20260804_006_v4_pit_factor_guards",
        "2df39d8dc3cda258a582bb45e1a66b770f402affe14b08d4cdf27b6b232818a0",
        9,
    ),
    (
        "20260804_007_v4_factor_lineage",
        "c39a1fccb2228d55eded94e08c846229764f9766f0786a97279a3112bbf4e75f",
        5,
    ),
)
FROZEN_JOB_LEASE_COLUMNS = (
    (
        "lease_token",
        "char(64)",
        "YES",
        None,
        "utf8mb4",
        "utf8mb4_bin",
        "",
    ),
    (
        "max_attempts",
        "int(10) unsigned",
        "NO",
        "3",
        None,
        None,
        "",
    ),
)
FROZEN_JOB_LEASE_INDEXES = (
    (
        "idx_v4_job_claim_due",
        False,
        (
            "status",
            "next_attempt_at",
            "lease_until",
            "scheduled_for",
            "attempt_count",
            "max_attempts",
            "job_id",
        ),
        (None, None, None, None, None, None, None),
        ("A", "A", "A", "A", "A", "A", "A"),
        "BTREE",
    ),
    (
        "uk_v4_job_lease_token",
        True,
        ("lease_token",),
        (None,),
        ("A",),
        "BTREE",
    ),
)
FROZEN_JOB_LEASE_TRIGGERS = (
    (
        "trg_v4_job_lease_bi",
        "INSERT",
        "BEFORE",
        1,
        "14efff90a577070cef900718743e748fff2cea98eba39277967df62b5fed6987",
    ),
    (
        "trg_v4_job_lease_bu",
        "UPDATE",
        "BEFORE",
        1,
        "ea8fe6af7ddfcd7753cc6b2f79fe70c16c7f5ed37fe4d2beaf8ae0c8e4fffc46",
    ),
)
FROZEN_CLAIM_TOKEN_COLUMNS = (
    ("attempt_count", "int(10) unsigned", "NO", None, None, None, ""),
    ("claimed_at", "datetime(6)", "NO", None, None, None, ""),
    (
        "job_id",
        "varchar(64)",
        "NO",
        None,
        "utf8mb4",
        "utf8mb4_bin",
        "",
    ),
    (
        "lease_owner",
        "varchar(160)",
        "NO",
        None,
        "utf8mb4",
        "utf8mb4_bin",
        "",
    ),
    (
        "lease_token",
        "char(64)",
        "NO",
        None,
        "utf8mb4",
        "utf8mb4_bin",
        "",
    ),
    ("lease_until", "datetime(6)", "NO", None, None, None, ""),
)
FROZEN_CLAIM_TOKEN_INDEXES = (
    (
        "PRIMARY",
        True,
        ("lease_token",),
        (None,),
        ("A",),
        "BTREE",
    ),
    (
        "uk_v4_job_claim_attempt",
        True,
        ("job_id", "attempt_count"),
        (None, None),
        ("A", "A"),
        "BTREE",
    ),
)
FROZEN_CLAIM_TOKEN_CONSTRAINTS = (
    (
        "fk_v4_job_claim_token_job",
        "job_id",
        "st_job_run_v4",
        "job_id",
        "RESTRICT",
    ),
)
FROZEN_CLAIM_TOKEN_TRIGGERS = (
    (
        "trg_v4_job_claim_token_bd",
        "DELETE",
        "BEFORE",
        1,
        "fb4f1e4cfcb0191014fcb4767b635d54e03d5ecdf96c3a84784f6f9e7d6bd0a9",
    ),
    (
        "trg_v4_job_claim_token_bi",
        "INSERT",
        "BEFORE",
        1,
        "c4bb4ae4f56caf5eab8c8345085e6857712fbf21da000c2a6e1bb6111ca303cc",
    ),
    (
        "trg_v4_job_claim_token_bu",
        "UPDATE",
        "BEFORE",
        1,
        "fb4f1e4cfcb0191014fcb4767b635d54e03d5ecdf96c3a84784f6f9e7d6bd0a9",
    ),
)
FROZEN_CONTROL_GUARD_TABLES = (
    "st_decision_channel_head_v4",
    "st_decision_context_v4",
    "st_decision_run_v4",
    "st_runtime_control_transition_v4",
    "st_runtime_control_v4",
    "st_source_watermark_v4",
)
FROZEN_CONTROL_GUARD_TRIGGERS = (
    (
        "trg_v4_context_bd",
        "DELETE",
        "st_decision_context_v4",
        "BEFORE",
        1,
        "1ddc6df52d596431eb814d10b1b0de0a124178002f1420bc96595290bc6c761e",
    ),
    (
        "trg_v4_context_bu",
        "UPDATE",
        "st_decision_context_v4",
        "BEFORE",
        1,
        "1ddc6df52d596431eb814d10b1b0de0a124178002f1420bc96595290bc6c761e",
    ),
    (
        "trg_v4_control_transition_bd",
        "DELETE",
        "st_runtime_control_transition_v4",
        "BEFORE",
        1,
        "9406138c0e1c53cc43cead8a4bc661c1b9f403aa227c50cb269405dbd47a6cae",
    ),
    (
        "trg_v4_control_transition_bi",
        "INSERT",
        "st_runtime_control_transition_v4",
        "BEFORE",
        1,
        "7963eb9047f0cbe2796eda14a9015aff6b9480fe601325bd3e29fb9ff4e03c00",
    ),
    (
        "trg_v4_control_transition_bu",
        "UPDATE",
        "st_runtime_control_transition_v4",
        "BEFORE",
        1,
        "9406138c0e1c53cc43cead8a4bc661c1b9f403aa227c50cb269405dbd47a6cae",
    ),
    (
        "trg_v4_head_bd",
        "DELETE",
        "st_decision_channel_head_v4",
        "BEFORE",
        1,
        "cfa936c5d6d48e47a73b00e562b71e02b1c65d6c8c8589a6d203b6a1cf2b0694",
    ),
    (
        "trg_v4_head_bi",
        "INSERT",
        "st_decision_channel_head_v4",
        "BEFORE",
        1,
        "9e8651667397529e2da532936561e777174389b30da105e3add58a822fc4d4db",
    ),
    (
        "trg_v4_head_bu",
        "UPDATE",
        "st_decision_channel_head_v4",
        "BEFORE",
        1,
        "e865611680b021695bbf1ad9f25027be54a1b350fa7e6dfdadb607a5fd82b866",
    ),
    (
        "trg_v4_run_bd",
        "DELETE",
        "st_decision_run_v4",
        "BEFORE",
        1,
        "75adb562555730d1ba5aa42958cd71ac979a80053732ab13ac31998dbd9595ca",
    ),
    (
        "trg_v4_run_bi",
        "INSERT",
        "st_decision_run_v4",
        "BEFORE",
        1,
        "fb4e4bec2cfb3b8e00467d3bc5933d55659dd0771a3cdbc7c2e1b7eb8f186ff3",
    ),
    (
        "trg_v4_run_bu",
        "UPDATE",
        "st_decision_run_v4",
        "BEFORE",
        1,
        "1fa268111bec88a32581d60b65c0f82561b74e545431a044265f4a55131fc6de",
    ),
    (
        "trg_v4_runtime_control_bd",
        "DELETE",
        "st_runtime_control_v4",
        "BEFORE",
        1,
        "babe52de9af13eef52402348bd61e7b2c4eee8db10a4ec2c2a34371a4d6c0290",
    ),
    (
        "trg_v4_runtime_control_bi",
        "INSERT",
        "st_runtime_control_v4",
        "BEFORE",
        1,
        "6d847071f121843da4555f25dd3219f3dc166d60c18a47975c54c52ee318266f",
    ),
    (
        "trg_v4_runtime_control_bu",
        "UPDATE",
        "st_runtime_control_v4",
        "BEFORE",
        1,
        "e39a3d1917066362c3536be338b27ebc0f81efdf613caf069121686acf24dd14",
    ),
    (
        "trg_v4_watermark_bd",
        "DELETE",
        "st_source_watermark_v4",
        "BEFORE",
        1,
        "d871168785b50cfcd4d3f2df3fa3dadd16c929a1819a3604440c16d72e165370",
    ),
    (
        "trg_v4_watermark_bu",
        "UPDATE",
        "st_source_watermark_v4",
        "BEFORE",
        1,
        "d871168785b50cfcd4d3f2df3fa3dadd16c929a1819a3604440c16d72e165370",
    ),
)
_DECLARED_TABLES = frozenset(
    match.group(1).lower()
    for statement in (
        MIGRATION_TABLE_DDL,
        *(sql for migration in MIGRATIONS for sql in migration["statements"]),
    )
    for match in _CREATE_TABLE_RE.finditer(str(statement))
)


def _assert_frozen_migration_contract() -> None:
    if _DECLARED_MIGRATIONS != FROZEN_EXPECTED_MIGRATIONS:
        raise RuntimeError(
            "V4 migration source changed from the independently frozen "
            "acceptance checksum contract"
        )
    if _DECLARED_TABLES != V4_CONTROL_PLANE_TABLES:
        raise RuntimeError(
            "V4 migration source changed from the frozen table inventory"
        )


@dataclass(frozen=True)
class MySQLAcceptanceReport:
    mode: str
    database: str
    server_version: str
    server_version_comment: str
    server_uuid: str
    least_privilege_attested: bool
    started_empty: bool
    initial_migration: tuple[str, ...]
    serial_replay: tuple[str, ...]
    concurrent_replays: tuple[tuple[str, ...], ...]
    observed_tables: tuple[str, ...]
    checksums: tuple[str, ...]
    production_activation_allowed: bool
    actionable_output_allowed: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MySQLConcurrentInitialReport:
    mode: str
    database: str
    server_version: str
    server_version_comment: str
    server_uuid: str
    least_privilege_attested: bool
    started_empty: bool
    concurrent_initial_runs: tuple[tuple[str, ...], ...]
    observed_tables: tuple[str, ...]
    checksums: tuple[str, ...]
    production_activation_allowed: bool
    actionable_output_allowed: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MySQLPartialRecoveryReport:
    mode: str
    database: str
    server_version: str
    server_version_comment: str
    server_uuid: str
    least_privilege_attested: bool
    started_empty: bool
    partial_migration_version: str
    partial_statement_count: int
    partial_observed_tables: tuple[str, ...]
    recovery_migration: tuple[str, ...]
    recovery_replay: tuple[str, ...]
    observed_tables: tuple[str, ...]
    checksums: tuple[str, ...]
    production_activation_allowed: bool
    actionable_output_allowed: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MySQLHeadCASReport:
    mode: str
    database: str
    server_version: str
    server_version_comment: str
    server_uuid: str
    least_privilege_attested: bool
    started_empty: bool
    initial_migration: tuple[str, ...]
    initial_head_version: int
    successful_run_uid: str
    conflicting_run_uid: str
    final_head_version: int
    final_head_run_uid: str
    production_activation_allowed: bool
    actionable_output_allowed: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MySQLTransactionRecoveryReport:
    mode: str
    database: str
    server_version: str
    server_version_comment: str
    server_uuid: str
    least_privilege_attested: bool
    started_empty: bool
    initial_migration: tuple[str, ...]
    explicit_rollback_absent: bool
    disconnect_rollback_absent: bool
    recovery_write_visible: bool
    production_activation_allowed: bool
    actionable_output_allowed: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class MySQLJobLeaseBehaviorReport:
    mode: str
    database: str
    server_version: str
    server_version_comment: str
    server_uuid: str
    least_privilege_attested: bool
    started_empty: bool
    acceptance_run_id: str
    initial_migration: tuple[str, ...]
    direct_sql_whitespace_rejections: tuple[str, ...]
    same_token_outcomes: tuple[str, ...]
    two_worker_claimed_count: int
    two_worker_empty_count: int
    lock_timeout_error_codes: tuple[int, ...]
    lock_timeout_retry_succeeded: bool
    deadlock_error_codes: tuple[int, ...]
    deadlock_attempts: tuple[int, ...]
    max_lease_duration_seconds: int
    over_limit_rejected: bool
    expired_job_status: str
    expired_job_error_code: str
    terminal_retry_conflict: bool
    historical_token_reuse_conflict: bool
    transaction_rollback_absent: bool
    production_activation_allowed: bool
    actionable_output_allowed: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True)
class _JobLeaseBehaviorMatrix:
    direct_sql_whitespace_rejections: tuple[str, ...]
    same_token_outcomes: tuple[str, ...]
    two_worker_claimed_count: int
    two_worker_empty_count: int
    lock_timeout_error_codes: tuple[int, ...]
    lock_timeout_retry_succeeded: bool
    deadlock_error_codes: tuple[int, ...]
    deadlock_attempts: tuple[int, ...]
    max_lease_duration_seconds: int
    over_limit_rejected: bool
    expired_job_status: str
    expired_job_error_code: str
    terminal_retry_conflict: bool
    historical_token_reuse_conflict: bool
    transaction_rollback_absent: bool


def require_dedicated_test_url(value: object) -> str:
    """Validate an explicit, dedicated V4 test database URL."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError("a dedicated V4 test MySQL URL is required")
    raw = value.strip()
    try:
        url = make_url(raw)
    except ArgumentError as exc:
        raise ValueError("invalid dedicated V4 test MySQL URL") from exc
    if url.get_backend_name().lower() != "mysql":
        raise ValueError("V4 acceptance requires the MySQL backend")
    if url.query:
        raise ValueError("V4 acceptance URL query parameters are forbidden")
    if not str(url.host or "").strip():
        raise ValueError("V4 acceptance URL requires an explicit host")
    database = str(url.database or "").strip()
    if not _SAFE_DATABASE_RE.fullmatch(database):
        raise ValueError(
            "database name must be an explicit *_v4_test* or *_v4_ci* database"
        )
    return raw


def resolve_test_url(
    env_name: str = DEFAULT_URL_ENV,
    *,
    environ: dict[str, str] | None = None,
) -> str:
    if not isinstance(env_name, str) or not env_name.strip():
        raise ValueError("env_name is required")
    normalized_env_name = env_name.strip()
    if normalized_env_name in _FORBIDDEN_URL_ENVS:
        raise ValueError(
            f"{normalized_env_name} is forbidden for V4 MySQL acceptance"
        )
    if not _SAFE_URL_ENV_RE.fullmatch(normalized_env_name):
        raise ValueError(
            "URL environment variable must match "
            "V4_TEST_*_MYSQL_URL or V4_CI_*_MYSQL_URL"
        )
    source = os.environ if environ is None else environ
    return require_dedicated_test_url(source.get(normalized_env_name, ""))


def require_expected_server_uuid(value: object) -> str:
    """Require a non-nil canonical UUID supplied independently of the URL."""

    if not isinstance(value, str):
        raise ValueError("an expected MySQL server UUID is required")
    normalized = value.strip().lower()
    if _CANONICAL_UUID_RE.fullmatch(normalized) is None:
        raise ValueError("expected MySQL server UUID must be canonical")
    try:
        parsed = uuid.UUID(normalized)
    except ValueError as exc:
        raise ValueError("expected MySQL server UUID must be canonical") from exc
    if parsed.int == 0:
        raise ValueError("expected MySQL server UUID must not be nil")
    return str(parsed)


def resolve_server_uuid(
    env_name: str = DEFAULT_SERVER_UUID_ENV,
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    if not isinstance(env_name, str) or not env_name.strip():
        raise ValueError("server UUID env_name is required")
    normalized = env_name.strip()
    if _SAFE_SERVER_UUID_ENV_RE.fullmatch(normalized) is None:
        raise ValueError(
            "server UUID environment variable must match "
            "V4_TEST_*_MYSQL_SERVER_UUID or V4_CI_*_MYSQL_SERVER_UUID"
        )
    source = os.environ if environ is None else environ
    return require_expected_server_uuid(source.get(normalized, ""))


def _assert_least_privilege_grants(
    grants: Iterable[object],
    *,
    expected_database: str,
) -> None:
    expected_target = f"{expected_database.lower()}.*"
    observed_schema_privileges: set[str] = set()
    observed = tuple(str(item).strip() for item in grants)
    if not observed:
        raise RuntimeError("V4 acceptance account grants could not be attested")
    for grant in observed:
        match = _GRANT_RE.match(grant)
        if match is None or " WITH GRANT OPTION" in grant.upper():
            raise RuntimeError(
                "V4 acceptance account has an unsupported or delegable grant"
            )
        privileges = " ".join(match.group("privileges").upper().split())
        target = match.group("target").replace("`", "").strip().lower()
        if target == "*.*" and privileges == "USAGE":
            continue
        if target != expected_target:
            raise RuntimeError(
                "V4 acceptance grants must be scoped only to the target schema"
            )
        parsed = {
            " ".join(item.strip().upper().split())
            for item in privileges.split(",")
            if item.strip()
        }
        unexpected = parsed - _REQUIRED_SCHEMA_PRIVILEGES
        if unexpected:
            raise RuntimeError(
                "V4 acceptance account has unnecessary grants: "
                + ", ".join(sorted(unexpected))
            )
        observed_schema_privileges.update(parsed)
    if observed_schema_privileges != _REQUIRED_SCHEMA_PRIVILEGES:
        missing = sorted(
            _REQUIRED_SCHEMA_PRIVILEGES - observed_schema_privileges
        )
        raise RuntimeError(
            "V4 acceptance account target-schema grants are incomplete; "
            "missing: " + ", ".join(missing)
        )


def _server_identity_from_connection(
    connection: Connection,
    expected_database: str,
    expected_server_uuid: str,
) -> tuple[str, str, str, str]:
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    backend = str(
        getattr(getattr(connection, "dialect", None), "name", "")
    ).lower()
    version = str(connection.execute(text("SELECT VERSION()")).scalar() or "")
    database = str(connection.execute(text("SELECT DATABASE()")).scalar() or "")
    server_uuid = str(
        connection.execute(text("SELECT @@server_uuid")).scalar() or ""
    ).strip().lower()
    version_comment = str(
        connection.execute(text("SELECT @@version_comment")).scalar() or ""
    ).strip()
    grants = tuple(
        connection.execute(text("SHOW GRANTS FOR CURRENT_USER()")).scalars()
    )
    if backend != "mysql":
        raise RuntimeError("V4 acceptance connection must use MySQL")
    if not is_oracle_mysql_distribution(version, version_comment):
        raise RuntimeError("V4 acceptance connection must be Oracle MySQL")
    if not is_isolated_acceptance_version(version):
        raise RuntimeError(
            "V4 acceptance requires Oracle MySQL "
            f"{isolated_acceptance_versions_label()} exactly"
        )
    if database != expected_database:
        raise RuntimeError(
            "connected database does not match the dedicated V4 acceptance URL"
        )
    if _CANONICAL_UUID_RE.fullmatch(server_uuid) is None:
        raise RuntimeError("connected MySQL server UUID is missing or invalid")
    if server_uuid != expected_uuid:
        raise RuntimeError(
            "connected MySQL server UUID does not match expectation"
        )
    _assert_least_privilege_grants(grants, expected_database=expected_database)
    return database, version, server_uuid, version_comment


def _schema_names_from_connection(
    connection: Connection,
    *,
    kind: str,
) -> frozenset[str]:
    contracts = {
        "tables": ("TABLE_NAME", "TABLES", "TABLE_SCHEMA"),
        "routines": ("ROUTINE_NAME", "ROUTINES", "ROUTINE_SCHEMA"),
        "events": ("EVENT_NAME", "EVENTS", "EVENT_SCHEMA"),
    }
    if kind not in contracts:
        raise RuntimeError("unsupported V4 schema inventory kind")
    column, table_name, schema_column = contracts[kind]
    rows = connection.execute(
        text(
            f"SELECT {column} FROM information_schema.{table_name} "
            f"WHERE {schema_column} = DATABASE() ORDER BY {column}"
        )
    ).scalars()
    return frozenset(str(item).lower() for item in rows)


def _preflight_empty_schema(
    engine: Engine,
    expected_database: str,
    expected_server_uuid: str,
) -> tuple[str, str, str, str]:
    """Bind identity and an all-object emptiness check to one connection."""

    if str(getattr(engine.dialect, "name", "")).lower() != "mysql":
        raise RuntimeError("V4 acceptance engine is not using MySQL")
    with engine.connect() as connection:
        identity = _server_identity_from_connection(
            connection,
            expected_database,
            expected_server_uuid,
        )
        objects = {
            kind: _schema_names_from_connection(connection, kind=kind)
            for kind in ("tables", "routines", "events")
        }
    nonempty = {kind: sorted(names) for kind, names in objects.items() if names}
    if nonempty:
        raise RuntimeError(
            "V4 acceptance database must start completely empty: "
            + json.dumps(nonempty, ensure_ascii=False, sort_keys=True)
        )
    return identity


def _assert_engine_identity(
    engine: Engine,
    expected_database: str,
    expected_server_uuid: str,
) -> tuple[str, str, str, str]:
    with engine.connect() as connection:
        return _server_identity_from_connection(
            connection,
            expected_database,
            expected_server_uuid,
        )


def _table_names(engine: Engine) -> frozenset[str]:
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                """
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                ORDER BY TABLE_NAME
                """
            )
        ).scalars()
        return frozenset(str(item) for item in rows)


def _normalized_trigger_body_hash(value: object) -> str:
    normalized = " ".join(
        str(value or "")
        .strip()
        .rstrip(";")
        .replace("`", "")
        .casefold()
        .split()
    )
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _expected_trigger_contract(
    connection: Connection,
    statements: tuple[str, ...],
    *,
    include_table: bool,
) -> tuple[tuple[object, ...], ...]:
    """Build the strict trigger contract from the frozen migration source."""

    expected: list[tuple[object, ...]] = []
    for statement in statements:
        if not statement.lstrip().upper().startswith("CREATE TRIGGER"):
            continue
        executable = _mysql_regexp_compatible_statement(connection, statement)
        match = _CREATE_TRIGGER_RE.search(executable)
        if match is None:
            raise RuntimeError("frozen V4 trigger preamble could not be parsed")
        identity: tuple[object, ...] = (
            match.group("name"),
            match.group("event").upper(),
        )
        if include_table:
            identity = (*identity, match.group("table"))
        expected.append(
            (
                *identity,
                "BEFORE",
                1,
                _normalized_trigger_body_hash(
                    _trigger_body_from_ddl(executable)
                ),
            )
        )
    return tuple(sorted(expected, key=lambda item: str(item[0])))


def _normalized_sql_mode(value: object) -> tuple[str, ...]:
    return tuple(
        sorted(
            item.strip().upper()
            for item in str(value or "").split(",")
            if item.strip()
        )
    )


def _normalized_definer(value: object) -> str:
    return (
        str(value or "")
        .replace("`", "")
        .replace("'", "")
        .strip()
        .casefold()
    )


def _normalized_column_type(value: object) -> str:
    """Normalize MySQL 5.7 integer widths removed from MySQL 8 metadata."""

    normalized = " ".join(str(value or "").casefold().split())
    integer = re.fullmatch(
        r"(smallint|mediumint|int|integer|bigint)\(\d+\)( unsigned)?",
        normalized,
    )
    if integer is None:
        return normalized
    return f"{integer.group(1)}{integer.group(2) or ''}"


def _normalized_column_contracts(
    columns: Iterable[tuple[object, ...]],
) -> tuple[tuple[object, ...], ...]:
    return tuple(
        sorted(
            (item[0], _normalized_column_type(item[1]), *item[2:])
            for item in columns
        )
    )


def _assert_job_lease_schema(engine: Engine) -> None:
    """Attest the frozen 002/003 contracts and 004 control guards."""

    with engine.connect() as connection:
        column_rows = tuple(
            connection.execute(
                text(
                    "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, "
                    "COLUMN_DEFAULT, CHARACTER_SET_NAME, COLLATION_NAME, EXTRA "
                    "FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = 'st_job_run_v4' "
                    "AND COLUMN_NAME IN ('lease_token', 'max_attempts') "
                    "ORDER BY COLUMN_NAME"
                )
            ).mappings()
        )
        columns = tuple(
            sorted(
                (
                    str(row["COLUMN_NAME"]),
                    _normalized_column_type(row["COLUMN_TYPE"]),
                    str(row["IS_NULLABLE"]).upper(),
                    (
                        str(row["COLUMN_DEFAULT"])
                        if row["COLUMN_DEFAULT"] is not None
                        else None
                    ),
                    (
                        str(row["CHARACTER_SET_NAME"]).casefold()
                        if row["CHARACTER_SET_NAME"] is not None
                        else None
                    ),
                    (
                        str(row["COLLATION_NAME"]).casefold()
                        if row["COLLATION_NAME"] is not None
                        else None
                    ),
                    " ".join(str(row["EXTRA"] or "").casefold().split()),
                )
                for row in column_rows
            )
        )
        if columns != _normalized_column_contracts(FROZEN_JOB_LEASE_COLUMNS):
            raise RuntimeError(
                "V4 acceptance job lease column contract drifted: "
                f"{columns!r}"
            )

        index_rows = tuple(
            connection.execute(
                text(
                    "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, "
                    "COLUMN_NAME, SUB_PART, COLLATION, INDEX_TYPE "
                    "FROM information_schema.STATISTICS "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = 'st_job_run_v4' "
                    "AND INDEX_NAME IN "
                    "('idx_v4_job_claim_due','uk_v4_job_lease_token') "
                    "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
                )
            ).mappings()
        )
        grouped_indexes: dict[str, dict[str, object]] = {}
        for row in index_rows:
            name = str(row["INDEX_NAME"])
            entry = grouped_indexes.setdefault(
                name,
                {
                    "unique": int(row["NON_UNIQUE"]) == 0,
                    "columns": [],
                    "sub_parts": [],
                    "collations": [],
                    "index_type": str(row["INDEX_TYPE"] or "").upper(),
                },
            )
            entry["columns"].append(str(row["COLUMN_NAME"]))
            entry["sub_parts"].append(
                int(row["SUB_PART"]) if row["SUB_PART"] is not None else None
            )
            entry["collations"].append(
                str(row["COLLATION"] or "").upper() or None
            )
        indexes = tuple(
            sorted(
                (
                    name,
                    bool(entry["unique"]),
                    tuple(entry["columns"]),
                    tuple(entry["sub_parts"]),
                    tuple(entry["collations"]),
                    str(entry["index_type"]),
                )
                for name, entry in grouped_indexes.items()
            )
        )
        if indexes != tuple(sorted(FROZEN_JOB_LEASE_INDEXES)):
            raise RuntimeError(
                "V4 acceptance job lease index contract drifted: "
                f"{indexes!r}"
            )

        context = connection.execute(
            text(
                "SELECT DATABASE() AS CURRENT_DATABASE, "
                "CURRENT_USER() AS CURRENT_DEFINER, "
                "@@SESSION.sql_mode AS CURRENT_SQL_MODE, "
                "@@SESSION.character_set_client "
                "    AS CURRENT_CHARACTER_SET_CLIENT, "
                "@@SESSION.collation_connection "
                "    AS CURRENT_COLLATION_CONNECTION, "
                "(SELECT DEFAULT_COLLATION_NAME "
                " FROM information_schema.SCHEMATA "
                " WHERE SCHEMA_NAME = DATABASE()) AS DATABASE_COLLATION"
            )
        ).mappings().one()
        trigger_rows = tuple(
            connection.execute(
                text(
                    "SELECT TRIGGER_SCHEMA, TRIGGER_NAME, EVENT_MANIPULATION, "
                    "EVENT_OBJECT_SCHEMA, EVENT_OBJECT_TABLE, ACTION_ORDER, "
                    "ACTION_CONDITION, ACTION_STATEMENT, ACTION_ORIENTATION, "
                    "ACTION_TIMING, SQL_MODE, DEFINER, CHARACTER_SET_CLIENT, "
                    "COLLATION_CONNECTION, DATABASE_COLLATION "
                    "FROM information_schema.TRIGGERS "
                    "WHERE TRIGGER_SCHEMA = DATABASE() "
                    "AND EVENT_OBJECT_TABLE = 'st_job_run_v4' "
                    "ORDER BY BINARY TRIGGER_NAME"
                )
            ).mappings()
        )
        database = str(context["CURRENT_DATABASE"])
        expected_definer = _normalized_definer(context["CURRENT_DEFINER"])
        expected_mode = _normalized_sql_mode(context["CURRENT_SQL_MODE"])
        expected_character_set = str(
            context["CURRENT_CHARACTER_SET_CLIENT"]
        ).casefold()
        expected_connection_collation = str(
            context["CURRENT_COLLATION_CONNECTION"]
        ).casefold()
        expected_database_collation = str(
            context["DATABASE_COLLATION"]
        ).casefold()
        triggers: list[tuple[str, str, str, int, str]] = []
        for row in trigger_rows:
            if (
                str(row["TRIGGER_SCHEMA"]) != database
                or str(row["EVENT_OBJECT_SCHEMA"]) != database
                or str(row["EVENT_OBJECT_TABLE"]) != "st_job_run_v4"
                or row["ACTION_CONDITION"] is not None
                or str(row["ACTION_ORIENTATION"]).upper() != "ROW"
                or _normalized_sql_mode(row["SQL_MODE"]) != expected_mode
                or _normalized_definer(row["DEFINER"]) != expected_definer
                or str(row["CHARACTER_SET_CLIENT"]).casefold()
                != expected_character_set
                or str(row["COLLATION_CONNECTION"]).casefold()
                != expected_connection_collation
                or str(row["DATABASE_COLLATION"]).casefold()
                != expected_database_collation
            ):
                raise RuntimeError(
                    "V4 acceptance job lease trigger context drifted"
                )
            triggers.append(
                (
                    str(row["TRIGGER_NAME"]),
                    str(row["EVENT_MANIPULATION"]).upper(),
                    str(row["ACTION_TIMING"]).upper(),
                    int(row["ACTION_ORDER"]),
                    _normalized_trigger_body_hash(row["ACTION_STATEMENT"]),
                )
            )
        expected_job_lease_triggers = _expected_trigger_contract(
            connection,
            tuple(MIGRATIONS[1]["statements"]),
            include_table=False,
        )
        if tuple(triggers) != expected_job_lease_triggers:
            raise RuntimeError(
                "V4 acceptance job lease trigger contract drifted: "
                f"{tuple(triggers)!r}"
            )

        registry_column_rows = tuple(
            connection.execute(
                text(
                    "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, "
                    "COLUMN_DEFAULT, CHARACTER_SET_NAME, COLLATION_NAME, "
                    "EXTRA FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = 'st_job_claim_token_v4' "
                    "ORDER BY COLUMN_NAME"
                )
            ).mappings()
        )
        registry_columns = tuple(
            (
                str(row["COLUMN_NAME"]),
                _normalized_column_type(row["COLUMN_TYPE"]),
                str(row["IS_NULLABLE"]).upper(),
                (
                    str(row["COLUMN_DEFAULT"])
                    if row["COLUMN_DEFAULT"] is not None
                    else None
                ),
                (
                    str(row["CHARACTER_SET_NAME"]).casefold()
                    if row["CHARACTER_SET_NAME"] is not None
                    else None
                ),
                (
                    str(row["COLLATION_NAME"]).casefold()
                    if row["COLLATION_NAME"] is not None
                    else None
                ),
                " ".join(str(row["EXTRA"] or "").casefold().split()),
            )
            for row in registry_column_rows
        )
        if registry_columns != _normalized_column_contracts(
            FROZEN_CLAIM_TOKEN_COLUMNS
        ):
            raise RuntimeError(
                "V4 acceptance claim token column contract drifted: "
                f"{registry_columns!r}"
            )

        registry_index_rows = tuple(
            connection.execute(
                text(
                    "SELECT INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, "
                    "COLUMN_NAME, SUB_PART, COLLATION, INDEX_TYPE "
                    "FROM information_schema.STATISTICS "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = 'st_job_claim_token_v4' "
                    "ORDER BY INDEX_NAME, SEQ_IN_INDEX"
                )
            ).mappings()
        )
        registry_grouped_indexes: dict[str, dict[str, object]] = {}
        for row in registry_index_rows:
            name = str(row["INDEX_NAME"])
            entry = registry_grouped_indexes.setdefault(
                name,
                {
                    "unique": int(row["NON_UNIQUE"]) == 0,
                    "columns": [],
                    "sub_parts": [],
                    "collations": [],
                    "index_type": str(row["INDEX_TYPE"] or "").upper(),
                },
            )
            entry["columns"].append(str(row["COLUMN_NAME"]))
            entry["sub_parts"].append(
                int(row["SUB_PART"])
                if row["SUB_PART"] is not None
                else None
            )
            entry["collations"].append(
                str(row["COLLATION"] or "").upper() or None
            )
        registry_indexes = tuple(
            sorted(
                (
                    name,
                    bool(entry["unique"]),
                    tuple(entry["columns"]),
                    tuple(entry["sub_parts"]),
                    tuple(entry["collations"]),
                    str(entry["index_type"]),
                )
                for name, entry in registry_grouped_indexes.items()
            )
        )
        if registry_indexes != tuple(sorted(FROZEN_CLAIM_TOKEN_INDEXES)):
            raise RuntimeError(
                "V4 acceptance claim token index contract drifted: "
                f"{registry_indexes!r}"
            )

        registry_constraint_rows = tuple(
            connection.execute(
                text(
                    "SELECT k.CONSTRAINT_NAME, k.COLUMN_NAME, "
                    "k.REFERENCED_TABLE_NAME, k.REFERENCED_COLUMN_NAME, "
                    "r.DELETE_RULE FROM information_schema.KEY_COLUMN_USAGE k "
                    "JOIN information_schema.REFERENTIAL_CONSTRAINTS r "
                    "ON r.CONSTRAINT_SCHEMA = k.CONSTRAINT_SCHEMA "
                    "AND r.TABLE_NAME = k.TABLE_NAME "
                    "AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME "
                    "WHERE k.CONSTRAINT_SCHEMA = DATABASE() "
                    "AND k.TABLE_NAME = 'st_job_claim_token_v4' "
                    "AND k.REFERENCED_TABLE_NAME IS NOT NULL "
                    "ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION"
                )
            ).mappings()
        )
        registry_constraints = tuple(
            (
                str(row["CONSTRAINT_NAME"]),
                str(row["COLUMN_NAME"]),
                str(row["REFERENCED_TABLE_NAME"]),
                str(row["REFERENCED_COLUMN_NAME"]),
                str(row["DELETE_RULE"]).upper(),
            )
            for row in registry_constraint_rows
        )
        if registry_constraints != FROZEN_CLAIM_TOKEN_CONSTRAINTS:
            raise RuntimeError(
                "V4 acceptance claim token FK contract drifted: "
                f"{registry_constraints!r}"
            )

        registry_trigger_rows = tuple(
            connection.execute(
                text(
                    "SELECT TRIGGER_SCHEMA, TRIGGER_NAME, EVENT_MANIPULATION, "
                    "EVENT_OBJECT_SCHEMA, EVENT_OBJECT_TABLE, ACTION_ORDER, "
                    "ACTION_CONDITION, ACTION_STATEMENT, ACTION_ORIENTATION, "
                    "ACTION_TIMING, SQL_MODE, DEFINER, CHARACTER_SET_CLIENT, "
                    "COLLATION_CONNECTION, DATABASE_COLLATION "
                    "FROM information_schema.TRIGGERS "
                    "WHERE TRIGGER_SCHEMA = DATABASE() "
                    "AND EVENT_OBJECT_TABLE = 'st_job_claim_token_v4' "
                    "ORDER BY BINARY TRIGGER_NAME"
                )
            ).mappings()
        )
        registry_triggers: list[tuple[str, str, str, int, str]] = []
        for row in registry_trigger_rows:
            if (
                str(row["TRIGGER_SCHEMA"]) != database
                or str(row["EVENT_OBJECT_SCHEMA"]) != database
                or str(row["EVENT_OBJECT_TABLE"])
                != "st_job_claim_token_v4"
                or row["ACTION_CONDITION"] is not None
                or str(row["ACTION_ORIENTATION"]).upper() != "ROW"
                or _normalized_sql_mode(row["SQL_MODE"]) != expected_mode
                or _normalized_definer(row["DEFINER"]) != expected_definer
                or str(row["CHARACTER_SET_CLIENT"]).casefold()
                != expected_character_set
                or str(row["COLLATION_CONNECTION"]).casefold()
                != expected_connection_collation
                or str(row["DATABASE_COLLATION"]).casefold()
                != expected_database_collation
            ):
                raise RuntimeError(
                    "V4 acceptance claim token trigger context drifted"
                )
            registry_triggers.append(
                (
                    str(row["TRIGGER_NAME"]),
                    str(row["EVENT_MANIPULATION"]).upper(),
                    str(row["ACTION_TIMING"]).upper(),
                    int(row["ACTION_ORDER"]),
                    _normalized_trigger_body_hash(row["ACTION_STATEMENT"]),
                )
            )
        expected_registry_triggers = _expected_trigger_contract(
            connection,
            tuple(MIGRATIONS[2]["statements"]),
            include_table=False,
        )
        if tuple(registry_triggers) != expected_registry_triggers:
            raise RuntimeError(
                "V4 acceptance claim token trigger contract drifted: "
                f"{tuple(registry_triggers)!r}"
            )

        guard_table_rows = tuple(
            connection.execute(
                text(
                    "SELECT TABLE_NAME, ENGINE, TABLE_COLLATION "
                    "FROM information_schema.TABLES "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME IN ("
                    "'st_decision_channel_head_v4',"
                    "'st_decision_context_v4',"
                    "'st_decision_run_v4',"
                    "'st_runtime_control_transition_v4',"
                    "'st_runtime_control_v4',"
                    "'st_source_watermark_v4') "
                    "ORDER BY TABLE_NAME"
                )
            ).mappings()
        )
        guard_tables = tuple(
            (
                str(row["TABLE_NAME"]),
                str(row["ENGINE"] or "").upper(),
                str(row["TABLE_COLLATION"] or "").casefold(),
            )
            for row in guard_table_rows
        )
        expected_guard_tables = tuple(
            (table_name, "INNODB", "utf8mb4_bin")
            for table_name in FROZEN_CONTROL_GUARD_TABLES
        )
        if guard_tables != expected_guard_tables:
            raise RuntimeError(
                "V4 acceptance control guard table contract drifted: "
                f"{guard_tables!r}"
            )

        guard_trigger_rows = tuple(
            connection.execute(
                text(
                    "SELECT TRIGGER_SCHEMA, TRIGGER_NAME, "
                    "EVENT_MANIPULATION, EVENT_OBJECT_SCHEMA, "
                    "EVENT_OBJECT_TABLE, ACTION_ORDER, ACTION_CONDITION, "
                    "ACTION_STATEMENT, ACTION_ORIENTATION, ACTION_TIMING, "
                    "SQL_MODE, DEFINER, CHARACTER_SET_CLIENT, "
                    "COLLATION_CONNECTION, DATABASE_COLLATION "
                    "FROM information_schema.TRIGGERS "
                    "WHERE TRIGGER_SCHEMA = DATABASE() "
                    "AND EVENT_OBJECT_TABLE IN ("
                    "'st_decision_channel_head_v4',"
                    "'st_decision_context_v4',"
                    "'st_decision_run_v4',"
                    "'st_runtime_control_transition_v4',"
                    "'st_runtime_control_v4',"
                    "'st_source_watermark_v4') "
                    "ORDER BY BINARY TRIGGER_NAME"
                )
            ).mappings()
        )
        guard_triggers: list[tuple[str, str, str, str, int, str]] = []
        for row in guard_trigger_rows:
            object_table = str(row["EVENT_OBJECT_TABLE"])
            if (
                str(row["TRIGGER_SCHEMA"]) != database
                or str(row["EVENT_OBJECT_SCHEMA"]) != database
                or object_table not in FROZEN_CONTROL_GUARD_TABLES
                or row["ACTION_CONDITION"] is not None
                or str(row["ACTION_ORIENTATION"]).upper() != "ROW"
                or _normalized_sql_mode(row["SQL_MODE"]) != expected_mode
                or _normalized_definer(row["DEFINER"]) != expected_definer
                or str(row["CHARACTER_SET_CLIENT"]).casefold()
                != expected_character_set
                or str(row["COLLATION_CONNECTION"]).casefold()
                != expected_connection_collation
                or str(row["DATABASE_COLLATION"]).casefold()
                != expected_database_collation
            ):
                raise RuntimeError(
                    "V4 acceptance control guard trigger context drifted"
                )
            guard_triggers.append(
                (
                    str(row["TRIGGER_NAME"]),
                    str(row["EVENT_MANIPULATION"]).upper(),
                    object_table,
                    str(row["ACTION_TIMING"]).upper(),
                    int(row["ACTION_ORDER"]),
                    _normalized_trigger_body_hash(row["ACTION_STATEMENT"]),
                )
            )
        expected_guard_triggers = _expected_trigger_contract(
            connection,
            tuple(MIGRATIONS[3]["statements"]),
            include_table=True,
        )
        if tuple(guard_triggers) != expected_guard_triggers:
            raise RuntimeError(
                "V4 acceptance control guard trigger contract drifted: "
                f"{tuple(guard_triggers)!r}"
            )


def _statuses(results: Iterable[V4MigrationResult]) -> tuple[str, ...]:
    return tuple(str(item.status) for item in results)


def _checksums(results: Iterable[V4MigrationResult]) -> tuple[str, ...]:
    return tuple(str(item.checksum) for item in results)


def _assert_isolated_tables(tables: frozenset[str]) -> None:
    unexpected = sorted(tables - V4_CONTROL_PLANE_TABLES)
    if unexpected:
        raise RuntimeError(
            "V4 acceptance database is not isolated; unexpected tables: "
            + ", ".join(unexpected)
        )


def _mysql_operational_error_code(error: OperationalError) -> int | None:
    original = getattr(error, "orig", None)
    candidates: tuple[object, ...] = (getattr(original, "errno", None),)
    args = getattr(original, "args", ())
    if isinstance(args, tuple):
        candidates += args[:2]
    for value in candidates:
        if type(value) is int:
            return value
        normalized = str(value or "").strip()
        if normalized.isdigit():
            return int(normalized)
    return None


def _run_bounded_mysql_transaction_retry(
    operation: Callable[[], _T],
    *,
    maximum_attempts: int = JOB_LEASE_ACCEPTANCE_MAX_TRANSIENT_ATTEMPTS,
) -> tuple[_T, int, tuple[int, ...]]:
    if type(maximum_attempts) is not int or maximum_attempts < 1:
        raise ValueError("maximum_attempts must be an exact positive int")
    observed: list[int] = []
    for attempt in range(1, maximum_attempts + 1):
        try:
            return operation(), attempt, tuple(observed)
        except OperationalError as exc:
            code = _mysql_operational_error_code(exc)
            if code not in _JOB_LEASE_TRANSIENT_CODES:
                raise
            observed.append(code)
            if attempt >= maximum_attempts:
                raise RuntimeError(
                    "V4 job lease transient transaction retry exhausted: "
                    f"codes={tuple(observed)!r} attempts={maximum_attempts}"
                ) from exc
    raise RuntimeError("V4 job lease transient retry state was unreachable")


def _database_utc_now(engine: Engine) -> datetime:
    with engine.connect() as connection:
        value = connection.execute(
            text("SELECT UTC_TIMESTAMP(6)")
        ).scalar()
    if type(value) is datetime:
        parsed = value
    elif type(value) is str:
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise RuntimeError("MySQL UTC clock returned invalid text") from exc
    else:
        raise RuntimeError("MySQL UTC clock returned an invalid value")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _probe_digest(run_id: str, label: str) -> str:
    return hashlib.sha256(f"{run_id}:{label}".encode("utf-8")).hexdigest()


def _probe_job_id(run_id: str, label: str) -> str:
    normalized = re.sub(r"[^a-z0-9_-]", "_", label.casefold())
    value = f"v4acc-{run_id[:20]}-{normalized}"
    if not value or len(value) > 64 or value != value.strip():
        raise RuntimeError("acceptance probe job_id is not exact")
    return value


def _probe_job_type(label: str) -> str:
    normalized = re.sub(r"[^A-Z0-9_]", "_", label.upper())
    value = f"V4_ACCEPT_{normalized}"
    if not value or len(value) > 80 or value != value.strip():
        raise RuntimeError("acceptance probe job_type is not exact")
    return value


def _insert_direct_job(
    connection: Connection,
    *,
    job_id: str,
    idempotency_key: str,
    job_type: str,
    occurred_at: datetime,
) -> None:
    result = connection.execute(
        text(
            """
            INSERT INTO st_job_run_v4 (
                job_id, idempotency_key, job_type, scheduled_for,
                input_context_id, input_hash, run_uid, status,
                attempt_count, max_attempts, lease_owner, lease_token,
                lease_until, next_attempt_at, error_code, error_message,
                started_at, completed_at, created_at, updated_at
            ) VALUES (
                :job_id, :idempotency_key, :job_type, :occurred_at,
                '', '', '', 'PENDING', 0, 3, '', NULL, NULL,
                :occurred_at, NULL, NULL, NULL, NULL,
                :occurred_at, :occurred_at
            )
            """
        ),
        {
            "job_id": job_id,
            "idempotency_key": idempotency_key,
            "job_type": job_type,
            "occurred_at": occurred_at.astimezone(timezone.utc).replace(
                tzinfo=None
            ),
        },
    )
    if type(result.rowcount) is not int or result.rowcount != 1:
        raise RuntimeError("direct V4 job probe did not insert exactly one row")


def _assert_exact_text_rejection(error: DBAPIError, label: str) -> None:
    message = str(getattr(error, "orig", error))
    if "invalid V4 job exact text contract" not in message:
        raise RuntimeError(
            f"V4 direct-SQL {label} failed for the wrong reason: {message}"
        ) from error


def _run_direct_sql_whitespace_probes(
    engine: Engine,
    *,
    run_id: str,
) -> tuple[str, ...]:
    occurred_at = _database_utc_now(engine)
    rejected: list[str] = []

    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            _insert_direct_job(
                connection,
                job_id=" " + _probe_job_id(run_id, "insert-leading"),
                idempotency_key=_probe_digest(run_id, "insert-leading"),
                job_type=_probe_job_type("insert-leading"),
                occurred_at=occurred_at,
            )
        except DBAPIError as exc:
            transaction.rollback()
            _assert_exact_text_rejection(exc, "insert-leading")
            rejected.append("insert-leading")
        else:
            transaction.rollback()
            raise RuntimeError(
                "V4 direct-SQL leading-whitespace INSERT was accepted"
            )

    valid_job_id = _probe_job_id(run_id, "update-trailing")
    valid_job_type = _probe_job_type("update-trailing")
    with engine.begin() as connection:
        _insert_direct_job(
            connection,
            job_id=valid_job_id,
            idempotency_key=_probe_digest(run_id, "update-trailing"),
            job_type=valid_job_type,
            occurred_at=occurred_at,
        )
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(
                text(
                    "UPDATE st_job_run_v4 SET job_type = :job_type, "
                    "updated_at = :updated_at WHERE job_id = :job_id"
                ),
                {
                    "job_id": valid_job_id,
                    "job_type": valid_job_type + " ",
                    "updated_at": (
                        occurred_at + timedelta(microseconds=1)
                    ).replace(tzinfo=None),
                },
            )
        except DBAPIError as exc:
            transaction.rollback()
            _assert_exact_text_rejection(exc, "update-trailing")
            rejected.append("update-trailing")
        else:
            transaction.rollback()
            raise RuntimeError(
                "V4 direct-SQL trailing-whitespace UPDATE was accepted"
            )

    with engine.connect() as connection:
        stored_type = connection.execute(
            text(
                "SELECT job_type FROM st_job_run_v4 WHERE job_id = :job_id"
            ),
            {"job_id": valid_job_id},
        ).scalar()
    if stored_type != valid_job_type:
        raise RuntimeError("rejected whitespace UPDATE changed persisted state")
    return tuple(rejected)


def _create_probe_job(
    repository: JobStoreRepository,
    engine: Engine,
    *,
    run_id: str,
    label: str,
    max_attempts: int = 3,
) -> tuple[str, str]:
    occurred_at = _database_utc_now(engine)
    job_id = _probe_job_id(run_id, label)
    job_type = _probe_job_type(label)
    result = repository.create_job(
        job_id=job_id,
        idempotency_key=_probe_digest(run_id, label),
        job_type=job_type,
        scheduled_for=occurred_at,
        max_attempts=max_attempts,
        created_at=occurred_at,
    )
    if not result.created or result.job.job_id != job_id:
        raise RuntimeError(f"V4 job probe create was not exact: {label}")
    return job_id, job_type


def _claim_outcome(value: object) -> str:
    if value is None:
        return "empty"
    if bool(getattr(value, "claimed", False)):
        return "claimed"
    if bool(getattr(value, "replayed", False)):
        return "replayed"
    raise RuntimeError("V4 claim probe returned an unsupported outcome")


def _run_claim_race(
    repository: JobStoreRepository,
    *,
    worker_tokens: tuple[tuple[str, str], tuple[str, str]],
    now: datetime,
    lease_until: datetime,
    job_type: str,
) -> tuple[str, ...]:
    barrier = Barrier(2)

    def claim(worker_id: str, lease_token: str) -> object:
        barrier.wait(timeout=10)
        return repository.claim_due_job(
            worker_id=worker_id,
            lease_token=lease_token,
            now=now,
            lease_until=lease_until,
            job_type=job_type,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = tuple(
            executor.submit(claim, worker_id, token)
            for worker_id, token in worker_tokens
        )
        values = tuple(future.result(timeout=20) for future in futures)
    return tuple(sorted(_claim_outcome(value) for value in values))


def _run_deadlock_probe(
    engine: Engine,
    *,
    first_job_id: str,
    second_job_id: str,
) -> tuple[tuple[int, ...], tuple[int, ...]]:
    barrier = Barrier(2)

    def worker(first: str, second: str) -> tuple[int, tuple[int, ...]]:
        invocations = 0

        def lock_pair() -> None:
            nonlocal invocations
            invocations += 1
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "SELECT job_id FROM st_job_run_v4 "
                        "WHERE job_id = :job_id FOR UPDATE"
                    ),
                    {"job_id": first},
                ).scalar_one()
                if invocations == 1:
                    barrier.wait(timeout=10)
                connection.execute(
                    text(
                        "SELECT job_id FROM st_job_run_v4 "
                        "WHERE job_id = :job_id FOR UPDATE"
                    ),
                    {"job_id": second},
                ).scalar_one()

        _value, attempts, codes = _run_bounded_mysql_transaction_retry(
            lock_pair
        )
        return attempts, codes

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (
            executor.submit(worker, first_job_id, second_job_id),
            executor.submit(worker, second_job_id, first_job_id),
        )
        results = tuple(future.result(timeout=20) for future in futures)
    attempts = tuple(sorted(item[0] for item in results))
    codes = tuple(code for item in results for code in item[1])
    return attempts, codes


def _run_lock_timeout_retry_probe(
    engine: Engine,
    *,
    safe_url: str,
    tls_config: MySQLAcceptanceTLSConfig | None,
    expected_database: str,
    expected_server_uuid: str,
    run_id: str,
    repository: JobStoreRepository,
) -> tuple[tuple[int, ...], bool]:
    job_id, job_type = _create_probe_job(
        repository,
        engine,
        run_id=run_id,
        label="lock-timeout",
    )
    claim_now = _database_utc_now(engine)
    lease_until = claim_now + timedelta(seconds=30)
    worker_engine = create_tool_engine(
        safe_url,
        tls_config=tls_config,
        future=True,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    observed_codes: list[int] = []
    timeout_seen = Event()

    def observe_error(exception_context: object) -> None:
        original = getattr(exception_context, "original_exception", None)
        candidates = getattr(original, "args", ())
        code = candidates[0] if isinstance(candidates, tuple) and candidates else None
        if type(code) is int and code == 1205:
            observed_codes.append(code)
            timeout_seen.set()

    blocker = None
    transaction = None
    listener_registered = False
    try:
        with worker_engine.begin() as connection:
            _server_identity_from_connection(
                connection,
                expected_database,
                expected_server_uuid,
            )
            connection.execute(
                text("SET SESSION innodb_lock_wait_timeout = 1")
            )
        event.listen(worker_engine, "handle_error", observe_error)
        listener_registered = True
        blocker = engine.connect()
        transaction = blocker.begin()
        blocker.execute(
            text(
                "SELECT job_id FROM st_job_run_v4 "
                "WHERE job_id = :job_id FOR UPDATE"
            ),
            {"job_id": job_id},
        ).scalar_one()
        worker_repository = JobStoreRepository(worker_engine)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(
                worker_repository.claim_due_job,
                worker_id="v4-acceptance-timeout-worker",
                lease_token=_probe_digest(run_id, "lock-timeout-token"),
                now=claim_now,
                lease_until=lease_until,
                job_type=job_type,
            )
            if not timeout_seen.wait(timeout=8):
                raise RuntimeError("MySQL 1205 lock timeout was not observed")
            transaction.commit()
            transaction = None
            result = future.result(timeout=12)
        succeeded = bool(result is not None and result.claimed)
    finally:
        if transaction is not None and transaction.is_active:
            transaction.rollback()
        if blocker is not None:
            blocker.close()
        if listener_registered:
            event.remove(worker_engine, "handle_error", observe_error)
        worker_engine.dispose()
    return tuple(observed_codes), succeeded


def _wait_until_expired(engine: Engine, lease_until: datetime) -> datetime:
    deadline = time.monotonic() + 5.0
    while True:
        database_now = _database_utc_now(engine)
        if database_now >= lease_until:
            return database_now
        if time.monotonic() >= deadline:
            raise RuntimeError("V4 acceptance lease did not expire in time")
        time.sleep(0.05)


def _run_job_transaction_rollback_probe(
    engine: Engine,
    *,
    run_id: str,
) -> bool:
    job_id = _probe_job_id(run_id, "rollback")
    connection = engine.connect()
    transaction = connection.begin()
    try:
        _insert_direct_job(
            connection,
            job_id=job_id,
            idempotency_key=_probe_digest(run_id, "rollback"),
            job_type=_probe_job_type("rollback"),
            occurred_at=_database_utc_now(engine),
        )
        transaction.rollback()
    finally:
        if transaction.is_active:
            transaction.rollback()
        connection.close()
    with engine.connect() as verification:
        count = verification.execute(
            text(
                "SELECT COUNT(*) FROM st_job_run_v4 WHERE job_id = :job_id"
            ),
            {"job_id": job_id},
        ).scalar()
    return type(count) is int and count == 0


def _run_job_lease_behavior_probes(
    engine: Engine,
    *,
    safe_url: str,
    tls_config: MySQLAcceptanceTLSConfig | None,
    expected_database: str,
    expected_server_uuid: str,
    run_id: str,
) -> _JobLeaseBehaviorMatrix:
    repository = JobStoreRepository(engine)
    whitespace_rejections = _run_direct_sql_whitespace_probes(
        engine,
        run_id=run_id,
    )

    _same_job_id, same_type = _create_probe_job(
        repository,
        engine,
        run_id=run_id,
        label="same-token",
    )
    same_now = _database_utc_now(engine)
    same_token = _probe_digest(run_id, "same-token-lease")
    same_outcomes = _run_claim_race(
        repository,
        worker_tokens=(
            ("v4-acceptance-same-worker", same_token),
            ("v4-acceptance-same-worker", same_token),
        ),
        now=same_now,
        lease_until=same_now + timedelta(seconds=30),
        job_type=same_type,
    )

    _worker_job_id, worker_type = _create_probe_job(
        repository,
        engine,
        run_id=run_id,
        label="two-workers",
    )
    worker_now = _database_utc_now(engine)
    worker_outcomes = _run_claim_race(
        repository,
        worker_tokens=(
            (
                "v4-acceptance-worker-a",
                _probe_digest(run_id, "two-workers-a"),
            ),
            (
                "v4-acceptance-worker-b",
                _probe_digest(run_id, "two-workers-b"),
            ),
        ),
        now=worker_now,
        lease_until=worker_now + timedelta(seconds=30),
        job_type=worker_type,
    )

    lock_timeout_codes, lock_timeout_succeeded = (
        _run_lock_timeout_retry_probe(
            engine,
            safe_url=safe_url,
            tls_config=tls_config,
            expected_database=expected_database,
            expected_server_uuid=expected_server_uuid,
            run_id=run_id,
            repository=repository,
        )
    )

    first_deadlock_job, _first_deadlock_type = _create_probe_job(
        repository,
        engine,
        run_id=run_id,
        label="deadlock-a",
    )
    second_deadlock_job, _second_deadlock_type = _create_probe_job(
        repository,
        engine,
        run_id=run_id,
        label="deadlock-b",
    )
    deadlock_attempts, deadlock_codes = _run_deadlock_probe(
        engine,
        first_job_id=first_deadlock_job,
        second_job_id=second_deadlock_job,
    )

    _ttl_job_id, ttl_type = _create_probe_job(
        repository,
        engine,
        run_id=run_id,
        label="ttl-boundary",
    )
    ttl_now = _database_utc_now(engine)
    try:
        repository.claim_due_job(
            worker_id="v4-acceptance-ttl-over",
            lease_token=_probe_digest(run_id, "ttl-over-token"),
            now=ttl_now,
            lease_until=ttl_now
            + timedelta(
                seconds=JOB_LEASE_MAX_DURATION_SECONDS,
                microseconds=1,
            ),
            job_type=ttl_type,
        )
    except ValueError:
        over_limit_rejected = True
    else:
        over_limit_rejected = False
    ttl_result = repository.claim_due_job(
        worker_id="v4-acceptance-ttl-boundary",
        lease_token=_probe_digest(run_id, "ttl-boundary-token"),
        now=ttl_now,
        lease_until=ttl_now
        + timedelta(seconds=JOB_LEASE_MAX_DURATION_SECONDS),
        job_type=ttl_type,
    )
    if ttl_result is None or not ttl_result.claimed:
        raise RuntimeError("V4 exact 900-second lease boundary was not accepted")

    expired_job_id, expired_type = _create_probe_job(
        repository,
        engine,
        run_id=run_id,
        label="expired-reap",
        max_attempts=1,
    )
    expired_claim_now = _database_utc_now(engine)
    expired_until = expired_claim_now + timedelta(seconds=1)
    expired_claim = repository.claim_due_job(
        worker_id="v4-acceptance-expired-owner",
        lease_token=_probe_digest(run_id, "expired-first-token"),
        now=expired_claim_now,
        lease_until=expired_until,
        job_type=expired_type,
    )
    if expired_claim is None or not expired_claim.claimed:
        raise RuntimeError("V4 expiration probe was not initially claimed")
    reap_now = _wait_until_expired(engine, expired_until)
    reap_result = repository.claim_due_job(
        worker_id="v4-acceptance-reaper",
        lease_token=_probe_digest(run_id, "expired-reap-token"),
        now=reap_now,
        lease_until=reap_now + timedelta(seconds=30),
        job_type=expired_type,
    )
    if reap_result is not None:
        raise RuntimeError("exhausted expired V4 lease was reclaimed")
    expired_job = repository.get_job(expired_job_id)
    if expired_job is None:
        raise RuntimeError("expired V4 job disappeared")

    terminal_job_id, terminal_type = _create_probe_job(
        repository,
        engine,
        run_id=run_id,
        label="terminal-conflict",
    )
    terminal_claim_now = _database_utc_now(engine)
    terminal_until = terminal_claim_now + timedelta(seconds=30)
    terminal_token = _probe_digest(run_id, "terminal-token")
    terminal_claim = repository.claim_due_job(
        worker_id="v4-acceptance-terminal-worker",
        lease_token=terminal_token,
        now=terminal_claim_now,
        lease_until=terminal_until,
        job_type=terminal_type,
    )
    if terminal_claim is None or not terminal_claim.claimed:
        raise RuntimeError("V4 terminal probe was not claimed")
    complete_now = _database_utc_now(engine)
    if complete_now <= terminal_claim.job.updated_at:
        complete_now = terminal_claim.job.updated_at + timedelta(microseconds=1)
    completed = repository.complete(
        terminal_job_id,
        worker_id="v4-acceptance-terminal-worker",
        lease_token=terminal_token,
        attempt_count=terminal_claim.job.attempt_count,
        observed_lease_until=terminal_until,
        run_uid=_probe_job_id(run_id, "terminal-run"),
        now=complete_now,
    )
    if completed.status != JOB_SUCCEEDED:
        raise RuntimeError("V4 terminal probe did not succeed")
    try:
        repository.complete(
            terminal_job_id,
            worker_id="v4-acceptance-terminal-worker",
            lease_token=terminal_token,
            attempt_count=terminal_claim.job.attempt_count,
            observed_lease_until=terminal_until,
            run_uid=_probe_job_id(run_id, "terminal-run"),
            now=complete_now + timedelta(microseconds=1),
        )
    except JobAlreadyTerminalError:
        terminal_retry_conflict = True
    else:
        terminal_retry_conflict = False

    _reuse_job_id, reuse_type = _create_probe_job(
        repository,
        engine,
        run_id=run_id,
        label="historical-token-reuse",
    )
    reuse_now = _database_utc_now(engine)
    try:
        repository.claim_due_job(
            worker_id="v4-acceptance-reuse-worker",
            lease_token=terminal_token,
            now=reuse_now,
            lease_until=reuse_now + timedelta(seconds=30),
            job_type=reuse_type,
        )
    except JobConflictError:
        historical_token_reuse_conflict = True
    else:
        historical_token_reuse_conflict = False

    rollback_absent = _run_job_transaction_rollback_probe(
        engine,
        run_id=run_id,
    )
    return _JobLeaseBehaviorMatrix(
        direct_sql_whitespace_rejections=whitespace_rejections,
        same_token_outcomes=same_outcomes,
        two_worker_claimed_count=worker_outcomes.count("claimed"),
        two_worker_empty_count=worker_outcomes.count("empty"),
        lock_timeout_error_codes=lock_timeout_codes,
        lock_timeout_retry_succeeded=lock_timeout_succeeded,
        deadlock_error_codes=deadlock_codes,
        deadlock_attempts=deadlock_attempts,
        max_lease_duration_seconds=JOB_LEASE_MAX_DURATION_SECONDS,
        over_limit_rejected=over_limit_rejected,
        expired_job_status=expired_job.status,
        expired_job_error_code=str(expired_job.error_code or ""),
        terminal_retry_conflict=terminal_retry_conflict,
        historical_token_reuse_conflict=historical_token_reuse_conflict,
        transaction_rollback_absent=rollback_absent,
    )


def _assert_job_lease_behavior_matrix(
    matrix: _JobLeaseBehaviorMatrix,
) -> None:
    checks = {
        "direct_sql_whitespace": matrix.direct_sql_whitespace_rejections
        == ("insert-leading", "update-trailing"),
        "same_token": matrix.same_token_outcomes
        == ("claimed", "replayed"),
        "two_workers": matrix.two_worker_claimed_count == 1
        and matrix.two_worker_empty_count == 1,
        "lock_timeout_retry": bool(matrix.lock_timeout_error_codes)
        and set(matrix.lock_timeout_error_codes) == {1205}
        and len(matrix.lock_timeout_error_codes)
        < JOB_LEASE_ACCEPTANCE_MAX_TRANSIENT_ATTEMPTS
        and matrix.lock_timeout_retry_succeeded,
        "deadlock_retry": 1213 in matrix.deadlock_error_codes
        and bool(matrix.deadlock_attempts)
        and max(matrix.deadlock_attempts)
        <= JOB_LEASE_ACCEPTANCE_MAX_TRANSIENT_ATTEMPTS
        and max(matrix.deadlock_attempts) > 1,
        "lease_duration": matrix.max_lease_duration_seconds == 900
        and matrix.over_limit_rejected,
        "expired_reap": matrix.expired_job_status == JOB_FAILED
        and matrix.expired_job_error_code == EXHAUSTED_LEASE_ERROR_CODE,
        "terminal_conflict": matrix.terminal_retry_conflict,
        "historical_token_reuse": matrix.historical_token_reuse_conflict,
        "transaction_rollback": matrix.transaction_rollback_absent,
    }
    failed = tuple(name for name, passed in checks.items() if not passed)
    if failed:
        raise RuntimeError(
            "V4 job lease behavior acceptance failed closed: "
            + ", ".join(failed)
        )


def run_mysql_acceptance(
    url: str,
    *,
    expected_server_uuid: str,
    concurrency: int = 2,
    tls_config: MySQLAcceptanceTLSConfig | None = None,
) -> MySQLAcceptanceReport:
    """Run one migration followed by replay checks in an empty database."""

    _assert_frozen_migration_contract()
    safe_url = require_dedicated_test_url(url)
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    if (
        not isinstance(concurrency, int)
        or isinstance(concurrency, bool)
        or concurrency < 2
        or concurrency > 8
    ):
        raise ValueError("concurrency must be an integer between 2 and 8")

    database = str(make_url(safe_url).database)
    serial_engine = create_tool_engine(
        safe_url,
        tls_config=tls_config,
        future=True,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    try:
        database, version, server_uuid, version_comment = (
            _preflight_empty_schema(serial_engine, database, expected_uuid)
        )
        before = frozenset()
        initial_results = tuple(run_v4_migrations(serial_engine))
        serial_replay_results = tuple(run_v4_migrations(serial_engine))
        after_serial = _table_names(serial_engine)
        _assert_isolated_tables(after_serial)
        if after_serial != V4_CONTROL_PLANE_TABLES:
            missing = sorted(V4_CONTROL_PLANE_TABLES - after_serial)
            raise RuntimeError(
                "V4 migration did not create the full schema: "
                + ", ".join(missing)
            )
        _assert_job_lease_schema(serial_engine)
    finally:
        serial_engine.dispose()

    concurrent_engine = create_tool_engine(
        safe_url,
        tls_config=tls_config,
        future=True,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    try:
        _assert_engine_identity(concurrent_engine, database, expected_uuid)
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(run_v4_migrations, concurrent_engine)
                for _ in range(concurrency)
            ]
            concurrent_replay_results = tuple(
                tuple(future.result()) for future in futures
            )
        observed = _table_names(concurrent_engine)
        _assert_isolated_tables(observed)
        if observed != V4_CONTROL_PLANE_TABLES:
            raise RuntimeError("V4 schema changed during concurrent replay")
        _assert_job_lease_schema(concurrent_engine)
    finally:
        concurrent_engine.dispose()

    expected_checksums = _checksums(initial_results)
    all_replays = (serial_replay_results, *concurrent_replay_results)
    if any(_checksums(run) != expected_checksums for run in all_replays):
        raise RuntimeError("V4 migration checksum diverged across replays")
    if not initial_results or any(
        status != "applied" for status in _statuses(initial_results)
    ):
        raise RuntimeError(
            "empty V4 acceptance database did not perform an initial migration"
        )
    if any(status != "exists" for status in _statuses(serial_replay_results)):
        raise RuntimeError("V4 migration replay was not idempotent")
    if any(
        status != "exists"
        for run in concurrent_replay_results
        for status in _statuses(run)
    ):
        raise RuntimeError("concurrent V4 migration replay was not idempotent")

    return MySQLAcceptanceReport(
        mode="serial-replay",
        database=database,
        server_version=version,
        server_version_comment=version_comment,
        server_uuid=server_uuid,
        least_privilege_attested=True,
        started_empty=not before,
        initial_migration=_statuses(initial_results),
        serial_replay=_statuses(serial_replay_results),
        concurrent_replays=tuple(
            _statuses(run) for run in concurrent_replay_results
        ),
        observed_tables=tuple(sorted(observed)),
        checksums=expected_checksums,
        production_activation_allowed=False,
        actionable_output_allowed=False,
    )


def run_mysql_concurrent_initial_acceptance(
    url: str,
    *,
    expected_server_uuid: str,
    concurrency: int = 2,
    tls_config: MySQLAcceptanceTLSConfig | None = None,
) -> MySQLConcurrentInitialReport:
    """Race first migrations in a dedicated empty database without cleanup.

    This mode must be run against a different fresh database from the serial
    acceptance mode.  The V4 migration named lock should allow exactly one
    worker to apply each migration while every other worker observes it.
    """

    _assert_frozen_migration_contract()
    safe_url = require_dedicated_test_url(url)
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    if (
        not isinstance(concurrency, int)
        or isinstance(concurrency, bool)
        or concurrency < 2
        or concurrency > 8
    ):
        raise ValueError("concurrency must be an integer between 2 and 8")
    database = str(make_url(safe_url).database)
    preflight_engine = create_tool_engine(
        safe_url,
        tls_config=tls_config,
        future=True,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    try:
        database, version, server_uuid, version_comment = (
            _preflight_empty_schema(preflight_engine, database, expected_uuid)
        )
        before = frozenset()
    finally:
        preflight_engine.dispose()

    engines = tuple(
        create_tool_engine(
            safe_url,
            tls_config=tls_config,
            future=True,
            pool_size=1,
            max_overflow=0,
            pool_pre_ping=True,
        )
        for _ in range(concurrency)
    )
    try:
        for worker_engine in engines:
            _assert_engine_identity(worker_engine, database, expected_uuid)
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(run_v4_migrations, worker_engine)
                for worker_engine in engines
            ]
            runs = tuple(tuple(future.result()) for future in futures)
        observed = _table_names(engines[0])
        _assert_isolated_tables(observed)
        if observed != V4_CONTROL_PLANE_TABLES:
            missing = sorted(V4_CONTROL_PLANE_TABLES - observed)
            raise RuntimeError(
                "concurrent initial migration did not create the full schema: "
                + ", ".join(missing)
            )
        _assert_job_lease_schema(engines[0])
    finally:
        for worker_engine in engines:
            worker_engine.dispose()

    if not runs or any(not run for run in runs):
        raise RuntimeError("concurrent initial migration returned no results")
    expected_checksums = _checksums(runs[0])
    if any(_checksums(run) != expected_checksums for run in runs[1:]):
        raise RuntimeError(
            "V4 migration checksum diverged across concurrent initial runs"
        )
    widths = {len(run) for run in runs}
    if len(widths) != 1:
        raise RuntimeError("concurrent initial migration result widths differ")
    for migration_index in range(len(runs[0])):
        statuses = tuple(
            _statuses(run)[migration_index]
            for run in runs
        )
        if statuses.count("applied") != 1 or any(
            status not in {"applied", "exists"} for status in statuses
        ):
            raise RuntimeError(
                "concurrent initial migration must have exactly one applied "
                "writer per migration"
            )

    return MySQLConcurrentInitialReport(
        mode="concurrent-initial",
        database=database,
        server_version=version,
        server_version_comment=version_comment,
        server_uuid=server_uuid,
        least_privilege_attested=True,
        started_empty=not before,
        concurrent_initial_runs=tuple(_statuses(run) for run in runs),
        observed_tables=tuple(sorted(observed)),
        checksums=expected_checksums,
        production_activation_allowed=False,
        actionable_output_allowed=False,
    )


def _apply_partial_migration_prefix(
    engine: Engine,
    statements: tuple[str, ...],
) -> None:
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(
                text(
                    _mysql_regexp_compatible_statement(
                        connection,
                        statement,
                    )
                )
            )


def _apply_completed_migration_prefix(
    engine: Engine,
    migration_count: int,
) -> None:
    if migration_count <= 0:
        return
    with engine.connect() as connection:
        connection.execute(text(MIGRATION_TABLE_DDL))
        connection.commit()
        for migration in MIGRATIONS[:migration_count]:
            statements = tuple(migration["statements"])
            for statement in statements:
                connection.execute(
                    text(
                        _mysql_regexp_compatible_statement(
                            connection,
                            statement,
                        )
                    )
                )
                connection.commit()
            connection.execute(
                text(
                    "INSERT INTO schema_migration_v4 "
                    "(version, checksum, statement_count) "
                    "VALUES (:version, :checksum, :statement_count)"
                ),
                {
                    "version": str(migration["version"]),
                    "checksum": _statement_checksum(statements),
                    "statement_count": len(statements),
                },
            )
            connection.commit()


def run_mysql_partial_recovery_acceptance(
    url: str,
    *,
    expected_server_uuid: str,
    partial_statement_count: int = 1,
    partial_migration_index: int = 0,
    tls_config: MySQLAcceptanceTLSConfig | None = None,
) -> MySQLPartialRecoveryReport:
    """Create an intentional DDL prefix, then verify expand-only recovery.

    The function never removes the partial schema.  It is restricted to a
    separately provisioned empty V4 test/CI database and verifies that the
    regular migration completes and then replays idempotently.
    """

    _assert_frozen_migration_contract()
    safe_url = require_dedicated_test_url(url)
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    if type(partial_statement_count) is not int:
        raise TypeError("partial_statement_count must be exactly int")
    if type(partial_migration_index) is not int:
        raise TypeError("partial_migration_index must be exactly int")
    if not 0 <= partial_migration_index < len(MIGRATIONS):
        raise ValueError("partial_migration_index is outside the migration plan")
    target_statements = tuple(
        MIGRATIONS[partial_migration_index]["statements"]
    )
    if not 1 <= partial_statement_count < len(target_statements):
        raise ValueError(
            "partial_statement_count must leave the target migration incomplete"
        )
    database = str(make_url(safe_url).database)
    engine = create_tool_engine(
        safe_url,
        tls_config=tls_config,
        future=True,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    try:
        database, version, server_uuid, version_comment = (
            _preflight_empty_schema(engine, database, expected_uuid)
        )
        before = frozenset()
        _apply_completed_migration_prefix(engine, partial_migration_index)
        _apply_partial_migration_prefix(
            engine,
            target_statements[:partial_statement_count],
        )
        partial_tables = _table_names(engine)
        _assert_isolated_tables(partial_tables)
        if not partial_tables or (
            partial_migration_index == 0
            and partial_tables == V4_CONTROL_PLANE_TABLES
        ):
            raise RuntimeError(
                "partial DDL prefix did not leave an observable incomplete schema"
            )
        recovery_results = tuple(run_v4_migrations(engine))
        replay_results = tuple(run_v4_migrations(engine))
        observed = _table_names(engine)
        _assert_isolated_tables(observed)
        if observed != V4_CONTROL_PLANE_TABLES:
            missing = sorted(V4_CONTROL_PLANE_TABLES - observed)
            raise RuntimeError(
                "V4 migration did not recover the full schema: "
                + ", ".join(missing)
            )
        _assert_job_lease_schema(engine)
    finally:
        engine.dispose()

    expected_checksums = _checksums(recovery_results)
    expected_recovery = tuple(
        "exists" if index < partial_migration_index else "applied"
        for index in range(len(MIGRATIONS))
    )
    if _statuses(recovery_results) != expected_recovery:
        raise RuntimeError("partial V4 schema was not recovered as applied")
    if _checksums(replay_results) != expected_checksums or any(
        status != "exists" for status in _statuses(replay_results)
    ):
        raise RuntimeError("recovered V4 migration did not replay idempotently")

    return MySQLPartialRecoveryReport(
        mode="partial-recovery",
        database=database,
        server_version=version,
        server_version_comment=version_comment,
        server_uuid=server_uuid,
        least_privilege_attested=True,
        started_empty=not before,
        partial_migration_version=str(
            MIGRATIONS[partial_migration_index]["version"]
        ),
        partial_statement_count=partial_statement_count,
        partial_observed_tables=tuple(sorted(partial_tables)),
        recovery_migration=_statuses(recovery_results),
        recovery_replay=_statuses(replay_results),
        observed_tables=tuple(sorted(observed)),
        checksums=expected_checksums,
        production_activation_allowed=False,
        actionable_output_allowed=False,
    )


def _acceptance_context(
    decision_at: datetime,
    *,
    marker: str,
) -> DecisionContext:
    knowledge_cutoff = decision_at - timedelta(seconds=2)
    watermark_at = knowledge_cutoff - timedelta(seconds=3)
    return DecisionContext(
        decision_time=decision_at,
        decision_clock=DecisionClock.INTRADAY,
        knowledge_cutoff=knowledge_cutoff,
        trade_date=decision_at.date(),
        universe_version=f"v4:acceptance:universe:{marker}",
        data_manifest=DataManifest(
            record_hashes={f"quotes-{marker}": marker * 64}
        ),
        portfolio_policy_version="v4:acceptance:portfolio:v1",
        execution_contract_version="v4:acceptance:execution:v1",
        fee_schedule_version="v4:acceptance:fees:v1",
        account_snapshot_id=f"v4-acceptance-account-{marker}",
        code_commit_sha=marker * 40,
        config_hash=marker * 64,
        random_seed=ord(marker),
        source_watermarks={
            "realtime_quotes": SourceWatermark(
                source="realtime_quotes",
                knowledge_time=watermark_at,
                record_count=3,
                quality_status=QualityStatus.PASS,
                snapshot_id=f"quotes-{marker}",
                valid_until=decision_at + timedelta(minutes=5),
                coverage=Decimal("1"),
                batch_id=f"quotes-batch-{marker}",
                schema_version="v4:acceptance:quotes:v1",
                content_hash=marker * 64,
            )
        },
        factor_spec_versions={"momentum": "v4:acceptance:factor:v1"},
        forecast_contract_ids=("v4:acceptance:forecast:v1",),
        model_versions={"cross_sectional": "v4:acceptance:model:v1"},
        model_artifact_hashes={"cross_sectional": marker * 64},
        model_training_cutoffs={
            "cross_sectional": knowledge_cutoff - timedelta(days=30)
        },
        model_available_at={
            "cross_sectional": knowledge_cutoff - timedelta(days=1)
        },
        calibration_versions={
            "cross_sectional": "v4:acceptance:calibration:v1"
        },
        calibration_artifact_hashes={"cross_sectional": marker * 64},
        calibration_training_cutoffs={
            "cross_sectional": knowledge_cutoff - timedelta(days=30)
        },
        calibration_available_at={
            "cross_sectional": knowledge_cutoff - timedelta(days=1)
        },
    )


def _seed_head_cas_scenario(
    repository: TradingV4Repository,
) -> tuple[str, tuple[str, str], datetime]:
    base = datetime(2026, 8, 3, 1, 0, tzinfo=timezone.utc)
    run_uids = ("v4-cas-run-a", "v4-cas-run-b", "v4-cas-run-c")
    for offset, (marker, run_uid) in enumerate(
        zip(("a", "b", "c"), run_uids)
    ):
        now = base + timedelta(minutes=offset)
        created_context = repository.create_or_get_context(
            _acceptance_context(now, marker=marker),
            created_at=now,
        )
        stored = created_context.context
        repository.create_or_get_run(
            context_id=str(stored["context_id"]),
            account_id="v4-acceptance-paper",
            channel="shadow",
            run_type="ACCEPTANCE",
            trigger_type="MANUAL_ACCEPTANCE",
            model_set_version=str(stored["model_set_version"]),
            config_version=str(stored["config_version"]),
            code_commit_sha=str(stored["code_commit_sha"]),
            run_uid=run_uid,
            created_at=now,
        )
        repository.mark_running(run_uid, occurred_at=now + timedelta(seconds=1))
        repository.mark_validating(
            run_uid,
            occurred_at=now + timedelta(seconds=2),
        )
        repository.commit_run(
            run_uid,
            result_hash=marker * 64,
            occurred_at=now + timedelta(seconds=3),
        )
    repository.publish_committed_head(
        run_uids[0],
        published_by="v4-acceptance",
        published_at=base + timedelta(seconds=4),
        expected_head_version=0,
    )
    return run_uids[0], (run_uids[1], run_uids[2]), base


def _publish_head_cas_worker(
    engine: Engine,
    *,
    run_uid: str,
    published_at: datetime,
    expected_database: str,
    expected_server_uuid: str,
) -> tuple[str, str]:
    repository = TradingV4Repository(engine)
    try:
        with engine.begin() as connection:
            _server_identity_from_connection(
                connection,
                expected_database,
                expected_server_uuid,
            )
            result = repository._publish_head(
                connection,
                run_uid,
                published_by="v4-acceptance-cas",
                published_at=published_at,
                expected_head_version=1,
            )
        if not result.changed or int(result.head["head_version"]) != 2:
            raise RuntimeError("V4 head CAS winner produced an invalid head")
        return run_uid, "published"
    except HeadPublishConflictError:
        return run_uid, "conflict"


def run_mysql_head_cas_acceptance(
    url: str,
    *,
    expected_server_uuid: str,
    tls_config: MySQLAcceptanceTLSConfig | None = None,
) -> MySQLHeadCASReport:
    """Verify one-winner repository head publication with pool size one.

    Every concurrent participant owns a separate single-connection pool and
    binds DATABASE(), server UUID, version and grants on the same transaction
    connection used by the repository CAS operation.
    """

    _assert_frozen_migration_contract()
    safe_url = require_dedicated_test_url(url)
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    expected_database = str(make_url(safe_url).database)
    seed_engine = create_tool_engine(
        safe_url,
        tls_config=tls_config,
        future=True,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    try:
        database, version, server_uuid, version_comment = (
            _preflight_empty_schema(
                seed_engine,
                expected_database,
                expected_uuid,
            )
        )
        migration_results = tuple(run_v4_migrations(seed_engine))
        if not migration_results or any(
            status != "applied" for status in _statuses(migration_results)
        ):
            raise RuntimeError("V4 head CAS acceptance did not migrate empty DB")
        _assert_job_lease_schema(seed_engine)
        repository = TradingV4Repository(seed_engine)
        _initial_uid, candidates, base = _seed_head_cas_scenario(repository)

        worker_engines = tuple(
            create_tool_engine(
                safe_url,
                tls_config=tls_config,
                future=True,
                pool_size=1,
                max_overflow=0,
                pool_pre_ping=True,
            )
            for _ in candidates
        )
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(
                        _publish_head_cas_worker,
                        worker_engine,
                        run_uid=run_uid,
                        published_at=base + timedelta(minutes=3),
                        expected_database=database,
                        expected_server_uuid=expected_uuid,
                    )
                    for worker_engine, run_uid in zip(worker_engines, candidates)
                ]
                outcomes = tuple(future.result() for future in futures)
        finally:
            for worker_engine in worker_engines:
                worker_engine.dispose()

        winners = tuple(uid for uid, status in outcomes if status == "published")
        conflicts = tuple(uid for uid, status in outcomes if status == "conflict")
        if len(winners) != 1 or len(conflicts) != 1:
            raise RuntimeError(
                "V4 head CAS must produce exactly one winner and one conflict"
            )
        head = repository.get_head(
            "shadow",
            account_id="v4-acceptance-paper",
        )
        if head is None:
            raise RuntimeError("V4 head CAS final head is missing")
        if int(head["head_version"]) != 2 or str(head["run_uid"]) != winners[0]:
            raise RuntimeError("V4 head CAS final state differs from winner")
    finally:
        seed_engine.dispose()

    return MySQLHeadCASReport(
        mode="head-cas",
        database=database,
        server_version=version,
        server_version_comment=version_comment,
        server_uuid=server_uuid,
        least_privilege_attested=True,
        started_empty=True,
        initial_migration=_statuses(migration_results),
        initial_head_version=1,
        successful_run_uid=winners[0],
        conflicting_run_uid=conflicts[0],
        final_head_version=int(head["head_version"]),
        final_head_run_uid=str(head["run_uid"]),
        production_activation_allowed=False,
        actionable_output_allowed=False,
    )


_CONTROL_TRANSITION_INSERT_SQL = text(
    """
    INSERT INTO st_runtime_control_transition_v4 (
        transition_id, control_key, previous_value_json, next_value_json,
        next_version, changed_by, reason, event_hash, changed_at
    ) VALUES (
        :event_hash, :control_key, NULL, :control_value_json, 1,
        'v4-acceptance', 'transaction recovery acceptance',
        :event_hash, :occurred_at
    )
    """
)
_CONTROL_INSERT_SQL = text(
    """
    INSERT INTO st_runtime_control_v4 (
        control_key, control_value_json, version, updated_by, reason,
        created_at, updated_at
    ) VALUES (
        :control_key, :control_value_json, 1, 'v4-acceptance',
        'transaction recovery acceptance', :occurred_at, :occurred_at
    )
    """
)


def _insert_recovery_probe(
    connection: Connection,
    *,
    control_key: str,
    occurred_at: datetime,
) -> None:
    parameters = {
        "control_key": control_key,
        "control_value_json": '{"enabled":false}',
        "event_hash": hashlib.sha256(
            f"v4-acceptance-control:{control_key}".encode("utf-8")
        ).hexdigest(),
        "occurred_at": occurred_at,
    }
    connection.execute(
        _CONTROL_TRANSITION_INSERT_SQL,
        parameters,
    )
    connection.execute(_CONTROL_INSERT_SQL, parameters)


def _probe_counts(engine: Engine, control_key: str) -> tuple[int, int]:
    with engine.connect() as connection:
        row = connection.execute(
            text(
                "SELECT "
                "(SELECT COUNT(*) FROM st_runtime_control_v4 c "
                " WHERE c.control_key = :control_key) AS control_count, "
                "(SELECT COUNT(*) "
                " FROM st_runtime_control_transition_v4 t "
                " WHERE t.control_key = :control_key) AS transition_count"
            ),
            {"control_key": control_key},
        ).mappings().one()
    return int(row["control_count"]), int(row["transition_count"])


def _force_disconnect(connection: Connection) -> None:
    """Invalidate the checked-out DBAPI connection without committing."""

    connection.invalidate()


def _assert_unpaired_current_write_rejected(
    engine: Engine,
    *,
    occurred_at: datetime,
) -> None:
    control_key = "v4.acceptance.unpaired-current"
    parameters = {
        "control_key": control_key,
        "control_value_json": '{"enabled":false}',
        "occurred_at": occurred_at,
    }
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(_CONTROL_INSERT_SQL, parameters)
        except DBAPIError:
            transaction.rollback()
        else:
            transaction.rollback()
            raise RuntimeError(
                "V4 runtime control accepted an unpaired current write"
            )
    if _probe_counts(engine, control_key) != (0, 0):
        raise RuntimeError(
            "V4 rejected unpaired current write left persistent residue"
        )


def _run_transaction_recovery_probes(
    engine: Engine,
    *,
    expected_database: str,
    expected_server_uuid: str,
) -> tuple[bool, bool, bool]:
    now = datetime(2026, 8, 3, 2, 0, tzinfo=timezone.utc)
    rollback_key = "v4.acceptance.rollback"
    disconnect_key = "v4.acceptance.disconnect"
    recovery_key = "v4.acceptance.recovered"

    _assert_unpaired_current_write_rejected(engine, occurred_at=now)

    with engine.connect() as connection:
        _server_identity_from_connection(
            connection,
            expected_database,
            expected_server_uuid,
        )
        connection.rollback()
        transaction = connection.begin()
        _insert_recovery_probe(
            connection,
            control_key=rollback_key,
            occurred_at=now,
        )
        transaction.rollback()
    explicit_absent = _probe_counts(engine, rollback_key) == (0, 0)

    connection = engine.connect()
    try:
        _server_identity_from_connection(
            connection,
            expected_database,
            expected_server_uuid,
        )
        connection.rollback()
        connection.begin()
        _insert_recovery_probe(
            connection,
            control_key=disconnect_key,
            occurred_at=now + timedelta(seconds=1),
        )
        _force_disconnect(connection)
    finally:
        connection.close()
    disconnect_absent = _probe_counts(engine, disconnect_key) == (0, 0)

    with engine.begin() as recovered_connection:
        _server_identity_from_connection(
            recovered_connection,
            expected_database,
            expected_server_uuid,
        )
        _insert_recovery_probe(
            recovered_connection,
            control_key=recovery_key,
            occurred_at=now + timedelta(seconds=2),
        )
    recovery_visible = _probe_counts(engine, recovery_key) == (1, 1)
    return explicit_absent, disconnect_absent, recovery_visible


def run_mysql_transaction_recovery_acceptance(
    url: str,
    *,
    expected_server_uuid: str,
    tls_config: MySQLAcceptanceTLSConfig | None = None,
) -> MySQLTransactionRecoveryReport:
    """Verify rollback and connection-loss recovery in a fresh V4 schema."""

    _assert_frozen_migration_contract()
    safe_url = require_dedicated_test_url(url)
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    expected_database = str(make_url(safe_url).database)
    engine = create_tool_engine(
        safe_url,
        tls_config=tls_config,
        future=True,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    try:
        database, version, server_uuid, version_comment = (
            _preflight_empty_schema(engine, expected_database, expected_uuid)
        )
        migration_results = tuple(run_v4_migrations(engine))
        if not migration_results or any(
            status != "applied" for status in _statuses(migration_results)
        ):
            raise RuntimeError(
                "V4 transaction recovery acceptance did not migrate empty DB"
            )
        _assert_job_lease_schema(engine)
        explicit_absent, disconnect_absent, recovery_visible = (
            _run_transaction_recovery_probes(
                engine,
                expected_database=database,
                expected_server_uuid=expected_uuid,
            )
        )
        if not (explicit_absent and disconnect_absent and recovery_visible):
            raise RuntimeError(
                "V4 transaction rollback/disconnect recovery invariant failed"
            )
    finally:
        engine.dispose()

    return MySQLTransactionRecoveryReport(
        mode="transaction-recovery",
        database=database,
        server_version=version,
        server_version_comment=version_comment,
        server_uuid=server_uuid,
        least_privilege_attested=True,
        started_empty=True,
        initial_migration=_statuses(migration_results),
        explicit_rollback_absent=explicit_absent,
        disconnect_rollback_absent=disconnect_absent,
        recovery_write_visible=recovery_visible,
        production_activation_allowed=False,
        actionable_output_allowed=False,
    )


def run_mysql_job_lease_behavior_acceptance(
    url: str,
    *,
    expected_server_uuid: str,
    tls_config: MySQLAcceptanceTLSConfig | None = None,
) -> MySQLJobLeaseBehaviorReport:
    """Run the real MySQL V4 lease matrix in one dedicated empty schema.

    The mode creates only UUID-scoped test facts after the normal migrations.
    It neither cleans the schema nor starts a worker, account, order, position,
    or actionable-output path.
    """

    _assert_frozen_migration_contract()
    safe_url = require_dedicated_test_url(url)
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    expected_database = str(make_url(safe_url).database)
    acceptance_run_id = str(uuid.uuid4())
    engine = create_tool_engine(
        safe_url,
        tls_config=tls_config,
        future=True,
        pool_size=8,
        max_overflow=0,
        pool_pre_ping=True,
    )
    try:
        database, version, server_uuid, version_comment = (
            _preflight_empty_schema(
                engine,
                expected_database,
                expected_uuid,
            )
        )
        migration_results = tuple(run_v4_migrations(engine))
        if not migration_results or any(
            status != "applied" for status in _statuses(migration_results)
        ):
            raise RuntimeError(
                "V4 job lease behavior acceptance did not migrate empty DB"
            )
        observed_tables = _table_names(engine)
        _assert_isolated_tables(observed_tables)
        if observed_tables != V4_CONTROL_PLANE_TABLES:
            raise RuntimeError(
                "V4 job lease behavior acceptance schema is incomplete"
            )
        _assert_job_lease_schema(engine)
        matrix = _run_job_lease_behavior_probes(
            engine,
            safe_url=safe_url,
            tls_config=tls_config,
            expected_database=database,
            expected_server_uuid=expected_uuid,
            run_id=acceptance_run_id,
        )
        _assert_job_lease_behavior_matrix(matrix)
    finally:
        engine.dispose()

    return MySQLJobLeaseBehaviorReport(
        mode="job-lease-behavior",
        database=database,
        server_version=version,
        server_version_comment=version_comment,
        server_uuid=server_uuid,
        least_privilege_attested=True,
        started_empty=True,
        acceptance_run_id=acceptance_run_id,
        initial_migration=_statuses(migration_results),
        direct_sql_whitespace_rejections=(
            matrix.direct_sql_whitespace_rejections
        ),
        same_token_outcomes=matrix.same_token_outcomes,
        two_worker_claimed_count=matrix.two_worker_claimed_count,
        two_worker_empty_count=matrix.two_worker_empty_count,
        lock_timeout_error_codes=matrix.lock_timeout_error_codes,
        lock_timeout_retry_succeeded=matrix.lock_timeout_retry_succeeded,
        deadlock_error_codes=matrix.deadlock_error_codes,
        deadlock_attempts=matrix.deadlock_attempts,
        max_lease_duration_seconds=matrix.max_lease_duration_seconds,
        over_limit_rejected=matrix.over_limit_rejected,
        expired_job_status=matrix.expired_job_status,
        expired_job_error_code=matrix.expired_job_error_code,
        terminal_retry_conflict=matrix.terminal_retry_conflict,
        historical_token_reuse_conflict=(
            matrix.historical_token_reuse_conflict
        ),
        transaction_rollback_absent=matrix.transaction_rollback_absent,
        production_activation_allowed=False,
        actionable_output_allowed=False,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run one initial V4 migration and serial/concurrent replays in an "
            "empty, dedicated MySQL test database"
        )
    )
    parser.add_argument(
        "--url-env",
        default=DEFAULT_URL_ENV,
        help="environment variable containing the dedicated test URL",
    )
    parser.add_argument(
        "--server-uuid-env",
        default=DEFAULT_SERVER_UUID_ENV,
        help="independent test/CI variable containing the expected server UUID",
    )
    parser.add_argument(
        "--ssl-ca-env",
        default=DEFAULT_SSL_CA_ENV,
        help="dedicated V4 TEST/CI variable containing the SSL CA file",
    )
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument(
        "--mode",
        choices=(
            "serial-replay",
            "concurrent-initial",
            "partial-recovery",
            "head-cas",
            "transaction-recovery",
            "job-lease-behavior",
        ),
        default="serial-replay",
        help=(
            "serial-replay runs one initial migration then replays; "
            "concurrent-initial races first migrations in a separate empty DB"
        ),
    )
    parser.add_argument(
        "--partial-statement-count",
        type=int,
        default=1,
        help="DDL prefix length used only by partial-recovery mode",
    )
    parser.add_argument(
        "--partial-migration-index",
        type=int,
        default=0,
        help="zero-based migration whose DDL prefix is interrupted",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    url = resolve_test_url(args.url_env)
    expected_server_uuid = resolve_server_uuid(args.server_uuid_env)
    tls_config = resolve_mysql_acceptance_tls_config("V4", args.ssl_ca_env)
    if args.mode == "concurrent-initial":
        report = run_mysql_concurrent_initial_acceptance(
            url,
            expected_server_uuid=expected_server_uuid,
            concurrency=args.concurrency,
            tls_config=tls_config,
        )
    elif args.mode == "partial-recovery":
        report = run_mysql_partial_recovery_acceptance(
            url,
            expected_server_uuid=expected_server_uuid,
            partial_statement_count=args.partial_statement_count,
            partial_migration_index=args.partial_migration_index,
            tls_config=tls_config,
        )
    elif args.mode == "head-cas":
        report = run_mysql_head_cas_acceptance(
            url,
            expected_server_uuid=expected_server_uuid,
            tls_config=tls_config,
        )
    elif args.mode == "transaction-recovery":
        report = run_mysql_transaction_recovery_acceptance(
            url,
            expected_server_uuid=expected_server_uuid,
            tls_config=tls_config,
        )
    elif args.mode == "job-lease-behavior":
        report = run_mysql_job_lease_behavior_acceptance(
            url,
            expected_server_uuid=expected_server_uuid,
            tls_config=tls_config,
        )
    else:
        report = run_mysql_acceptance(
            url,
            expected_server_uuid=expected_server_uuid,
            concurrency=args.concurrency,
            tls_config=tls_config,
        )
    print(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
