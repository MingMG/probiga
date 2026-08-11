"""Opt-in exact-version MySQL acceptance for the V2 evidence schema.

The command accepts only an explicitly named V2 evidence test/CI URL and an
empty, dedicated database.  It never falls back to an application database
URL and never cleans or reuses a populated schema.  A passing structural
report contains 51 migration-owned tables, one runner bootstrap control table,
and exactly one INACTIVE maintenance-fence row; it never authorizes production
or actionable output.
"""
from __future__ import annotations

import argparse
from concurrent.futures import (
    ThreadPoolExecutor,
    TimeoutError as FutureTimeoutError,
)
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
import re
from threading import Barrier, BrokenBarrierError
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine, make_url
from sqlalchemy.exc import ArgumentError

from server.common.mysql_version_policy import (
    is_isolated_acceptance_version,
    is_oracle_mysql_distribution,
    isolated_acceptance_versions_label,
)
from server.db.migrations_v2 import (
    MIGRATIONS,
    MIGRATION_TABLE_DDL,
    V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE,
    V2_EVIDENCE_MAINTENANCE_FENCE_DDL,
    V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
    V2_EVIDENCE_MAINTENANCE_FENCE_NAME,
    V2_EVIDENCE_MAINTENANCE_FENCE_TABLE,
    V2MigrationResult,
    run_v2_migrations,
)
from server.trading_v2.execution_evidence_schema_gate import (
    EVIDENCE_TABLES,
    inspect_v2_execution_evidence_schema,
)
from server.integrations.v2_execution_evidence_audit import (
    V2EvidenceHashAuditReport,
    audit_v2_execution_evidence_database,
)
from server.integrations.v2_execution_evidence_authority import (
    AuthorityVerificationLevel,
    AuthorityVerificationError,
    MySQLRegistryBackedAuthorityVerifier,
    require_verified_authority,
)
from server.integrations.v2_execution_evidence_authority_audit import (
    AUTHORITY_AUDIT_TABLES,
    V2AuthorityStoredRowAuditReport,
    audit_v2_execution_evidence_authority_database,
)
from server.integrations.v2_accounting_evidence_audit import (
    ACCOUNTING_AUDIT_TABLES,
    FINALIZATION_TABLE,
    LOT_EFFECT_TABLE,
    OUTCOME_TABLE,
    V2AccountingEvidenceAuditReport,
    audit_v2_accounting_evidence_database,
)
from server.integrations.v2_accounting_evidence_writer import (
    AccountingEvidenceAppendConflictError,
    AccountingEvidenceAppendStatus,
    append_fill_accounting_outcome,
)
from server.integrations.v2_execution_evidence_writer import (
    EvidenceAppendConflictError,
    EvidenceAppendStatus,
    append_cash_event_binding,
    append_fill_execution_evidence,
    append_market_calendar_evidence,
    append_order_transition_evidence,
    append_quote_receipt_evidence,
)
from tools.trading_v2_evidence_behavioral_scenario import (
    BehavioralEvidenceCase,
    BehavioralScenario,
    ConflictingBehavioralEvidencePair,
    ConflictingDoubleWriterScenario,
    build_behavioral_scenario,
    build_conflicting_double_writer_scenario,
)
from tools.trading_v2_evidence_extended_behavioral_scenario import (
    AccountingBehavioralScenario,
    AuthorityBehavioralScenario,
    build_accounting_behavioral_scenario,
    build_authority_behavioral_scenario,
)
from tools.trading_v2_evidence_negative_probes import (
    ALL_NEGATIVE_PROBE_OPERATIONS,
    EvidenceNegativeProbeCase,
    NegativeProbeOperation,
    run_negative_probes,
)
from tools.mysql_acceptance_tls import (
    MySQLAcceptanceTLSConfig,
    create_mysql_acceptance_engine as create_tool_engine,
    resolve_mysql_acceptance_tls_config,
)


DEFAULT_URL_ENV = "V2_EVIDENCE_TEST_MYSQL_URL"
DEFAULT_SERVER_UUID_ENV = "V2_EVIDENCE_TEST_MYSQL_SERVER_UUID"
DEFAULT_SSL_CA_ENV = "V2_EVIDENCE_TEST_MYSQL_SSL_CA"
_SAFE_URL_ENV_RE = re.compile(
    r"^V2_EVIDENCE_(?:TEST|CI)(?:_[A-Z0-9]+)*_MYSQL_URL$"
)
_SAFE_SERVER_UUID_ENV_RE = re.compile(
    r"^V2_EVIDENCE_(?:TEST|CI)(?:_[A-Z0-9]+)*_MYSQL_SERVER_UUID$"
)
_FORBIDDEN_URL_ENVS = frozenset({"MYSQL_URL", "DATABASE_URL"})
_SAFE_DATABASE_RE = re.compile(
    r"^[a-z0-9]+(?:_[a-z0-9]+)*_v2_evidence_"
    r"(?:test|ci)(?:_[a-z0-9]+)*$",
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
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "ALTER",
        "INDEX",
        "REFERENCES",
        "TRIGGER",
    }
)
_CREATE_TABLE_RE = re.compile(
    r"\bCREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+`?([a-z0-9_]+)`?",
    re.IGNORECASE,
)
EXPECTED_MIGRATION_COUNT = 15
EXPECTED_MIGRATION_STATEMENT_COUNT = 150
FROZEN_MAINTENANCE_FENCE_DDL_CHECKSUM = (
    "b6de54b8bee51e1dd87e7afd4cb9716b33db03d13755079b927ad38a310bb8b2"
)
EVIDENCE_TRIGGER_NAMES = frozenset(
    f"trg_{stem}_guard_{suffix}"
    for stem in (
        "market_calendar_evidence_v2",
        "quote_receipt_evidence_v2",
        "fill_execution_evidence_v2",
        "cash_event_binding_v2",
        "order_transition_v2",
    )
    for suffix in ("bi", "bu", "bd")
) | frozenset(
    {
        "trg_market_calendar_evidence_v2_authority_bi",
        "trg_quote_receipt_evidence_v2_authority_bi",
    }
)
EXPECTED_TRIGGER_NAMES = EVIDENCE_TRIGGER_NAMES | frozenset(
    {
        "trg_trade_account_v2_real_disabled_bi",
        "trg_trade_account_v2_real_disabled_bu",
    }
) | frozenset(
    f"trg_{stem}_guard_{suffix}"
    for stem in (
        "execution_authority_trust_key_v2",
        "execution_authority_receipt_v2",
        "execution_authority_key_revocation_v2",
        "execution_authority_receipt_revocation_v2",
        "execution_authority_attestation_v2",
        "fill_accounting_outcome_v2",
        "lot_transition_evidence_v2",
        "fill_accounting_finalization_v2",
    )
    for suffix in ("bi", "bu", "bd")
)
CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES = (
    "MARKET_CALENDAR",
    "QUOTE_RECEIPT",
    "FILL_EXECUTION",
    "CASH_EVENT",
    "ORDER_TRANSITION",
)
EXTENDED_BEHAVIORAL_COVERED_EVIDENCE_TYPES = (
    "EXTERNAL_AUTHORITY_REGISTRY",
    "ACCOUNTING_OUTCOME_FINALIZATION",
)
BEHAVIORAL_COVERED_EVIDENCE_TYPES = (
    *CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES,
    *EXTENDED_BEHAVIORAL_COVERED_EVIDENCE_TYPES,
)
BEHAVIORAL_NOT_COVERED_EVIDENCE_TYPES: tuple[str, ...] = ()
CORE_BEHAVIORAL_PROBES_COVERED = (
    "LEGAL_INSERT",
    "NEW_TRANSACTION_IDEMPOTENT",
    "NOOP_UPDATE_GUARD",
    "DELETE_GUARD",
    "OUTER_TRANSACTION_ROLLBACK",
    "INVALID_INSERT",
    "REPLACE",
    "ON_DUPLICATE_KEY_UPDATE",
    "IDENTICAL_EVIDENCE_DOUBLE_WRITER",
    "CONFLICTING_EVIDENCE_DOUBLE_WRITER",
)
EXTENDED_BEHAVIORAL_PROBES_COVERED = (
    "AUTHORITY_KEY_REGISTRATION",
    "AUTHORITY_RECEIPT_REGISTRATION",
    "AUTHORITY_NONCE_REPLAY_REJECTION",
    "AUTHORITY_SIGNATURE_REJECTION",
    "AUTHORITY_KEY_REVOCATION",
    "AUTHORITY_RECEIPT_REVOCATION",
    "AUTHORITY_CONCURRENT_REGISTRATION",
    "AUTHORITY_HISTORICAL_RECHECK",
    "ACCOUNTING_OUTCOME_EFFECT_FINALIZATION_ORDER",
    "ACCOUNTING_INTERRUPTION_ROLLBACK",
    "ACCOUNTING_EXACT_REPLAY",
    "ACCOUNTING_DIFFERENT_CONTENT_CONFLICT",
    "ACCOUNTING_FIFO",
    "ACCOUNTING_WHOLE_BATCH_ROLLBACK",
    "THREE_LAYER_NONEMPTY_DATABASE_AUDIT",
)
BEHAVIORAL_PROBES_COVERED = (
    *CORE_BEHAVIORAL_PROBES_COVERED,
    *EXTENDED_BEHAVIORAL_PROBES_COVERED,
)
BEHAVIORAL_PROBES_NOT_COVERED: tuple[str, ...] = ()
_DOUBLE_WRITER_LOCK_WAIT_SECONDS = 5
_DOUBLE_WRITER_FUTURE_TIMEOUT_SECONDS = 20
_SAFE_SQL_IDENTIFIER_RE = re.compile(r"^[a-z][a-z0-9_]*$")


def _checksum(statements: tuple[str, ...]) -> str:
    payload = "\n".join(item.strip() for item in statements).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


_DECLARED_MIGRATIONS = tuple(
    (str(item["version"]), _checksum(tuple(item["statements"])))
    for item in MIGRATIONS
)
FROZEN_EXPECTED_MIGRATIONS = (
    ("20260725_001_trading_v2_core", "c21ed007b17ff18604d6f022945db330cc2d8e4aef270104570e5eb60ccd6a40"),
    ("20260725_002_trading_v2_jobs_and_lifecycle", "e54264bcb2392b186a1ee3b9b8c478ced6d80bf7d120710ae3a5c1d36663676d"),
    ("20260725_003_trading_v2_execution_research_ops", "7be2c5567b0c52c97bdcd80fc17a4eb3d28c303b95acff799e5c5c35705b2a6c"),
    ("20260725_004_trading_v2_etf_truth_and_forward", "451b18146a10140b3d7256018fbf885823112ec62239eae1e00e6a6ea5894435"),
    ("20260725_005_trading_v2_theme_risk_chain", "2356709db6d2cb906cf4c839873e3b1ab668d9fb74e0be04e4dcd2eb227e3c8b"),
    ("20260726_006_real_trading_hard_guard", "16f75c5f0e9e329ebb632cc8cd895c96a626ce76b0c364134e9c54f1b31f9016"),
    ("20260726_007_market_regime_transition_state", "e47ac4757eb6990a4c741cd80d74f638f3429a89cf3103ab4f17497602b8b0f1"),
    ("20260727_008_intraday_dynamic_activation", "7aa2c2a51f1a69afbbfeb2408520172d8103aa8f8cfa46cb2ae5558ac9e26d63"),
    ("20260730_009_public_quote_failover", "d53d1315dd695bb570e1b9058156a3f6a77a86d68fe71d939aec523a4827fb61"),
    ("20260730_010_qmt_end_to_end_health", "d4a17a3f04c8d5fb0a51ea99c7cfea271abd6576a2ec829d8e57743d55f4d2b8"),
    ("20260803_011_v2_execution_evidence_bindings", "234a2b7a82573b5551b1485dd68598156e26d050d3b2d9b6a6ea76d3c34072d1"),
    ("20260803_012_v2_execution_evidence_guards", "cf596bc5157ea5f6d835c07089556164cde9c0fcaf0c3ace10f10b15ba4b6fd1"),
    ("20260803_013_v2_execution_evidence_natural_keys", "51addc459d4caae896ee656e901123646deb6a46584ac274092aa65026917eb8"),
    ("20260803_014_v2_execution_authority_attestations", "984e2ea7c637c728745b9b21c3b508980cc046c1c434d9851619984918a3823d"),
    ("20260803_015_v2_accounting_outcome_evidence", "8e06e57c38f7365fa471a7bde09f5cd4a3ea5aef5fee03c6195fd2930b725a2c"),
)
EXPECTED_MIGRATIONS = _DECLARED_MIGRATIONS
_DECLARED_MIGRATION_OWNED_TABLES = frozenset(
    match.group(1).lower()
    for statement in (
        MIGRATION_TABLE_DDL,
        *(
            sql
            for migration in MIGRATIONS
            for sql in tuple(migration["statements"])
        ),
    )
    for match in _CREATE_TABLE_RE.finditer(statement)
)
_DECLARED_RUNNER_BOOTSTRAP_TABLES = frozenset(
    match.group(1).lower()
    for match in _CREATE_TABLE_RE.finditer(
        V2_EVIDENCE_MAINTENANCE_FENCE_DDL
    )
)
EXPECTED_MIGRATION_OWNED_TABLES = frozenset(
    {
        "schema_migration_v2",
        "si_etf_code",
        "sm_etf_kline",
        "st_backtest_run_v2",
        "st_backtest_trade_v2",
        "st_cash_event_binding_v2",
        "st_cash_ledger_v2",
        "st_data_snapshot_v2",
        "st_decision_run_v2",
        "st_equity_daily_v2",
        "st_etf_forward_observation",
        "st_etf_forward_strategy",
        "st_execution_capability_v2",
        "st_execution_authority_attestation_v2",
        "st_execution_authority_key_revocation_v2",
        "st_execution_authority_receipt_v2",
        "st_execution_authority_receipt_revocation_v2",
        "st_execution_authority_trust_key_v2",
        "st_fault_drill_v2",
        "st_fee_profile_v2",
        "st_fill_execution_evidence_v2",
        "st_fill_accounting_outcome_v2",
        "st_fill_accounting_outcome_finalization_v2",
        "st_fill_v2",
        "st_instrument_rule_v2",
        "st_intraday_activation_v2",
        "st_intraday_market_state_v2",
        "st_intraday_watch_quote_v2",
        "st_job_v2",
        "st_market_calendar_evidence_v2",
        "st_lot_transition_evidence_v2",
        "st_order_transition_v2",
        "st_order_v2",
        "st_portfolio_plan_v2",
        "st_position_lot_v2",
        "st_public_quote_current_v2",
        "st_public_quote_receipt_v2",
        "st_qmt_minute_sync_receipt_v2",
        "st_qmt_realtime_sync_receipt_v2",
        "st_quote_event_v2",
        "st_quote_receipt_evidence_v2",
        "st_reconciliation_v2",
        "st_risk_decision_v2",
        "st_strategy_health_daily_v2",
        "st_strategy_lifecycle_event_v2",
        "st_strategy_signal_v2",
        "st_strategy_version_v2",
        "st_trade_account_v2",
        "st_trade_event_v2",
        "st_trade_intent_v2",
        "st_worker_heartbeat_v2",
    }
)
EXPECTED_RUNNER_BOOTSTRAP_TABLES = frozenset(
    {"schema_migration_v2_maintenance_fence"}
)
EXPECTED_TABLES = (
    EXPECTED_MIGRATION_OWNED_TABLES | EXPECTED_RUNNER_BOOTSTRAP_TABLES
)
_CONTROL_TABLES = frozenset(
    {"schema_migration_v2", "schema_migration_v2_maintenance_fence"}
)


@dataclass(frozen=True, slots=True)
class EvidenceAcceptanceSnapshot:
    migration_versions: tuple[str, ...]
    checksums: tuple[str, ...]
    observed_tables: tuple[str, ...]
    observed_triggers: tuple[str, ...]
    evidence_tables: tuple[str, ...]
    evidence_triggers: tuple[str, ...]
    migration_ledger_rows: int
    maintenance_fence_rows: int
    maintenance_fence_state: str
    business_tables_empty: bool
    stored_routines_empty: bool
    scheduled_events_empty: bool
    metadata_preflight_passed: bool
    production_activation_allowed: bool
    actionable_output_allowed: bool


@dataclass(frozen=True, slots=True)
class SerialReplayAcceptanceReport:
    mode: str
    database: str
    server_version: str
    server_version_comment: str
    server_uuid: str
    least_privilege_attested: bool
    started_empty: bool
    initial_migration: tuple[str, ...]
    serial_replay: tuple[str, ...]
    snapshot: EvidenceAcceptanceSnapshot

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ConcurrentInitialAcceptanceReport:
    mode: str
    database: str
    server_version: str
    server_version_comment: str
    server_uuid: str
    least_privilege_attested: bool
    started_empty: bool
    concurrent_initial_runs: tuple[tuple[str, ...], ...]
    snapshot: EvidenceAcceptanceSnapshot

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class BehavioralProbeOutcome:
    legal_inserted: tuple[str, ...]
    idempotent_replay: tuple[str, ...]
    identical_double_writer: tuple[str, ...]
    conflicting_double_writer: tuple[str, ...]
    append_only_update_guards: tuple[str, ...]
    append_only_delete_guards: tuple[str, ...]
    invalid_insert_guards: tuple[str, ...]
    replace_guards: tuple[str, ...]
    on_duplicate_key_update_guards: tuple[str, ...]
    rollback_verified: bool
    rollback_verified_evidence_types: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AuthorityBehavioralProbeOutcome:
    key_registration: tuple[str, ...]
    receipt_registration: tuple[str, ...]
    concurrent_registration: tuple[str, ...]
    nonce_replay_rejected: bool
    signature_rejected: bool
    revocations: tuple[str, ...]
    historical_recheck: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AccountingBehavioralProbeOutcome:
    ordered_insert_tags: tuple[str, ...]
    interruption_rolled_back: bool
    whole_batch_rolled_back: bool
    exact_replay_status: str
    different_content_conflict: bool
    fifo_lot_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ExtendedBehavioralProbeOutcome:
    authority: AuthorityBehavioralProbeOutcome
    accounting: AccountingBehavioralProbeOutcome
    authority_audit_report: V2AuthorityStoredRowAuditReport
    accounting_audit_report: V2AccountingEvidenceAuditReport


@dataclass(frozen=True, slots=True)
class CanonicalHashAuditAcceptanceOutcome:
    report: V2EvidenceHashAuditReport
    schema_blocker_removed: bool
    production_activation_allowed: bool = False
    actionable_output_allowed: bool = False


@dataclass(frozen=True, slots=True)
class BehavioralAcceptanceReport:
    mode: str
    database: str
    server_version: str
    server_version_comment: str
    server_uuid: str
    least_privilege_attested: bool
    started_empty: bool
    initial_migration: tuple[str, ...]
    behavioral_coverage: tuple[str, ...]
    behavioral_not_covered: tuple[str, ...]
    behavioral_probes_covered: tuple[str, ...]
    behavioral_probes_not_covered: tuple[str, ...]
    all_five_evidence_types_covered: bool
    all_declared_evidence_types_covered: bool
    legal_inserted: tuple[str, ...]
    idempotent_replay: tuple[str, ...]
    identical_double_writer: tuple[str, ...]
    conflicting_double_writer: tuple[str, ...]
    append_only_update_guards: tuple[str, ...]
    append_only_delete_guards: tuple[str, ...]
    invalid_insert_guards: tuple[str, ...]
    replace_guards: tuple[str, ...]
    on_duplicate_key_update_guards: tuple[str, ...]
    rollback_verified: bool
    rollback_verified_evidence_types: tuple[str, ...]
    authority_key_registration: tuple[str, ...]
    authority_receipt_registration: tuple[str, ...]
    authority_concurrent_registration: tuple[str, ...]
    authority_nonce_replay_rejected: bool
    authority_signature_rejected: bool
    authority_revocations: tuple[str, ...]
    authority_historical_recheck: tuple[str, ...]
    accounting_ordered_insert_tags: tuple[str, ...]
    accounting_interruption_rolled_back: bool
    accounting_whole_batch_rolled_back: bool
    accounting_exact_replay_status: str
    accounting_different_content_conflict: bool
    accounting_fifo_lot_ids: tuple[str, ...]
    canonical_hash_audit_report: V2EvidenceHashAuditReport
    canonical_hash_audit_passed: bool
    canonical_hash_schema_blocker_removed: bool
    authority_audit_report: V2AuthorityStoredRowAuditReport
    authority_audit_passed: bool
    accounting_audit_report: V2AccountingEvidenceAuditReport
    accounting_audit_passed: bool
    three_layer_nonempty_audit_passed: bool
    production_activation_allowed: bool
    actionable_output_allowed: bool
    pre_behavior_snapshot: EvidenceAcceptanceSnapshot

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def require_dedicated_test_url(value: object) -> str:
    """Require an explicit MySQL URL for a dedicated V2 evidence test DB."""

    if not isinstance(value, str) or not value.strip():
        raise ValueError("a dedicated V2 evidence test MySQL URL is required")
    raw = value.strip()
    try:
        url = make_url(raw)
    except ArgumentError as exc:
        raise ValueError("invalid V2 evidence test MySQL URL") from exc
    if url.get_backend_name().lower() != "mysql":
        raise ValueError("V2 evidence acceptance requires the MySQL backend")
    if url.query:
        raise ValueError(
            "V2 evidence acceptance URL query parameters are forbidden"
        )
    if not str(url.host or "").strip():
        raise ValueError("V2 evidence acceptance URL requires an explicit host")
    database = str(url.database or "").strip()
    if not _SAFE_DATABASE_RE.fullmatch(database):
        raise ValueError(
            "database name must be an explicit *_v2_evidence_test* or "
            "*_v2_evidence_ci* database"
        )
    return raw


def resolve_test_url(
    env_name: str = DEFAULT_URL_ENV,
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    """Resolve only a V2-evidence-specific test/CI environment variable."""

    if not isinstance(env_name, str) or not env_name.strip():
        raise ValueError("env_name is required")
    normalized = env_name.strip()
    if normalized in _FORBIDDEN_URL_ENVS:
        raise ValueError(f"{normalized} is forbidden for V2 evidence acceptance")
    if not _SAFE_URL_ENV_RE.fullmatch(normalized):
        raise ValueError(
            "URL environment variable must match "
            "V2_EVIDENCE_TEST_*_MYSQL_URL or V2_EVIDENCE_CI_*_MYSQL_URL"
        )
    source = os.environ if environ is None else environ
    return require_dedicated_test_url(source.get(normalized, ""))


def require_expected_server_uuid(value: object) -> str:
    """Require one canonical server UUID supplied independently from the URL."""

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
    """Resolve only a V2-evidence-specific expected server UUID variable."""

    if not isinstance(env_name, str) or not env_name.strip():
        raise ValueError("server UUID env_name is required")
    normalized = env_name.strip()
    if _SAFE_SERVER_UUID_ENV_RE.fullmatch(normalized) is None:
        raise ValueError(
            "server UUID environment variable must match "
            "V2_EVIDENCE_TEST_*_MYSQL_SERVER_UUID or "
            "V2_EVIDENCE_CI_*_MYSQL_SERVER_UUID"
        )
    source = os.environ if environ is None else environ
    return require_expected_server_uuid(source.get(normalized, ""))


def _validate_concurrency(value: object) -> int:
    if type(value) is not int or not 2 <= value <= 8:
        raise ValueError("concurrency must be an integer between 2 and 8")
    return value


def _assert_frozen_migration_contract() -> None:
    if not (
        _DECLARED_MIGRATIONS
        == EXPECTED_MIGRATIONS
        == FROZEN_EXPECTED_MIGRATIONS
    ):
        raise RuntimeError(
            "V2 migration source no longer matches the independently frozen "
            "acceptance checksum contract"
        )
    if (
        len(MIGRATIONS) != EXPECTED_MIGRATION_COUNT
        or sum(len(tuple(item["statements"])) for item in MIGRATIONS)
        != EXPECTED_MIGRATION_STATEMENT_COUNT
    ):
        raise RuntimeError(
            "V2 acceptance requires exactly 15 migrations and 150 "
            "migration-owned statements"
        )
    if (
        _DECLARED_MIGRATION_OWNED_TABLES
        != EXPECTED_MIGRATION_OWNED_TABLES
    ):
        raise RuntimeError(
            "V2 migration source no longer matches the frozen 51-table "
            "migration-owned inventory"
        )
    if (
        _DECLARED_RUNNER_BOOTSTRAP_TABLES
        != EXPECTED_RUNNER_BOOTSTRAP_TABLES
        or V2_EVIDENCE_MAINTENANCE_FENCE_TABLE
        != "schema_migration_v2_maintenance_fence"
        or V2_EVIDENCE_MAINTENANCE_FENCE_NAME
        != "execution_evidence_011_015"
        or V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE != "ACTIVE"
        or V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE != "INACTIVE"
        or hashlib.sha256(
            V2_EVIDENCE_MAINTENANCE_FENCE_DDL.strip().encode("utf-8")
        ).hexdigest()
        != FROZEN_MAINTENANCE_FENCE_DDL_CHECKSUM
    ):
        raise RuntimeError(
            "V2 migration runner bootstrap maintenance-fence contract drifted"
        )
    if (
        len(EXPECTED_MIGRATION_OWNED_TABLES) != 51
        or len(EXPECTED_RUNNER_BOOTSTRAP_TABLES) != 1
        or len(EXPECTED_TABLES) != 52
        or EXPECTED_MIGRATION_OWNED_TABLES
        & EXPECTED_RUNNER_BOOTSTRAP_TABLES
    ):
        raise RuntimeError(
            "V2 acceptance requires 51 migration-owned tables plus exactly "
            "one runner bootstrap control table"
        )


def _assert_least_privilege_grants(
    grants: Iterable[object],
    *,
    expected_database: str,
) -> None:
    expected_target = f"{expected_database.lower()}.*"
    observed_schema_privileges: set[str] = set()
    observed = tuple(str(item).strip() for item in grants)
    if not observed:
        raise RuntimeError("acceptance account grants could not be attested")
    for grant in observed:
        match = _GRANT_RE.match(grant)
        if match is None or " WITH GRANT OPTION" in grant.upper():
            raise RuntimeError(
                "acceptance account has an unsupported or delegable grant"
            )
        privileges = " ".join(match.group("privileges").upper().split())
        target = match.group("target").replace("`", "").strip().lower()
        if target == "*.*" and privileges == "USAGE":
            continue
        if target != expected_target:
            raise RuntimeError(
                "acceptance account grants must be scoped only to the target schema"
            )
        parsed_privileges = {
            " ".join(item.strip().upper().split())
            for item in privileges.split(",")
            if item.strip()
        }
        unexpected = parsed_privileges - _REQUIRED_SCHEMA_PRIVILEGES
        if unexpected:
            raise RuntimeError(
                "acceptance account has unnecessary target-schema grants: "
                + ", ".join(sorted(unexpected))
            )
        observed_schema_privileges.update(parsed_privileges)
    if not observed_schema_privileges:
        raise RuntimeError(
            "acceptance account has no grant on the target schema"
        )
    if observed_schema_privileges != _REQUIRED_SCHEMA_PRIVILEGES:
        missing = sorted(_REQUIRED_SCHEMA_PRIVILEGES - observed_schema_privileges)
        raise RuntimeError(
            "acceptance account target-schema grants are incomplete; missing: "
            + ", ".join(missing)
        )


def _server_identity_from_connection(
    connection: Connection,
    expected_database: str,
    expected_server_uuid: str,
) -> tuple[str, str, str, str]:
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    connection_backend = str(
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
    if connection_backend != "mysql" or not is_oracle_mysql_distribution(
        version,
        version_comment,
    ):
        raise RuntimeError("acceptance connection must be Oracle MySQL")
    if not is_isolated_acceptance_version(version):
        raise RuntimeError(
            "V2 evidence acceptance requires Oracle MySQL "
            f"{isolated_acceptance_versions_label()} exactly"
        )
    if database != expected_database:
        raise RuntimeError(
            "connected database does not match the dedicated acceptance URL"
        )
    if _CANONICAL_UUID_RE.fullmatch(server_uuid) is None:
        raise RuntimeError("connected MySQL server UUID is missing or invalid")
    if server_uuid != expected_uuid:
        raise RuntimeError("connected MySQL server UUID does not match expectation")
    _assert_least_privilege_grants(
        grants,
        expected_database=expected_database,
    )
    return database, version, server_uuid, version_comment


def _server_identity(
    engine: Engine,
    expected_database: str,
    expected_server_uuid: str,
) -> tuple[str, str, str, str]:
    runtime_backend = str(getattr(engine.dialect, "name", "")).lower()
    if runtime_backend != "mysql":
        raise RuntimeError("acceptance connection is not using the MySQL backend")
    with engine.connect() as connection:
        return _server_identity_from_connection(
            connection,
            expected_database,
            expected_server_uuid,
        )


def _schema_object_names_from_connection(
    connection: Connection,
    *,
    object_kind: str,
) -> frozenset[str]:
    contracts = {
        "tables": ("TABLE_NAME", "TABLES", "TABLE_SCHEMA"),
        "routines": ("ROUTINE_NAME", "ROUTINES", "ROUTINE_SCHEMA"),
        "events": ("EVENT_NAME", "EVENTS", "EVENT_SCHEMA"),
    }
    if object_kind not in contracts:
        raise RuntimeError("unsupported schema object inventory kind")
    name_column, catalog_table, schema_column = contracts[object_kind]
    rows = connection.execute(
        text(
            f"SELECT {name_column} FROM information_schema.{catalog_table} "
            f"WHERE {schema_column} = DATABASE() ORDER BY {name_column}"
        )
    ).scalars()
    return frozenset(str(item).lower() for item in rows)


def _all_table_names(engine: Engine) -> frozenset[str]:
    with engine.connect() as connection:
        return _schema_object_names_from_connection(
            connection,
            object_kind="tables",
        )


def _all_routine_names(engine: Engine) -> frozenset[str]:
    with engine.connect() as connection:
        return _schema_object_names_from_connection(
            connection,
            object_kind="routines",
        )


def _all_event_names(engine: Engine) -> frozenset[str]:
    with engine.connect() as connection:
        return _schema_object_names_from_connection(
            connection,
            object_kind="events",
        )


def _schema_object_inventory_from_connection(
    connection: Connection,
) -> dict[str, frozenset[str]]:
    return {
        kind: _schema_object_names_from_connection(
            connection,
            object_kind=kind,
        )
        for kind in ("tables", "routines", "events")
    }


def _schema_object_inventory(engine: Engine) -> dict[str, frozenset[str]]:
    return {
        "tables": _all_table_names(engine),
        "routines": _all_routine_names(engine),
        "events": _all_event_names(engine),
    }


def _assert_empty_inventory(
    inventory: Mapping[str, frozenset[str]],
    *,
    mode: str,
) -> None:
    populated = {
        kind: names for kind, names in inventory.items() if names
    }
    if populated:
        details = "; ".join(
            f"{kind}=" + ",".join(sorted(names))
            for kind, names in sorted(populated.items())
        )
        raise RuntimeError(
            f"{mode} V2 evidence acceptance database must start empty; "
            "found schema objects: " + details
        )


def _assert_empty(engine: Engine, *, mode: str) -> None:
    _assert_empty_inventory(_schema_object_inventory(engine), mode=mode)


def _assert_empty_on_connection(connection: Connection, *, mode: str) -> None:
    _assert_empty_inventory(
        _schema_object_inventory_from_connection(connection),
        mode=mode,
    )


def _result_statuses(results: Iterable[V2MigrationResult]) -> tuple[str, ...]:
    return tuple(str(item.status) for item in results)


def _validate_run(
    results: Iterable[V2MigrationResult],
    *,
    allowed_statuses: frozenset[str],
) -> tuple[V2MigrationResult, ...]:
    _assert_frozen_migration_contract()
    run = tuple(results)
    expected_versions = tuple(
        version for version, _checksum_value in FROZEN_EXPECTED_MIGRATIONS
    )
    actual_versions = tuple(str(item.version) for item in run)
    if len(FROZEN_EXPECTED_MIGRATIONS) != EXPECTED_MIGRATION_COUNT:
        raise RuntimeError("V2 migration contract no longer contains exactly 15 entries")
    if actual_versions != expected_versions:
        raise RuntimeError("V2 migration result versions are incomplete or out of order")
    if any(str(item.status) not in allowed_statuses for item in run):
        raise RuntimeError("V2 migration returned an unexpected status")
    return run


def _post_migration_row_counts_from_connection(
    connection: Connection,
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in sorted(EXPECTED_TABLES):
        if _SAFE_SQL_IDENTIFIER_RE.fullmatch(table) is None:
            raise RuntimeError("frozen V2 table inventory contains an unsafe name")
        count = connection.execute(
            text(f"SELECT COUNT(*) FROM `{table}`")
        ).scalar()
        counts[table] = int(count or 0)
    return counts


def _post_migration_row_counts(engine: Engine) -> dict[str, int]:
    with engine.connect() as connection:
        return _post_migration_row_counts_from_connection(connection)


def _maintenance_fence_state_from_connection(
    connection: Connection,
    *,
    expected_state: str,
) -> tuple[int, str]:
    """Require the one frozen maintenance-fence row and its exact state."""

    if expected_state not in {
        V2_EVIDENCE_MAINTENANCE_FENCE_ACTIVE,
        V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
    }:
        raise ValueError("expected maintenance-fence state is invalid")
    rows = tuple(
        connection.execute(
            text(
                "SELECT fence_name, state, target_version, generation, "
                "activated_at, updated_at "
                f"FROM {V2_EVIDENCE_MAINTENANCE_FENCE_TABLE} "
                "ORDER BY fence_name LOCK IN SHARE MODE"
            )
        ).mappings()
    )
    if len(rows) != 1:
        raise RuntimeError(
            "V2 maintenance fence must contain exactly one durable row"
        )
    row = rows[0]
    fence_name = str(
        row.get("fence_name") or row.get("FENCE_NAME") or ""
    )
    state = str(row.get("state") or row.get("STATE") or "").upper()
    if fence_name != V2_EVIDENCE_MAINTENANCE_FENCE_NAME:
        raise RuntimeError("V2 maintenance fence row identity drifted")
    if state != expected_state:
        raise RuntimeError(
            "V2 maintenance fence must be exactly "
            f"{expected_state}; observed {state or 'MISSING'}"
        )
    target_version = str(
        row.get("target_version") or row.get("TARGET_VERSION") or ""
    )
    try:
        generation = int(
            row.get("generation")
            if "generation" in row
            else row.get("GENERATION")
        )
    except (TypeError, ValueError):
        generation = -1
    expected_versions = {
        version for version, _checksum_value in FROZEN_EXPECTED_MIGRATIONS
    }
    if (
        target_version not in expected_versions
        or generation < 0
        or (row.get("activated_at") or row.get("ACTIVATED_AT")) is None
        or (row.get("updated_at") or row.get("UPDATED_AT")) is None
    ):
        raise RuntimeError("V2 maintenance fence row metadata drifted")
    return (1, state)


def _post_migration_snapshot(
    engine: Engine,
    *,
    connection: Connection | None = None,
) -> EvidenceAcceptanceSnapshot:
    _assert_frozen_migration_contract()
    connection_scope = (
        nullcontext(connection) if connection is not None else engine.connect()
    )
    with connection_scope as active_connection:
        ledger_rows = tuple(
            active_connection.execute(
                text(
                    "SELECT version, checksum FROM schema_migration_v2 "
                    "ORDER BY version"
                )
            ).mappings()
        )
        all_table_rows = tuple(
            active_connection.execute(
                text(
                    """
                    SELECT TABLE_NAME
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = DATABASE()
                    ORDER BY TABLE_NAME
                    """
                )
            ).scalars()
        )
        trigger_rows = tuple(
            active_connection.execute(
                text(
                    """
                    SELECT TRIGGER_NAME
                    FROM information_schema.TRIGGERS
                    WHERE TRIGGER_SCHEMA = DATABASE()
                    ORDER BY TRIGGER_NAME
                    """
                )
            ).scalars()
        )
        routine_rows = tuple(
            active_connection.execute(
                text(
                    """
                    SELECT ROUTINE_NAME
                    FROM information_schema.ROUTINES
                    WHERE ROUTINE_SCHEMA = DATABASE()
                    ORDER BY ROUTINE_NAME
                    """
                )
            ).scalars()
        )
        event_rows = tuple(
            active_connection.execute(
                text(
                    """
                    SELECT EVENT_NAME
                    FROM information_schema.EVENTS
                    WHERE EVENT_SCHEMA = DATABASE()
                    ORDER BY EVENT_NAME
                    """
                )
            ).scalars()
        )
        observed_tables = frozenset(
            str(item).lower() for item in all_table_rows
        )
        evidence_tables = observed_tables & EVIDENCE_TABLES
        observed_triggers = frozenset(str(item).lower() for item in trigger_rows)
        evidence_triggers = observed_triggers & EVIDENCE_TRIGGER_NAMES
        observed_routines = frozenset(str(item).lower() for item in routine_rows)
        observed_events = frozenset(str(item).lower() for item in event_rows)
        schema_report = inspect_v2_execution_evidence_schema(
            active_connection,
            require_guards=True,
            require_migration_ledger=True,
            include_activation_blockers=True,
        )
        row_counts = _post_migration_row_counts_from_connection(
            active_connection
        )
        maintenance_fence_rows, maintenance_fence_state = (
            _maintenance_fence_state_from_connection(
                active_connection,
                expected_state=V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
            )
        )

    observed_ledger = tuple(
        (
            str(row.get("version") or row.get("VERSION") or ""),
            str(row.get("checksum") or row.get("CHECKSUM") or ""),
        )
        for row in ledger_rows
    )
    if observed_ledger != FROZEN_EXPECTED_MIGRATIONS:
        raise RuntimeError("V2 migration ledger versions or checksums drifted")
    if observed_tables != EXPECTED_TABLES:
        missing = sorted(EXPECTED_TABLES - observed_tables)
        unexpected = sorted(observed_tables - EXPECTED_TABLES)
        raise RuntimeError(
            "V2 migration schema table allowlist drifted; missing="
            + ",".join(missing)
            + "; unexpected="
            + ",".join(unexpected)
        )
    if evidence_tables != EVIDENCE_TABLES:
        raise RuntimeError("V2 evidence acceptance did not observe exactly 5 tables")
    if observed_triggers != EXPECTED_TRIGGER_NAMES:
        raise RuntimeError("V2 migration trigger inventory drifted")
    if evidence_triggers != EVIDENCE_TRIGGER_NAMES:
        raise RuntimeError("V2 evidence acceptance did not observe exactly 17 triggers")
    if observed_routines or observed_events:
        raise RuntimeError(
            "V2 migration unexpectedly left stored routines or scheduled events"
        )
    if row_counts.get("schema_migration_v2") != EXPECTED_MIGRATION_COUNT:
        raise RuntimeError("V2 migration ledger must contain exactly 15 rows")
    if (
        row_counts.get(V2_EVIDENCE_MAINTENANCE_FENCE_TABLE) != 1
        or maintenance_fence_rows != 1
        or maintenance_fence_state
        != V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE
    ):
        raise RuntimeError(
            "V2 maintenance fence must finish with exactly one INACTIVE row"
        )
    nonempty_business_tables = tuple(
        table
        for table, count in sorted(row_counts.items())
        if table not in _CONTROL_TABLES and count != 0
    )
    if nonempty_business_tables:
        raise RuntimeError(
            "V2 evidence structural acceptance requires all business tables "
            "to remain empty; found rows in: "
            + ", ".join(nonempty_business_tables)
        )
    if not schema_report.metadata_preflight_passed:
        raise RuntimeError(
            "V2 evidence metadata preflight failed: "
            + ", ".join(schema_report.structural_blockers)
        )
    if (
        schema_report.production_activation_allowed
        or schema_report.actionable_output_allowed
    ):
        raise RuntimeError(
            "acceptance must not enable V2 evidence production or "
            "actionable output"
        )
    if not (
        schema_report.guards_checked
        and schema_report.migration_ledger_checked
        and schema_report.activation_checks_included
        and schema_report.maintenance_fence_checked
        and not schema_report.maintenance_fence_active
        and schema_report.activation_blockers
    ):
        raise RuntimeError("V2 evidence activation gate was not fully evaluated")
    return EvidenceAcceptanceSnapshot(
        migration_versions=tuple(version for version, _value in observed_ledger),
        checksums=tuple(value for _version, value in observed_ledger),
        observed_tables=tuple(sorted(observed_tables)),
        observed_triggers=tuple(sorted(observed_triggers)),
        evidence_tables=tuple(sorted(evidence_tables)),
        evidence_triggers=tuple(sorted(evidence_triggers)),
        migration_ledger_rows=EXPECTED_MIGRATION_COUNT,
        maintenance_fence_rows=maintenance_fence_rows,
        maintenance_fence_state=maintenance_fence_state,
        business_tables_empty=True,
        stored_routines_empty=True,
        scheduled_events_empty=True,
        metadata_preflight_passed=True,
        production_activation_allowed=False,
        actionable_output_allowed=False,
    )


def run_mysql_serial_replay_acceptance(
    url: str,
    *,
    expected_server_uuid: str,
    tls_config: MySQLAcceptanceTLSConfig | None = None,
) -> SerialReplayAcceptanceReport:
    """Apply all 15 migrations and the fence, then verify one replay."""

    _assert_frozen_migration_contract()
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    safe_url = require_dedicated_test_url(url)
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
        with engine.connect() as connection:
            (
                database,
                server_version,
                server_uuid,
                version_comment,
            ) = _server_identity_from_connection(
                connection,
                expected_database,
                expected_uuid,
            )
            _assert_empty_on_connection(connection, mode="serial-replay")
            connection.rollback()
            initial = _validate_run(
                run_v2_migrations(
                    engine,
                    allow_execution_evidence=True,
                    connection=connection,
                ),
                allowed_statuses=frozenset({"applied"}),
            )
            connection.rollback()
            replay = _validate_run(
                run_v2_migrations(
                    engine,
                    allow_execution_evidence=True,
                    connection=connection,
                ),
                allowed_statuses=frozenset({"exists"}),
            )
            connection.rollback()
            snapshot = _post_migration_snapshot(
                engine,
                connection=connection,
            )
    finally:
        engine.dispose()
    return SerialReplayAcceptanceReport(
        mode="serial-replay",
        database=database,
        server_version=server_version,
        server_version_comment=version_comment,
        server_uuid=server_uuid,
        least_privilege_attested=True,
        started_empty=True,
        initial_migration=_result_statuses(initial),
        serial_replay=_result_statuses(replay),
        snapshot=snapshot,
    )


def run_mysql_concurrent_initial_acceptance(
    url: str,
    *,
    expected_server_uuid: str,
    concurrency: int = 2,
    tls_config: MySQLAcceptanceTLSConfig | None = None,
) -> ConcurrentInitialAcceptanceReport:
    """Race first migration runs in a separately provisioned empty database."""

    _assert_frozen_migration_contract()
    worker_count = _validate_concurrency(concurrency)
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    safe_url = require_dedicated_test_url(url)
    expected_database = str(make_url(safe_url).database)
    engine = create_tool_engine(
        safe_url,
        tls_config=tls_config,
        future=True,
        pool_size=worker_count,
        max_overflow=0,
        pool_pre_ping=True,
    )
    try:
        with engine.connect() as preflight_connection:
            (
                database,
                server_version,
                server_uuid,
                version_comment,
            ) = _server_identity_from_connection(
                preflight_connection,
                expected_database,
                expected_uuid,
            )
            _assert_empty_on_connection(
                preflight_connection,
                mode="concurrent-initial",
            )
            preflight_connection.rollback()

        def concurrent_worker() -> tuple[V2MigrationResult, ...]:
            with engine.connect() as worker_connection:
                _server_identity_from_connection(
                    worker_connection,
                    expected_database,
                    expected_uuid,
                )
                worker_connection.rollback()
                return tuple(
                    run_v2_migrations(
                        engine,
                        allow_execution_evidence=True,
                        connection=worker_connection,
                    )
                )

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = tuple(
                executor.submit(concurrent_worker)
                for _index in range(worker_count)
            )
            runs = tuple(
                _validate_run(
                    future.result(),
                    allowed_statuses=frozenset({"applied", "exists"}),
                )
                for future in futures
            )
        status_runs = tuple(_result_statuses(run) for run in runs)
        complete_applied = ("applied",) * EXPECTED_MIGRATION_COUNT
        complete_exists = ("exists",) * EXPECTED_MIGRATION_COUNT
        if not (
            status_runs.count(complete_applied) == 1
            and status_runs.count(complete_exists) == worker_count - 1
        ):
            raise RuntimeError(
                "concurrent initial migration must have exactly one complete "
                "applied run and all remaining runs complete exists"
            )
        with engine.connect() as snapshot_connection:
            _server_identity_from_connection(
                snapshot_connection,
                expected_database,
                expected_uuid,
            )
            snapshot_connection.rollback()
            snapshot = _post_migration_snapshot(
                engine,
                connection=snapshot_connection,
            )
    finally:
        engine.dispose()
    return ConcurrentInitialAcceptanceReport(
        mode="concurrent-initial",
        database=database,
        server_version=server_version,
        server_version_comment=version_comment,
        server_uuid=server_uuid,
        least_privilege_attested=True,
        started_empty=True,
        concurrent_initial_runs=status_runs,
        snapshot=snapshot,
    )


def _safe_sql_identifier(value: str) -> str:
    if _SAFE_SQL_IDENTIFIER_RE.fullmatch(value) is None:
        raise RuntimeError(f"unsafe behavioral acceptance SQL identifier: {value}")
    return value


class _IdentityBoundEngine:
    """Verify every behavioral checkout before beginning its transaction."""

    def __init__(
        self,
        engine: Engine,
        *,
        expected_database: str,
        expected_server_uuid: str,
    ) -> None:
        self._engine = engine
        self._expected_database = expected_database
        self._expected_server_uuid = expected_server_uuid

    @contextmanager
    def connect(self):
        with self._engine.connect() as connection:
            _server_identity_from_connection(
                connection,
                self._expected_database,
                self._expected_server_uuid,
            )
            connection.rollback()
            yield connection

    @contextmanager
    def begin(self):
        with self.connect() as connection:
            with connection.begin():
                yield connection


def _run_database_canonical_hash_audit(
    engine: _IdentityBoundEngine,
) -> CanonicalHashAuditAcceptanceOutcome:
    """Run the real DB SHA2/reconstruction audit and re-evaluate its blocker."""

    with engine.begin() as connection:
        audit_report = audit_v2_execution_evidence_database(connection)
        if type(audit_report) is not V2EvidenceHashAuditReport:
            raise RuntimeError("canonical hash auditor returned an invalid report")
        if not (
            audit_report.audit_passed
            and audit_report.database_sha2_used
            and audit_report.shared_row_locks_used
        ):
            raise RuntimeError(
                "canonical hash audit did not prove DB SHA2 and shared row locks"
            )
        schema_report = inspect_v2_execution_evidence_schema(
            connection,
            require_guards=True,
            require_natural_keys=True,
            require_migration_ledger=True,
            include_activation_blockers=True,
            canonical_hash_audit_passed=True,
        )
        blocker_removed = (
            "CANONICAL_HASH_NOT_DATABASE_RECOMPUTABLE"
            not in schema_report.activation_blockers
        )
        if not blocker_removed:
            raise RuntimeError(
                "schema gate retained the canonical hash blocker after audit"
            )
        if (
            schema_report.production_activation_allowed
            or schema_report.actionable_output_allowed
        ):
            raise RuntimeError(
                "canonical hash audit must not enable production/actionable output"
            )
    return CanonicalHashAuditAcceptanceOutcome(
        report=audit_report,
        schema_blocker_removed=blocker_removed,
        production_activation_allowed=False,
        actionable_output_allowed=False,
    )


def _insert_behavioral_seed(
    engine: Engine,
    scenario: BehavioralScenario | ConflictingDoubleWriterScenario,
) -> None:
    with engine.begin() as connection:
        for seed in scenario.seed_rows:
            table = _safe_sql_identifier(seed.table)
            columns = tuple(_safe_sql_identifier(str(item)) for item in seed.values)
            if not columns:
                raise RuntimeError("behavioral canonical seed row cannot be empty")
            connection.execute(
                text(
                    f"INSERT INTO {table} ({', '.join(columns)}) VALUES ("
                    + ", ".join(f":{column}" for column in columns)
                    + ")"
                ),
                dict(seed.values),
            )


def _append_behavioral_case(connection, case: BehavioralEvidenceCase):
    if case.evidence_type == "MARKET_CALENDAR":
        return append_market_calendar_evidence(connection, case.evidence)
    if case.evidence_type == "QUOTE_RECEIPT":
        return append_quote_receipt_evidence(connection, case.evidence)
    if case.evidence_type == "FILL_EXECUTION":
        return append_fill_execution_evidence(connection, case.evidence)
    if case.evidence_type == "CASH_EVENT":
        return append_cash_event_binding(connection, case.evidence)
    if case.evidence_type == "ORDER_TRANSITION":
        return append_order_transition_evidence(connection, case.evidence)
    raise RuntimeError(
        "behavioral scenario contains an unsupported evidence type: "
        + case.evidence_type
    )


def _run_idempotent_replay_probes(
    engine: Engine,
    scenario: BehavioralScenario,
) -> tuple[str, ...]:
    replayed: list[str] = []
    for case in scenario.cases:
        with engine.begin() as connection:
            result = _append_behavioral_case(connection, case)
        if result.status is not EvidenceAppendStatus.IDEMPOTENT:
            raise RuntimeError(
                f"{case.evidence_type} did not return IDEMPOTENT on replay"
            )
        replayed.append(case.evidence_type)
    return tuple(replayed)


@dataclass(frozen=True, slots=True)
class _DoubleWriterAttempt:
    status: EvidenceAppendStatus | None
    retryable_mysql_errno: int | None


def _run_double_writer_attempt(
    engine: Engine,
    case: BehavioralEvidenceCase,
    barrier: Barrier,
) -> _DoubleWriterAttempt:
    with engine.connect() as connection:
        connection.execute(
            text(
                "SET SESSION innodb_lock_wait_timeout = "
                f"{_DOUBLE_WRITER_LOCK_WAIT_SECONDS}"
            )
        )
        connection.rollback()
        transaction = connection.begin()
        try:
            try:
                barrier.wait(timeout=_DOUBLE_WRITER_LOCK_WAIT_SECONDS * 2)
            except BrokenBarrierError as exc:
                raise RuntimeError(
                    f"{case.evidence_type} double-writer barrier broke"
                ) from exc
            result = _append_behavioral_case(connection, case)
            transaction.commit()
            return _DoubleWriterAttempt(
                status=result.status,
                retryable_mysql_errno=None,
            )
        except Exception as exc:
            transaction.rollback()
            mysql_errno, _message = _mysql_error_code_message(exc)
            if (
                case.evidence_type == "MARKET_CALENDAR"
                and mysql_errno in {1062, 1213}
            ):
                return _DoubleWriterAttempt(
                    status=None,
                    retryable_mysql_errno=mysql_errno,
                )
            raise


def _retry_double_writer_attempt(
    engine: Engine,
    case: BehavioralEvidenceCase,
) -> EvidenceAppendStatus:
    with engine.begin() as connection:
        result = _append_behavioral_case(connection, case)
    if result.status is not EvidenceAppendStatus.IDEMPOTENT:
        raise RuntimeError(
            f"{case.evidence_type} double-writer retry was not IDEMPOTENT"
        )
    return result.status


def _run_identical_double_writer_probes(
    engine: Engine,
    scenario: BehavioralScenario,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    inserted: list[str] = []
    outcomes: list[str] = []
    for case in scenario.cases:
        barrier = Barrier(2)
        with ThreadPoolExecutor(
            max_workers=2,
            thread_name_prefix=f"v2e-{case.evidence_type.lower()}",
        ) as executor:
            futures = tuple(
                executor.submit(
                    _run_double_writer_attempt,
                    engine,
                    case,
                    barrier,
                )
                for _index in range(2)
            )
            try:
                attempts = tuple(
                    future.result(
                        timeout=_DOUBLE_WRITER_FUTURE_TIMEOUT_SECONDS
                    )
                    for future in futures
                )
            except FutureTimeoutError as exc:
                raise RuntimeError(
                    f"{case.evidence_type} double-writer probe timed out"
                ) from exc

        statuses = [
            attempt.status
            for attempt in attempts
            if attempt.status is not None
        ]
        retry_errnos = tuple(
            attempt.retryable_mysql_errno
            for attempt in attempts
            if attempt.retryable_mysql_errno is not None
        )
        if retry_errnos:
            if retry_errnos not in ((1062,), (1213,)):
                raise RuntimeError(
                    f"{case.evidence_type} returned an invalid retry matrix: "
                    f"{retry_errnos!r}"
                )
            statuses.append(_retry_double_writer_attempt(engine, case))
        if statuses.count(EvidenceAppendStatus.INSERTED) != 1:
            raise RuntimeError(
                f"{case.evidence_type} double writer did not produce exactly "
                "one INSERTED result"
            )
        if statuses.count(EvidenceAppendStatus.IDEMPOTENT) != 1:
            raise RuntimeError(
                f"{case.evidence_type} double writer did not converge to "
                "one IDEMPOTENT result"
            )

        table = _safe_sql_identifier(case.table)
        primary = _safe_sql_identifier(case.primary_column)
        with engine.connect() as connection:
            retained = connection.execute(
                text(
                    f"SELECT COUNT(*) FROM {table} "
                    f"WHERE {primary} = :primary_value"
                ),
                {"primary_value": case.primary_value},
            ).scalar()
        if int(retained or 0) != 1:
            raise RuntimeError(
                f"{case.evidence_type} double writer did not retain exactly one row"
            )
        inserted.append(case.evidence_type)
        retry_suffix = (
            "DIRECT"
            if not retry_errnos
            else f"RETRY_{retry_errnos[0]}"
        )
        outcomes.append(
            f"{case.table}:IDENTICAL_DOUBLE_WRITER:"
            f"INSERTED+IDEMPOTENT:{retry_suffix}:ONE_ROW"
        )
    return tuple(inserted), tuple(outcomes)


def _mysql_error_code_message(exc: Exception) -> tuple[int | None, str]:
    current: object | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        args = getattr(current, "args", ())
        if isinstance(args, tuple) and args:
            try:
                code = int(args[0])
            except (TypeError, ValueError):
                code = None
            if code is not None:
                message = str(args[1]) if len(args) > 1 else str(current)
                return code, message
        current = getattr(current, "orig", None)
    return None, str(exc)


@dataclass(frozen=True, slots=True)
class _ConflictingDoubleWriterAttempt:
    side: int
    status: EvidenceAppendStatus | None
    direct_conflict: bool
    transient_mysql_errno: int | None


def _run_conflicting_double_writer_attempt(
    engine: Engine,
    case: BehavioralEvidenceCase,
    barrier: Barrier,
    side: int,
) -> _ConflictingDoubleWriterAttempt:
    with engine.connect() as connection:
        connection.execute(
            text(
                "SET SESSION innodb_lock_wait_timeout = "
                f"{_DOUBLE_WRITER_LOCK_WAIT_SECONDS}"
            )
        )
        connection.rollback()
        transaction = connection.begin()
        try:
            try:
                barrier.wait(timeout=_DOUBLE_WRITER_LOCK_WAIT_SECONDS * 2)
            except BrokenBarrierError as exc:
                raise RuntimeError(
                    f"{case.evidence_type} conflicting-writer barrier broke"
                ) from exc
            result = _append_behavioral_case(connection, case)
            transaction.commit()
            return _ConflictingDoubleWriterAttempt(
                side=side,
                status=result.status,
                direct_conflict=False,
                transient_mysql_errno=None,
            )
        except EvidenceAppendConflictError:
            transaction.rollback()
            return _ConflictingDoubleWriterAttempt(
                side=side,
                status=None,
                direct_conflict=True,
                transient_mysql_errno=None,
            )
        except Exception as exc:
            transaction.rollback()
            mysql_errno, _message = _mysql_error_code_message(exc)
            if mysql_errno in {1062, 1205, 1213}:
                return _ConflictingDoubleWriterAttempt(
                    side=side,
                    status=None,
                    direct_conflict=False,
                    transient_mysql_errno=mysql_errno,
                )
            raise


def _require_new_transaction_conflict(
    engine: Engine,
    case: BehavioralEvidenceCase,
) -> None:
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            result = _append_behavioral_case(connection, case)
        except EvidenceAppendConflictError:
            transaction.rollback()
            return
        except Exception:
            transaction.rollback()
            raise
        transaction.rollback()
    raise RuntimeError(
        f"{case.evidence_type} losing writer did not raise "
        "EvidenceAppendConflictError in a fresh transaction; "
        f"status={result.status.value}"
    )


def _conflicting_retention_counts(
    engine: Engine,
    pair: ConflictingBehavioralEvidencePair,
    *,
    winner: BehavioralEvidenceCase,
    loser: BehavioralEvidenceCase,
) -> tuple[int, int, int]:
    table = _safe_sql_identifier(pair.table)
    primary = _safe_sql_identifier(pair.primary_column)
    if len(pair.natural_key_columns) != len(pair.natural_key_values):
        raise RuntimeError(
            f"{pair.evidence_type} conflicting natural key shape drifted"
        )
    natural_parts: list[str] = []
    natural_params: dict[str, object] = {}
    for index, (column, value) in enumerate(
        zip(
            pair.natural_key_columns,
            pair.natural_key_values,
            strict=True,
        )
    ):
        safe_column = _safe_sql_identifier(column)
        parameter = f"natural_value_{index}"
        natural_parts.append(f"{safe_column} <=> :{parameter}")
        natural_params[parameter] = value
    if not natural_parts:
        raise RuntimeError(
            f"{pair.evidence_type} conflicting natural key is empty"
        )
    with engine.connect() as connection:
        natural_count = connection.execute(
            text(
                f"SELECT COUNT(*) FROM {table} WHERE "
                + " AND ".join(natural_parts)
            ),
            natural_params,
        ).scalar()
        winner_count = connection.execute(
            text(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE {primary} = :primary_value"
            ),
            {"primary_value": winner.primary_value},
        ).scalar()
        loser_count = connection.execute(
            text(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE {primary} = :primary_value"
            ),
            {"primary_value": loser.primary_value},
        ).scalar()
    return (
        int(natural_count or 0),
        int(winner_count or 0),
        int(loser_count or 0),
    )


def _run_conflicting_double_writer_probes(
    engine: Engine,
    scenario: ConflictingDoubleWriterScenario,
) -> tuple[str, ...]:
    outcomes: list[str] = []
    observed_types = tuple(pair.evidence_type for pair in scenario.pairs)
    if observed_types != CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES:
        raise RuntimeError(
            "conflicting double-writer coverage declaration drifted"
        )
    for pair in scenario.pairs:
        cases = (pair.left, pair.right)
        barrier = Barrier(2)
        with ThreadPoolExecutor(
            max_workers=2,
            thread_name_prefix=(
                f"v2e-conflict-{pair.evidence_type.lower()}"
            ),
        ) as executor:
            futures = tuple(
                executor.submit(
                    _run_conflicting_double_writer_attempt,
                    engine,
                    case,
                    barrier,
                    side,
                )
                for side, case in enumerate(cases)
            )
            try:
                attempts = tuple(
                    future.result(
                        timeout=_DOUBLE_WRITER_FUTURE_TIMEOUT_SECONDS
                    )
                    for future in futures
                )
            except FutureTimeoutError as exc:
                raise RuntimeError(
                    f"{pair.evidence_type} conflicting double-writer timed out"
                ) from exc

        inserted_attempts = tuple(
            attempt
            for attempt in attempts
            if attempt.status is EvidenceAppendStatus.INSERTED
        )
        losing_attempts = tuple(
            attempt for attempt in attempts if attempt.status is None
        )
        if len(inserted_attempts) != 1 or len(losing_attempts) != 1:
            raise RuntimeError(
                f"{pair.evidence_type} conflicting double writer must return "
                "exactly one INSERTED and one losing attempt"
            )
        loser_attempt = losing_attempts[0]
        if loser_attempt.direct_conflict == (
            loser_attempt.transient_mysql_errno is not None
        ):
            raise RuntimeError(
                f"{pair.evidence_type} conflicting loser signal is ambiguous"
            )
        winner = cases[inserted_attempts[0].side]
        loser = cases[loser_attempt.side]

        # An initial duplicate/deadlock/timeout is only a transient signal.
        # The acceptance proof always retries after rollback on a brand-new
        # transaction and requires the writer's semantic conflict type.
        _require_new_transaction_conflict(engine, loser)
        natural_count, winner_count, loser_count = (
            _conflicting_retention_counts(
                engine,
                pair,
                winner=winner,
                loser=loser,
            )
        )
        if (natural_count, winner_count, loser_count) != (1, 1, 0):
            raise RuntimeError(
                f"{pair.evidence_type} conflicting retention differs; "
                f"natural={natural_count}, winner={winner_count}, "
                f"loser={loser_count}"
            )
        initial_signal = (
            "DIRECT_CONFLICT"
            if loser_attempt.direct_conflict
            else f"TRANSIENT_{loser_attempt.transient_mysql_errno}"
        )
        outcomes.append(
            f"{pair.table}:CONFLICTING_DOUBLE_WRITER:"
            f"ONE_INSERTED+ONE_LOSER:{initial_signal}:"
            "NEW_TRANSACTION_CONFLICT:WINNER_ONLY"
        )
    return tuple(outcomes)


def _require_1644_guard(exc: Exception, expected_message: str) -> None:
    code, message = _mysql_error_code_message(exc)
    if code != 1644 or expected_message.lower() not in message.lower():
        raise RuntimeError(
            "append-only guard did not return SQLSTATE 45000 / errno 1644; "
            f"code={code}; message={message}"
        ) from exc


def _execute_guard_rejection(
    engine: Engine,
    case: BehavioralEvidenceCase,
    *,
    operation: str,
) -> str:
    table = _safe_sql_identifier(case.table)
    primary = _safe_sql_identifier(case.primary_column)
    if operation == "UPDATE":
        statement = text(
            f"UPDATE {table} SET {primary} = {primary} "
            f"WHERE {primary} = :primary_value"
        )
        expected_message = case.update_guard_message
    elif operation == "DELETE":
        statement = text(
            f"DELETE FROM {table} WHERE {primary} = :primary_value"
        )
        expected_message = case.delete_guard_message
    else:
        raise RuntimeError(f"unsupported append-only guard operation: {operation}")

    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(
                statement,
                {"primary_value": case.primary_value},
            )
        except Exception as exc:
            transaction.rollback()
            _require_1644_guard(exc, expected_message)
        else:
            transaction.rollback()
            raise RuntimeError(
                f"{case.table} unexpectedly allowed append-only {operation}"
            )

    with engine.connect() as connection:
        retained = connection.execute(
            text(
                f"SELECT COUNT(*) FROM {table} "
                f"WHERE {primary} = :primary_value"
            ),
            {"primary_value": case.primary_value},
        ).scalar()
    if int(retained or 0) != 1:
        raise RuntimeError(
            f"{case.table} row was not retained after rejected {operation}"
        )
    return f"{case.table}:{operation}:1644/45000:ROW_RETAINED"


def _run_append_only_guard_probes(
    engine: Engine,
    scenario: BehavioralScenario,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    updates = tuple(
        _execute_guard_rejection(engine, case, operation="UPDATE")
        for case in scenario.cases
    )
    deletes = tuple(
        _execute_guard_rejection(engine, case, operation="DELETE")
        for case in scenario.cases
    )
    return updates, deletes


def _run_negative_guard_probes(
    engine: Engine,
    scenario: BehavioralScenario,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    cases = tuple(
        EvidenceNegativeProbeCase(
            evidence_type=case.evidence_type,
            primary_value=case.primary_value,
        )
        for case in scenario.cases
    )
    results = run_negative_probes(engine, cases)
    expected_matrix = tuple(
        (case.evidence_type, operation)
        for case in scenario.cases
        for operation in ALL_NEGATIVE_PROBE_OPERATIONS
    )
    observed_matrix = tuple(
        (result.evidence_type, result.operation)
        for result in results
    )
    if observed_matrix != expected_matrix:
        raise RuntimeError("negative evidence probe matrix drifted")

    grouped: dict[NegativeProbeOperation, list[str]] = {
        operation: [] for operation in ALL_NEGATIVE_PROBE_OPERATIONS
    }
    for result in results:
        if (
            result.mysql_errno != 1644
            or not result.baseline_retained
            or result.row_count_before != result.row_count_after
        ):
            raise RuntimeError(
                f"{result.evidence_type} {result.operation.value} did not "
                "prove errno 1644 and exact row retention"
            )
        grouped[result.operation].append(
            f"{result.table}:{result.operation.value}:"
            "1644/45000:ROW_RETAINED"
        )

    expected_count = len(CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES)
    for operation in ALL_NEGATIVE_PROBE_OPERATIONS:
        if len(grouped[operation]) != expected_count:
            raise RuntimeError(
                f"negative evidence probe coverage drifted for {operation.value}"
            )
    return (
        tuple(grouped[NegativeProbeOperation.INVALID_INSERT]),
        tuple(grouped[NegativeProbeOperation.REPLACE]),
        tuple(grouped[NegativeProbeOperation.ON_DUPLICATE_KEY_UPDATE]),
    )


class _ExpectedBehavioralRollback(RuntimeError):
    pass


def _run_outer_transaction_rollback_probe(
    engine: Engine,
    scenario: BehavioralScenario,
) -> tuple[str, ...]:
    by_type = {case.evidence_type: case for case in scenario.cases}
    if len(by_type) != len(scenario.cases):
        raise RuntimeError("behavioral scenario evidence types must be unique")
    verified: list[str] = []
    for case in scenario.cases:
        dependency_cases: list[BehavioralEvidenceCase] = []
        seen_dependencies: set[str] = set()
        for evidence_type in case.rollback_dependencies:
            if evidence_type == case.evidence_type:
                raise RuntimeError(
                    f"{case.evidence_type} cannot depend on itself for rollback"
                )
            if evidence_type in seen_dependencies:
                raise RuntimeError(
                    f"{case.evidence_type} has duplicate rollback dependency "
                    f"{evidence_type}"
                )
            dependency = by_type.get(evidence_type)
            if dependency is None:
                raise RuntimeError(
                    f"{case.evidence_type} has unknown rollback dependency "
                    f"{evidence_type}"
                )
            seen_dependencies.add(evidence_type)
            dependency_cases.append(dependency)
        try:
            with engine.begin() as connection:
                for dependency in dependency_cases:
                    dependency_result = _append_behavioral_case(
                        connection,
                        dependency,
                    )
                    if dependency_result.status is not EvidenceAppendStatus.INSERTED:
                        raise RuntimeError(
                            f"{case.evidence_type} rollback dependency "
                            f"{dependency.evidence_type} was not inserted"
                        )
                result = _append_behavioral_case(connection, case)
                if result.status is not EvidenceAppendStatus.INSERTED:
                    raise RuntimeError(
                        f"{case.evidence_type} rollback probe was not inserted"
                    )
                raise _ExpectedBehavioralRollback(
                    "force outer transaction rollback"
                )
        except _ExpectedBehavioralRollback:
            pass

        for observed_case in (*dependency_cases, case):
            table = _safe_sql_identifier(observed_case.table)
            primary = _safe_sql_identifier(observed_case.primary_column)
            with engine.connect() as connection:
                count = connection.execute(
                    text(
                        f"SELECT COUNT(*) FROM {table} "
                        f"WHERE {primary} = :primary_value"
                    ),
                    {"primary_value": observed_case.primary_value},
                ).scalar()
            if int(count or 0) != 0:
                raise RuntimeError(
                    f"{case.evidence_type} outer transaction left "
                    f"{observed_case.evidence_type} evidence committed"
                )
        verified.append(case.evidence_type)
    return tuple(verified)


def _run_behavioral_probes(
    engine: Engine,
    scenario: BehavioralScenario,
) -> BehavioralProbeOutcome:
    observed_types = tuple(case.evidence_type for case in scenario.cases)
    if observed_types != CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES:
        raise RuntimeError("behavioral scenario coverage declaration drifted")
    conflicting_scenario = build_conflicting_double_writer_scenario(scenario)
    _insert_behavioral_seed(engine, scenario)
    _insert_behavioral_seed(engine, conflicting_scenario)
    rollback_types = _run_outer_transaction_rollback_probe(engine, scenario)
    if rollback_types != CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES:
        raise RuntimeError("behavioral rollback coverage declaration drifted")
    inserted, double_writer = _run_identical_double_writer_probes(
        engine,
        scenario,
    )
    if inserted != CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES:
        raise RuntimeError("double-writer evidence coverage declaration drifted")
    conflicting_double_writer = _run_conflicting_double_writer_probes(
        engine,
        conflicting_scenario,
    )
    if len(conflicting_double_writer) != len(
        CORE_BEHAVIORAL_COVERED_EVIDENCE_TYPES
    ):
        raise RuntimeError(
            "conflicting double-writer evidence coverage declaration drifted"
        )
    replayed = _run_idempotent_replay_probes(engine, scenario)
    invalid_inserts, replaces, on_duplicate_updates = (
        _run_negative_guard_probes(engine, scenario)
    )
    updates, deletes = _run_append_only_guard_probes(engine, scenario)
    return BehavioralProbeOutcome(
        legal_inserted=inserted,
        idempotent_replay=replayed,
        identical_double_writer=double_writer,
        conflicting_double_writer=conflicting_double_writer,
        append_only_update_guards=updates,
        append_only_delete_guards=deletes,
        invalid_insert_guards=invalid_inserts,
        replace_guards=replaces,
        on_duplicate_key_update_guards=on_duplicate_updates,
        rollback_verified=True,
        rollback_verified_evidence_types=rollback_types,
    )


def _insert_exact_mapping(
    connection: Connection,
    *,
    table: str,
    values: Mapping[str, object],
) -> None:
    safe_table = _safe_sql_identifier(table)
    columns = tuple(_safe_sql_identifier(str(column)) for column in values)
    if not columns:
        raise RuntimeError(f"{safe_table} registry row cannot be empty")
    result = connection.execute(
        text(
            f"INSERT INTO {safe_table} ({', '.join(columns)}) VALUES ("
            + ", ".join(f":{column}" for column in columns)
            + ")"
        ),
        dict(values),
    )
    if int(getattr(result, "rowcount", -1)) != 1:
        raise RuntimeError(f"{safe_table} insert did not affect exactly one row")


@dataclass(frozen=True, slots=True)
class _ConcurrentRegistryInsertAttempt:
    status: str
    mysql_errno: int | None = None


def _concurrent_registry_insert_attempt(
    engine: Engine,
    *,
    table: str,
    values: Mapping[str, object],
    barrier: Barrier,
) -> _ConcurrentRegistryInsertAttempt:
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            try:
                barrier.wait(timeout=_DOUBLE_WRITER_LOCK_WAIT_SECONDS * 2)
            except BrokenBarrierError as exc:
                raise RuntimeError(
                    f"{table} concurrent registration barrier broke"
                ) from exc
            _insert_exact_mapping(connection, table=table, values=values)
            transaction.commit()
            return _ConcurrentRegistryInsertAttempt("INSERTED")
        except Exception as exc:
            transaction.rollback()
            code, _message = _mysql_error_code_message(exc)
            if code in {1062, 1205, 1213}:
                return _ConcurrentRegistryInsertAttempt("RETRY", code)
            raise


def _assert_exact_registry_row(
    engine: Engine,
    *,
    table: str,
    identity_where: str,
    identity_values: Mapping[str, object],
) -> None:
    safe_table = _safe_sql_identifier(table)
    with engine.connect() as connection:
        count = connection.execute(
            text(
                f"SELECT COUNT(*) FROM {safe_table} WHERE {identity_where}"
            ),
            dict(identity_values),
        ).scalar()
    if int(count or 0) != 1:
        raise RuntimeError(
            f"{safe_table} concurrent registration retained an invalid row count"
        )


def _run_concurrent_registry_insert(
    engine: Engine,
    *,
    table: str,
    values: Mapping[str, object],
    identity_where: str,
    identity_values: Mapping[str, object],
) -> str:
    barrier = Barrier(2)
    with ThreadPoolExecutor(
        max_workers=2,
        thread_name_prefix=f"v2e-{table}-registration",
    ) as executor:
        futures = tuple(
            executor.submit(
                _concurrent_registry_insert_attempt,
                engine,
                table=table,
                values=values,
                barrier=barrier,
            )
            for _index in range(2)
        )
        try:
            attempts = tuple(
                future.result(timeout=_DOUBLE_WRITER_FUTURE_TIMEOUT_SECONDS)
                for future in futures
            )
        except FutureTimeoutError as exc:
            raise RuntimeError(
                f"{table} concurrent registration timed out"
            ) from exc

    inserted = sum(attempt.status == "INSERTED" for attempt in attempts)
    retryable = tuple(
        attempt.mysql_errno
        for attempt in attempts
        if attempt.status == "RETRY"
    )
    if inserted != 1 or len(retryable) != 1:
        raise RuntimeError(
            f"{table} concurrent registration did not produce one winner; "
            f"attempts={attempts!r}"
        )
    # A duplicate/deadlock/timeout is not the final semantic proof.  Retry in
    # a new transaction and require the immutable registry identity to reject.
    try:
        with engine.begin() as connection:
            _insert_exact_mapping(connection, table=table, values=values)
    except Exception as exc:
        code, _message = _mysql_error_code_message(exc)
        if code != 1062:
            raise RuntimeError(
                f"{table} fresh registration retry did not return 1062"
            ) from exc
    else:
        raise RuntimeError(
            f"{table} fresh registration retry unexpectedly inserted"
        )
    _assert_exact_registry_row(
        engine,
        table=table,
        identity_where=identity_where,
        identity_values=identity_values,
    )
    return (
        f"{table}:CONCURRENT_ONE_INSERTED+ONE_{retryable[0]}:"
        "FRESH_1062:ONE_ROW"
    )


def _database_utc_now(engine: Engine) -> datetime:
    with engine.connect() as connection:
        value = connection.execute(text("SELECT UTC_TIMESTAMP(6)")).scalar()
    if type(value) is not datetime:
        raise RuntimeError("MySQL UTC_TIMESTAMP(6) returned an invalid value")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    else:
        value = value.astimezone(timezone.utc)
    return value


def _wait_for_database_time(
    engine: Engine,
    target: datetime,
    *,
    timeout_seconds: float = 30.0,
) -> None:
    if target.tzinfo is None or target.utcoffset() is None:
        raise ValueError("database wait target must be timezone-aware")
    deadline = time.monotonic() + timeout_seconds
    while _database_utc_now(engine) < target.astimezone(timezone.utc):
        if time.monotonic() >= deadline:
            raise RuntimeError("MySQL clock did not reach authority revocation time")
        time.sleep(0.05)


class _InjectedAccountingInterruption(RuntimeError):
    pass


class _AccountingExecutionProbe:
    def __init__(
        self,
        connection: Connection,
        *,
        interrupt_tag: str | None = None,
    ) -> None:
        self._connection = connection
        self._interrupt_tag = interrupt_tag
        self.tags: list[str] = []

    def in_transaction(self) -> bool:
        return self._connection.in_transaction()

    def execute(self, statement, parameters=None):
        sql = str(statement)
        match = re.search(r"/\*\s*v2ao:([a-z0-9_]+)\s*\*/", sql)
        if match is not None:
            tag = match.group(1)
            self.tags.append(tag)
            if tag == self._interrupt_tag:
                raise _InjectedAccountingInterruption(
                    f"injected interruption at {tag}"
                )
        return self._connection.execute(statement, parameters or {})


def _accounting_row_counts(
    engine: Engine,
    accounting_outcome_id: str,
) -> tuple[int, int, int]:
    with engine.connect() as connection:
        outcome_count = connection.execute(
            text(
                f"SELECT COUNT(*) FROM {OUTCOME_TABLE} "
                "WHERE accounting_outcome_id = :outcome_id"
            ),
            {"outcome_id": accounting_outcome_id},
        ).scalar()
        effect_count = connection.execute(
            text(
                f"SELECT COUNT(*) FROM {LOT_EFFECT_TABLE} "
                "WHERE accounting_outcome_id = :outcome_id"
            ),
            {"outcome_id": accounting_outcome_id},
        ).scalar()
        finalization_count = connection.execute(
            text(
                f"SELECT COUNT(*) FROM {FINALIZATION_TABLE} "
                "WHERE accounting_outcome_id = :outcome_id"
            ),
            {"outcome_id": accounting_outcome_id},
        ).scalar()
    return (
        int(outcome_count or 0),
        int(effect_count or 0),
        int(finalization_count or 0),
    )


def _require_mysql_rejection(
    exc: Exception,
    *,
    errno: int,
    message_fragment: str,
    operation: str,
) -> None:
    code, message = _mysql_error_code_message(exc)
    if code != errno or message_fragment.casefold() not in message.casefold():
        raise RuntimeError(
            f"{operation} returned the wrong MySQL rejection; "
            f"code={code}; message={message}"
        ) from exc


def _run_authority_behavioral_probes(
    engine: Engine,
    scenario: AuthorityBehavioralScenario,
) -> AuthorityBehavioralProbeOutcome:
    if len(scenario.cases) != 2:
        raise RuntimeError("authority scenario must cover key and receipt revocation")
    kinds = tuple(case.revocation_kind for case in scenario.cases)
    if frozenset(kinds) != frozenset({"KEY", "RECEIPT"}):
        raise RuntimeError("authority revocation scenario coverage drifted")

    key_registration: list[str] = []
    receipt_registration: list[str] = []
    concurrent: list[str] = []
    for index, case in enumerate(scenario.cases):
        key_identity = {
            name: case.trust_key_values[name]
            for name in ("source_provider", "key_id", "key_version")
        }
        if index == 0:
            concurrent.append(
                _run_concurrent_registry_insert(
                    engine,
                    table="st_execution_authority_trust_key_v2",
                    values=case.trust_key_values,
                    identity_where=(
                        "source_provider = :source_provider AND "
                        "key_id = :key_id AND key_version = :key_version"
                    ),
                    identity_values=key_identity,
                )
            )
        else:
            with engine.begin() as connection:
                _insert_exact_mapping(
                    connection,
                    table="st_execution_authority_trust_key_v2",
                    values=case.trust_key_values,
                )
        _assert_exact_registry_row(
            engine,
            table="st_execution_authority_trust_key_v2",
            identity_where=(
                "source_provider = :source_provider AND key_id = :key_id "
                "AND key_version = :key_version"
            ),
            identity_values=key_identity,
        )
        key_registration.append(
            f"{case.claim.source_provider}:{case.receipt.key_id}:REGISTERED"
        )

        receipt_identity = {"receipt_id": case.receipt.receipt_id}
        if index == 0:
            concurrent.append(
                _run_concurrent_registry_insert(
                    engine,
                    table="st_execution_authority_receipt_v2",
                    values=case.receipt_values,
                    identity_where="receipt_id = :receipt_id",
                    identity_values=receipt_identity,
                )
            )
        else:
            with engine.begin() as connection:
                _insert_exact_mapping(
                    connection,
                    table="st_execution_authority_receipt_v2",
                    values=case.receipt_values,
                )
        _assert_exact_registry_row(
            engine,
            table="st_execution_authority_receipt_v2",
            identity_where="receipt_id = :receipt_id",
            identity_values=receipt_identity,
        )
        receipt_registration.append(
            f"{case.receipt.receipt_id}:REGISTERED"
        )

    nonce = scenario.nonce_replay_case
    try:
        with engine.begin() as connection:
            _insert_exact_mapping(
                connection,
                table="st_execution_authority_receipt_v2",
                values=nonce.receipt_values,
            )
    except Exception as exc:
        _require_mysql_rejection(
            exc,
            errno=1062,
            message_fragment="uk_authority_receipt_v2_replay",
            operation="authority nonce replay",
        )
    else:
        raise RuntimeError("authority nonce replay unexpectedly registered")
    with engine.connect() as connection:
        nonce_count = connection.execute(
            text(
                "SELECT COUNT(*) FROM st_execution_authority_receipt_v2 "
                "WHERE receipt_id = :receipt_id"
            ),
            {"receipt_id": nonce.receipt.receipt_id},
        ).scalar()
    if int(nonce_count or 0) != 0:
        raise RuntimeError("authority nonce replay left a registry row")

    invalid = scenario.invalid_signature_case
    try:
        with engine.begin() as connection:
            _insert_exact_mapping(
                connection,
                table="st_execution_authority_receipt_v2",
                values=invalid.receipt_values,
            )
            require_verified_authority(
                connection,
                invalid.evidence,
                MySQLRegistryBackedAuthorityVerifier(
                    clock=lambda: invalid.evidence.available_at
                ),
                minimum_level=AuthorityVerificationLevel.CRYPTOGRAPHIC,
            )
    except AuthorityVerificationError as exc:
        if "SIGNATURE_INVALID" not in str(exc):
            raise RuntimeError(
                "invalid authority signature returned the wrong denial"
            ) from exc
    else:
        raise RuntimeError("invalid authority signature unexpectedly verified")
    with engine.connect() as connection:
        invalid_count = connection.execute(
            text(
                "SELECT COUNT(*) FROM st_execution_authority_receipt_v2 "
                "WHERE receipt_id = :receipt_id"
            ),
            {"receipt_id": invalid.receipt.receipt_id},
        ).scalar()
    if int(invalid_count or 0) != 0:
        raise RuntimeError("invalid signature probe failed to roll back its receipt")

    for case in scenario.cases:
        verifier = MySQLRegistryBackedAuthorityVerifier(
            clock=lambda value=case.evidence.available_at: value
        )
        with engine.begin() as connection:
            result = append_market_calendar_evidence(
                connection,
                case.evidence,
                authority_verifier=verifier,
            )
        if result.status is not EvidenceAppendStatus.INSERTED:
            raise RuntimeError(
                "external authority calendar was not inserted with attestation"
            )

    revoked_at = scenario.available_at + timedelta(microseconds=1)
    _wait_for_database_time(engine, revoked_at)
    revocations: list[str] = []
    for case in scenario.cases:
        with engine.begin() as connection:
            _insert_exact_mapping(
                connection,
                table=case.revocation_table,
                values=case.revocation_values(revoked_at),
            )
        revocations.append(
            f"{case.revocation_table}:{case.revocation_kind}:INSERTED"
        )

    historical: list[str] = []
    expected_reasons = {
        "KEY": "TRUST_KEY_REVOKED",
        "RECEIPT": "AUTHORITY_RECEIPT_REVOKED",
    }
    for case in scenario.cases:
        verifier = MySQLRegistryBackedAuthorityVerifier(
            clock=lambda value=revoked_at: value
        )
        try:
            with engine.begin() as connection:
                verifier.require_registered_claim_active(
                    connection,
                    case.claim,
                )
        except AuthorityVerificationError as exc:
            expected = expected_reasons[case.revocation_kind]
            if expected not in str(exc):
                raise RuntimeError(
                    f"{case.revocation_kind} historical recheck returned the "
                    "wrong denial"
                ) from exc
            historical.append(
                f"{case.revocation_kind}:HISTORICAL_PROOF_RETAINED:"
                f"NEW_USE_REJECTED_{expected}"
            )
        else:
            raise RuntimeError(
                f"{case.revocation_kind} revocation allowed a new authority use"
            )
    return AuthorityBehavioralProbeOutcome(
        key_registration=tuple(key_registration),
        receipt_registration=tuple(receipt_registration),
        concurrent_registration=tuple(concurrent),
        nonce_replay_rejected=True,
        signature_rejected=True,
        revocations=tuple(revocations),
        historical_recheck=tuple(historical),
    )


def _append_accounting_execution_parents(
    engine: Engine,
    scenario: AccountingBehavioralScenario,
) -> None:
    with engine.begin() as connection:
        result = append_fill_execution_evidence(
            connection,
            scenario.fill_evidence,
        )
    if result.status is not EvidenceAppendStatus.INSERTED:
        raise RuntimeError("accounting fill evidence was not inserted")

    genesis, fill_cash = scenario.cash_evidence_rows
    with engine.begin() as connection:
        genesis_result = append_cash_event_binding(connection, genesis)
    if genesis_result.status is not EvidenceAppendStatus.INSERTED:
        raise RuntimeError("accounting cash genesis was not inserted")

    with engine.begin() as connection:
        updated = connection.execute(
            text(
                "UPDATE st_trade_account_v2 SET cash_balance = :cash_after, "
                "updated_at = :updated_at WHERE account_id = :account_id "
                "AND cash_balance = :cash_before"
            ),
            {
                "cash_after": scenario.account_cash_after,
                "updated_at": scenario.fill_evidence.bound_at.replace(tzinfo=None),
                "account_id": scenario.account_id,
                "cash_before": scenario.account_cash_before,
            },
        )
        if int(getattr(updated, "rowcount", -1)) != 1:
            raise RuntimeError("accounting account cash transition was not exact")

    with engine.begin() as connection:
        cash_result = append_cash_event_binding(connection, fill_cash)
    if cash_result.status is not EvidenceAppendStatus.INSERTED:
        raise RuntimeError("accounting fill cash evidence was not inserted")

    with engine.begin() as connection:
        transition_result = append_order_transition_evidence(
            connection,
            scenario.order_transition,
        )
    if transition_result.status is not EvidenceAppendStatus.INSERTED:
        raise RuntimeError("accounting fill order transition was not inserted")


def _run_accounting_behavioral_probes(
    engine: Engine,
    scenario: AccountingBehavioralScenario,
) -> AccountingBehavioralProbeOutcome:
    if len(scenario.outcome.lot_effects) != 2:
        raise RuntimeError("accounting scenario must consume two FIFO lots")
    _insert_behavioral_seed(engine, scenario)
    _append_accounting_execution_parents(engine, scenario)

    interruption_tags: tuple[str, ...] = ()
    try:
        with engine.begin() as connection:
            probe = _AccountingExecutionProbe(
                connection,
                interrupt_tag="insert_finalization",
            )
            try:
                append_fill_accounting_outcome(probe, scenario.outcome)
            finally:
                interruption_tags = tuple(probe.tags)
    except _InjectedAccountingInterruption:
        pass
    else:
        raise RuntimeError("accounting finalization interruption was not injected")
    if _accounting_row_counts(
        engine,
        scenario.outcome.accounting_outcome_id,
    ) != (0, 0, 0):
        raise RuntimeError("interrupted accounting batch was not wholly rolled back")
    interruption_inserts = tuple(
        tag for tag in interruption_tags if tag.startswith("insert_")
    )
    if interruption_inserts != (
        "insert_outcome",
        "insert_lot_effect",
        "insert_lot_effect",
        "insert_finalization",
    ):
        raise RuntimeError(
            "accounting interruption did not occur after outcome/effects and "
            "before the FINAL marker"
        )

    with engine.begin() as connection:
        probe = _AccountingExecutionProbe(connection)
        inserted = append_fill_accounting_outcome(probe, scenario.outcome)
        insert_tags = tuple(
            tag for tag in probe.tags if tag.startswith("insert_")
        )
    if inserted.status is not AccountingEvidenceAppendStatus.INSERTED:
        raise RuntimeError("accounting outcome was not atomically inserted")
    if insert_tags != (
        "insert_outcome",
        "insert_lot_effect",
        "insert_lot_effect",
        "insert_finalization",
    ):
        raise RuntimeError("accounting outcome/effect/finalization order drifted")

    with engine.begin() as connection:
        replay = append_fill_accounting_outcome(connection, scenario.outcome)
    if replay.status is not AccountingEvidenceAppendStatus.IDEMPOTENT:
        raise RuntimeError("accounting exact replay was not IDEMPOTENT")

    try:
        with engine.begin() as connection:
            append_fill_accounting_outcome(
                connection,
                scenario.conflicting_outcome,
            )
    except AccountingEvidenceAppendConflictError:
        pass
    else:
        raise RuntimeError("same-fill different accounting content did not conflict")

    expected_lot_ids = tuple(
        effect.after_lot.lot_id for effect in scenario.outcome.lot_effects
    )
    with engine.connect() as connection:
        actual_lot_ids = tuple(
            connection.execute(
                text(
                    f"SELECT lot_id FROM {LOT_EFFECT_TABLE} "
                    "WHERE accounting_outcome_id = :outcome_id "
                    "ORDER BY effect_sequence"
                ),
                {"outcome_id": scenario.outcome.accounting_outcome_id},
            ).scalars().all()
        )
    if actual_lot_ids != expected_lot_ids:
        raise RuntimeError("persisted accounting effects are not exact FIFO order")
    expected_counts = (1, len(expected_lot_ids), 1)
    if _accounting_row_counts(
        engine,
        scenario.outcome.accounting_outcome_id,
    ) != expected_counts:
        raise RuntimeError("finalized accounting batch cardinality differs")
    return AccountingBehavioralProbeOutcome(
        ordered_insert_tags=insert_tags,
        interruption_rolled_back=True,
        whole_batch_rolled_back=True,
        exact_replay_status=replay.status.value,
        different_content_conflict=True,
        fifo_lot_ids=actual_lot_ids,
    )


def _exact_positive_table_counts(
    values: tuple[tuple[str, int], ...],
    expected_tables: Iterable[str],
    *,
    layer: str,
) -> dict[str, int]:
    if type(values) is not tuple:
        raise RuntimeError(f"{layer} audit table_counts must be tuple")
    counts: dict[str, int] = {}
    for item in values:
        if (
            type(item) is not tuple
            or len(item) != 2
            or type(item[0]) is not str
            or type(item[1]) is not int
            or item[1] <= 0
            or item[0] in counts
        ):
            raise RuntimeError(f"{layer} audit is not non-empty and exact")
        counts[item[0]] = item[1]
    if frozenset(counts) != frozenset(expected_tables):
        raise RuntimeError(f"{layer} audit table coverage differs")
    return counts


def _require_nonempty_three_layer_audits(
    core: V2EvidenceHashAuditReport,
    authority: V2AuthorityStoredRowAuditReport,
    accounting: V2AccountingEvidenceAuditReport,
) -> bool:
    if not (
        core.audit_passed
        and authority.audit_passed
        and accounting.audit_passed
        and core.database_sha2_used
        and core.shared_row_locks_used
        and authority.database_sha2_used
        and authority.shared_row_locks_used
        and accounting.database_sha2_used
        and accounting.shared_row_locks_used
    ):
        raise RuntimeError(
            "three-layer database auditors did not all pass with DB SHA2 and locks"
        )
    _exact_positive_table_counts(
        core.table_counts,
        EVIDENCE_TABLES,
        layer="execution evidence",
    )
    _exact_positive_table_counts(
        authority.table_counts,
        AUTHORITY_AUDIT_TABLES,
        layer="authority evidence",
    )
    accounting_counts = _exact_positive_table_counts(
        accounting.table_counts,
        ACCOUNTING_AUDIT_TABLES,
        layer="accounting evidence",
    )
    if (
        core.external_authority_claims < 1
        or accounting.finalized_outcomes != accounting_counts[OUTCOME_TABLE]
        or accounting.lot_chains_checked < 1
    ):
        raise RuntimeError(
            "three-layer database auditors did not inspect non-empty authority "
            "and finalized FIFO accounting evidence"
        )
    if (
        core.production_activation_allowed
        or authority.production_activation_allowed
        or accounting.production_activation_allowed
        or accounting.actionable_output_allowed
    ):
        raise RuntimeError(
            "three-layer database audits must remain non-production/non-actionable"
        )
    return True


def _run_extended_database_audits(
    engine: Engine,
    authority: AuthorityBehavioralProbeOutcome,
    accounting: AccountingBehavioralProbeOutcome,
) -> ExtendedBehavioralProbeOutcome:
    with engine.begin() as connection:
        authority_report = audit_v2_execution_evidence_authority_database(
            connection
        )
    if (
        type(authority_report) is not V2AuthorityStoredRowAuditReport
        or not authority_report.audit_passed
        or not authority_report.database_sha2_used
        or not authority_report.shared_row_locks_used
    ):
        raise RuntimeError(
            "authority database audit did not independently reconstruct 014"
        )
    _exact_positive_table_counts(
        authority_report.table_counts,
        AUTHORITY_AUDIT_TABLES,
        layer="authority evidence",
    )

    with engine.begin() as connection:
        accounting_report = audit_v2_accounting_evidence_database(connection)
    if (
        type(accounting_report) is not V2AccountingEvidenceAuditReport
        or not accounting_report.audit_passed
        or not accounting_report.database_sha2_used
        or not accounting_report.shared_row_locks_used
    ):
        raise RuntimeError(
            "accounting database audit did not independently reconstruct 015"
        )
    _exact_positive_table_counts(
        accounting_report.table_counts,
        ACCOUNTING_AUDIT_TABLES,
        layer="accounting evidence",
    )
    return ExtendedBehavioralProbeOutcome(
        authority=authority,
        accounting=accounting,
        authority_audit_report=authority_report,
        accounting_audit_report=accounting_report,
    )


def _run_extended_behavioral_probes(
    engine: Engine,
) -> ExtendedBehavioralProbeOutcome:
    authority_scenario = build_authority_behavioral_scenario(
        _database_utc_now(engine)
    )
    authority = _run_authority_behavioral_probes(
        engine,
        authority_scenario,
    )
    accounting = _run_accounting_behavioral_probes(
        engine,
        build_accounting_behavioral_scenario(),
    )
    return _run_extended_database_audits(engine, authority, accounting)


def run_mysql_behavioral_acceptance(
    url: str,
    *,
    expected_server_uuid: str,
    tls_config: MySQLAcceptanceTLSConfig | None = None,
    canonical_hash_audit_runner: Callable[
        [_IdentityBoundEngine],
        CanonicalHashAuditAcceptanceOutcome,
    ] = _run_database_canonical_hash_audit,
) -> BehavioralAcceptanceReport:
    """Run the complete 011-015 behavioral slice after all V2 migrations.

    The harness covers the five core execution-evidence writers, the 014
    cryptographic authority registry/revocation lifecycle, and the 015
    finalized FIFO accounting batch.  Passing remains an isolated test proof;
    it never authorizes production or actionable output.
    """

    _assert_frozen_migration_contract()
    if not callable(canonical_hash_audit_runner):
        raise TypeError("canonical_hash_audit_runner must be callable")
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    safe_url = require_dedicated_test_url(url)
    expected_database = str(make_url(safe_url).database)
    engine = create_tool_engine(
        safe_url,
        tls_config=tls_config,
        future=True,
        pool_size=2,
        max_overflow=0,
        pool_pre_ping=True,
    )
    try:
        with engine.connect() as migration_connection:
            (
                database,
                server_version,
                server_uuid,
                version_comment,
            ) = _server_identity_from_connection(
                migration_connection,
                expected_database,
                expected_uuid,
            )
            _assert_empty_on_connection(
                migration_connection,
                mode="behavioral",
            )
            migration_connection.rollback()
            initial = _validate_run(
                run_v2_migrations(
                    engine,
                    allow_execution_evidence=True,
                    connection=migration_connection,
                ),
                allowed_statuses=frozenset({"applied"}),
            )
            migration_connection.rollback()
            snapshot = _post_migration_snapshot(
                engine,
                connection=migration_connection,
            )
        identity_bound_engine = _IdentityBoundEngine(
            engine,
            expected_database=expected_database,
            expected_server_uuid=expected_uuid,
        )
        outcome = _run_behavioral_probes(
            identity_bound_engine,
            build_behavioral_scenario(),
        )
        extended = _run_extended_behavioral_probes(identity_bound_engine)
        hash_audit = canonical_hash_audit_runner(identity_bound_engine)
        if type(hash_audit) is not CanonicalHashAuditAcceptanceOutcome:
            raise RuntimeError(
                "canonical hash audit runner returned an invalid outcome"
            )
        if type(hash_audit.report) is not V2EvidenceHashAuditReport:
            raise RuntimeError("canonical hash audit report type is invalid")
        if not (
            hash_audit.report.audit_passed
            and hash_audit.report.database_sha2_used
            and hash_audit.report.shared_row_locks_used
            and hash_audit.schema_blocker_removed
        ):
            raise RuntimeError(
                "behavioral acceptance requires a passing database hash audit"
            )
        if (
            hash_audit.production_activation_allowed
            or hash_audit.actionable_output_allowed
            or hash_audit.report.production_activation_allowed
        ):
            raise RuntimeError(
                "canonical hash audit must remain non-production and non-actionable"
            )
        three_layer_audit = _require_nonempty_three_layer_audits(
            hash_audit.report,
            extended.authority_audit_report,
            extended.accounting_audit_report,
        )
    finally:
        engine.dispose()
    return BehavioralAcceptanceReport(
        mode="behavioral",
        database=database,
        server_version=server_version,
        server_version_comment=version_comment,
        server_uuid=server_uuid,
        least_privilege_attested=True,
        started_empty=True,
        initial_migration=_result_statuses(initial),
        behavioral_coverage=BEHAVIORAL_COVERED_EVIDENCE_TYPES,
        behavioral_not_covered=BEHAVIORAL_NOT_COVERED_EVIDENCE_TYPES,
        behavioral_probes_covered=BEHAVIORAL_PROBES_COVERED,
        behavioral_probes_not_covered=BEHAVIORAL_PROBES_NOT_COVERED,
        all_five_evidence_types_covered=True,
        all_declared_evidence_types_covered=True,
        legal_inserted=outcome.legal_inserted,
        idempotent_replay=outcome.idempotent_replay,
        identical_double_writer=outcome.identical_double_writer,
        conflicting_double_writer=outcome.conflicting_double_writer,
        append_only_update_guards=outcome.append_only_update_guards,
        append_only_delete_guards=outcome.append_only_delete_guards,
        invalid_insert_guards=outcome.invalid_insert_guards,
        replace_guards=outcome.replace_guards,
        on_duplicate_key_update_guards=(
            outcome.on_duplicate_key_update_guards
        ),
        rollback_verified=outcome.rollback_verified,
        rollback_verified_evidence_types=(
            outcome.rollback_verified_evidence_types
        ),
        authority_key_registration=extended.authority.key_registration,
        authority_receipt_registration=extended.authority.receipt_registration,
        authority_concurrent_registration=(
            extended.authority.concurrent_registration
        ),
        authority_nonce_replay_rejected=(
            extended.authority.nonce_replay_rejected
        ),
        authority_signature_rejected=extended.authority.signature_rejected,
        authority_revocations=extended.authority.revocations,
        authority_historical_recheck=extended.authority.historical_recheck,
        accounting_ordered_insert_tags=(
            extended.accounting.ordered_insert_tags
        ),
        accounting_interruption_rolled_back=(
            extended.accounting.interruption_rolled_back
        ),
        accounting_whole_batch_rolled_back=(
            extended.accounting.whole_batch_rolled_back
        ),
        accounting_exact_replay_status=(
            extended.accounting.exact_replay_status
        ),
        accounting_different_content_conflict=(
            extended.accounting.different_content_conflict
        ),
        accounting_fifo_lot_ids=extended.accounting.fifo_lot_ids,
        canonical_hash_audit_report=hash_audit.report,
        canonical_hash_audit_passed=True,
        canonical_hash_schema_blocker_removed=(
            hash_audit.schema_blocker_removed
        ),
        authority_audit_report=extended.authority_audit_report,
        authority_audit_passed=True,
        accounting_audit_report=extended.accounting_audit_report,
        accounting_audit_passed=True,
        three_layer_nonempty_audit_passed=three_layer_audit,
        production_activation_allowed=False,
        actionable_output_allowed=False,
        pre_behavior_snapshot=snapshot,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run isolated V2 execution-evidence acceptance on an empty "
            "validated Oracle MySQL test/CI database; require the final "
            "52-table "
            "inventory and one INACTIVE maintenance-fence row without "
            "authorizing production or actionable output"
        )
    )
    parser.add_argument("--url-env", default=DEFAULT_URL_ENV)
    parser.add_argument(
        "--server-uuid-env",
        default=DEFAULT_SERVER_UUID_ENV,
    )
    parser.add_argument(
        "--ssl-ca-env",
        default=DEFAULT_SSL_CA_ENV,
        help="dedicated V2 evidence TEST/CI variable containing the SSL CA file",
    )
    parser.add_argument(
        "--mode",
        choices=("serial-replay", "concurrent-initial", "behavioral"),
        default="serial-replay",
    )
    parser.add_argument("--concurrency", type=int, default=2)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    url = resolve_test_url(args.url_env)
    expected_server_uuid = resolve_server_uuid(args.server_uuid_env)
    tls_config = resolve_mysql_acceptance_tls_config(
        "V2_EVIDENCE",
        args.ssl_ca_env,
    )
    if args.mode == "concurrent-initial":
        report = run_mysql_concurrent_initial_acceptance(
            url,
            expected_server_uuid=expected_server_uuid,
            concurrency=args.concurrency,
            tls_config=tls_config,
        )
    elif args.mode == "behavioral":
        report = run_mysql_behavioral_acceptance(
            url,
            expected_server_uuid=expected_server_uuid,
            tls_config=tls_config,
        )
    else:
        report = run_mysql_serial_replay_acceptance(
            url,
            expected_server_uuid=expected_server_uuid,
            tls_config=tls_config,
        )
    print(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
