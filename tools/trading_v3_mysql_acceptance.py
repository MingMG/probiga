"""Strict TEST/CI-only exact-version MySQL acceptance for the V3 chain.

Every mode requires a separately provisioned, initially empty ``*_v3_test*``
or ``*_v3_ci*`` schema.  The runner creates only two explicitly declared
application prerequisites, applies the real V2 migration chain, then applies
the real V3 chain.  It never fabricates a V2 or V3 migration-ledger row and it
never enables the disabled V3 projection worker or production output.
"""
from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import sys
import uuid


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import text
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.exc import ArgumentError

from server.common.mysql_version_policy import (
    MYSQL_84_ISOLATED_ACCEPTANCE,
    is_isolated_acceptance_version,
    is_oracle_mysql_distribution,
    isolated_acceptance_versions_label,
)
from server.db.migrations_v2 import (
    MIGRATION_TABLE_DDL as V2_MIGRATION_TABLE_DDL,
    MIGRATIONS as V2_MIGRATIONS,
    V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE,
    V2_EVIDENCE_MAINTENANCE_FENCE_NAME,
    V2_EVIDENCE_MAINTENANCE_FENCE_TABLE,
    run_v2_migrations,
)
from server.db.migrations_v3 import (
    FORWARD_EXIT_ALLOCATION_MIGRATION_VERSION,
    FORWARD_STRATEGY_VERSION_MIGRATION_VERSION,
    V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION,
    MIGRATION_PROGRESS_TABLE,
    MIGRATION_TABLE_DDL,
    MIGRATIONS,
    V3MigrationAcceptanceFault,
    V3MigrationAcceptanceFaultHook,
    V3MigrationResult,
    V3_PROJECTION_OUTBOX_MIGRATION_VERSION,
    HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION,
    HORIZON_PROTOCOL_V2_MIGRATION_VERSION,
    _checksum,
    run_v3_migrations,
    validate_forward_exit_allocation_schema,
)
from server.integrations.v3_execution_projection import (
    bind_v3_execution_plan,
    project_execution_result,
)
from server.integrations.v3_execution_projection_outbox import (
    OUTBOX_RUNTIME_ENABLED,
    V3ProjectionOutboxAppendStatus,
    append_v3_transition_outbox,
    validate_v3_projection_outbox_schema,
)
from server.integrations.v3_execution_projection_outbox import legacy_guard
from server.integrations.v3_execution_projection_outbox import schema as outbox_schema
from server.trading_v3.horizon_candidate_ledger_schema import (
    validate_horizon_candidate_ledger_schema,
)
from server.trading_core.contracts import (
    ExecutionIntent,
    ExecutionResult,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
    execution_intent_idempotency_key,
    execution_result_idempotency_key,
)
from server.trading_core.execution import (
    apply_execution_result_with_receipt,
    new_order_state,
)
from tools.mysql_acceptance_tls import (
    MySQLAcceptanceTLSConfig,
    create_mysql_acceptance_engine as create_tool_engine,
    resolve_mysql_acceptance_tls_config,
)


DEFAULT_URL_ENV = "V3_TEST_MYSQL_URL"
DEFAULT_SERVER_UUID_ENV = "V3_TEST_MYSQL_SERVER_UUID"
DEFAULT_SSL_CA_ENV = "V3_TEST_MYSQL_SSL_CA"
_SAFE_URL_ENV_RE = re.compile(
    r"^V3_(?:TEST|CI)(?:_[A-Z0-9]+)*_MYSQL_URL$"
)
_SAFE_SERVER_UUID_ENV_RE = re.compile(
    r"^V3_(?:TEST|CI)(?:_[A-Z0-9]+)*_MYSQL_SERVER_UUID$"
)
_FORBIDDEN_URL_ENVS = frozenset({"MYSQL_URL", "DATABASE_URL"})
_SAFE_DATABASE_RE = re.compile(
    r"^[a-z0-9]+(?:_[a-z0-9]+)*_v3_(?:test|ci)(?:_[a-z0-9]+)*$",
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
_CREATE_TABLE_RE = re.compile(
    r"\bCREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+`?([a-z0-9_]+)`?",
    re.IGNORECASE,
)
_DROP_TRIGGER_RE = re.compile(
    r"^\s*DROP\s+TRIGGER\s+IF\s+EXISTS\s+`?([a-z0-9_]+)`?",
    re.IGNORECASE,
)
_CREATE_TRIGGER_RE = re.compile(
    r"^\s*CREATE\s+TRIGGER\s+`?([a-z0-9_]+)`?",
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

FROZEN_EXPECTED_V3_MIGRATIONS = (
    ("20260728_001_trading_v3_core", "bb0a83fb8e69ae0c41a768031c6007204b13234a42a8a37a81422d3df8fc6c3b", 10),
    ("20260729_002_restore_real_trading_hard_guard", "6d851da61cd9355aa5254d67cb0fc05a266ae94f528daae8a114fedd5c557674", 5),
    ("20260730_003_add_forecast_feature_snapshot", "919b8eb0365b00a8adea4fc389b36a497ad472290b0dc36a3a19116a025e7ef3", 1),
    ("20260730_004_trade_hypothesis_ledger", "08aa40b4ceb66ae390d6f38b6385893945971583aca0a19fd2ec13162e84d96c", 2),
    ("20260730_005_decision_provenance", "8418ef71200c7aff6598b8519618fb2f481a2e963931ffc9339b2cee99517aaf", 1),
    ("20260730_006_target_theme_exposure", "5b8c2bb379e15129c9b889a69e4bffa662d39567c372b8625e22d56122ab30be", 1),
    ("20260730_007_retire_legacy_models", "2809d324a19919d0ff473fb367348ff76f205ef44d556030cb7abc3e118e9753", 1),
    ("20260730_008_disable_legacy_entry_routes", "2bbfae40ea84371977386ef5a187f2e34a2fd22b678fe9ff90f4cca20ecc80eb", 1),
    ("20260730_009_suspend_legacy_entry_strategies", "f7c7df2fb07e135c43722867c91007ff64675ca919655a8ba47d669879c2d032", 1),
    ("20260730_010_cancel_legacy_entry_orders", "5287abe6d623a0a7bb41c7aa9e30c5dce905b9c282a780be05dc06e6e70653a8", 1),
    ("20260801_001_block_real_execution_plans", "ad48d6d6126566569966918174a9ea90e9ce066cb238ee9dcbf8f2c7b856670e", 5),
    ("20260801_002_repair_counterfactual_attribution", "56b57d72d379ff276962d25978e3b7d9dc14b3180c7723f54c4e783c17ae0db7", 1),
    ("20260801_003_unify_forward_execution_evidence", "27d477ee8578ede6874e042cf70ebec4ee4f6c37d0ae240e06fe3263b0867bd5", 6),
    ("20260801_004_tag_opportunity_recall_evidence", "3340e3e0ad673cb68d0a946f44c1c91db5dbe7f22c9f9ea9e2d4fe4ba9c74b93", 2),
    ("20260801_005_freeze_sample_ownership", "ac0beda914c87b6f9dcb0de8b8424fbcb8adc8084fe8ec8170b3fbe6e70f45c9", 5),
    ("20260801_006_counterfactual_backlog_queue", "162a18d2cde53909e2f26a4dc88e0218cf9e2f458432d2fdb88b3809be20a607", 1),
    ("20260802_001_shadow_portfolio_evidence_isolation", "0c24a9cf72bca860f0585f660bd131d4279c3ad644e8ac0ad2a382ab8d136cd2", 12),
    ("20260802_002_generic_theme_signal_ledger", "3f1ca400f243d8aa69f46d2d418c0c07e4bfce8ca38ee9a153b03278e879dca6", 8),
    ("20260802_003_news_point_in_time_knowledge", "3885c522999aa32bf4e74d8d2846a36be5672bef9583b4f685610d3a339c470c", 8),
    ("20260803_001_v3_execution_projection_subscriber", "6ef9383dac24775a8a804517c35b01c180613c4d21a0a29e6bd6cfa1ec4bb6dc", 17),
    ("20260804_000_shadow_intelligence_runtime", "b09f22e64601c91023b866e6cc90c3bffedc3e7bae131457e1c65ee41ce91735", 46),
    ("20260804_001_v3_execution_projection_outbox", "5f53bf5258705e410b93db2b5034bbf9683ebe03b17f854625f6854fb55f7e78", 4),
    ("20260817_000_horizon_protocol_v2_governance", "9430f7bf8014d8758339f6754ac51989283c648657b5fdb1db813ab32afce118", 10),
    ("20260817_001_horizon_candidate_ledger_registration", "88cbd1d4bd57fa65164d2d35e07a6344d9a8d8e255d09efb0e79cabfd0d8c522", 9),
    (FORWARD_STRATEGY_VERSION_MIGRATION_VERSION, "08e0e3d6e6bb9b31227f33780eb54bf3afd38d226f456fa41985137614ce7602", 7),
    (V2_RAW_LEDGER_IMMUTABILITY_MIGRATION_VERSION, "5e7d735be4d24659786e17091b89df06e3dba713c3646fd00ef817a67bc8eedf", 8),
    (FORWARD_EXIT_ALLOCATION_MIGRATION_VERSION, "deeff7acffcea37b535a25a3f00216b91b15ffb8c2d9bf8fa05db7426e32053a", 5),
)

FROZEN_V3_TABLES = frozenset(
    {
        "st_alpha_forecast_v3",
        "st_counterfactual_queue_v3",
        "st_counterfactual_v3",
        "st_decision_run_v3",
        "st_execution_plan_binding_v3",
        "st_execution_plan_v3",
        "st_execution_projection_dead_letter_reconciliation_v3",
        "st_execution_projection_head_v3",
        "st_execution_projection_inbox_v3",
        "st_execution_projection_order_baseline_v3",
        "st_execution_projection_outbox_v2",
        "st_execution_projection_worker_checkpoint_v3",
        "st_forward_trade_evidence_v3",
        "st_forward_exit_allocation_v3",
        "st_hypothesis_evidence_v3",
        "st_horizon_forecast_contract_v3",
        "st_horizon_model_artifact_v3",
        "st_horizon_outcome_v3",
        "st_shadow_release_v3",
        "st_calibration_gate_v3",
        "st_counterfactual_learning_run_v3",
        "st_model_registry_v3",
        "st_opportunity_recall_v3",
        "st_position_state_v3",
        "st_shadow_portfolio_v3",
        "st_target_portfolio_v3",
        "st_tca_v3",
        "st_theme_signal_v3",
        "st_trade_hypothesis_v3",
        "st_validation_result_v3",
    }
)

CONTROLLED_PREREQUISITE_DDL = (
    """
    CREATE TABLE st_scheduled_tasks (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        task_type VARCHAR(50) NOT NULL,
        enabled TINYINT(1) NOT NULL DEFAULT 0,
        description VARCHAR(500) NOT NULL DEFAULT ''
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
    """
    CREATE TABLE st_news_flash (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        publish_time DATETIME NOT NULL,
        etl_sync_at DATETIME NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """,
)
CONTROLLED_PREREQUISITE_TABLES = frozenset(
    {"st_scheduled_tasks", "st_news_flash"}
)


@dataclass(frozen=True)
class V3MySQLAcceptanceReport:
    mode: str
    database: str
    server_version: str
    server_version_comment: str
    server_uuid: str
    least_privilege_attested: bool
    started_empty: bool
    controlled_prerequisites: tuple[str, ...]
    v2_migration: tuple[str, ...]
    initial_v3_migration: tuple[str, ...]
    replay_v3_migration: tuple[str, ...]
    concurrent_v3_migrations: tuple[tuple[str, ...], ...]
    partial_migration_version: str
    partial_committed_statement_count: int
    partial_outbox_table_count: int
    partial_horizon_column_count: int
    partial_horizon_check_count: int
    partial_horizon_index_count: int
    partial_horizon_trigger_count: int
    observed_table_count: int
    observed_trigger_count: int
    migration_ledger_rows: int
    migration_progress_rows: int
    schema_gate_passed: bool
    rollback_absent: bool
    committed_append_visible: bool
    idempotent_append_verified: bool
    outbox_runtime_enabled: bool
    production_activation_allowed: bool
    actionable_output_allowed: bool
    worker_activation_allowed: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _declared_v3_contract() -> tuple[tuple[str, str, int], ...]:
    return tuple(
        (
            str(item["version"]),
            _checksum(tuple(item["statements"])),
            len(tuple(item["statements"])),
        )
        for item in MIGRATIONS
    )


def _declared_tables(statements: Iterable[str]) -> frozenset[str]:
    return frozenset(
        match.group(1).lower()
        for statement in statements
        for match in _CREATE_TABLE_RE.finditer(statement)
    )


def _assert_frozen_contract() -> None:
    if _declared_v3_contract() != FROZEN_EXPECTED_V3_MIGRATIONS:
        raise RuntimeError("V3 migration source changed from the frozen contract")
    declared = _declared_tables(
        str(statement)
        for migration in MIGRATIONS
        for statement in migration["statements"]
    )
    if declared != FROZEN_V3_TABLES:
        raise RuntimeError("V3 migration table inventory changed")


def require_dedicated_test_url(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("a dedicated V3 test MySQL URL is required")
    raw = value.strip()
    try:
        url = make_url(raw)
    except ArgumentError as exc:
        raise ValueError("invalid dedicated V3 test MySQL URL") from exc
    if url.get_backend_name().lower() != "mysql":
        raise ValueError("V3 acceptance requires the MySQL backend")
    if url.query:
        raise ValueError("V3 acceptance URL query parameters are forbidden")
    if not str(url.host or "").strip():
        raise ValueError("V3 acceptance URL requires an explicit host")
    if not _SAFE_DATABASE_RE.fullmatch(str(url.database or "")):
        raise ValueError(
            "database name must be an explicit *_v3_test* or *_v3_ci* database"
        )
    return raw


def resolve_test_url(
    env_name: str = DEFAULT_URL_ENV,
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    normalized = str(env_name or "").strip()
    if normalized in _FORBIDDEN_URL_ENVS:
        raise ValueError(f"{normalized} is forbidden for V3 MySQL acceptance")
    if _SAFE_URL_ENV_RE.fullmatch(normalized) is None:
        raise ValueError(
            "URL environment variable must match V3_TEST_*_MYSQL_URL or "
            "V3_CI_*_MYSQL_URL"
        )
    source = os.environ if environ is None else environ
    return require_dedicated_test_url(source.get(normalized, ""))


def require_expected_server_uuid(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("an expected MySQL server UUID is required")
    normalized = value.strip().lower()
    if _CANONICAL_UUID_RE.fullmatch(normalized) is None:
        raise ValueError("expected MySQL server UUID must be canonical")
    parsed = uuid.UUID(normalized)
    if parsed.int == 0:
        raise ValueError("expected MySQL server UUID must not be nil")
    return str(parsed)


def resolve_server_uuid(
    env_name: str = DEFAULT_SERVER_UUID_ENV,
    *,
    environ: Mapping[str, str] | None = None,
) -> str:
    normalized = str(env_name or "").strip()
    if _SAFE_SERVER_UUID_ENV_RE.fullmatch(normalized) is None:
        raise ValueError(
            "server UUID environment variable must match V3 TEST/CI policy"
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
        raise RuntimeError("V3 acceptance account grants could not be attested")
    for grant in observed:
        match = _GRANT_RE.match(grant)
        if match is None or " WITH GRANT OPTION" in grant.upper():
            raise RuntimeError("V3 acceptance account has unsupported grants")
        privileges = " ".join(match.group("privileges").upper().split())
        target = match.group("target").replace("`", "").strip().lower()
        if target == "*.*" and privileges == "USAGE":
            continue
        if target != expected_target:
            raise RuntimeError("V3 acceptance grants escape the target schema")
        parsed = {
            " ".join(item.strip().upper().split())
            for item in privileges.split(",")
            if item.strip()
        }
        if parsed - _REQUIRED_SCHEMA_PRIVILEGES:
            raise RuntimeError("V3 acceptance account has unnecessary grants")
        observed_schema_privileges.update(parsed)
    if observed_schema_privileges != _REQUIRED_SCHEMA_PRIVILEGES:
        raise RuntimeError("V3 acceptance account grants are incomplete")


def _identity(
    engine: Engine,
    *,
    expected_database: str,
    expected_server_uuid: str,
) -> tuple[str, str, str, str]:
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    with engine.connect() as connection:
        backend = str(connection.dialect.name).lower()
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
    if backend != "mysql" or not is_oracle_mysql_distribution(
        version,
        version_comment,
    ):
        raise RuntimeError("V3 acceptance connection must use Oracle MySQL")
    if not is_isolated_acceptance_version(version):
        raise RuntimeError(
            "V3 acceptance requires Oracle MySQL "
            f"{isolated_acceptance_versions_label()} exactly"
        )
    if database != expected_database or server_uuid != expected_uuid:
        raise RuntimeError("V3 acceptance database/server identity mismatch")
    _assert_least_privilege_grants(grants, expected_database=expected_database)
    return database, version, server_uuid, version_comment


def _schema_names(engine: Engine, kind: str) -> frozenset[str]:
    contracts = {
        "tables": ("TABLE_NAME", "TABLES", "TABLE_SCHEMA"),
        "routines": ("ROUTINE_NAME", "ROUTINES", "ROUTINE_SCHEMA"),
        "events": ("EVENT_NAME", "EVENTS", "EVENT_SCHEMA"),
        "triggers": ("TRIGGER_NAME", "TRIGGERS", "TRIGGER_SCHEMA"),
    }
    column, table_name, schema_column = contracts[kind]
    with engine.connect() as connection:
        rows = connection.execute(
            text(
                f"SELECT {column} FROM information_schema.{table_name} "
                f"WHERE {schema_column} = DATABASE() ORDER BY {column}"
            )
        ).scalars()
        return frozenset(str(item).lower() for item in rows)


def _preflight_empty_schema(
    engine: Engine,
    *,
    expected_database: str,
    expected_server_uuid: str,
) -> tuple[str, str, str, str]:
    identity = _identity(
        engine,
        expected_database=expected_database,
        expected_server_uuid=expected_server_uuid,
    )
    if not str(identity[1]).startswith(f"{MYSQL_84_ISOLATED_ACCEPTANCE}"):
        raise RuntimeError(
            "current Horizon V3 candidate-ledger acceptance requires Oracle "
            f"MySQL {MYSQL_84_ISOLATED_ACCEPTANCE} exactly"
        )
    objects = {
        kind: _schema_names(engine, kind)
        for kind in ("tables", "routines", "events", "triggers")
    }
    nonempty = {key: sorted(value) for key, value in objects.items() if value}
    if nonempty:
        raise RuntimeError(
            "V3 acceptance database must start completely empty: "
            + json.dumps(nonempty, ensure_ascii=False, sort_keys=True)
        )
    return identity


def _bootstrap_controlled_prerequisites(engine: Engine) -> None:
    if any("schema_migration" in ddl.casefold() for ddl in CONTROLLED_PREREQUISITE_DDL):
        raise RuntimeError("controlled prerequisites must not fabricate ledger")
    with engine.connect() as connection:
        for statement in CONTROLLED_PREREQUISITE_DDL:
            connection.execute(text(statement))
            connection.commit()
    if _schema_names(engine, "tables") != CONTROLLED_PREREQUISITE_TABLES:
        raise RuntimeError("controlled V3 prerequisite inventory drifted")


def _statuses(results: Iterable[V3MigrationResult]) -> tuple[str, ...]:
    return tuple(item.status for item in results)


def _final_trigger_contract() -> frozenset[str]:
    triggers: set[str] = set()
    for migrations in (V2_MIGRATIONS, MIGRATIONS):
        for migration in migrations:
            for statement in migration["statements"]:
                dropped = _DROP_TRIGGER_RE.match(str(statement))
                created = _CREATE_TRIGGER_RE.match(str(statement))
                if dropped is not None:
                    triggers.discard(dropped.group(1).lower())
                if created is not None:
                    triggers.add(created.group(1).lower())
    return frozenset(triggers)


def _expected_final_tables() -> frozenset[str]:
    v2_tables = _declared_tables(
        (
            V2_MIGRATION_TABLE_DDL,
            *(str(sql) for migration in V2_MIGRATIONS for sql in migration["statements"]),
        )
    )
    return frozenset(
        {
            *v2_tables,
            V2_EVIDENCE_MAINTENANCE_FENCE_TABLE,
            *CONTROLLED_PREREQUISITE_TABLES,
            *FROZEN_V3_TABLES,
            "schema_migration_v3",
            MIGRATION_PROGRESS_TABLE,
        }
    )


def _assert_final_schema(engine: Engine) -> tuple[int, int, int, int]:
    tables = _schema_names(engine, "tables")
    expected_tables = _expected_final_tables()
    if tables != expected_tables:
        raise RuntimeError(
            "V2+V3 final table inventory drifted: "
            + json.dumps(
                {
                    "missing": sorted(expected_tables - tables),
                    "unexpected": sorted(tables - expected_tables),
                },
                sort_keys=True,
            )
        )
    triggers = _schema_names(engine, "triggers")
    expected_triggers = _final_trigger_contract()
    if triggers != expected_triggers:
        raise RuntimeError("V2+V3 final trigger inventory drifted")
    if _schema_names(engine, "routines") or _schema_names(engine, "events"):
        raise RuntimeError("V3 acceptance created routines or scheduled events")

    expected_ledger = _declared_v3_contract()
    with engine.connect() as connection:
        validate_v3_projection_outbox_schema(connection)
        validate_horizon_candidate_ledger_schema(connection)
        validate_forward_exit_allocation_schema(connection)
        ledger = tuple(
            (
                str(row["version"]),
                str(row["checksum"]),
                int(row["statement_count"]),
            )
            for row in connection.execute(
                text(
                    "SELECT version, checksum, statement_count "
                    "FROM schema_migration_v3 ORDER BY version"
                )
            ).mappings()
        )
        progress = tuple(
            (
                str(row["version"]),
                str(row["checksum"]),
                int(row["statement_count"]),
                int(row["completed_statement_count"]),
            )
            for row in connection.execute(
                text(
                    f"SELECT version, checksum, statement_count, "
                    f"completed_statement_count FROM {MIGRATION_PROGRESS_TABLE} "
                    "ORDER BY version"
                )
            ).mappings()
        )
        fence = connection.execute(
            text(
                f"SELECT state FROM {V2_EVIDENCE_MAINTENANCE_FENCE_TABLE} "
                "WHERE fence_name = :fence_name"
            ),
            {"fence_name": V2_EVIDENCE_MAINTENANCE_FENCE_NAME},
        ).scalar()
    if ledger != expected_ledger:
        raise RuntimeError("V3 migration ledger contract drifted")
    expected_progress = tuple((*item, item[2]) for item in expected_ledger)
    if progress != expected_progress:
        raise RuntimeError("V3 migration progress ledger contract drifted")
    if str(fence or "").upper() != V2_EVIDENCE_MAINTENANCE_FENCE_INACTIVE:
        raise RuntimeError("V2 evidence maintenance fence is not INACTIVE")
    return len(tables), len(triggers), len(ledger), len(progress)


def _projection():
    now = datetime(2026, 8, 4, 2, 0, tzinfo=timezone.utc)
    earliest = now - timedelta(minutes=1)
    semantics = {
        "account_id": "v3-acceptance-account",
        "decision_id": "v3-acceptance-decision",
        "instrument_id": "600001.SH",
        "side": OrderSide.BUY,
        "quantity": 200,
        "order_type": OrderType.LIMIT,
        "time_in_force": TimeInForce.DAY,
        "earliest_at": earliest,
        "expires_at": now + timedelta(hours=1),
        "limit_price": Decimal("10.01"),
        "rule_version": "v3-acceptance-rules-v1",
        "fee_profile_version": "v3-acceptance-fees-v1",
        "execution_policy_version": "v3-acceptance-execution-v1",
    }
    intent = ExecutionIntent(
        intent_id="v3-acceptance-intent-1",
        created_at=earliest,
        idempotency_key=execution_intent_idempotency_key(**semantics),
        **semantics,
    )
    state = new_order_state(order_id="v3-acceptance-order-1", intent=intent)
    event_id = "v3-acceptance-order-1-accepted"
    result = ExecutionResult(
        intent_id=state.intent_id,
        order_id=state.order_id,
        event_id=event_id,
        status=OrderStatus.ACCEPTED,
        occurred_at=now,
        received_at=now + timedelta(milliseconds=5),
        source_sequence=1,
        idempotency_key=execution_result_idempotency_key(
            order_id=state.order_id,
            event_id=event_id,
        ),
    )
    receipt = apply_execution_result_with_receipt(state, result)
    binding = bind_v3_execution_plan(
        execution_plan_id="v3-acceptance-plan-1",
        source_intent_id=intent.intent_id,
        source_order_id=state.order_id,
        bound_at=state.created_at,
    )
    return project_execution_result(binding=binding, transition=receipt), now


def _outbox_count(engine: Engine) -> int:
    with engine.connect() as connection:
        return int(
            connection.execute(
                text("SELECT COUNT(*) FROM st_execution_projection_outbox_v2")
            ).scalar()
            or 0
        )


def _assert_outbox_transaction_behavior(
    engine: Engine,
) -> tuple[bool, bool, bool]:
    projection, occurred_at = _projection()
    with engine.connect() as connection:
        transaction = connection.begin()
        result = append_v3_transition_outbox(
            connection,
            projection,
            created_at=occurred_at + timedelta(seconds=1),
        )
        if result.status is not V3ProjectionOutboxAppendStatus.INSERTED:
            raise RuntimeError("V3 rollback probe was not inserted")
        transaction.rollback()
    rollback_absent = _outbox_count(engine) == 0

    with engine.connect() as connection:
        transaction = connection.begin()
        inserted = append_v3_transition_outbox(
            connection,
            projection,
            created_at=occurred_at + timedelta(seconds=2),
        )
        transaction.commit()
    committed_visible = (
        inserted.status is V3ProjectionOutboxAppendStatus.INSERTED
        and _outbox_count(engine) == 1
    )

    with engine.connect() as connection:
        transaction = connection.begin()
        replay = append_v3_transition_outbox(
            connection,
            projection,
            created_at=occurred_at + timedelta(seconds=3),
        )
        transaction.commit()
    idempotent = (
        replay.status is V3ProjectionOutboxAppendStatus.IDEMPOTENT
        and _outbox_count(engine) == 1
    )
    if not (rollback_absent and committed_visible and idempotent):
        raise RuntimeError("V3 outbox transaction behavior failed closed")
    return rollback_absent, committed_visible, idempotent


def _bootstrap_v2(engine: Engine) -> tuple[str, ...]:
    results = tuple(
        run_v2_migrations(engine, allow_execution_evidence=True)
    )
    statuses = tuple(item.status for item in results)
    if len(results) != len(V2_MIGRATIONS) or any(
        status != "applied" for status in statuses
    ):
        raise RuntimeError("fresh V3 acceptance did not apply the real V2 chain")
    return statuses


def _expected_recovery_statuses(fault_version: str) -> tuple[str, ...]:
    """Return statuses for a replay after a committed-DDL fault.

    Migrations before the fault target were already completed; the target and
    every later migration were not ledgered and must be applied by recovery.
    """

    versions = tuple(str(item["version"]) for item in MIGRATIONS)
    try:
        fault_index = versions.index(fault_version)
    except ValueError as exc:
        raise RuntimeError("V3 acceptance fault target is not declared") from exc
    return tuple(
        "exists" if index < fault_index else "applied"
        for index in range(len(versions))
    )


def _horizon_v2_partial_object_counts(
    engine: Engine,
) -> tuple[int, int, int, int]:
    with engine.connect() as connection:
        columns = int(
            connection.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = 'st_horizon_model_artifact_v3' "
                    "AND COLUMN_NAME IN ('artifact_schema_version', "
                    "'model_protocol', 'selection_policy_hash')"
                )
            ).scalar()
            or 0
        )
        checks = int(
            connection.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS "
                    "WHERE CONSTRAINT_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = 'st_horizon_model_artifact_v3' "
                    "AND CONSTRAINT_NAME = "
                    "'chk_v3_horizon_model_protocol_projection' "
                    "AND CONSTRAINT_TYPE = 'CHECK'"
                )
            ).scalar()
            or 0
        )
        indexes = int(
            connection.execute(
                text(
                    "SELECT COUNT(DISTINCT INDEX_NAME) "
                    "FROM information_schema.STATISTICS "
                    "WHERE TABLE_SCHEMA = DATABASE() "
                    "AND TABLE_NAME = 'st_horizon_model_artifact_v3' "
                    "AND INDEX_NAME = 'idx_v3_horizon_model_current_protocol'"
                )
            ).scalar()
            or 0
        )
        triggers = int(
            connection.execute(
                text(
                    "SELECT COUNT(*) FROM information_schema.TRIGGERS "
                    "WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME IN ("
                    "'trg_v3_horizon_model_protocol_v2_bi',"
                    "'trg_v3_horizon_contract_protocol_v2_bi',"
                    "'trg_v3_horizon_outcome_protocol_v2_bi',"
                    "'trg_v3_shadow_release_protocol_v2_bi')"
                )
            ).scalar()
            or 0
        )
    return columns, checks, indexes, triggers


def _horizon_candidate_ledger_partial_object_counts(
    engine: Engine,
) -> tuple[int, int, int, int]:
    with engine.connect() as connection:
        columns = int(connection.execute(text(
            "SELECT COUNT(*) FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() "
            "AND TABLE_NAME = 'st_horizon_model_artifact_v3' "
            "AND COLUMN_NAME IN ('candidate_ledger_schema_version', "
            "'candidate_ledger_content_sha256', "
            "'candidate_ledger_row_count', "
            "'ledger_registration_evidence_hash', "
            "'registration_verification_hash')"
        )).scalar() or 0)
        checks = int(connection.execute(text(
            "SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS tc "
            "JOIN information_schema.CHECK_CONSTRAINTS cc "
            "ON cc.CONSTRAINT_SCHEMA=tc.CONSTRAINT_SCHEMA "
            "AND cc.CONSTRAINT_NAME=tc.CONSTRAINT_NAME "
            "WHERE tc.CONSTRAINT_SCHEMA=DATABASE() "
            "AND tc.TABLE_NAME='st_horizon_model_artifact_v3' "
            "AND tc.CONSTRAINT_NAME="
            "'chk_v3_horizon_model_protocol_projection' "
            "AND cc.CHECK_CLAUSE LIKE '%candidate_ledger_schema_version%'"
        )).scalar() or 0)
        indexes = int(connection.execute(text(
            "SELECT COUNT(DISTINCT INDEX_NAME) "
            "FROM information_schema.STATISTICS "
            "WHERE TABLE_SCHEMA=DATABASE() "
            "AND TABLE_NAME='st_horizon_model_artifact_v3' "
            "AND INDEX_NAME='idx_v3_horizon_model_current_protocol'"
        )).scalar() or 0)
        triggers = int(connection.execute(text(
            "SELECT COUNT(*) FROM information_schema.TRIGGERS "
            "WHERE TRIGGER_SCHEMA=DATABASE() "
            "AND TRIGGER_NAME IN ("
            "'trg_v3_horizon_model_protocol_v2_bi',"
            "'trg_v3_horizon_contract_protocol_v2_bi',"
            "'trg_v3_horizon_outcome_protocol_v2_bi',"
            "'trg_v3_shadow_release_protocol_v2_bi') "
            "AND ACTION_STATEMENT LIKE '%registration_verification_hash%'"
        )).scalar() or 0)
    return columns, checks, indexes, triggers


def run_mysql_acceptance(
    url: str,
    *,
    expected_server_uuid: str,
    mode: str = "serial-replay",
    concurrency: int = 2,
    partial_statement_count: int = 2,
    tls_config: MySQLAcceptanceTLSConfig | None = None,
) -> V3MySQLAcceptanceReport:
    _assert_frozen_contract()
    safe_url = require_dedicated_test_url(url)
    expected_uuid = require_expected_server_uuid(expected_server_uuid)
    if mode not in {
        "serial-replay",
        "concurrent-initial",
        "partial-recovery",
        "horizon-v2-partial-recovery",
        "horizon-v3-ledger-partial-recovery",
    }:
        raise ValueError("unsupported V3 acceptance mode")
    if type(concurrency) is not int or not 2 <= concurrency <= 8:
        raise ValueError("concurrency must be an integer between 2 and 8")
    if mode == "horizon-v2-partial-recovery":
        if (
            type(partial_statement_count) is not int
            or partial_statement_count not in {1, 2, 4, 6, 8}
        ):
            raise ValueError(
                "horizon V2 partial_statement_count must be one of 1, 2, 4, 6, 8"
            )
    elif mode == "horizon-v3-ledger-partial-recovery":
        if (
            type(partial_statement_count) is not int
            or partial_statement_count not in {1, 3, 5, 7}
        ):
            raise ValueError(
                "horizon V3 ledger partial_statement_count must be one of "
                "1, 3, 5, 7"
            )
    elif type(partial_statement_count) is not int or not (
        1 <= partial_statement_count < 4
    ):
        raise ValueError("partial_statement_count must be between 1 and 3")
    database = str(make_url(safe_url).database)
    engine = create_tool_engine(
        safe_url,
        tls_config=tls_config,
        future=True,
        pool_size=max(2, concurrency),
        max_overflow=0,
        pool_pre_ping=True,
    )
    try:
        database, version, server_uuid, version_comment = _preflight_empty_schema(
            engine,
            expected_database=database,
            expected_server_uuid=expected_uuid,
        )
        _bootstrap_controlled_prerequisites(engine)
        v2_statuses = _bootstrap_v2(engine)
        initial_statuses: tuple[str, ...] = ()
        replay_statuses: tuple[str, ...] = ()
        concurrent_statuses: tuple[tuple[str, ...], ...] = ()
        partial_table_count = 0
        partial_migration_version = ""
        partial_horizon_counts = (0, 0, 0, 0)

        if mode == "concurrent-initial":
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [
                    executor.submit(run_v3_migrations, engine)
                    for _ in range(concurrency)
                ]
                runs = tuple(tuple(future.result()) for future in futures)
            concurrent_statuses = tuple(_statuses(run) for run in runs)
            for migration_index in range(len(MIGRATIONS)):
                statuses = tuple(run[migration_index] for run in concurrent_statuses)
                if statuses.count("applied") != 1 or any(
                    status not in {"applied", "exists"} for status in statuses
                ):
                    raise RuntimeError(
                        "V3 concurrent initial migration did not serialize"
                    )
            replay_statuses = _statuses(run_v3_migrations(engine))
        elif mode == "partial-recovery":
            partial_migration_version = V3_PROJECTION_OUTBOX_MIGRATION_VERSION
            hook = V3MigrationAcceptanceFaultHook.after_outbox_ddl_commit(
                partial_statement_count
            )
            try:
                run_v3_migrations(engine, acceptance_fault_hook=hook)
            except V3MigrationAcceptanceFault:
                pass
            else:
                raise RuntimeError("V3 partial acceptance fault did not fire")
            if not hook.triggered:
                raise RuntimeError("V3 partial acceptance fault was not observed")
            with engine.connect() as connection:
                partial_table_count = int(
                    connection.execute(
                        text(
                            "SELECT COUNT(*) FROM information_schema.TABLES "
                            "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ("
                            "'st_execution_projection_outbox_v2',"
                            "'st_execution_projection_worker_checkpoint_v3',"
                            "'st_execution_projection_order_baseline_v3',"
                            "'st_execution_projection_dead_letter_reconciliation_v3'"
                            ")"
                        )
                    ).scalar()
                    or 0
                )
            if partial_table_count != partial_statement_count:
                raise RuntimeError("V3 partial outbox DDL inventory is incorrect")
            initial_statuses = _statuses(run_v3_migrations(engine))
            expected = _expected_recovery_statuses(
                V3_PROJECTION_OUTBOX_MIGRATION_VERSION
            )
            if initial_statuses != expected:
                raise RuntimeError("V3 partial migration did not recover exactly")
            replay_statuses = _statuses(run_v3_migrations(engine))
        elif mode == "horizon-v2-partial-recovery":
            partial_migration_version = HORIZON_PROTOCOL_V2_MIGRATION_VERSION
            hook = (
                V3MigrationAcceptanceFaultHook
                .after_horizon_protocol_v2_ddl_commit(partial_statement_count)
            )
            try:
                run_v3_migrations(engine, acceptance_fault_hook=hook)
            except V3MigrationAcceptanceFault:
                pass
            else:
                raise RuntimeError("V3 horizon V2 acceptance fault did not fire")
            if not hook.triggered:
                raise RuntimeError("V3 horizon V2 acceptance fault was not observed")
            partial_horizon_counts = _horizon_v2_partial_object_counts(engine)
            expected_partial_counts = {
                1: (3, 1, 0, 0),
                2: (3, 1, 1, 0),
                4: (3, 1, 1, 1),
                6: (3, 1, 1, 2),
                8: (3, 1, 1, 3),
            }[partial_statement_count]
            if partial_horizon_counts != expected_partial_counts:
                raise RuntimeError(
                    "V3 horizon V2 partial DDL inventory is incorrect"
                )
            initial_statuses = _statuses(run_v3_migrations(engine))
            expected = _expected_recovery_statuses(
                HORIZON_PROTOCOL_V2_MIGRATION_VERSION
            )
            if initial_statuses != expected:
                raise RuntimeError(
                    "V3 horizon V2 partial migration did not recover exactly"
                )
            replay_statuses = _statuses(run_v3_migrations(engine))
        elif mode == "horizon-v3-ledger-partial-recovery":
            partial_migration_version = (
                HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION
            )
            hook = (
                V3MigrationAcceptanceFaultHook
                .after_horizon_candidate_ledger_ddl_commit(
                    partial_statement_count
                )
            )
            try:
                run_v3_migrations(engine, acceptance_fault_hook=hook)
            except V3MigrationAcceptanceFault:
                pass
            else:
                raise RuntimeError(
                    "V3 horizon candidate-ledger acceptance fault did not fire"
                )
            if not hook.triggered:
                raise RuntimeError(
                    "V3 horizon candidate-ledger acceptance fault was not observed"
                )
            partial_horizon_counts = (
                _horizon_candidate_ledger_partial_object_counts(engine)
            )
            expected_partial_counts = {
                1: (5, 1, 1, 0),
                3: (5, 1, 1, 1),
                5: (5, 1, 1, 2),
                7: (5, 1, 1, 3),
            }[partial_statement_count]
            if partial_horizon_counts != expected_partial_counts:
                raise RuntimeError(
                    "V3 horizon candidate-ledger partial DDL inventory is "
                    "incorrect"
                )
            initial_statuses = _statuses(run_v3_migrations(engine))
            expected = _expected_recovery_statuses(
                HORIZON_CANDIDATE_LEDGER_MIGRATION_VERSION
            )
            if initial_statuses != expected:
                raise RuntimeError(
                    "V3 horizon candidate-ledger migration did not recover "
                    "exactly"
                )
            replay_statuses = _statuses(run_v3_migrations(engine))
        else:
            initial_statuses = _statuses(run_v3_migrations(engine))
            replay_statuses = _statuses(run_v3_migrations(engine))
            if any(status != "applied" for status in initial_statuses):
                raise RuntimeError("fresh V3 migration did not apply every version")

        if any(status != "exists" for status in replay_statuses):
            raise RuntimeError("V3 migration replay was not idempotent")
        table_count, trigger_count, ledger_count, progress_count = (
            _assert_final_schema(engine)
        )
        rollback_absent, committed_visible, idempotent = (
            _assert_outbox_transaction_behavior(engine)
        )
        _identity(
            engine,
            expected_database=database,
            expected_server_uuid=expected_uuid,
        )
    finally:
        engine.dispose()

    if (
        OUTBOX_RUNTIME_ENABLED is not False
        or legacy_guard.PRODUCTION_ACTIVATION_ALLOWED is not False
        or outbox_schema.PRODUCTION_ACTIVATION_ALLOWED is not False
    ):
        raise RuntimeError("V3 outbox runtime/production guard is unexpectedly open")
    return V3MySQLAcceptanceReport(
        mode=mode,
        database=database,
        server_version=version,
        server_version_comment=version_comment,
        server_uuid=server_uuid,
        least_privilege_attested=True,
        started_empty=True,
        controlled_prerequisites=tuple(sorted(CONTROLLED_PREREQUISITE_TABLES)),
        v2_migration=v2_statuses,
        initial_v3_migration=initial_statuses,
        replay_v3_migration=replay_statuses,
        concurrent_v3_migrations=concurrent_statuses,
        partial_migration_version=partial_migration_version,
        partial_committed_statement_count=(
            partial_statement_count
            if mode in {
                "partial-recovery",
                "horizon-v2-partial-recovery",
                "horizon-v3-ledger-partial-recovery",
            }
            else 0
        ),
        partial_outbox_table_count=partial_table_count,
        partial_horizon_column_count=partial_horizon_counts[0],
        partial_horizon_check_count=partial_horizon_counts[1],
        partial_horizon_index_count=partial_horizon_counts[2],
        partial_horizon_trigger_count=partial_horizon_counts[3],
        observed_table_count=table_count,
        observed_trigger_count=trigger_count,
        migration_ledger_rows=ledger_count,
        migration_progress_rows=progress_count,
        schema_gate_passed=True,
        rollback_absent=rollback_absent,
        committed_append_visible=committed_visible,
        idempotent_append_verified=idempotent,
        outbox_runtime_enabled=False,
        production_activation_allowed=False,
        actionable_output_allowed=False,
        worker_activation_allowed=False,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-env", default=DEFAULT_URL_ENV)
    parser.add_argument("--server-uuid-env", default=DEFAULT_SERVER_UUID_ENV)
    parser.add_argument(
        "--ssl-ca-env",
        default=DEFAULT_SSL_CA_ENV,
        help="dedicated V3 TEST/CI variable containing the SSL CA file",
    )
    parser.add_argument(
        "--mode",
        choices=(
            "serial-replay",
            "concurrent-initial",
            "partial-recovery",
            "horizon-v2-partial-recovery",
            "horizon-v3-ledger-partial-recovery",
        ),
        default="serial-replay",
    )
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--partial-statement-count", type=int, default=2)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    report = run_mysql_acceptance(
        resolve_test_url(args.url_env),
        expected_server_uuid=resolve_server_uuid(args.server_uuid_env),
        mode=args.mode,
        concurrency=args.concurrency,
        partial_statement_count=args.partial_statement_count,
        tls_config=resolve_mysql_acceptance_tls_config(
            "V3",
            args.ssl_ca_env,
        ),
    )
    print(json.dumps(report.to_dict(), ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
