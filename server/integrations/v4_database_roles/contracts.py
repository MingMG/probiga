"""Frozen Stage-3 runtime database-role contract for V2/V3/V4.

MySQL 5.7 has no native roles, so deployment maps each logical identity to a
dedicated user.  This module contains no admin connection and never creates a
user.  It can render a DBA-reviewed grant plan and independently audit the
effective privileges of the currently connected runtime user.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any, Mapping

from sqlalchemy import text


PRODUCTION_ACTIVATION_ALLOWED = False
ACTIONABLE_OUTPUT_ALLOWED = False
ROLE_MANIFEST_VERSION = "v4-stage3-research-roles-v6"

_IDENTIFIER = re.compile(r"^[A-Za-z0-9_]+$")
_PRINCIPAL = re.compile(r"^[A-Za-z0-9_.-]+$")
_ALLOWED_PRIVILEGES = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE"})
_ALLOWED_COLUMN_PRIVILEGES = frozenset({"SELECT", "INSERT", "UPDATE"})


class V4RuntimeDatabaseRole(str, Enum):
    PREDICTOR = "v4_predictor"
    OUTBOX_WORKER = "v4_outbox_worker"
    V2_EXECUTOR = "v2_executor"
    EVALUATOR = "v4_evaluator"
    API_READER = "v4_api_reader"


class V4RuntimeRoleContractError(RuntimeError):
    pass


_V4_TABLES = (
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
)

_V4_RESEARCH_SOURCE_TABLES = (
    # Read-only legacy facts admitted through fixed PIT adapters.  Keeping
    # this list explicit prevents the predictor identity from becoming a
    # general-purpose reader of the business schema.
    "sm_stock_kline",
    "sm_stock_minute",
    "st_news_flash",
    "si_notice_eastmoney",
    "si_stock_finance",
    "qmt_concept_member_snapshot",
)

_V4_RESEARCH_REGISTRY_TABLES = (
    "st_data_source_certification_v4",
    "st_factor_definition_v4",
)

_V4_RESEARCH_SNAPSHOT_TABLES = (
    "st_entity_feature_snapshot_v4",
)

_V3_EVALUATION_TABLES = (
    "st_decision_run_v3",
    "st_alpha_forecast_v3",
    "st_target_portfolio_v3",
    "st_position_state_v3",
    "st_execution_plan_v3",
    "st_counterfactual_v3",
    "st_opportunity_recall_v3",
    "st_tca_v3",
    "st_shadow_portfolio_v3",
    "st_execution_plan_binding_v3",
    "st_execution_projection_head_v3",
    "st_execution_projection_inbox_v3",
)

_V2_EXECUTOR_READ = (
    "schema_migration_v2",
    "schema_migration_v2_maintenance_fence",
    "si_trade_calendar",
    "sm_stock_current",
    "st_fee_profile_v2",
    "st_instrument_rule_v2",
    "st_public_quote_current_v2",
    "st_public_quote_receipt_v2",
    "st_qmt_minute_sync_receipt_v2",
    "st_qmt_realtime_sync_receipt_v2",
    "st_execution_authority_trust_key_v2",
    "st_execution_authority_receipt_v2",
    "st_execution_authority_key_revocation_v2",
    "st_execution_authority_receipt_revocation_v2",
    "st_execution_projection_order_baseline_v3",
)

_V2_EXECUTOR_APPEND = MappingProxyType(
    {
        "st_market_calendar_evidence_v2": frozenset({"SELECT", "INSERT"}),
        "st_quote_event_v2": frozenset({"SELECT", "INSERT"}),
        "st_quote_receipt_evidence_v2": frozenset({"SELECT", "INSERT"}),
        "st_trade_event_v2": frozenset({"INSERT"}),
        "st_execution_authority_attestation_v2": frozenset(
            {"SELECT", "INSERT"}
        ),
        "st_execution_projection_outbox_v2": frozenset(
            {"SELECT", "INSERT"}
        ),
    }
)

_V3_PROJECTION_OUTBOX_WORKER = MappingProxyType(
    {
        # The bridge consumes committed V2 projection events but may not
        # create them; the canonical V2 executor remains the sole producer.
        "st_execution_projection_outbox_v2": frozenset({"SELECT"}),
        "st_execution_projection_worker_checkpoint_v3": frozenset(
            {"SELECT", "INSERT"}
        ),
        "st_execution_projection_order_baseline_v3": frozenset(
            {"SELECT", "INSERT"}
        ),
        "st_execution_projection_dead_letter_reconciliation_v3": frozenset(
            {"SELECT", "INSERT"}
        ),
        # The subscriber projects outbox payloads into V3 read models.  Its
        # only canonical V2 access is read-only identity/binding validation.
        "st_trade_intent_v2": frozenset({"SELECT"}),
        "st_order_v2": frozenset({"SELECT"}),
        "st_execution_plan_v3": frozenset({"SELECT"}),
        "st_execution_plan_binding_v3": frozenset({"SELECT", "INSERT"}),
        "st_execution_projection_head_v3": frozenset(
            {"SELECT", "INSERT"}
        ),
        "st_execution_projection_inbox_v3": frozenset({"SELECT", "INSERT"}),
    }
)

_V3_PROJECTION_OUTBOX_WORKER_COLUMN_GRANTS = MappingProxyType(
    {
        "st_execution_projection_outbox_v2": MappingProxyType(
            {
                column: frozenset({"UPDATE"})
                for column in (
                    "attempt_count",
                    "available_at",
                    "last_error",
                    "lease_owner",
                    "lease_token",
                    "lease_until",
                    "published_at",
                    "status",
                    "updated_at",
                )
            }
        ),
        "st_execution_projection_worker_checkpoint_v3": MappingProxyType(
            {
                column: frozenset({"UPDATE"})
                for column in (
                    "last_outbox_id",
                    "last_outbox_sequence",
                    "last_projection_id",
                    "updated_at",
                )
            }
        ),
        "st_execution_plan_v3": MappingProxyType(
            {
                "state": frozenset({"UPDATE"}),
                "updated_at": frozenset({"UPDATE"}),
            }
        ),
        "st_execution_projection_head_v3": MappingProxyType(
            {
                column: frozenset({"UPDATE"})
                for column in (
                    "last_payload_hash",
                    "last_plan_state",
                    "last_projection_id",
                    "last_source_sequence",
                    "updated_at",
                )
            }
        ),
    }
)

_V2_EXECUTOR_MUTABLE = MappingProxyType(
    {
        "st_trade_account_v2": frozenset({"SELECT", "UPDATE"}),
        "st_trade_intent_v2": frozenset({"SELECT", "INSERT", "UPDATE"}),
        "st_risk_decision_v2": frozenset({"SELECT", "INSERT"}),
        "st_order_v2": frozenset({"SELECT", "INSERT", "UPDATE"}),
        "st_fill_v2": frozenset({"SELECT", "INSERT"}),
        "st_cash_ledger_v2": frozenset({"SELECT", "INSERT"}),
        "st_position_lot_v2": frozenset({"SELECT", "INSERT", "UPDATE"}),
        "st_fill_execution_evidence_v2": frozenset({"SELECT", "INSERT"}),
        "st_cash_event_binding_v2": frozenset({"SELECT", "INSERT"}),
        "st_order_transition_v2": frozenset({"SELECT", "INSERT"}),
        "st_fill_accounting_outcome_v2": frozenset({"SELECT", "INSERT"}),
        "st_lot_transition_evidence_v2": frozenset({"SELECT", "INSERT"}),
        "st_fill_accounting_outcome_finalization_v2": frozenset(
            {"SELECT", "INSERT"}
        ),
    }
)


def _select(*tables: str) -> dict[str, frozenset[str]]:
    return {table: frozenset({"SELECT"}) for table in tables}


_ROLE_GRANTS: Mapping[
    V4RuntimeDatabaseRole,
    Mapping[str, frozenset[str]],
] = MappingProxyType(
    {
        V4RuntimeDatabaseRole.PREDICTOR: MappingProxyType(
            {
                "schema_migration_v4": frozenset({"SELECT"}),
                "st_decision_context_v4": frozenset({"SELECT", "INSERT"}),
                "st_source_watermark_v4": frozenset({"SELECT", "INSERT"}),
                "st_decision_run_v4": frozenset(
                    {"SELECT", "INSERT", "UPDATE"}
                ),
                "st_job_run_v4": frozenset({"SELECT", "INSERT"}),
                "st_decision_channel_head_v4": frozenset(
                    {"SELECT", "INSERT", "UPDATE"}
                ),
                "st_runtime_control_v4": frozenset({"SELECT"}),
                "st_runtime_control_transition_v4": frozenset({"SELECT"}),
                **_select(*_V4_RESEARCH_SOURCE_TABLES),
                **_select(*_V4_RESEARCH_REGISTRY_TABLES),
                **{
                    table: frozenset({"SELECT", "INSERT"})
                    for table in _V4_RESEARCH_SNAPSHOT_TABLES
                },
            }
        ),
        V4RuntimeDatabaseRole.OUTBOX_WORKER: _V3_PROJECTION_OUTBOX_WORKER,
        V4RuntimeDatabaseRole.V2_EXECUTOR: MappingProxyType(
            {
                **_select(*_V2_EXECUTOR_READ),
                **dict(_V2_EXECUTOR_MUTABLE),
                **dict(_V2_EXECUTOR_APPEND),
                "schema_migration_v4": frozenset({"SELECT"}),
                "st_decision_run_v4": frozenset({"SELECT"}),
                "st_decision_channel_head_v4": frozenset({"SELECT"}),
            }
        ),
        V4RuntimeDatabaseRole.EVALUATOR: MappingProxyType(
            _select(*_V4_TABLES, *_V3_EVALUATION_TABLES)
        ),
        V4RuntimeDatabaseRole.API_READER: MappingProxyType(
            _select(
                "schema_migration_v4",
                "st_decision_context_v4",
                "st_source_watermark_v4",
                "st_decision_run_v4",
                "st_decision_channel_head_v4",
                "st_runtime_control_v4",
                "st_runtime_control_transition_v4",
                *_V4_RESEARCH_REGISTRY_TABLES,
                *_V4_RESEARCH_SNAPSHOT_TABLES,
            )
        ),
    }
)

_EMPTY_COLUMN_GRANTS: Mapping[str, Mapping[str, frozenset[str]]] = (
    MappingProxyType({})
)
_ROLE_COLUMN_GRANTS: Mapping[
    V4RuntimeDatabaseRole,
    Mapping[str, Mapping[str, frozenset[str]]],
] = MappingProxyType(
    {
        V4RuntimeDatabaseRole.PREDICTOR: _EMPTY_COLUMN_GRANTS,
        V4RuntimeDatabaseRole.OUTBOX_WORKER: (
            _V3_PROJECTION_OUTBOX_WORKER_COLUMN_GRANTS
        ),
        V4RuntimeDatabaseRole.V2_EXECUTOR: _EMPTY_COLUMN_GRANTS,
        V4RuntimeDatabaseRole.EVALUATOR: _EMPTY_COLUMN_GRANTS,
        V4RuntimeDatabaseRole.API_READER: _EMPTY_COLUMN_GRANTS,
    }
)


def _canonical_manifest() -> dict[str, Any]:
    return {
        "manifest_version": ROLE_MANIFEST_VERSION,
        "roles": {
            role.value: {
                "table_grants": {
                    table: sorted(privileges)
                    for table, privileges in sorted(_ROLE_GRANTS[role].items())
                },
                "column_grants": {
                    table: {
                        column: sorted(privileges)
                        for column, privileges in sorted(columns.items())
                    }
                    for table, columns in sorted(
                        _ROLE_COLUMN_GRANTS[role].items()
                    )
                },
            }
            for role in V4RuntimeDatabaseRole
        },
        "production_activation_allowed": False,
        "actionable_output_allowed": False,
    }


ROLE_MANIFEST_HASH = hashlib.sha256(
    json.dumps(
        _canonical_manifest(),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()


def role_grants(
    role: V4RuntimeDatabaseRole,
) -> Mapping[str, frozenset[str]]:
    if type(role) is not V4RuntimeDatabaseRole:
        raise TypeError("role must be exactly V4RuntimeDatabaseRole")
    return _ROLE_GRANTS[role]


def role_column_grants(
    role: V4RuntimeDatabaseRole,
) -> Mapping[str, Mapping[str, frozenset[str]]]:
    if type(role) is not V4RuntimeDatabaseRole:
        raise TypeError("role must be exactly V4RuntimeDatabaseRole")
    return _ROLE_COLUMN_GRANTS[role]


def _identifier(value: object, name: str) -> str:
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise V4RuntimeRoleContractError(f"{name} is not a safe identifier")
    return value


def _principal(value: object, name: str) -> str:
    if type(value) is not str or _PRINCIPAL.fullmatch(value) is None:
        raise V4RuntimeRoleContractError(f"{name} is not a safe principal")
    maximum = 255 if name.endswith(" host") else 32
    if len(value) > maximum:
        raise V4RuntimeRoleContractError(f"{name} exceeds MySQL 5.7 limits")
    return value


def render_mysql57_grant_plan(
    *,
    database: str,
    principals: Mapping[V4RuntimeDatabaseRole, tuple[str, str]],
) -> tuple[str, ...]:
    """Render a password-free plan for existing users; never execute it."""

    db = _identifier(database, "database")
    if set(principals) != set(V4RuntimeDatabaseRole):
        raise V4RuntimeRoleContractError(
            "principals must bind every and only the five runtime roles"
        )
    normalized_principals: dict[
        V4RuntimeDatabaseRole, tuple[str, str]
    ] = {}
    for role in V4RuntimeDatabaseRole:
        raw_user, raw_host = principals[role]
        normalized_principals[role] = (
            _principal(raw_user, f"{role.value} user"),
            _principal(raw_host, f"{role.value} host"),
        )
    if len(set(normalized_principals.values())) != len(V4RuntimeDatabaseRole):
        raise V4RuntimeRoleContractError(
            "each runtime role must bind a distinct MySQL account"
        )
    statements: list[str] = []
    # MySQL 5.7 leaves mysql.proxies_priv rows intact after REVOKE ALL.
    # The plan is DBA-only and password-free, so remove only rows where one
    # of the five dedicated runtime accounts is the proxy grantee, then make
    # that targeted grant-table change visible before issuing normal GRANTs.
    for role in V4RuntimeDatabaseRole:
        user, host = normalized_principals[role]
        statements.append(
            "DELETE FROM mysql.proxies_priv "
            f"WHERE User = '{user}' AND Host = '{host}'"
        )
    statements.append("FLUSH PRIVILEGES")
    for role in V4RuntimeDatabaseRole:
        user, host = normalized_principals[role]
        principal = f"'{user}'@'{host}'"
        statements.append(
            f"REVOKE ALL PRIVILEGES, GRANT OPTION FROM {principal}"
        )
        for table, privileges in sorted(role_grants(role).items()):
            _identifier(table, f"{role.value} table")
            if not privileges or not privileges <= _ALLOWED_PRIVILEGES:
                raise V4RuntimeRoleContractError(
                    f"unsafe privilege in role manifest: {role.value}.{table}"
                )
            privilege_sql = ", ".join(sorted(privileges))
            statements.append(
                f"GRANT {privilege_sql} ON `{db}`.`{table}` TO {principal}"
            )
        for table, columns in sorted(role_column_grants(role).items()):
            _identifier(table, f"{role.value} column-grant table")
            if not columns:
                raise V4RuntimeRoleContractError(
                    f"empty column-grant table in role manifest: {role.value}.{table}"
                )
            grouped: dict[str, list[str]] = {}
            for column, privileges in sorted(columns.items()):
                _identifier(column, f"{role.value}.{table} column")
                if (
                    not privileges
                    or not privileges <= _ALLOWED_COLUMN_PRIVILEGES
                ):
                    raise V4RuntimeRoleContractError(
                        "unsafe column privilege in role manifest: "
                        f"{role.value}.{table}.{column}"
                    )
                redundant = privileges & role_grants(role).get(
                    table,
                    frozenset(),
                )
                if redundant:
                    raise V4RuntimeRoleContractError(
                        "column privilege duplicates a table privilege: "
                        f"{role.value}.{table}.{column}"
                    )
                for privilege in privileges:
                    grouped.setdefault(privilege, []).append(column)
            for privilege, columns_for_privilege in sorted(grouped.items()):
                column_sql = ", ".join(
                    f"`{column}`" for column in sorted(columns_for_privilege)
                )
                statements.append(
                    f"GRANT {privilege} ({column_sql}) ON "
                    f"`{db}`.`{table}` TO {principal}"
                )
    return tuple(statements)


@dataclass(frozen=True, slots=True)
class V4RuntimeRoleAudit:
    role: V4RuntimeDatabaseRole
    database: str
    current_user: str
    table_grants: Mapping[str, frozenset[str]]
    column_grants: Mapping[str, Mapping[str, frozenset[str]]]
    grant_options_checked: bool
    column_privileges_checked: bool
    routine_privileges_checked: bool
    proxy_privileges_checked: bool
    physical_tables_checked: bool
    manifest_hash: str = ROLE_MANIFEST_HASH
    production_activation_allowed: bool = False
    actionable_output_allowed: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "table_grants",
            MappingProxyType(dict(self.table_grants)),
        )
        object.__setattr__(
            self,
            "column_grants",
            MappingProxyType(
                {
                    table: MappingProxyType(dict(columns))
                    for table, columns in self.column_grants.items()
                }
            ),
        )


def _rows(result: Any) -> tuple[Mapping[str, Any], ...]:
    return tuple(result.mappings())


def _scalar_texts(result: Any) -> tuple[str, ...]:
    try:
        return tuple(str(value) for value in result.scalars())
    except Exception as exc:
        raise V4RuntimeRoleContractError(
            "SHOW GRANTS did not return scalar rows"
        ) from exc


def _grantable(row: Mapping[str, Any], scope: str) -> None:
    value = str(row.get("IS_GRANTABLE") or "").upper()
    if value not in {"YES", "NO"}:
        raise V4RuntimeRoleContractError(
            f"{scope} grantability metadata is invalid"
        )
    if value == "YES":
        raise V4RuntimeRoleContractError(
            f"runtime user has delegable {scope} privileges"
        )


def _mysql_grantee(current_user: str) -> str:
    if current_user.count("@") != 1:
        raise V4RuntimeRoleContractError("CURRENT_USER() is not canonical")
    user, host = current_user.split("@", 1)
    _principal(user, "CURRENT_USER user")
    _principal(host, "CURRENT_USER host")
    return f"'{user}'@'{host}'"


def audit_current_user_role(
    connection: Any,
    *,
    role: V4RuntimeDatabaseRole,
    expected_database: str,
) -> V4RuntimeRoleAudit:
    if connection is None or not callable(getattr(connection, "execute", None)):
        raise V4RuntimeRoleContractError(
            "a SQLAlchemy-like connection is required"
        )
    expected_db = _identifier(expected_database, "expected_database")
    identity = connection.execute(
        text(
            "SELECT DATABASE() AS database_name, "
            "CURRENT_USER() AS authenticated_user"
        )
    ).mappings().first()
    if identity is None or set(identity) != {
        "database_name",
        "authenticated_user",
    }:
        raise V4RuntimeRoleContractError("database identity query is incomplete")
    database = str(identity["database_name"] or "")
    current_user = str(identity["authenticated_user"] or "")
    if database != expected_db:
        raise V4RuntimeRoleContractError("runtime role database identity differs")
    grantee = _mysql_grantee(current_user)
    rendered_grants = _scalar_texts(
        connection.execute(text("SHOW GRANTS FOR CURRENT_USER()"))
    )
    if not rendered_grants:
        raise V4RuntimeRoleContractError("runtime user grants are unavailable")
    for raw_grant in rendered_grants:
        normalized_grant = " ".join(raw_grant.upper().split())
        if normalized_grant.startswith("GRANT PROXY ON "):
            raise V4RuntimeRoleContractError(
                "runtime user has a PROXY privilege"
            )
        if (
            " ON PROCEDURE " in normalized_grant
            or " ON FUNCTION " in normalized_grant
        ):
            raise V4RuntimeRoleContractError(
                "runtime user has a routine privilege"
            )
        if " WITH GRANT OPTION" in normalized_grant:
            raise V4RuntimeRoleContractError(
                "runtime user has a delegable privilege"
            )
    global_rows = _rows(
        connection.execute(
            text(
                "SELECT PRIVILEGE_TYPE, IS_GRANTABLE "
                "FROM information_schema.USER_PRIVILEGES "
                "WHERE GRANTEE = :grantee ORDER BY PRIVILEGE_TYPE"
            ),
            {"grantee": grantee},
        )
    )
    for row in global_rows:
        _grantable(row, "global")
    global_privileges = {
        str(row["PRIVILEGE_TYPE"]).upper() for row in global_rows
    }
    if global_privileges - {"USAGE"}:
        raise V4RuntimeRoleContractError("runtime user has a global privilege")
    schema_rows = _rows(
        connection.execute(
            text(
                "SELECT TABLE_SCHEMA, PRIVILEGE_TYPE, IS_GRANTABLE "
                "FROM information_schema.SCHEMA_PRIVILEGES "
                "WHERE GRANTEE = :grantee ORDER BY TABLE_SCHEMA, PRIVILEGE_TYPE"
            ),
            {"grantee": grantee},
        )
    )
    for row in schema_rows:
        _grantable(row, "schema")
    if schema_rows:
        raise V4RuntimeRoleContractError("runtime user has a schema-wide privilege")
    observed: dict[str, set[str]] = {}
    table_rows = _rows(
        connection.execute(
            text(
                "SELECT TABLE_SCHEMA, TABLE_NAME, PRIVILEGE_TYPE, "
                "IS_GRANTABLE "
                "FROM information_schema.TABLE_PRIVILEGES "
                "WHERE GRANTEE = :grantee "
                "ORDER BY TABLE_SCHEMA, TABLE_NAME, PRIVILEGE_TYPE"
            ),
            {"grantee": grantee},
        )
    )
    for row in table_rows:
        _grantable(row, "table")
        schema = str(row["TABLE_SCHEMA"])
        if schema != expected_db:
            raise V4RuntimeRoleContractError(
                "runtime user has a cross-schema table privilege"
            )
        table = str(row["TABLE_NAME"])
        privilege = str(row["PRIVILEGE_TYPE"]).upper()
        observed.setdefault(table, set()).add(privilege)
    frozen_observed = {
        table: frozenset(privileges) for table, privileges in observed.items()
    }
    expected = dict(role_grants(role))
    if frozen_observed != expected:
        raise V4RuntimeRoleContractError(
            f"effective table grants differ from role manifest: {role.value}"
        )
    column_rows = _rows(
        connection.execute(
            text(
                "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, "
                "PRIVILEGE_TYPE, IS_GRANTABLE "
                "FROM information_schema.COLUMN_PRIVILEGES "
                "WHERE GRANTEE = :grantee "
                "ORDER BY TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, "
                "PRIVILEGE_TYPE"
            ),
            {"grantee": grantee},
        )
    )
    observed_columns: dict[str, dict[str, set[str]]] = {}
    seen_column_rows: set[tuple[str, str, str]] = set()
    for row in column_rows:
        _grantable(row, "column")
        schema = str(row["TABLE_SCHEMA"])
        if schema != expected_db:
            raise V4RuntimeRoleContractError(
                "runtime user has a cross-schema column privilege"
            )
        table = str(row["TABLE_NAME"])
        column = str(row["COLUMN_NAME"])
        privilege = str(row["PRIVILEGE_TYPE"]).upper()
        row_key = (table, column, privilege)
        if row_key in seen_column_rows:
            raise V4RuntimeRoleContractError(
                "runtime user column privilege metadata contains a duplicate"
            )
        seen_column_rows.add(row_key)
        observed_columns.setdefault(table, {}).setdefault(column, set()).add(
            privilege
        )
    frozen_observed_columns = {
        table: {
            column: frozenset(privileges)
            for column, privileges in columns.items()
        }
        for table, columns in observed_columns.items()
    }
    expected_columns = {
        table: dict(columns)
        for table, columns in role_column_grants(role).items()
    }
    if frozen_observed_columns != expected_columns:
        raise V4RuntimeRoleContractError(
            f"effective column-level grants differ from role manifest: {role.value}"
        )
    expected_tables = tuple(sorted(set(expected) | set(expected_columns)))
    inventory_params: dict[str, str] = {"table_schema": expected_db}
    inventory_placeholders: list[str] = []
    for index, table in enumerate(expected_tables):
        _identifier(table, f"{role.value} table")
        parameter = f"table_{index}"
        inventory_placeholders.append(f":{parameter}")
        inventory_params[parameter] = table
    inventory_rows = _rows(
        connection.execute(
            text(
                "SELECT TABLE_NAME, TABLE_TYPE "
                "FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA = :table_schema AND TABLE_NAME IN ("
                + ", ".join(inventory_placeholders)
                + ") ORDER BY TABLE_NAME"
            ),
            inventory_params,
        )
    )
    inventory: dict[str, str] = {}
    for row in inventory_rows:
        table = str(row["TABLE_NAME"])
        if table in inventory:
            raise V4RuntimeRoleContractError(
                "runtime role table inventory contains a duplicate"
            )
        inventory[table] = str(row["TABLE_TYPE"]).upper()
    if set(inventory) != set(expected_tables):
        raise V4RuntimeRoleContractError(
            f"physical table inventory differs from role manifest: {role.value}"
        )
    if any(table_type != "BASE TABLE" for table_type in inventory.values()):
        raise V4RuntimeRoleContractError(
            "runtime role grant targets must be physical base tables"
        )
    return V4RuntimeRoleAudit(
        role=role,
        database=database,
        current_user=current_user,
        table_grants=frozen_observed,
        column_grants=frozen_observed_columns,
        grant_options_checked=True,
        column_privileges_checked=True,
        routine_privileges_checked=True,
        proxy_privileges_checked=True,
        physical_tables_checked=True,
    )


__all__ = [
    "ACTIONABLE_OUTPUT_ALLOWED",
    "PRODUCTION_ACTIVATION_ALLOWED",
    "ROLE_MANIFEST_HASH",
    "ROLE_MANIFEST_VERSION",
    "V4RuntimeDatabaseRole",
    "V4RuntimeRoleAudit",
    "V4RuntimeRoleContractError",
    "audit_current_user_role",
    "render_mysql57_grant_plan",
    "role_column_grants",
    "role_grants",
]
