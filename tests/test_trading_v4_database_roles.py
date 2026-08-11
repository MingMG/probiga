from __future__ import annotations

import ast
from collections.abc import Mapping
from pathlib import Path
import re

import pytest

from server.integrations.v4_database_roles import (
    ROLE_MANIFEST_HASH,
    V4RuntimeDatabaseRole,
    V4RuntimeRoleContractError,
    audit_current_user_role,
    render_mysql57_grant_plan,
    role_column_grants,
    role_grants,
)


class _Result:
    def __init__(self, rows=(), first=None):
        self._rows = tuple(rows)
        self._first = first

    def mappings(self):
        return self

    def __iter__(self):
        return iter(self._rows)

    def first(self):
        return self._first

    def scalars(self):
        return iter(self._rows)


class _Connection:
    def __init__(
        self,
        role: V4RuntimeDatabaseRole,
        *,
        extra_global: str | None = None,
        cross_schema: bool = False,
        grantable: bool = False,
        column_privilege: bool = False,
        missing_column_privilege: bool = False,
        routine_privilege: bool = False,
        proxy_privilege: bool = False,
        missing_table: bool = False,
        extra_physical_table: bool = False,
        view_target: bool = False,
    ):
        self.role = role
        self.extra_global = extra_global
        self.cross_schema = cross_schema
        self.grantable = grantable
        self.column_privilege = column_privilege
        self.missing_column_privilege = missing_column_privilege
        self.routine_privilege = routine_privilege
        self.proxy_privilege = proxy_privilege
        self.missing_table = missing_table
        self.extra_physical_table = extra_physical_table
        self.view_target = view_target
        self.inventory_sql = ""

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        if sql.startswith("SELECT DATABASE()"):
            return _Result(
                first={
                    "database_name": "pb_v4_test_roles",
                    "authenticated_user": f"{self.role.value}@127.0.0.1",
                }
            )
        if sql.startswith("SHOW GRANTS"):
            rows = [
                f"GRANT USAGE ON *.* TO '{self.role.value}'@'127.0.0.1'"
            ]
            if self.proxy_privilege:
                rows.append(
                    "GRANT PROXY ON 'admin'@'127.0.0.1' TO "
                    f"'{self.role.value}'@'127.0.0.1'"
                )
            if self.routine_privilege:
                rows.append(
                    "GRANT EXECUTE ON PROCEDURE "
                    "`pb_v4_test_roles`.`unsafe_writer` TO "
                    f"'{self.role.value}'@'127.0.0.1'"
                )
            return _Result(rows)
        if "USER_PRIVILEGES" in sql:
            values = ["USAGE"]
            if self.extra_global:
                values.append(self.extra_global)
            return _Result(
                {
                    "PRIVILEGE_TYPE": value,
                    "IS_GRANTABLE": "YES" if self.grantable else "NO",
                }
                for value in values
            )
        if "SCHEMA_PRIVILEGES" in sql:
            return _Result()
        if "COLUMN_PRIVILEGES" in sql:
            rows = [
                {
                    "TABLE_SCHEMA": "pb_v4_test_roles",
                    "TABLE_NAME": table,
                    "COLUMN_NAME": column,
                    "PRIVILEGE_TYPE": privilege,
                    "IS_GRANTABLE": "NO",
                }
                for table, columns in role_column_grants(self.role).items()
                for column, privileges in columns.items()
                for privilege in privileges
            ]
            if self.missing_column_privilege and rows:
                rows.pop()
            if self.column_privilege:
                rows.append(
                    {
                        "TABLE_SCHEMA": "pb_v4_test_roles",
                        "TABLE_NAME": "st_decision_run_v4",
                        "COLUMN_NAME": "status",
                        "PRIVILEGE_TYPE": "UPDATE",
                        "IS_GRANTABLE": "NO",
                    }
                )
            return _Result(rows)
        if "TABLE_PRIVILEGES" in sql:
            rows = []
            for table, privileges in role_grants(self.role).items():
                for privilege in privileges:
                    rows.append(
                        {
                            "TABLE_SCHEMA": (
                                "other" if self.cross_schema and not rows
                                else "pb_v4_test_roles"
                            ),
                            "TABLE_NAME": table,
                            "PRIVILEGE_TYPE": privilege,
                            "IS_GRANTABLE": "NO",
                        }
                    )
            return _Result(rows)
        if "INFORMATION_SCHEMA.TABLES" in sql.upper():
            self.inventory_sql = sql
            available = set(role_grants(self.role)) | set(
                role_column_grants(self.role)
            )
            if self.extra_physical_table:
                available.add("unrelated_ungranted_table_v2")
            requested = {
                value
                for key, value in (parameters or {}).items()
                if str(key).startswith("table_")
            }
            tables = sorted(available & requested)
            if self.missing_table:
                tables.pop()
            return _Result(
                {
                    "TABLE_NAME": table,
                    "TABLE_TYPE": (
                        "VIEW"
                        if self.view_target and table == tables[0]
                        else "BASE TABLE"
                    ),
                }
                for table in tables
            )
        raise AssertionError(sql)


def _principals() -> Mapping[V4RuntimeDatabaseRole, tuple[str, str]]:
    return {
        role: (f"test_{role.value}", "127.0.0.1")
        for role in V4RuntimeDatabaseRole
    }


def test_manifest_has_five_exact_non_admin_runtime_roles():
    assert len(ROLE_MANIFEST_HASH) == 64
    assert tuple(role.value for role in V4RuntimeDatabaseRole) == (
        "v4_predictor",
        "v4_outbox_worker",
        "v2_executor",
        "v4_evaluator",
        "v4_api_reader",
    )
    for role in V4RuntimeDatabaseRole:
        grants = role_grants(role)
        assert grants
        assert all(privileges <= {"SELECT", "INSERT", "UPDATE", "DELETE"} for privileges in grants.values())
    assert all(
        privileges == {"SELECT"}
        for privileges in role_grants(V4RuntimeDatabaseRole.EVALUATOR).values()
    )
    assert all(
        privileges == {"SELECT"}
        for privileges in role_grants(V4RuntimeDatabaseRole.API_READER).values()
    )
    assert {
        role: len(role_grants(role)) for role in V4RuntimeDatabaseRole
    } == {
        V4RuntimeDatabaseRole.PREDICTOR: 17,
        V4RuntimeDatabaseRole.OUTBOX_WORKER: 10,
        V4RuntimeDatabaseRole.V2_EXECUTOR: 37,
        V4RuntimeDatabaseRole.EVALUATOR: 24,
        V4RuntimeDatabaseRole.API_READER: 10,
    }


def test_predictor_has_no_v3_grant_and_outbox_worker_cannot_write_v2_facts():
    predictor = role_grants(V4RuntimeDatabaseRole.PREDICTOR)
    assert not any(table.endswith("_v3") for table in predictor)
    assert {
        table: predictor[table]
        for table in (
            "sm_stock_kline",
            "sm_stock_minute",
            "st_news_flash",
            "si_notice_eastmoney",
            "si_stock_finance",
            "qmt_concept_member_snapshot",
        )
    } == {
        table: {"SELECT"}
        for table in (
            "sm_stock_kline",
            "sm_stock_minute",
            "st_news_flash",
            "si_notice_eastmoney",
            "si_stock_finance",
            "qmt_concept_member_snapshot",
        )
    }
    for table in (
        "st_data_source_certification_v4",
        "st_factor_definition_v4",
    ):
        assert predictor[table] == {"SELECT"}
        assert role_grants(V4RuntimeDatabaseRole.EVALUATOR)[table] == {
            "SELECT"
        }
        assert role_grants(V4RuntimeDatabaseRole.API_READER)[table] == {
            "SELECT"
        }
    assert predictor["st_entity_feature_snapshot_v4"] == {
        "SELECT",
        "INSERT",
    }
    assert role_grants(V4RuntimeDatabaseRole.EVALUATOR)[
        "st_entity_feature_snapshot_v4"
    ] == {"SELECT"}
    assert role_grants(V4RuntimeDatabaseRole.API_READER)[
        "st_entity_feature_snapshot_v4"
    ] == {"SELECT"}
    outbox = role_grants(V4RuntimeDatabaseRole.OUTBOX_WORKER)
    for forbidden in (
        "st_fill_v2",
        "st_position_lot_v2",
        "st_cash_ledger_v2",
        "st_risk_decision_v2",
    ):
        assert forbidden not in outbox
    assert outbox["st_order_v2"] == {"SELECT"}
    assert outbox["st_trade_intent_v2"] == {"SELECT"}
    canonical_v2_facts = {
        "st_trade_account_v2",
        "st_trade_intent_v2",
        "st_risk_decision_v2",
        "st_order_v2",
        "st_fill_v2",
        "st_cash_ledger_v2",
        "st_position_lot_v2",
        "st_trade_event_v2",
        "st_quote_event_v2",
    }
    for table in canonical_v2_facts & set(outbox):
        assert outbox[table] <= {"SELECT"}, (table, outbox[table])


def test_outbox_worker_covers_exact_projection_worker_and_subscriber_sql():
    root = Path(__file__).resolve().parents[1]
    paths = (
        root / "server/integrations/v3_execution_projection_outbox/worker.py",
        root / "server/integrations/v3_execution_projection/subscriber.py",
    )
    string_literals = tuple(
        node.value
        for path in paths
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8")))
        if isinstance(node, ast.Constant) and type(node.value) is str
    )
    operation_patterns = {
        "SELECT": re.compile(
            r"\b(?:FROM|JOIN)\s+`?([a-z][a-z0-9_]*)`?",
            re.IGNORECASE,
        ),
        "INSERT": re.compile(
            r"\bINSERT(?:\s+IGNORE)?\s+INTO\s+`?([a-z][a-z0-9_]*)`?",
            re.IGNORECASE,
        ),
        "UPDATE": re.compile(
            r"\bUPDATE\s+`?([a-z][a-z0-9_]*)`?",
            re.IGNORECASE,
        ),
        "DELETE": re.compile(
            r"\bDELETE\s+FROM\s+`?([a-z][a-z0-9_]*)`?",
            re.IGNORECASE,
        ),
    }
    physical_table = re.compile(
        r"\b(?:st|si|sm)_[a-z0-9_]+(?:_v[234])?\b",
        re.IGNORECASE,
    )
    observed: dict[str, set[str]] = {}
    observed_update_columns: dict[str, set[str]] = {}
    update_statement = re.compile(
        r"\bUPDATE\s+`?([a-z][a-z0-9_]*)`?\s+SET\s+(.*?)\s+WHERE\b",
        re.IGNORECASE | re.DOTALL,
    )
    update_assignment = re.compile(
        r"(?:^|,)\s*`?([a-z][a-z0-9_]*)`?\s*=",
        re.IGNORECASE,
    )
    for literal in string_literals:
        referenced = {
            match.group(0).lower()
            for match in physical_table.finditer(literal)
        }
        for privilege, pattern in operation_patterns.items():
            for table in pattern.findall(literal):
                normalized = table.lower()
                if normalized in referenced:
                    observed.setdefault(normalized, set()).add(privilege)
        for table, set_clause in update_statement.findall(literal):
            normalized = table.lower()
            if normalized in referenced:
                observed_update_columns.setdefault(normalized, set()).update(
                    column.lower()
                    for column in update_assignment.findall(set_clause)
                )

    expected = {
        "st_execution_projection_outbox_v2": {"SELECT", "UPDATE"},
        "st_execution_projection_worker_checkpoint_v3": {
            "SELECT",
            "INSERT",
            "UPDATE",
        },
        "st_execution_projection_order_baseline_v3": {"SELECT", "INSERT"},
        "st_execution_projection_dead_letter_reconciliation_v3": {
            "SELECT",
            "INSERT",
        },
        "st_trade_intent_v2": {"SELECT"},
        "st_order_v2": {"SELECT"},
        "st_execution_plan_v3": {"SELECT", "UPDATE"},
        "st_execution_plan_binding_v3": {"SELECT", "INSERT"},
        "st_execution_projection_head_v3": {"SELECT", "INSERT", "UPDATE"},
        "st_execution_projection_inbox_v3": {"SELECT", "INSERT"},
    }
    assert observed == expected
    expected_update_columns = {
        "st_execution_projection_outbox_v2": {
            "attempt_count",
            "available_at",
            "last_error",
            "lease_owner",
            "lease_token",
            "lease_until",
            "published_at",
            "status",
            "updated_at",
        },
        "st_execution_projection_worker_checkpoint_v3": {
            "last_outbox_id",
            "last_outbox_sequence",
            "last_projection_id",
            "updated_at",
        },
        "st_execution_plan_v3": {"state", "updated_at"},
        "st_execution_projection_head_v3": {
            "last_payload_hash",
            "last_plan_state",
            "last_projection_id",
            "last_source_sequence",
            "updated_at",
        },
    }
    assert observed_update_columns == expected_update_columns

    expected_table_grants = {
        table: frozenset(privileges - {"UPDATE"})
        for table, privileges in expected.items()
    }
    assert dict(role_grants(V4RuntimeDatabaseRole.OUTBOX_WORKER)) == (
        expected_table_grants
    )
    expected_column_grants = {
        table: {
            column: frozenset({"UPDATE"}) for column in sorted(columns)
        }
        for table, columns in expected_update_columns.items()
    }
    actual_column_grants = {
        table: dict(columns)
        for table, columns in role_column_grants(
            V4RuntimeDatabaseRole.OUTBOX_WORKER
        ).items()
    }
    assert actual_column_grants == expected_column_grants


def test_v2_executor_covers_real_prepared_commit_and_mechanical_sql_tables():
    root = Path(__file__).resolve().parents[1]
    paths = (
        root / "server/trading_v2/execution.py",
        root / "server/integrations/v2_execution_evidence_writer/writer.py",
        root / "server/integrations/v2_accounting_evidence_writer/writer.py",
        root / "server/integrations/v2_execution_evidence_authority/verifier.py",
        root / "server/integrations/v3_execution_projection_outbox/outbox.py",
    )
    sources = {
        path: path.read_text(encoding="utf-8") for path in paths
    }
    executor = role_grants(V4RuntimeDatabaseRole.V2_EXECUTOR)
    physical_table = re.compile(
        r"\b(?:st|si|sm)_[a-z0-9_]+(?:_v[234])?\b",
        re.IGNORECASE,
    )
    referenced = {
        match.group(0).lower()
        for source in sources.values()
        for match in physical_table.finditer(source)
    }
    # The only deliberately excluded table belongs to the legacy post-sync
    # branch, which execution.py skips whenever canonical_cutover is active.
    referenced.remove("st_execution_plan_v3")
    assert referenced <= set(executor)

    operation_patterns = {
        "SELECT": re.compile(
            r"\b(?:FROM|JOIN)\s+`?([a-z][a-z0-9_]*)`?",
            re.IGNORECASE,
        ),
        "INSERT": re.compile(
            r"\bINSERT(?:\s+IGNORE)?\s+INTO\s+`?([a-z][a-z0-9_]*)`?",
            re.IGNORECASE,
        ),
        "UPDATE": re.compile(
            r"\bUPDATE\s+`?([a-z][a-z0-9_]*)`?",
            re.IGNORECASE,
        ),
        "DELETE": re.compile(
            r"\bDELETE\s+FROM\s+`?([a-z][a-z0-9_]*)`?",
            re.IGNORECASE,
        ),
    }
    for privilege, pattern in operation_patterns.items():
        for source in sources.values():
            for table in pattern.findall(source):
                table = table.lower()
                if table == "st_execution_plan_v3":
                    continue
                if table in referenced:
                    assert privilege in executor[table], (
                        table,
                        privilege,
                    )

    # These writers pass literal table names into generic INSERT helpers, so
    # their write requirement is not visible as an inline ``INSERT INTO``.
    generic_append_tables = {
        "st_market_calendar_evidence_v2",
        "st_quote_receipt_evidence_v2",
        "st_fill_execution_evidence_v2",
        "st_cash_event_binding_v2",
        "st_order_transition_v2",
        "st_fill_accounting_outcome_v2",
        "st_lot_transition_evidence_v2",
        "st_fill_accounting_outcome_finalization_v2",
    }
    assert all("INSERT" in executor[table] for table in generic_append_tables)
    assert executor["st_execution_projection_order_baseline_v3"] == {"SELECT"}
    assert "st_execution_projection_order_baseline_v2" not in executor
    for table in (
        "st_execution_authority_trust_key_v2",
        "st_execution_authority_receipt_v2",
        "st_execution_authority_key_revocation_v2",
        "st_execution_authority_receipt_revocation_v2",
    ):
        assert executor[table] == {"SELECT"}
    assert executor["st_execution_authority_attestation_v2"] == {
        "SELECT",
        "INSERT",
    }


def test_password_free_mysql57_plan_revokes_before_exact_table_grants():
    plan = render_mysql57_grant_plan(
        database="pb_v4_test_roles",
        principals=_principals(),
    )
    assert len([item for item in plan if item.startswith("REVOKE ALL")]) == 5
    assert len([item for item in plan if item.startswith("DELETE FROM mysql.proxies_priv")]) == 5
    assert plan[5] == "FLUSH PRIVILEGES"
    assert all("IDENTIFIED" not in item and "PASSWORD" not in item for item in plan)
    assert all("CREATE" not in item and "ALTER" not in item and "TRIGGER" not in item for item in plan)
    assert any(
        "`pb_v4_test_roles`.`st_fill_v2`" in item
        and "'test_v2_executor'@'127.0.0.1'" in item
        for item in plan
    )
    outbox_principal = "'test_v4_outbox_worker'@'127.0.0.1'"
    outbox_column_updates = {
        item
        for item in plan
        if item.startswith("GRANT UPDATE (") and item.endswith(outbox_principal)
    }
    assert outbox_column_updates == {
        "GRANT UPDATE (`attempt_count`, `available_at`, `last_error`, "
        "`lease_owner`, `lease_token`, `lease_until`, `published_at`, "
        "`status`, `updated_at`) ON `pb_v4_test_roles`."
        "`st_execution_projection_outbox_v2` TO " + outbox_principal,
        "GRANT UPDATE (`last_outbox_id`, `last_outbox_sequence`, "
        "`last_projection_id`, `updated_at`) ON `pb_v4_test_roles`."
        "`st_execution_projection_worker_checkpoint_v3` TO "
        + outbox_principal,
        "GRANT UPDATE (`state`, `updated_at`) ON `pb_v4_test_roles`."
        "`st_execution_plan_v3` TO " + outbox_principal,
        "GRANT UPDATE (`last_payload_hash`, `last_plan_state`, "
        "`last_projection_id`, `last_source_sequence`, `updated_at`) ON "
        "`pb_v4_test_roles`.`st_execution_projection_head_v3` TO "
        + outbox_principal,
    }
    assert not any(
        item.startswith("GRANT UPDATE ON ") and item.endswith(outbox_principal)
        for item in plan
    )
    with pytest.raises(V4RuntimeRoleContractError, match="safe identifier"):
        render_mysql57_grant_plan(
            database="prod; DROP DATABASE prod",
            principals=_principals(),
        )
    duplicated = dict(_principals())
    duplicated[V4RuntimeDatabaseRole.API_READER] = duplicated[
        V4RuntimeDatabaseRole.EVALUATOR
    ]
    with pytest.raises(V4RuntimeRoleContractError, match="distinct"):
        render_mysql57_grant_plan(
            database="pb_v4_test_roles",
            principals=duplicated,
        )


@pytest.mark.parametrize("role", tuple(V4RuntimeDatabaseRole))
def test_effective_runtime_grants_must_exactly_match(role):
    report = audit_current_user_role(
        _Connection(role),
        role=role,
        expected_database="pb_v4_test_roles",
    )
    assert report.role is role
    assert report.table_grants == role_grants(role)
    assert report.column_grants == role_column_grants(role)
    assert report.manifest_hash == ROLE_MANIFEST_HASH
    assert report.production_activation_allowed is False
    assert report.actionable_output_allowed is False
    assert report.grant_options_checked is True
    assert report.column_privileges_checked is True
    assert report.routine_privileges_checked is True
    assert report.proxy_privileges_checked is True
    assert report.physical_tables_checked is True


def test_auditor_rejects_global_and_cross_schema_privileges():
    with pytest.raises(V4RuntimeRoleContractError, match="global"):
        audit_current_user_role(
            _Connection(V4RuntimeDatabaseRole.API_READER, extra_global="FILE"),
            role=V4RuntimeDatabaseRole.API_READER,
            expected_database="pb_v4_test_roles",
        )
    with pytest.raises(V4RuntimeRoleContractError, match="cross-schema"):
        audit_current_user_role(
            _Connection(V4RuntimeDatabaseRole.API_READER, cross_schema=True),
            role=V4RuntimeDatabaseRole.API_READER,
            expected_database="pb_v4_test_roles",
        )


@pytest.mark.parametrize(
    ("option", "message"),
    (
        ({"grantable": True}, "delegable"),
        ({"column_privilege": True}, "column-level"),
        ({"routine_privilege": True}, "routine"),
        ({"proxy_privilege": True}, "PROXY"),
        ({"missing_table": True}, "physical table inventory"),
        ({"view_target": True}, "physical base tables"),
    ),
)
def test_auditor_rejects_non_table_privilege_and_inventory_bypasses(
    option,
    message,
):
    with pytest.raises(V4RuntimeRoleContractError, match=message):
        audit_current_user_role(
            _Connection(V4RuntimeDatabaseRole.API_READER, **option),
            role=V4RuntimeDatabaseRole.API_READER,
            expected_database="pb_v4_test_roles",
        )


def test_auditor_rejects_a_missing_declared_column_privilege():
    with pytest.raises(V4RuntimeRoleContractError, match="column-level"):
        audit_current_user_role(
            _Connection(
                V4RuntimeDatabaseRole.OUTBOX_WORKER,
                missing_column_privilege=True,
            ),
            role=V4RuntimeDatabaseRole.OUTBOX_WORKER,
            expected_database="pb_v4_test_roles",
        )


def test_auditor_ignores_ungranted_database_tables_but_binds_manifest_inventory():
    connection = _Connection(
        V4RuntimeDatabaseRole.API_READER,
        extra_physical_table=True,
    )
    report = audit_current_user_role(
        connection,
        role=V4RuntimeDatabaseRole.API_READER,
        expected_database="pb_v4_test_roles",
    )
    assert report.physical_tables_checked is True
    assert "TABLE_NAME IN" in connection.inventory_sql
