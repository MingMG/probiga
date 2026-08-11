from __future__ import annotations

import re

import pytest

from server.db.mysql84_check_constraints import (
    CheckConstraintSpec,
    ExistingCheckConstraint,
    declared_v2_check_constraints,
    materialize_mysql84_check_constraints,
    plan_check_constraints,
)


class _Dialect:
    name = "mysql"


class _Result:
    def __init__(self, rows=()):
        self._rows = tuple(rows)

    def mappings(self):
        return self

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class _FakeConnection:
    dialect = _Dialect()

    def __init__(
        self,
        *,
        version: str = "8.4.11",
        violation: bool = False,
    ) -> None:
        self.version = version
        self.violation = violation
        self.tables = {"st_trade_account_v2"}
        self.constraints: list[ExistingCheckConstraint] = []
        self.statements: list[str] = []
        self.commit_count = 0
        self.rollback_count = 0

    def in_transaction(self):
        return False

    def commit(self):
        self.commit_count += 1

    def rollback(self):
        self.rollback_count += 1

    def execute(self, statement, parameters=None):
        sql = " ".join(str(statement).split())
        self.statements.append(sql)
        if sql.startswith("SELECT @@version AS server_version"):
            return _Result(
                (
                    {
                        "server_version": self.version,
                        "version_comment": "MySQL Community Server - GPL",
                        "server_uuid": "810354d6-9061-11f1-84ae-74d4dd7f8500",
                        "server_port": 33084,
                        "current_schema": "probiga",
                    },
                )
            )
        if "FROM information_schema.TABLES" in sql:
            return _Result(
                tuple({"TABLE_NAME": table} for table in sorted(self.tables))
            )
        if "FROM information_schema.TABLE_CONSTRAINTS" in sql:
            return _Result(
                tuple(
                    {
                        "TABLE_NAME": item.table_name,
                        "CONSTRAINT_NAME": item.constraint_name,
                        "CHECK_CLAUSE": item.expression,
                        "ENFORCED": "YES" if item.enforced else "NO",
                    }
                    for item in self.constraints
                )
            )
        if sql.startswith("ALTER TABLE") and " ADD CONSTRAINT " in sql:
            match = re.fullmatch(
                r"ALTER TABLE `([^`]+)` ADD CONSTRAINT `([^`]+)` "
                r"CHECK \((.*)\) NOT ENFORCED",
                sql,
            )
            assert match is not None
            self.constraints.append(
                ExistingCheckConstraint(
                    table_name=match.group(1),
                    constraint_name=match.group(2),
                    expression=match.group(3),
                    enforced=False,
                )
            )
            return _Result()
        if sql.startswith("ALTER TABLE") and " ALTER CHECK " in sql:
            match = re.fullmatch(
                r"ALTER TABLE `([^`]+)` ALTER CHECK `([^`]+)` ENFORCED",
                sql,
            )
            assert match is not None
            table_name, constraint_name = match.groups()
            self.constraints = [
                ExistingCheckConstraint(
                    table_name=item.table_name,
                    constraint_name=item.constraint_name,
                    expression=item.expression,
                    enforced=(
                        True
                        if item.table_name == table_name
                        and item.constraint_name == constraint_name
                        else item.enforced
                    ),
                )
                for item in self.constraints
            ]
            return _Result()
        if sql.startswith("SELECT COUNT(*) AS `row_count`"):
            aliases = re.findall(r"AS `(violation_\d{3})`", sql)
            row = {"row_count": 1}
            for alias in aliases:
                row[alias] = int(self.violation)
            return _Result((row,))
        raise AssertionError(f"unexpected SQL: {sql}")


def _account_cash_spec() -> CheckConstraintSpec:
    return CheckConstraintSpec(
        migration_version="20260725_001_trading_v2_core",
        table_name="st_trade_account_v2",
        ordinal=2,
        constraint_name="ck84_st_trade_account_v2_002_testcash",
        expression="cash_balance >= 0",
    )


TARGET_IDENTITY = {
    "expected_server_uuid": "810354d6-9061-11f1-84ae-74d4dd7f8500",
    "expected_server_port": 33084,
}


def test_frozen_v2_declarations_yield_complete_stable_check_inventory():
    specs = declared_v2_check_constraints()

    assert len(specs) == 134
    assert len({spec.constraint_name for spec in specs}) == 134
    assert all(len(spec.constraint_name) <= 64 for spec in specs)
    assert {
        (spec.table_name, spec.expression)
        for spec in specs
        if spec.table_name == "st_trade_account_v2"
    } == {
        ("st_trade_account_v2", "initial_cash >= 0"),
        ("st_trade_account_v2", "cash_balance >= 0"),
        ("st_trade_account_v2", "real_trading_enabled = 0"),
    }
    assert not any(
        spec.table_name.startswith("st_") and "_v3" in spec.table_name
        for spec in specs
    )


def test_plan_reuses_equivalent_enforced_constraint_with_server_formatting():
    spec = _account_cash_spec()
    plan = plan_check_constraints(
        (spec,),
        present_tables=(spec.table_name,),
        existing_constraints=(
            ExistingCheckConstraint(
                table_name=spec.table_name,
                constraint_name="st_trade_account_v2_chk_2",
                expression="((`cash_balance` >= 0))",
                enforced=True,
            ),
        ),
    )

    assert plan.actions[0].action == "covered"
    assert plan.actions[0].existing_name == "st_trade_account_v2_chk_2"


def test_plan_normalizes_mysql84_utf8mb4_literal_introducers():
    spec = CheckConstraintSpec(
        migration_version="20260803_001_test",
        table_name="st_example",
        ordinal=1,
        constraint_name="ck84_st_example_001_test",
        expression="status IN ('UNKNOWN', 'READY')",
    )

    plan = plan_check_constraints(
        (spec,),
        present_tables=(spec.table_name,),
        existing_constraints=(
            ExistingCheckConstraint(
                table_name=spec.table_name,
                constraint_name=spec.constraint_name,
                expression=r"(`status` in (_utf8mb4\'UNKNOWN\',_utf8mb4\'READY\'))",
                enforced=False,
            ),
        ),
    )

    assert plan.actions[0].action == "enforce_existing"
    assert plan.actions[0].existing_name == spec.constraint_name


def test_plan_prefers_enforced_equivalent_over_partial_named_duplicate():
    spec = CheckConstraintSpec(
        migration_version="20260803_001_test",
        table_name="st_example",
        ordinal=1,
        constraint_name="ck84_st_example_001_test",
        expression="status IN ('UNKNOWN', 'READY')",
    )

    plan = plan_check_constraints(
        (spec,),
        present_tables=(spec.table_name,),
        existing_constraints=(
            ExistingCheckConstraint(
                table_name=spec.table_name,
                constraint_name=spec.constraint_name,
                expression="status IN ('UNKNOWN', 'READY')",
                enforced=False,
            ),
            ExistingCheckConstraint(
                table_name=spec.table_name,
                constraint_name="st_example_chk_1",
                expression=r"(`status` in (_utf8mb4\'UNKNOWN\',_utf8mb4\'READY\'))",
                enforced=True,
            ),
        ),
    )

    assert plan.actions[0].action == "covered"
    assert plan.actions[0].existing_name == "st_example_chk_1"


def test_plan_normalizes_mysql84_redundant_boolean_operand_parentheses():
    spec = CheckConstraintSpec(
        migration_version="20260803_001_test",
        table_name="st_example",
        ordinal=2,
        constraint_name="ck84_st_example_002_test",
        expression=(
            "authority_status <> 'VERIFIED' OR authority_receipt_hash IS NOT NULL"
        ),
    )

    plan = plan_check_constraints(
        (spec,),
        present_tables=(spec.table_name,),
        existing_constraints=(
            ExistingCheckConstraint(
                table_name=spec.table_name,
                constraint_name="st_example_chk_2",
                expression=(
                    r"((`authority_status` <> _utf8mb4\'VERIFIED\') or "
                    r"(`authority_receipt_hash` is not null))"
                ),
                enforced=True,
            ),
        ),
    )

    assert plan.actions[0].action == "covered"
    assert plan.actions[0].existing_name == "st_example_chk_2"


def test_plan_fails_closed_on_expected_constraint_name_drift():
    spec = _account_cash_spec()

    with pytest.raises(RuntimeError, match="constraint name drift"):
        plan_check_constraints(
            (spec,),
            present_tables=(spec.table_name,),
            existing_constraints=(
                ExistingCheckConstraint(
                    table_name=spec.table_name,
                    constraint_name=spec.constraint_name,
                    expression="cash_balance >= -1",
                    enforced=True,
                ),
            ),
        )


def test_mysql57_is_rejected_before_information_schema_or_ddl():
    connection = _FakeConnection(version="5.7.38-log")

    with pytest.raises(RuntimeError, match="fail-closed"):
        materialize_mysql84_check_constraints(
            connection,
            expected_schema="probiga",
            **TARGET_IDENTITY,
            specs=(_account_cash_spec(),),
        )

    assert not any("information_schema" in sql for sql in connection.statements)
    assert not any(sql.startswith("ALTER TABLE") for sql in connection.statements)


def test_apply_requires_explicit_offline_target_confirmation_before_queries():
    connection = _FakeConnection()

    with pytest.raises(RuntimeError, match="offline confirmation"):
        materialize_mysql84_check_constraints(
            connection,
            expected_schema="probiga",
            **TARGET_IDENTITY,
            apply=True,
            specs=(_account_cash_spec(),),
        )

    assert connection.statements == []


def test_apply_adds_not_enforced_audits_then_enforces_and_is_idempotent():
    connection = _FakeConnection()
    spec = _account_cash_spec()

    first = materialize_mysql84_check_constraints(
        connection,
        expected_schema="probiga",
        **TARGET_IDENTITY,
        apply=True,
        restored_target_offline=True,
        specs=(spec,),
    )
    second = materialize_mysql84_check_constraints(
        connection,
        expected_schema="probiga",
        **TARGET_IDENTITY,
        apply=True,
        restored_target_offline=True,
        specs=(spec,),
    )

    assert first.complete is True
    assert first.added_not_enforced == (spec.constraint_name,)
    assert first.enforced_constraints == (spec.constraint_name,)
    assert second.complete is True
    assert second.added_not_enforced == ()
    assert second.enforced_constraints == ()
    assert sum(" ADD CONSTRAINT " in sql for sql in connection.statements) == 1
    assert sum(" ALTER CHECK " in sql for sql in connection.statements) == 1


def test_historical_violation_leaves_new_constraint_not_enforced_and_blocks():
    connection = _FakeConnection(violation=True)
    spec = _account_cash_spec()

    report = materialize_mysql84_check_constraints(
        connection,
        expected_schema="probiga",
        **TARGET_IDENTITY,
        apply=True,
        restored_target_offline=True,
        specs=(spec,),
    )

    assert report.complete is False
    assert dict(report.violation_counts) == {spec.constraint_name: 1}
    assert report.added_not_enforced == (spec.constraint_name,)
    assert report.enforced_constraints == ()
    assert connection.constraints[0].enforced is False
    assert not any(" ALTER CHECK " in sql for sql in connection.statements)


def test_apply_rejects_server_uuid_mismatch_before_information_schema_or_ddl():
    connection = _FakeConnection()

    with pytest.raises(RuntimeError, match="UUID mismatch"):
        materialize_mysql84_check_constraints(
            connection,
            expected_schema="probiga",
            expected_server_uuid="11111111-1111-1111-1111-111111111111",
            expected_server_port=33084,
            apply=True,
            restored_target_offline=True,
            specs=(_account_cash_spec(),),
        )

    assert not any("information_schema" in sql for sql in connection.statements)
    assert not any(sql.startswith("ALTER TABLE") for sql in connection.statements)


def test_apply_requires_explicit_server_identity_before_first_query():
    connection = _FakeConnection()

    with pytest.raises(RuntimeError, match="expected server UUID and port"):
        materialize_mysql84_check_constraints(
            connection,
            expected_schema="probiga",
            apply=True,
            restored_target_offline=True,
            specs=(_account_cash_spec(),),
        )

    assert connection.statements == []
