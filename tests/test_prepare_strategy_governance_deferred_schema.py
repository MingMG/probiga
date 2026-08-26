from __future__ import annotations

import pytest

from server.engine import strategy_funding_checkpoint as funding
from server.engine import strategy_governance as governance
from tools import prepare_strategy_governance_deferred_schema as deferred


class _Rows:
    def __init__(self, rows=()):
        self._rows = [dict(row) for row in rows]

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)

    def scalar(self):
        if not self._rows:
            return 0
        return next(iter(self._rows[0].values()))


def _append_trigger_row(name: str) -> dict:
    timing, event, table_name, body = (
        governance.GOVERNANCE_APPEND_ONLY_TRIGGER_CONTRACTS[name]
    )
    return {
        "trigger_name": name,
        "action_timing": timing,
        "event_manipulation": event,
        "event_object_table": table_name,
        "action_orientation": "ROW",
        "action_statement": body,
    }


def test_deferred_trigger_inventory_accepts_exact_subset_and_counts_gap():
    name = sorted(
        governance.EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES
    )[0]

    class Connection:
        def execute(self, statement, _params=None):
            sql = str(statement)
            if "EVENT_OBJECT_TABLE='st_strategy_metric_input'" in sql:
                return _Rows()
            return _Rows([_append_trigger_row(name)])

    detail = governance.validate_deferred_governance_trigger_inventory(
        Connection()
    )

    assert detail["expected_trigger_count"] == 40
    assert detail["installed_trigger_count"] == 1
    assert detail["missing_trigger_count"] == 39
    assert name in detail["installed_trigger_names"]
    assert detail["trigger_installation_asserted"] is False
    assert detail["database_triggers_installed"] is False


def test_deferred_trigger_inventory_rejects_installed_metadata_drift():
    name = sorted(
        governance.EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES
    )[0]
    drifted = _append_trigger_row(name)
    drifted["action_timing"] = "AFTER"

    class Connection:
        def execute(self, statement, _params=None):
            if "EVENT_OBJECT_TABLE='st_strategy_metric_input'" in str(statement):
                return _Rows()
            return _Rows([drifted])

    with pytest.raises(RuntimeError, match="元数据漂移"):
        governance.validate_deferred_governance_trigger_inventory(Connection())


def test_deferred_trigger_inventory_rejects_expected_name_on_wrong_table():
    name = sorted(
        governance.EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES
    )[0]

    class Connection:
        def execute(self, statement, _params=None):
            sql = str(statement)
            if "TRIGGER_NAME IN" in sql:
                return _Rows([{"trigger_name": name}])
            return _Rows()

    with pytest.raises(RuntimeError, match="错误目标表"):
        governance.validate_deferred_governance_trigger_inventory(Connection())


def test_funding_deferred_phase_never_executes_trigger_ddl(monkeypatch):
    statements: list[str] = []

    class Connection:
        def execute(self, statement, _params=None):
            statements.append(str(statement))
            return _Rows()

    monkeypatch.setattr(
        funding,
        "validate_strategy_funding_checkpoint_base_schema",
        lambda _connection: {
            "table_count": 2,
            "trigger_installation_asserted": False,
        },
    )

    result = funding.ensure_strategy_funding_checkpoint_schema(
        Connection(),
        defer_triggers=True,
    )

    assert result["table_count"] == 2
    assert len(statements) == 2
    assert all("CREATE TRIGGER" not in statement.upper() for statement in statements)


def test_funding_deferred_phase_rejects_trigger_executor():
    with pytest.raises(ValueError, match="cannot accept"):
        funding.ensure_strategy_funding_checkpoint_schema(
            object(),
            defer_triggers=True,
            trigger_ddl_executor=lambda _statement: None,
        )


def test_deferred_base_validator_requires_base_markers_not_full_marker(
    monkeypatch,
):
    expected_rows = [
        {
            "migration_key": governance.RUN_REVISION_MIGRATION_KEY,
            "migration_hash": governance.RUN_REVISION_MIGRATION_HASH,
        },
        {
            "migration_key": governance.STRATEGY_CONTENT_HASH_MIGRATION_KEY,
            "migration_hash": governance.STRATEGY_CONTENT_HASH_MIGRATION_HASH,
        },
        {
            "migration_key": funding.FUNDING_CHECKPOINT_BASE_MIGRATION_KEY,
            "migration_hash": funding.FUNDING_CHECKPOINT_BASE_MIGRATION_HASH,
        },
        {
            "migration_key": governance.DEFERRED_BASE_SCHEMA_MIGRATION_KEY,
            "migration_hash": governance.DEFERRED_BASE_SCHEMA_MIGRATION_HASH,
        },
    ]

    class Connection:
        def execute(self, statement, _params=None):
            sql = str(statement)
            if "migration_key IN" in sql:
                return _Rows(expected_rows)
            if "SELECT COUNT(*)" in sql:
                return _Rows([{"count": 0}])
            raise AssertionError(sql)

    class Connect:
        def __enter__(self):
            return Connection()

        def __exit__(self, *_args):
            return False

    class Engine:
        def connect(self):
            return Connect()

    monkeypatch.setattr(governance, "validate_governance_table_schema", lambda _c: {
        "table_count": 15, "column_count": 222, "index_count": 45,
    })
    monkeypatch.setattr(governance, "validate_dynamic_shadow_ledger_schema", lambda _c: {
        "table_count": 4, "column_count": 100, "index_count": 20,
    })
    monkeypatch.setattr(
        governance,
        "validate_strategy_funding_checkpoint_base_schema",
        lambda _c: {"table_count": 2, "column_count": 50, "index_count": 10},
    )
    monkeypatch.setattr(
        governance,
        "validate_deferred_governance_trigger_inventory",
        lambda _c: {
            "expected_trigger_count": 40,
            "installed_trigger_count": 0,
            "missing_trigger_count": 40,
            "database_triggers_installed": False,
        },
    )
    monkeypatch.setattr(
        governance,
        "validate_default_governance_seed_contract",
        lambda _engine: {
            "seeded_strategy_count": 12,
            "seeded_combination_count": 6,
            "seed_contract_hash": "a" * 64,
        },
    )

    result = governance.validate_deferred_governance_base_schema(Engine())

    assert result["migration_count"] == 4
    assert funding.FUNDING_CHECKPOINT_BASE_MIGRATION_KEY in result["migration_keys"]
    assert funding.FUNDING_CHECKPOINT_MIGRATION_KEY not in result["migration_keys"]
    assert result["missing_trigger_count"] == 40


def test_deferred_tool_apply_is_fresh_verified_and_machine_readable(monkeypatch):
    calls: list[tuple[str, object]] = []

    class Engine:
        def __init__(self, name: str):
            self.name = name

        def dispose(self):
            calls.append(("dispose", self.name))

    engines = iter((Engine("apply"), Engine("verify")))
    api = {
        "ensure": lambda **kwargs: calls.append(("ensure", kwargs)),
        "seed": lambda **kwargs: calls.append(("seed", kwargs)),
        "validate": lambda _engine: {
            "mode": deferred.MODE,
            "schema_ready_without_triggers": True,
            "missing_trigger_count": 40,
            "expected_trigger_count": 40,
            "installed_trigger_count": 0,
            "migration_count": 4,
            "seeded_strategy_count": 12,
            "seeded_combination_count": 6,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    }
    monkeypatch.setattr(deferred, "load_project_env", lambda: None)
    monkeypatch.setenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", "DEFERRED_DB")
    monkeypatch.setattr(deferred, "_governance_api", lambda: api)
    monkeypatch.setattr(deferred, "_new_engine", lambda: next(engines))
    monkeypatch.setattr(deferred, "_identity", lambda _engine: {
        "database_name": "probiga",
        "runtime_identity_verified": True,
        "database_read_only": False,
    })
    monkeypatch.setattr(deferred, "_preflight", lambda _engine, _api: {
        "read_only": True,
        "missing_table_count": 6,
        "missing_trigger_count": 40,
    })

    result = deferred.prepare_deferred_base_schema(
        apply=True,
        writers_fenced=True,
    )

    assert result["status"] == "ok"
    assert result["mode"] == "DEFERRED_DB_BASE_SCHEMA"
    assert result["schema_ready_without_triggers"] is True
    assert result["missing_trigger_count"] == 40
    assert result["database_triggers_installed"] is False
    assert result["fresh_connection_verified"] is True
    ensure_call = next(item for item in calls if item[0] == "ensure")
    assert ensure_call[1]["writers_fenced"] is True
    assert ensure_call[1]["defer_triggers"] is True


def test_deferred_tool_apply_requires_explicit_writer_fence():
    with pytest.raises(deferred.DeferredBaseSchemaError, match="writer fence"):
        deferred.prepare_deferred_base_schema(
            apply=True,
            writers_fenced=False,
        )


def test_deferred_tool_rejects_base_schema_write_outside_deferred_mode(
    monkeypatch,
):
    monkeypatch.setattr(deferred, "load_project_env", lambda: None)
    monkeypatch.delenv("PROBIGA_STRATEGY_GOVERNANCE_MODE", raising=False)
    with pytest.raises(deferred.DeferredBaseSchemaError, match="DEFERRED_DB"):
        deferred.prepare_deferred_base_schema(
            apply=True,
            writers_fenced=True,
        )


def test_deferred_marker_never_asserts_trigger_installation():
    assert governance.DEFERRED_BASE_SCHEMA_MIGRATION_KEY == (
        "20260826_001_strategy_governance_deferred_base_schema"
    )
    assert governance.DEFERRED_BASE_SCHEMA_MIGRATION_HASH == (
        "a45fa150c5c6b35ab3214957ab8adb9ec101f4a3b6bc78722ebe809d3410c41c"
    )
    assert len(governance.DEFERRED_BASE_SCHEMA_MIGRATION_HASH) == 64
    assert funding.FUNDING_CHECKPOINT_BASE_MIGRATION_HASH != (
        funding.FUNDING_CHECKPOINT_MIGRATION_HASH
    )
