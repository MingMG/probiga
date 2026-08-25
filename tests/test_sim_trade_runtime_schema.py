from __future__ import annotations

import copy
import inspect
from unittest.mock import Mock, patch

import pytest

from server.common import sim_trade_schema
from server.engine import sim_trade_engine


def _healthy_inventory():
    return {
        "tables": {
            table: {
                "engine": sim_trade_schema.EXPECTED_ENGINE,
                "collation": sim_trade_schema.EXPECTED_COLLATION,
            }
            for table in sim_trade_schema.TABLE_DDL
        },
        "columns": {
            table: {
                name: {
                    key: value
                    for key, value in contract.items()
                    if key != "ddl"
                }
                for name, contract in expected["columns"].items()
            }
            for table, expected in sim_trade_schema.EXPECTED_CONTRACTS.items()
        },
        "indexes": {
            table: set(expected["indexes"])
            for table, expected in sim_trade_schema.EXPECTED_CONTRACTS.items()
        },
    }


def test_runtime_validator_accepts_full_physical_contract():
    engine = Mock()
    connection = Mock()
    with patch.object(
        sim_trade_schema, "_load_inventory", return_value=_healthy_inventory()
    ):
        result = sim_trade_schema.validate_sim_trade_runtime_schema(
            engine, connection=connection
        )

    assert result["status"] == "HEALTHY"
    assert result["table_count"] == 7
    assert result["runtime_ddl_required"] is False
    assert result["read_only"] is True


@pytest.mark.parametrize(
    ("mutation", "needle"),
    [
        (lambda value: value["tables"].pop("st_sim_event"), "missing-table"),
        (
            lambda value: value["tables"]["st_sim_order"].update(
                {"collation": "utf8mb4_general_ci"}
            ),
            "collation",
        ),
        (
            lambda value: value["columns"]["st_sim_signal"]["risk_budget_amount"].update(
                {"column_type": "decimal(10,2)"}
            ),
            "column_type",
        ),
        (
            lambda value: value["columns"]["st_trade_flow"]["order_id"].update(
                {"is_nullable": "NO"}
            ),
            "is_nullable",
        ),
        (
            lambda value: value["indexes"]["st_sim_position"].discard(
                (False, ("signal_id",))
            ),
            "index",
        ),
    ],
)
def test_runtime_validator_fails_closed_on_physical_drift(mutation, needle):
    inventory = copy.deepcopy(_healthy_inventory())
    mutation(inventory)
    with patch.object(sim_trade_schema, "_load_inventory", return_value=inventory):
        with pytest.raises(RuntimeError, match=needle):
            sim_trade_schema.validate_sim_trade_runtime_schema(
                Mock(), connection=Mock()
            )


def test_engine_compatibility_guard_is_read_only_validator():
    engine = Mock()
    expected = {"status": "HEALTHY"}
    with patch.object(sim_trade_engine, "get_engine", return_value=engine), patch.object(
        sim_trade_engine,
        "validate_sim_trade_runtime_schema",
        return_value=expected,
    ) as validator:
        assert sim_trade_engine._ensure_tables() == expected

    validator.assert_called_once_with(engine)
    runtime_source = inspect.getsource(sim_trade_engine._ensure_tables).upper()
    assert "CREATE TABLE" not in runtime_source
    assert "ALTER TABLE" not in runtime_source


def test_all_required_columns_and_indexes_come_from_immutable_ddl_contract():
    assert set(sim_trade_schema.EXPECTED_CONTRACTS) == set(
        sim_trade_schema.TABLE_DDL
    )
    for table_name, contract in sim_trade_schema.EXPECTED_CONTRACTS.items():
        assert contract["columns"], table_name
        assert (True, ("id",)) in contract["indexes"], table_name
        assert contract["columns"]["id"]["extra"] == "auto_increment"
        assert contract["columns"]["id"]["is_nullable"] == "NO"
