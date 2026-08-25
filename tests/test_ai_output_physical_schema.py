from __future__ import annotations

import inspect
from copy import deepcopy
from unittest.mock import MagicMock, patch

import pytest

from biz.analysis import sync_analysis_fast
from server.common import analysis_output_schema as output_schema
from server.common import recommended_run_history_schema as history_schema


def _actual_columns(contract):
    return {
        name: {
            key: value
            for key, value in spec.items()
            if key != "extra_contains"
        }
        | {"extra": " ".join(spec["extra_contains"])}
        for name, spec in contract.items()
    }


def _history_indexes():
    return set(history_schema._REQUIRED_INDEX_SHAPES)


def _output_indexes(table_name: str):
    return set(output_schema._REQUIRED_INDEXES[table_name])


def _metadata():
    return {
        "engine": output_schema.EXPECTED_ENGINE,
        "table_collation": output_schema.EXPECTED_COLLATION,
    }


def _valid_history_data(row_count: int = 2):
    return {
        "row_count": row_count,
        "invalid_run_uid_count": 0,
        "duplicate_run_uid_count": 0,
        "invalid_scheduler_job_count": 0,
        "unbound_active_count": 0,
        "unbuilt_active_count": 0,
        "invalid_build_sha_count": 0,
    }


def test_recommended_history_validation_freezes_full_physical_contract() -> None:
    engine = MagicMock()
    with patch.object(
        history_schema,
        "_column_inventory",
        return_value=_actual_columns(history_schema.EXPECTED_COLUMN_CONTRACT),
    ), patch.object(
        history_schema, "_table_metadata", return_value=_metadata()
    ), patch.object(
        history_schema,
        "_index_inventory",
        return_value=(_history_indexes(), {"PRIMARY", "uid", "date", "status"}),
    ), patch.object(
        history_schema,
        "_history_data_contract",
        return_value=_valid_history_data(),
    ):
        result = history_schema.validate_recommended_run_history_schema(engine)

    assert result["physical_contract_verified"] is True
    assert result["identity_data_verified"] is True
    assert result["runtime_ddl_required"] is False
    engine.begin.assert_not_called()


def test_recommended_history_rejects_uid_width_and_identity_data_drift() -> None:
    engine = MagicMock()
    columns = _actual_columns(history_schema.EXPECTED_COLUMN_CONTRACT)
    columns["run_uid"] = {**columns["run_uid"], "column_type": "varchar(40)"}
    bad_data = _valid_history_data()
    bad_data["invalid_scheduler_job_count"] = 1
    with patch.object(
        history_schema, "_column_inventory", return_value=columns
    ), patch.object(
        history_schema, "_table_metadata", return_value=_metadata()
    ), patch.object(
        history_schema,
        "_index_inventory",
        return_value=(_history_indexes(), set()),
    ), patch.object(
        history_schema, "_history_data_contract", return_value=bad_data
    ), pytest.raises(RuntimeError) as exc_info:
        history_schema.validate_recommended_run_history_schema(engine)

    message = str(exc_info.value)
    assert "run_uid" in message
    assert "invalid_scheduler_job_count" in message


def test_recommended_history_migration_safely_narrows_only_verified_uids() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    columns = _actual_columns(history_schema.EXPECTED_COLUMN_CONTRACT)
    columns["run_uid"] = {**columns["run_uid"], "column_type": "varchar(40)"}
    columns["scheduler_job_id"] = {
        **columns["scheduler_job_id"],
        "column_type": "varchar(64)",
    }
    with patch.object(
        history_schema, "_column_inventory", return_value=columns
    ), patch.object(
        history_schema, "_table_metadata", return_value=_metadata()
    ), patch.object(
        history_schema,
        "_history_data_contract",
        return_value=_valid_history_data(row_count=8),
    ), patch.object(
        history_schema,
        "_index_inventory",
        return_value=(_history_indexes(), {"PRIMARY", "uid", "date", "status"}),
    ), patch.object(
        history_schema,
        "validate_recommended_run_history_schema",
        return_value={"physical_contract_verified": True},
    ):
        result = history_schema.migrate_recommended_run_history(engine)

    sql = [str(call.args[0]).upper() for call in connection.execute.call_args_list]
    assert any("MODIFY COLUMN `RUN_UID`" in statement for statement in sql)
    assert any("MODIFY COLUMN `SCHEDULER_JOB_ID`" in statement for statement in sql)
    assert result["normalized_columns"] == ["run_uid", "scheduler_job_id"]


def test_recommended_history_migration_rejects_unbound_active_legacy_rows() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    columns = _actual_columns(history_schema.EXPECTED_COLUMN_CONTRACT)
    bad_data = _valid_history_data(row_count=1)
    bad_data["unbound_active_count"] = 1
    with patch.object(
        history_schema, "_column_inventory", return_value=columns
    ), patch.object(
        history_schema, "_table_metadata", return_value=_metadata()
    ), patch.object(
        history_schema, "_history_data_contract", return_value=bad_data
    ), pytest.raises(RuntimeError, match="terminally reconciled"):
        history_schema.migrate_recommended_run_history(engine)

    statements = [
        str(call.args[0]).upper() for call in connection.execute.call_args_list
    ]
    assert not any("MODIFY COLUMN" in statement for statement in statements)


def _output_inventory(table_name: str):
    contracts = {
        output_schema.RECOMMENDATION_TABLE:
            output_schema.RECOMMENDATION_COLUMN_CONTRACT,
        output_schema.ANALYSIS_TABLE: output_schema.ANALYSIS_COLUMN_CONTRACT,
        output_schema.FAILURE_TABLE:
            output_schema.FAILURE_SAMPLE_COLUMN_CONTRACT,
    }
    return _actual_columns(contracts[table_name])


def test_analysis_output_validation_rejects_type_and_business_index_drift() -> None:
    engine = MagicMock()
    inventories = {
        table: _output_inventory(table)
        for table in (
            output_schema.RECOMMENDATION_TABLE,
            output_schema.ANALYSIS_TABLE,
            output_schema.FAILURE_TABLE,
        )
    }
    inventories[output_schema.ANALYSIS_TABLE]["analysis_date"][
        "column_type"
    ] = "datetime"
    indexes = {
        table: _output_indexes(table)
        for table in inventories
    }
    indexes[output_schema.RECOMMENDATION_TABLE].remove(
        (True, ("stock_code", "pick_date"))
    )
    with patch.object(
        output_schema,
        "_column_inventory",
        side_effect=lambda _connection, table: inventories[table],
    ), patch.object(
        output_schema,
        "_table_metadata",
        side_effect=lambda _connection, _table: _metadata(),
    ), patch.object(
        output_schema,
        "_index_inventory",
        side_effect=lambda _connection, table: (indexes[table], set()),
    ), pytest.raises(RuntimeError) as exc_info:
        output_schema.validate_analysis_output_schema(engine)

    assert "missing_indexes" in str(exc_info.value)
    assert "stock_code" in str(exc_info.value)


def test_analysis_output_validation_rejects_analysis_column_type_drift() -> None:
    engine = MagicMock()
    inventories = {
        table: _output_inventory(table)
        for table in (
            output_schema.RECOMMENDATION_TABLE,
            output_schema.ANALYSIS_TABLE,
            output_schema.FAILURE_TABLE,
        )
    }
    inventories[output_schema.ANALYSIS_TABLE]["analysis_date"][
        "column_type"
    ] = "datetime"
    with patch.object(
        output_schema,
        "_column_inventory",
        side_effect=lambda _connection, table: inventories[table],
    ), patch.object(
        output_schema,
        "_table_metadata",
        side_effect=lambda _connection, _table: _metadata(),
    ), patch.object(
        output_schema,
        "_index_inventory",
        side_effect=lambda _connection, table: (
            _output_indexes(table), set()
        ),
    ), pytest.raises(RuntimeError) as exc_info:
        output_schema.validate_analysis_output_schema(engine)

    assert "analysis_date" in str(exc_info.value)
    assert "datetime" in str(exc_info.value)


def test_analysis_output_migration_refuses_nonempty_physical_reinterpretation() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    inventories = {
        table: _output_inventory(table)
        for table in (
            output_schema.RECOMMENDATION_TABLE,
            output_schema.ANALYSIS_TABLE,
            output_schema.FAILURE_TABLE,
        )
    }
    inventories[output_schema.RECOMMENDATION_TABLE]["ai_score"][
        "column_type"
    ] = "varchar(20)"
    with patch.object(
        output_schema,
        "_column_inventory",
        side_effect=lambda _connection, table: inventories[table],
    ), patch.object(
        output_schema,
        "_table_metadata",
        side_effect=lambda _connection, _table: _metadata(),
    ), patch.object(
        output_schema, "_row_count", return_value=25
    ), pytest.raises(RuntimeError, match="cannot be modified in place"):
        output_schema.migrate_analysis_output_schema(engine)

    statements = [
        str(call.args[0]).upper() for call in connection.execute.call_args_list
    ]
    assert not any("MODIFY COLUMN `AI_SCORE`" in sql for sql in statements)


def test_analysis_output_migration_rejects_duplicate_business_key() -> None:
    engine = MagicMock()
    inventories = {
        table: _output_inventory(table)
        for table in (
            output_schema.RECOMMENDATION_TABLE,
            output_schema.ANALYSIS_TABLE,
            output_schema.FAILURE_TABLE,
        )
    }
    with patch.object(
        output_schema,
        "_column_inventory",
        side_effect=lambda _connection, table: inventories[table],
    ), patch.object(
        output_schema,
        "_table_metadata",
        side_effect=lambda _connection, _table: _metadata(),
    ), patch.object(
        output_schema, "_row_count", return_value=10
    ), patch.object(
        output_schema, "_duplicate_key", side_effect=(True, False)
    ), pytest.raises(RuntimeError, match="business key contains duplicates"):
        output_schema.migrate_analysis_output_schema(engine)


def test_analysis_runtime_schema_paths_are_read_only() -> None:
    save_source = inspect.getsource(sync_analysis_fast.save_outputs).upper()
    load_source = inspect.getsource(
        sync_analysis_fast._validate_learning_tables
    ).upper()
    assert "CREATE TABLE" not in save_source
    assert "ALTER TABLE" not in save_source
    assert "CREATE TABLE" not in load_source
    assert "ALTER TABLE" not in load_source
