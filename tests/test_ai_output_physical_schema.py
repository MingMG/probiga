from __future__ import annotations

import inspect
from copy import deepcopy
from datetime import datetime
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


def test_recommended_history_identity_probe_is_mysql84_collation_safe() -> None:
    connection = MagicMock()
    connection.execute.return_value.mappings.return_value.one.return_value = {
        key: 0 for key in _valid_history_data()
    }

    history_schema._history_data_contract(connection)

    sql = str(connection.execute.call_args.args[0]).upper()
    assert "REGEXP_LIKE(RUN_UID" in sql
    assert "REGEXP_LIKE(SCHEDULER_JOB_ID" in sql
    assert "REGEXP_LIKE(BUILD_SHA" in sql
    assert "COLLATE UTF8MB4_BIN" in sql
    assert "BINARY RUN_UID" not in sql
    assert "BINARY SCHEDULER_JOB_ID" not in sql
    assert "BINARY BUILD_SHA" not in sql


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
        history_schema, "ensure_evidence_table"
    ), patch.object(
        history_schema, "load_pending_physical_rewrite_plan", return_value=None
    ), patch.object(
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
        history_schema, "ensure_evidence_table"
    ), patch.object(
        history_schema, "load_pending_physical_rewrite_plan", return_value=None
    ), patch.object(
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
    ), patch.object(
        history_schema,
        "persist_and_verify_evidence",
        return_value={"evidence_verified": True, "evidence_row_count": 1},
    ), patch.object(
        history_schema,
        "table_content_fingerprint",
        return_value={"row_count": 8, "content_sha256": "a" * 64},
    ), patch.object(
        history_schema, "_history_physical_contract_ready", return_value=True
    ):
        result = history_schema.migrate_recommended_run_history(engine)

    sql = [str(call.args[0]).upper() for call in connection.execute.call_args_list]
    assert any("MODIFY COLUMN `RUN_UID`" in statement for statement in sql)
    assert any("MODIFY COLUMN `SCHEDULER_JOB_ID`" in statement for statement in sql)
    assert result["normalized_columns"] == ["run_uid", "scheduler_job_id"]


def test_recommended_history_legacy_running_plan_is_deterministic() -> None:
    row = {
        "id": 85,
        "run_uid": "300ddcdec37a470783834624234cdfdf",
        "status": "running",
        "started_at": datetime(2026, 8, 26, 9, 43, 5),
        "execution_time": datetime(2026, 8, 26, 9, 43, 3),
        "created_at": datetime(2026, 8, 26, 9, 43, 3),
        "message": "legacy progress",
    }

    first = history_schema._build_legacy_terminal_plan([row])
    second = history_schema._build_legacy_terminal_plan([dict(reversed(row.items()))])

    assert first["plan_sha256"] == second["plan_sha256"]
    assert first["terminalize_count"] == 1
    assert first["actions"][0]["terminal_status"] == "error"
    assert first["actions"][0]["terminal_error"] == (
        history_schema.LEGACY_TERMINAL_ERROR
    )
    assert first["actions"][0]["terminal_finished_at"].startswith(
        "2026-08-26T09:43:05"
    )


def test_recommended_history_terminalization_requires_evidence_first() -> None:
    row = {
        "id": 85,
        "run_uid": "300ddcdec37a470783834624234cdfdf",
        "status": "running",
        "started_at": datetime(2026, 8, 26, 9, 43, 5),
        "message": "legacy progress",
    }
    plan = history_schema._build_legacy_terminal_plan([row])
    connection = MagicMock()
    connection.execute.return_value.rowcount = 1
    with patch.object(
        history_schema,
        "persist_and_verify_evidence",
        return_value={"evidence_verified": True, "evidence_row_count": 1},
    ) as persist:
        evidence = history_schema._persist_legacy_terminal_plan(connection, plan)
        changed = history_schema._apply_legacy_terminal_plan(connection, plan)

    record = persist.call_args.args[1][0]
    assert evidence["evidence_verified"] is True
    assert record["source_row_id"] == 85
    assert "legacy progress" in record["source_row_json"]
    assert len(record["source_row_sha256"]) == 64
    assert changed == 1
    update_sql = str(connection.execute.call_args.args[0]).upper()
    assert "SET STATUS='ERROR'" in update_sql
    assert "LEGACY_SCHEMA_RECOVERY" in update_sql


def test_recommended_history_migration_rejects_unbound_active_legacy_rows() -> None:
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    columns = _actual_columns(history_schema.EXPECTED_COLUMN_CONTRACT)
    bad_data = _valid_history_data(row_count=1)
    bad_data["unbound_active_count"] = 1
    with patch.object(
        history_schema, "ensure_evidence_table"
    ), patch.object(
        history_schema, "load_pending_physical_rewrite_plan", return_value=None
    ), patch.object(
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


def test_recommended_history_resumes_crash_after_alter_and_only_verifies_plan():
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    columns = _actual_columns(history_schema.EXPECTED_COLUMN_CONTRACT)
    fingerprint = {"row_count": 8, "content_sha256": "a" * 64}
    manifest = {
        "before_fingerprint": fingerprint,
        "fingerprint_columns": list(columns),
        "column_drift": ["run_uid"],
        "engine": history_schema.EXPECTED_ENGINE,
        "table_collation": "utf8mb4_general_ci",
    }
    payload = {"table": history_schema.TABLE_NAME, **manifest}
    record = history_schema.make_evidence_record(
        recovery_version=history_schema.PHYSICAL_RECOVERY_VERSION,
        source_table=history_schema.TABLE_NAME,
        source_row_id=0,
        action="PHYSICAL_REWRITE_PLAN",
        business_key={"table": history_schema.TABLE_NAME},
        source_row=manifest,
        plan_payload=payload,
    )
    pending = {
        "record": record,
        "business_key": {"table": history_schema.TABLE_NAME},
        "source_row": manifest,
        "plan_payload": payload,
        "plan_sha256": record["plan_sha256"],
    }
    with patch.object(
        history_schema, "ensure_evidence_table"
    ), patch.object(
        history_schema, "load_pending_physical_rewrite_plan", return_value=pending
    ), patch.object(
        history_schema, "verify_pending_plan_content", return_value=fingerprint
    ), patch.object(
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
        history_schema, "_legacy_terminal_plan",
        return_value=history_schema._build_legacy_terminal_plan([]),
    ), patch.object(
        history_schema, "_persist_legacy_terminal_plan",
        return_value={"evidence_verified": True, "evidence_row_count": 0},
    ), patch.object(
        history_schema, "_apply_legacy_terminal_plan", return_value=0
    ), patch.object(
        history_schema, "_history_physical_contract_ready", return_value=True
    ), patch.object(
        history_schema, "table_content_fingerprint", return_value=fingerprint
    ), patch.object(
        history_schema, "persist_and_verify_evidence",
        return_value={"evidence_verified": True, "evidence_row_count": 1},
    ) as persist, patch.object(
        history_schema,
        "validate_recommended_run_history_schema",
        return_value={"physical_contract_verified": True},
    ):
        result = history_schema.migrate_recommended_run_history(engine)

    persisted = persist.call_args.args[1]
    assert [item["action"] for item in persisted] == ["PHYSICAL_REWRITE_VERIFIED"]
    assert persisted[0]["plan_sha256"] == record["plan_sha256"]
    assert result["physical_rewrite"]["resumed_pending_plan"] is True
    sql = [str(call.args[0]).upper() for call in connection.execute.call_args_list]
    assert not any(statement.startswith("ALTER TABLE") for statement in sql)


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
        output_schema, "ensure_evidence_table"
    ), patch.object(
        output_schema, "load_pending_physical_rewrite_plan", return_value=None
    ), patch.object(
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
        output_schema, "ensure_evidence_table"
    ), patch.object(
        output_schema, "load_pending_physical_rewrite_plan", return_value=None
    ), patch.object(
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
        output_schema, "ensure_evidence_table"
    ), patch.object(
        output_schema, "load_pending_physical_rewrite_plan", return_value=None
    ), patch.object(
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
        output_schema, "ensure_evidence_table"
    ), patch.object(
        output_schema, "load_pending_physical_rewrite_plan", return_value=None
    ), patch.object(
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


def test_analysis_output_resumes_crash_after_alter_and_only_verifies_plan():
    engine = MagicMock()
    inventories = {
        table: _output_inventory(table)
        for table in (
            output_schema.RECOMMENDATION_TABLE,
            output_schema.ANALYSIS_TABLE,
            output_schema.FAILURE_TABLE,
        )
    }
    table_name = output_schema.ANALYSIS_TABLE
    fingerprint = {"row_count": 10, "content_sha256": "c" * 64}
    manifest = {
        "before_fingerprint": fingerprint,
        "fingerprint_columns": list(inventories[table_name]),
        "column_drift": ["stock_name"],
        "engine": output_schema.EXPECTED_ENGINE,
        "table_collation": "utf8mb4_general_ci",
    }
    payload = {"table": table_name, **manifest}
    record = output_schema.make_evidence_record(
        recovery_version=output_schema.PHYSICAL_RECOVERY_VERSION,
        source_table=table_name,
        source_row_id=0,
        action="PHYSICAL_REWRITE_PLAN",
        business_key={"table": table_name},
        source_row=manifest,
        plan_payload=payload,
    )
    pending = {
        "record": record,
        "business_key": {"table": table_name},
        "source_row": manifest,
        "plan_payload": payload,
        "plan_sha256": record["plan_sha256"],
    }

    def load_pending(_connection, *, source_table, **_kwargs):
        return pending if source_table == table_name else None

    with patch.object(
        output_schema, "ensure_evidence_table"
    ), patch.object(
        output_schema, "load_pending_physical_rewrite_plan", side_effect=load_pending
    ), patch.object(
        output_schema, "verify_pending_plan_content", return_value=fingerprint
    ), patch.object(
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
        output_schema, "_recommendation_inbound_foreign_keys", return_value=[]
    ), patch.object(
        output_schema, "_duplicate_key", return_value=False
    ), patch.object(
        output_schema,
        "_recommendation_duplicate_plan",
        return_value=output_schema._build_recommendation_duplicate_plan([]),
    ), patch.object(
        output_schema, "_persist_duplicate_plan",
        return_value={"evidence_verified": True, "evidence_row_count": 0},
    ), patch.object(
        output_schema, "_apply_duplicate_plan", return_value=0
    ), patch.object(
        output_schema,
        "_index_inventory",
        side_effect=lambda _connection, table: (
            _output_indexes(table), {"PRIMARY", "business", "lookup"}
        ),
    ), patch.object(
        output_schema, "table_content_fingerprint", return_value=fingerprint
    ), patch.object(
        output_schema, "persist_and_verify_evidence",
        return_value={"evidence_verified": True, "evidence_row_count": 1},
    ) as persist, patch.object(
        output_schema,
        "validate_analysis_output_schema",
        return_value={"physical_contract_verified": True},
    ):
        result = output_schema.migrate_analysis_output_schema(engine)

    persisted = persist.call_args.args[1]
    assert [item["action"] for item in persisted] == ["PHYSICAL_REWRITE_VERIFIED"]
    assert persisted[0]["plan_sha256"] == record["plan_sha256"]
    assert result["physical_rewrite_evidence"][table_name][
        "resumed_pending_plan"
    ] is True


def test_recommendation_duplicate_plan_keeps_latest_id_and_hashes_every_row() -> None:
    rows = [
        {
            "id": 5862,
            "stock_code": "600971",
            "pick_date": "2026-07-15",
            "final_trade_score": 99,
            "reason": "older higher score",
        },
        {
            "id": 5874,
            "stock_code": "600971",
            "pick_date": "2026-07-15",
            "final_trade_score": 80,
            "reason": "latest id",
        },
    ]

    forward = output_schema._build_recommendation_duplicate_plan(rows)
    reverse = output_schema._build_recommendation_duplicate_plan(list(reversed(rows)))

    assert forward["plan_sha256"] == reverse["plan_sha256"]
    assert forward["duplicate_group_count"] == 1
    assert forward["delete_row_count"] == 1
    group = forward["duplicate_groups"][0]
    assert group["canonical_rule"] == "MAX(id)"
    assert group["keep_id"] == 5874
    assert group["remove_ids"] == [5862]
    assert {len(item["source_row_sha256"]) for item in group["rows"]} == {64}


def test_recommendation_duplicate_delete_is_blocked_until_full_evidence() -> None:
    rows = [
        {"id": 1, "stock_code": "600000", "pick_date": "2026-08-25"},
        {"id": 2, "stock_code": "600000", "pick_date": "2026-08-25"},
    ]
    plan = output_schema._build_recommendation_duplicate_plan(rows)
    connection = MagicMock()
    connection.execute.return_value.rowcount = 1
    with patch.object(
        output_schema,
        "persist_and_verify_evidence",
        return_value={"evidence_verified": True, "evidence_row_count": 2},
    ) as persist:
        evidence = output_schema._persist_duplicate_plan(connection, plan)
        deleted = output_schema._apply_duplicate_plan(connection, plan)

    records = persist.call_args.args[1]
    assert evidence["evidence_verified"] is True
    assert {record["action"] for record in records} == {"KEEP", "DELETE_DUPLICATE"}
    assert all(record["source_row_json"] for record in records)
    assert deleted == 1
    delete_sql = str(connection.execute.call_args.args[0]).upper()
    assert "DELETE FROM `ST_RECOMMENDED_STOCKS`" in delete_sql


def test_analysis_contract_matches_fail_closed_production_shape() -> None:
    recommendation = output_schema.RECOMMENDATION_COLUMN_CONTRACT
    analysis = output_schema.ANALYSIS_COLUMN_CONTRACT
    assert recommendation["id"]["column_type"] == "bigint"
    assert recommendation["short_name"]["column_default"] is None
    assert recommendation["sources"]["column_default"] is None
    assert recommendation["recommend_status"]["column_default"] == "BLOCK"
    assert recommendation["model_version"]["column_type"] == "varchar(64)"
    assert analysis["recommend_status"]["column_default"] == "BLOCK"
    assert analysis["model_version"]["column_type"] == "varchar(64)"
    assert output_schema._default("now()") == "current_timestamp"


def test_analysis_runtime_schema_paths_are_read_only() -> None:
    save_source = inspect.getsource(sync_analysis_fast.save_outputs).upper()
    load_source = inspect.getsource(
        sync_analysis_fast._validate_learning_tables
    ).upper()
    assert "CREATE TABLE" not in save_source
    assert "ALTER TABLE" not in save_source
    assert "CREATE TABLE" not in load_source
    assert "ALTER TABLE" not in load_source
