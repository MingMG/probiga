from __future__ import annotations

import copy
import inspect
from unittest.mock import MagicMock, Mock, patch

import pytest
from sqlalchemy import create_engine, text

from server.api.routers import hot_data, sim_trade
from server.common import sim_trade_schema
from server.engine import sim_trade_engine
from tools import data_quality_check


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
    engine = MagicMock()
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
        assert contract["columns"]["created_at"]["is_nullable"] == "NO"


def test_runtime_validator_preserves_newer_evidence_columns_and_ignores_ordinals():
    inventory = copy.deepcopy(_healthy_inventory())
    order_columns = inventory["columns"]["st_sim_order"]
    order_columns["execution_gate_evidence"] = {
        "ordinal_position": 12,
        "column_type": "longtext",
        "is_nullable": "YES",
        "column_default": None,
        "extra": "",
        "character_set_name": "utf8mb4",
        "collation_name": sim_trade_schema.EXPECTED_COLLATION,
    }
    for position, column in enumerate(order_columns.values(), 10):
        column["ordinal_position"] = position

    with patch.object(sim_trade_schema, "_load_inventory", return_value=inventory):
        result = sim_trade_schema.validate_sim_trade_runtime_schema(
            Mock(), connection=Mock()
        )

    assert result["status"] == "HEALTHY"


def test_sim_trade_dry_run_accepts_exact_legacy_general_ci_shape_with_no_nulls():
    inventory = copy.deepcopy(_healthy_inventory())
    for table in inventory["tables"].values():
        table["collation"] = "utf8mb4_general_ci"
    for columns in inventory["columns"].values():
        for column in columns.values():
            if column["character_set_name"] is not None:
                column["collation_name"] = "utf8mb4_general_ci"
            if column is columns.get("created_at"):
                column["is_nullable"] = "YES"
    inventory["columns"]["st_sim_order"]["execution_gate_status"] = {
        "ordinal_position": 999,
        "column_type": "varchar(20)",
        "is_nullable": "YES",
        "column_default": None,
        "extra": "",
        "character_set_name": "utf8mb4",
        "collation_name": "utf8mb4_general_ci",
    }
    connection = Mock()
    fingerprint = {"row_count": 7, "content_sha256": "b" * 64}
    with patch.object(
        sim_trade_schema, "_load_inventory", return_value=inventory
    ), patch.object(
        sim_trade_schema, "_row_count", return_value=7
    ), patch.object(
        sim_trade_schema, "_null_count", return_value=0
    ), patch.object(
        sim_trade_schema, "_duplicate_key_exists", return_value=False
    ), patch.object(
        sim_trade_schema, "table_content_fingerprint", return_value=fingerprint
    ):
        plan = sim_trade_schema._build_sim_trade_recovery_plan(connection)

    assert plan["safe_automatic_rewrite"] is True
    assert plan["rewrite_table_count"] == 7
    assert len(plan["plan_sha256"]) == 64
    assert plan["tables"]["st_sim_order"]["extra_columns"] == [
        "execution_gate_status"
    ]
    assert plan["tables"]["st_sim_order"]["before_fingerprint"] == fingerprint
    assert all(
        detail["created_at_null_count"] == 0
        for detail in plan["tables"].values()
    )


def test_sim_trade_dry_run_rejects_nullable_created_at_with_stored_nulls():
    inventory = copy.deepcopy(_healthy_inventory())
    inventory["columns"]["st_sim_event"]["created_at"]["is_nullable"] = "YES"
    with patch.object(
        sim_trade_schema, "_load_inventory", return_value=inventory
    ), patch.object(
        sim_trade_schema, "_row_count", return_value=3
    ), patch.object(
        sim_trade_schema,
        "_null_count",
        side_effect=lambda _connection, table, _column: 1
        if table == "st_sim_event" else 0,
    ), patch.object(
        sim_trade_schema, "_duplicate_key_exists", return_value=False
    ):
        plan = sim_trade_schema._build_sim_trade_recovery_plan(Mock())

    assert plan["safe_automatic_rewrite"] is False
    assert plan["tables"]["st_sim_event"]["safe_automatic_rewrite"] is False
    assert plan["tables"]["st_sim_event"]["before_fingerprint"] is None


def test_sim_trade_resumes_crash_after_alter_and_only_verifies_plan():
    engine = MagicMock()
    connection = engine.begin.return_value.__enter__.return_value
    inventory = _healthy_inventory()
    table_name = "st_sim_order"
    fingerprint = {"row_count": 4, "content_sha256": "d" * 64}
    source_detail = {
        "table_exists": True,
        "create_table": False,
        "row_count": 4,
        "engine": sim_trade_schema.EXPECTED_ENGINE,
        "table_collation": "utf8mb4_general_ci",
        "extra_columns": [],
        "missing_columns": [],
        "missing_indexes": [],
        "column_drift": {"created_at": ["is_nullable"]},
        "created_at_null_count": 0,
        "duplicate_unique_indexes": [],
        "rewrite_required": True,
        "safe_automatic_rewrite": True,
        "before_fingerprint": fingerprint,
        "fingerprint_columns": ["id"],
    }
    old_payload = {"tables": {table_name: source_detail}}
    record = sim_trade_schema.make_evidence_record(
        recovery_version=sim_trade_schema.RECOVERY_VERSION,
        source_table=table_name,
        source_row_id=0,
        action="PHYSICAL_REWRITE_PLAN",
        business_key={"table": table_name},
        source_row=source_detail,
        plan_payload=old_payload,
    )
    pending = {
        "record": record,
        "business_key": {"table": table_name},
        "source_row": source_detail,
        "plan_payload": old_payload,
        "plan_sha256": record["plan_sha256"],
    }
    current_tables = {
        table: {
            "table_exists": True,
            "create_table": False,
            "row_count": 4,
            "engine": sim_trade_schema.EXPECTED_ENGINE,
            "table_collation": sim_trade_schema.EXPECTED_COLLATION,
            "extra_columns": [],
            "missing_columns": [],
            "missing_indexes": [],
            "column_drift": {},
            "created_at_null_count": 0,
            "duplicate_unique_indexes": [],
            "rewrite_required": False,
            "safe_automatic_rewrite": True,
            "before_fingerprint": None,
            "fingerprint_columns": list(inventory["columns"][table]),
        }
        for table in sim_trade_schema.TABLE_DDL
    }
    current_payload = {"tables": current_tables}
    current_plan = {
        "schema": "probiga.sim-trade-legacy-recovery-plan.v1",
        "recovery_version": sim_trade_schema.RECOVERY_VERSION,
        "table_count": 7,
        "rewrite_table_count": 0,
        "safe_automatic_rewrite": True,
        "tables": current_tables,
        "plan_sha256": sim_trade_schema.plan_sha256(
            recovery_version=sim_trade_schema.RECOVERY_VERSION,
            payload=current_payload,
        ),
        "read_only": True,
        "_plan_payload": current_payload,
        "_tables": current_tables,
    }

    def load_pending(_connection, *, source_table, **_kwargs):
        return pending if source_table == table_name else None

    with patch.object(
        sim_trade_schema, "ensure_evidence_table"
    ), patch.object(
        sim_trade_schema, "load_pending_physical_rewrite_plan", side_effect=load_pending
    ), patch.object(
        sim_trade_schema, "verify_pending_plan_content", return_value=fingerprint
    ), patch.object(
        sim_trade_schema, "_load_inventory", return_value=inventory
    ), patch.object(
        sim_trade_schema, "_build_sim_trade_recovery_plan", return_value=current_plan
    ), patch.object(
        sim_trade_schema, "table_content_fingerprint", return_value=fingerprint
    ), patch.object(
        sim_trade_schema, "persist_and_verify_evidence",
        return_value={"evidence_verified": True, "evidence_row_count": 1},
    ) as persist, patch.object(
        sim_trade_schema,
        "_repair_known_watchlist_manual_bookkeeping_flows",
        return_value={"status": "NOT_APPLICABLE"},
    ):
        result = sim_trade_schema.privileged_migrate_sim_trade_schema(engine)

    plan_records = persist.call_args_list[0].args[1]
    verified_records = persist.call_args_list[-1].args[1]
    assert plan_records == []
    assert [item["action"] for item in verified_records] == [
        "PHYSICAL_REWRITE_VERIFIED"
    ]
    assert verified_records[0]["plan_sha256"] == record["plan_sha256"]
    assert result["physical_rewrite_evidence"][table_name][
        "resumed_pending_plan"
    ] is True


def _synthetic_manual_incident_rows() -> list[dict]:
    base = {
        "order_id": None,
        "source": "watchlist",
        "strategy_type": "",
        "trade_mode": "live",
        "reason": "synthetic manual ledger",
        "ai_score": "0.00",
        "trans_date": "2025-01-02",
    }
    return [
        {
            **base,
            "id": 187,
            "stock_code": "000101",
            "short_name": "SYNTHETIC-A",
            "flow_type": "watch_buy",
            "trans_type": "buy",
            "price": "10.0000",
            "shares": 100,
            "amount": "1000.00",
            "fee": "1.00",
            "trans_time": "16:10",
            "created_at": "2025-01-02 16:10:01",
        },
        {
            **base,
            "id": 188,
            "stock_code": "000102",
            "short_name": "SYNTHETIC-B",
            "flow_type": "watch_sell",
            "trans_type": "sell",
            "price": "11.0000",
            "shares": 200,
            "amount": "2200.00",
            "fee": "2.00",
            "trans_time": "16:11",
            "created_at": "2025-01-02 16:11:01",
        },
        {
            **base,
            "id": 189,
            "stock_code": "000103",
            "short_name": "SYNTHETIC-C",
            "flow_type": "watch_buy",
            "trans_type": "buy",
            "price": "12.0000",
            "shares": 300,
            "amount": "3600.00",
            "fee": "3.00",
            "trans_time": "16:12",
            "created_at": "2025-01-02 16:12:01",
        },
        {
            **base,
            "id": 190,
            "stock_code": "000104",
            "short_name": "SYNTHETIC-D",
            "flow_type": "watch_sell",
            "trans_type": "sell",
            "price": "13.0000",
            "shares": 400,
            "amount": "5200.00",
            "fee": "4.00",
            "trans_time": "16:13",
            "created_at": "2025-01-02 16:13:01",
        },
    ]


def _manual_incident_hash_manifest(rows: list[dict]) -> tuple[dict[int, str], str]:
    hashes = {
        int(row["id"]): sim_trade_schema.sha256_json(row)
        for row in rows
    }
    aggregate = sim_trade_schema.sha256_json({
        str(row_id): hashes[row_id] for row_id in sorted(hashes)
    })
    return hashes, aggregate


def _manual_incident_engine(*, extra_offhours: bool = False):
    rows = _synthetic_manual_incident_rows()
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_trade_flow (
                id INTEGER PRIMARY KEY,
                order_id INTEGER NULL,
                stock_code TEXT NOT NULL,
                short_name TEXT NOT NULL,
                flow_type TEXT NOT NULL,
                source TEXT NOT NULL,
                strategy_type TEXT NOT NULL,
                trade_mode TEXT NOT NULL,
                trans_type TEXT NOT NULL,
                price NUMERIC NOT NULL,
                shares INTEGER NOT NULL,
                amount NUMERIC NOT NULL,
                fee NUMERIC NOT NULL,
                reason TEXT NULL,
                ai_score NUMERIC NOT NULL,
                trans_date TEXT NOT NULL,
                trans_time TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
        """))
        insert_sql = text("""
            INSERT INTO st_trade_flow (
                id, order_id, stock_code, short_name, flow_type, source,
                strategy_type, trade_mode, trans_type, price, shares, amount,
                fee, reason, ai_score, trans_date, trans_time, created_at
            ) VALUES (
                :id, :order_id, :stock_code, :short_name, :flow_type, :source,
                :strategy_type, :trade_mode, :trans_type, :price, :shares,
                :amount, :fee, :reason, :ai_score, :trans_date, :trans_time,
                :created_at
            )
        """)
        for row in rows:
            connection.execute(insert_sql, row)
        if extra_offhours:
            connection.execute(insert_sql, {
                **rows[0],
                "id": 191,
                "stock_code": "600001",
                "short_name": "UNRELATED",
                "flow_type": "sim_buy",
                "source": "simulation",
                "created_at": "2025-01-02 16:14:01",
                "trans_time": "16:14",
            })
    return engine, rows


def _manual_all_rows(engine) -> list[dict]:
    with engine.connect() as connection:
        return [
            dict(row)
            for row in connection.execute(text(
                "SELECT * FROM st_trade_flow ORDER BY id"
            )).mappings().all()
        ]


def _manual_evidence_sink():
    stored: dict[str, dict] = {}

    def persist(_connection, records):
        for record in records:
            candidate = dict(record)
            current = stored.get(candidate["recovery_key"])
            if current is not None and current != candidate:
                raise RuntimeError("test evidence changed across replay")
            stored[candidate["recovery_key"]] = candidate
        return {
            "evidence_row_count": len(records),
            "evidence_verified": True,
            "evidence_table": "st_privileged_schema_recovery_evidence",
        }

    return stored, persist


def _manual_hash_patches(rows: list[dict]):
    hashes, aggregate = _manual_incident_hash_manifest(rows)
    return (
        patch.object(
            sim_trade_schema,
            "_KNOWN_WATCHLIST_MANUAL_ROW_SHA256",
            hashes,
        ),
        patch.object(
            sim_trade_schema,
            "_KNOWN_WATCHLIST_MANUAL_AGGREGATE_SHA256",
            aggregate,
        ),
    )


def test_watchlist_writer_uses_manual_bookkeeping_and_keeps_portfolio_ledger():
    writes = []
    with patch.object(
        hot_data,
        "_exec_sql",
        side_effect=lambda sql, params=None: writes.append((sql, params)),
    ):
        hot_data._watchlist_write_flow(
            "000101", "SYNTHETIC-A", "buy", 10.0, 100
        )

    assert len(writes) == 1
    sql, params = writes[0]
    assert "trade_mode" in sql
    assert ":trade_mode" in sql
    assert params["trade_mode"] == "manual_bookkeeping"
    transact_source = inspect.getsource(hot_data.portfolio_transact)
    assert transact_source.index("_portfolio_log_trans(") < transact_source.index(
        "_watchlist_write_flow("
    )
    assert "st_portfolio_trans_log" in inspect.getsource(
        hot_data._portfolio_log_trans
    )


def test_manual_bookkeeping_is_excluded_from_live_dashboards_and_strategy_stats():
    assert sim_trade._normalize_trade_mode("manual_bookkeeping") == "live"
    flow_source = inspect.getsource(sim_trade.sim_trade_flow)
    assert "COALESCE(trade_mode, 'live') = :mode" in flow_source
    stats_source = inspect.getsource(sim_trade.sim_trade_stats)
    assert "FROM st_sim_position" in stats_source
    assert "FROM st_trade_flow" not in stats_source
    dashboard_source = inspect.getsource(sim_trade.sim_trade_dashboard)
    assert "COALESCE(trade_mode, 'live') = :mode" in dashboard_source


def test_exact_manual_recovery_is_idempotent_and_preserves_all_facts():
    engine, source_rows = _manual_incident_engine()
    before = _manual_all_rows(engine)
    stored, persist = _manual_evidence_sink()
    hash_patch, aggregate_patch = _manual_hash_patches(source_rows)

    with patch.object(data_quality_check, "_table_exists", return_value=True):
        assert data_quality_check.check_sim_trade_integrity(engine).status == "FAIL"

    with hash_patch, aggregate_patch, patch.object(
        sim_trade_schema,
        "persist_and_verify_evidence",
        side_effect=persist,
    ):
        with engine.begin() as connection:
            first = sim_trade_schema._repair_known_watchlist_manual_bookkeeping_flows(
                connection,
                lock_rows=False,
            )
        after_first = _manual_all_rows(engine)
        with engine.begin() as connection:
            second = sim_trade_schema._repair_known_watchlist_manual_bookkeeping_flows(
                connection,
                lock_rows=False,
            )
        after_second = _manual_all_rows(engine)

    assert first["status"] == "VERIFIED"
    assert first["reclassified_ids"] == [187, 188, 189, 190]
    assert first["reclassified_count"] == 4
    assert first["already_reclassified_count"] == 0
    assert first["remaining_live_offhours_count"] == 0
    assert first["facts_preserved"] is True
    assert second["status"] == "VERIFIED"
    assert second["reclassified_ids"] == []
    assert second["reclassified_count"] == 0
    assert second["already_reclassified_count"] == 4
    assert second["receipt_sha256"] == first["receipt_sha256"]
    assert second["receipt_recovery_key"] == first["receipt_recovery_key"]
    assert len(stored) == 9
    assert after_second == after_first
    assert {row["trade_mode"] for row in after_first} == {
        "manual_bookkeeping"
    }
    for old, new in zip(before, after_first, strict=True):
        assert {
            key: value for key, value in old.items() if key != "trade_mode"
        } == {
            key: value for key, value in new.items() if key != "trade_mode"
        }
    plan_rows = [
        record for record in stored.values()
        if record["action"] == sim_trade_schema.WATCHLIST_MANUAL_PLAN_ACTION
    ]
    assert len(plan_rows) == 4
    assert {
        record["source_row_sha256"] for record in plan_rows
    } == set(_manual_incident_hash_manifest(source_rows)[0].values())

    with patch.object(data_quality_check, "_table_exists", return_value=True):
        quality = data_quality_check.check_sim_trade_integrity(engine)
    assert quality.status == "PASS"
    assert quality.details == {"live_offhours_count": 0, "invalid_modes": []}


def test_exact_manual_recovery_fails_closed_on_hash_drift():
    engine, source_rows = _manual_incident_engine()
    with engine.begin() as connection:
        connection.execute(text(
            "UPDATE st_trade_flow SET price=12.5 WHERE id=189"
        ))
    before = copy.deepcopy(_manual_all_rows(engine))
    stored, persist = _manual_evidence_sink()
    hash_patch, aggregate_patch = _manual_hash_patches(source_rows)

    with hash_patch, aggregate_patch, patch.object(
        sim_trade_schema,
        "persist_and_verify_evidence",
        side_effect=persist,
    ), pytest.raises(RuntimeError, match="row 189 canonical hash differs"):
        with engine.begin() as connection:
            sim_trade_schema._repair_known_watchlist_manual_bookkeeping_flows(
                connection,
                lock_rows=False,
            )

    assert _manual_all_rows(engine) == before
    assert stored == {}


def test_exact_manual_recovery_rolls_back_if_other_live_offhours_remains():
    engine, source_rows = _manual_incident_engine(extra_offhours=True)
    before = copy.deepcopy(_manual_all_rows(engine))
    _stored, persist = _manual_evidence_sink()
    hash_patch, aggregate_patch = _manual_hash_patches(source_rows)

    with hash_patch, aggregate_patch, patch.object(
        sim_trade_schema,
        "persist_and_verify_evidence",
        side_effect=persist,
    ), pytest.raises(RuntimeError, match="live off-hours flows remain"):
        with engine.begin() as connection:
            sim_trade_schema._repair_known_watchlist_manual_bookkeeping_flows(
                connection,
                lock_rows=False,
            )

    assert _manual_all_rows(engine) == before


def test_sim_trade_migration_owns_exact_manual_bookkeeping_recovery():
    source = inspect.getsource(
        sim_trade_schema.privileged_migrate_sim_trade_schema
    )
    assert "_repair_known_watchlist_manual_bookkeeping_flows" in source
    assert "watchlist_manual_bookkeeping_recovery" in source
