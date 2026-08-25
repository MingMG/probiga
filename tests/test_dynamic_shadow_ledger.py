from __future__ import annotations

import hashlib
import inspect
import json
from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text

from server.engine.dynamic_shadow_ledger import (
    DynamicShadowLedgerError,
    bind_dynamic_shadow_trial_to_existing_paper_evidence,
    bind_pending_dynamic_shadow_trials,
    create_dynamic_shadow_trial_plan,
    create_dynamic_shadow_trial_plans_from_candidate_facts,
    dynamic_shadow_ledger_readiness,
    persist_strategy_adapter_candidate_facts,
    verify_dynamic_shadow_trial,
    verify_dynamic_shadow_trial_plan,
)
from server.engine.dynamic_shadow_ledger_schema import (
    DYNAMIC_SHADOW_LEDGER_TABLE_NAMES,
    dynamic_shadow_ledger_ddl_statements,
)
from server.engine.strategy_execution_adapters import (
    batch_dynamic_shadow_ledger_readiness,
    verify_dynamic_shadow_ledger_chain,
)
from server.trading_v3.paper_execution import (
    materialize_dynamic_shadow_bootstrap_orders,
)
from server.trading_v2.execution import _execution_buy_gate_decision


def _digest(value):
    return hashlib.sha256(json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def test_readiness_errors_never_expose_infrastructure_credentials(
    monkeypatch, caplog,
):
    from server.engine import dynamic_shadow_ledger as ledger
    from server.engine import strategy_execution_adapters as adapters

    credential = "mysql://root:" + "SuperSecret@db.internal/PROBIGA"

    def raise_secret(*_args, **_kwargs):
        raise RuntimeError(credential)

    monkeypatch.setattr(ledger, "_readiness_on_connection", raise_secret)
    single = ledger.dynamic_shadow_ledger_readiness(
        connection=object(), strategy_key="alpha", strategy_version="v1",
    )
    serialized_single = json.dumps(single, ensure_ascii=False)
    assert credential not in serialized_single
    assert single["invalid_chains"][0]["error_code"] == (
        "DYNAMIC_SHADOW_LEDGER_INTERNAL_FAILURE"
    )
    assert len(single["invalid_chains"][0]["incident_id"]) == 32

    identity = ("alpha", "v1", "a" * 64, "b" * 64)
    monkeypatch.setattr(adapters, "_mapping_rows", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(adapters, "_bounded_mapping_rows", raise_secret)
    batched = adapters.batch_dynamic_shadow_ledger_readiness(
        object(), [identity]
    )[identity]
    serialized_batch = json.dumps(batched, ensure_ascii=False)
    assert credential not in serialized_batch
    assert batched["invalid_chains"][0]["error_code"] == (
        "DYNAMIC_ADAPTER_LEDGER_INTERNAL_FAILURE"
    )
    assert len(batched["invalid_chains"][0]["incident_id"]) == 32
    assert credential not in caplog.text
    assert '"reason": str(exc)' not in inspect.getsource(
        adapters.batch_dynamic_shadow_ledger_readiness
    )

    controlled = ledger._readiness_error_detail(
        DynamicShadowLedgerError("同一计划存在多条完整链")
    )
    assert controlled["reason"] == "同一计划存在多条完整链"
    actionable = adapters._dynamic_ledger_error_detail(
        ValueError("执行适配器版本字段缺失")
    )
    assert actionable["reason"] == "执行适配器版本字段缺失"


def _schema(connection):
    statements = (
        """
        CREATE TABLE st_strategy_adapter_run_receipt (
            run_uid TEXT PRIMARY KEY, strategy_key TEXT, strategy_version TEXT,
            strategy_version_hash TEXT, execution_binding_hash TEXT,
            adapter_artifact_sha256 TEXT, cost_model_hash TEXT,
            adapter_key TEXT, adapter_version TEXT, trade_date TEXT,
            completed_at TEXT, status TEXT, input_hash TEXT, output_hash TEXT,
            stable_result_hash TEXT, candidate_count INTEGER,
            candidate_identity_json TEXT, receipt_json TEXT,
            receipt_hash TEXT UNIQUE
        )
        """,
        """
        CREATE TABLE st_strategy_adapter_candidate_fact (
            candidate_run_uid TEXT, stock_code TEXT, candidate_index INTEGER,
            trade_date TEXT, candidate_json TEXT, candidate_hash TEXT,
            PRIMARY KEY (candidate_run_uid, stock_code)
        )
        """,
        """
        CREATE TABLE st_trade_intent_v2 (
            intent_id TEXT PRIMARY KEY, account_id TEXT, decision_run_uid TEXT,
            strategy_version TEXT, stock_code TEXT, action TEXT,
            current_quantity INTEGER, target_quantity INTEGER,
            target_weight NUMERIC, earliest_at TEXT, expires_at TEXT,
            limit_price NUMERIC, worst_price NUMERIC, initial_stop NUMERIC,
            protective_stop NUMERIC, invalidation_condition TEXT,
            reason_code TEXT, evidence_json TEXT, intent_version INTEGER,
            idempotency_key TEXT, created_at TEXT, theme_code TEXT DEFAULT ''
        )
        """,
        """
        CREATE TABLE st_risk_decision_v2 (
            intent_id TEXT PRIMARY KEY, decision_status TEXT,
            requested_quantity INTEGER, approved_quantity INTEGER,
            trade_risk NUMERIC, post_single_weight NUMERIC,
            post_total_weight NUMERIC, post_theme_weight NUMERIC,
            post_open_risk NUMERIC, post_cash NUMERIC, checks_json TEXT,
            first_failure TEXT, decision_hash TEXT, created_at TEXT
        )
        """,
        """
        CREATE TABLE st_order_v2 (
            order_id TEXT PRIMARY KEY, account_id TEXT, intent_id TEXT,
            stock_code TEXT, side TEXT, order_type TEXT, limit_price NUMERIC,
            quantity INTEGER, filled_quantity INTEGER, status TEXT,
            waiting_reason TEXT, earliest_at TEXT, expires_at TEXT,
            idempotency_key TEXT, created_at TEXT, updated_at TEXT
        )
        """,
        """
        CREATE TABLE st_fill_v2 (
            fill_id TEXT PRIMARY KEY, order_id TEXT, account_id TEXT,
            stock_code TEXT, side TEXT, quantity INTEGER, price NUMERIC,
            gross_amount NUMERIC, fee_amount NUMERIC, net_cash_amount NUMERIC,
            quote_event_id TEXT, match_event_id TEXT, idempotency_key TEXT,
            filled_at TEXT, created_at TEXT
        )
        """,
        """
        CREATE TABLE st_forward_trade_evidence_v3 (
            evidence_id TEXT PRIMARY KEY, account_id TEXT,
            source_run_uid TEXT, source_forecast_id TEXT,
            source_intent_id TEXT, stock_code TEXT, strategy_key TEXT,
            strategy_version TEXT, sample_owner_role TEXT,
            attribution_status TEXT, attribution_version TEXT,
            supporting_strategy_keys_json TEXT, ownership_hash TEXT,
            evidence_kind TEXT, protocol_version TEXT, entry_order_id TEXT,
            entry_fill_id TEXT, entry_trade_date TEXT, entry_at TEXT,
            entry_quantity INTEGER, entry_price NUMERIC,
            entry_gross_cny NUMERIC, entry_fee_cny NUMERIC,
            closed_quantity INTEGER, exit_fill_ids_json TEXT,
            exit_order_ids_json TEXT, exit_at TEXT,
            exit_average_price NUMERIC, exit_gross_cny NUMERIC,
            exit_fee_cny NUMERIC, realized_net_pnl_cny NUMERIC,
            realized_net_return_pct NUMERIC, realized_mae_pct NUMERIC,
            realized_mfe_pct NUMERIC, exit_reason TEXT, evidence_status TEXT
        )
        """,
        """
        CREATE TABLE st_forward_exit_allocation_v3 (
            allocation_id TEXT PRIMARY KEY, evidence_id TEXT,
            attribution_status TEXT, account_id TEXT, stock_code TEXT,
            entry_fill_id TEXT, exit_fill_id TEXT, exit_order_id TEXT,
            allocation_sequence INTEGER, allocated_quantity INTEGER,
            allocated_gross_cny NUMERIC, allocated_fee_cny NUMERIC,
            exit_filled_at TEXT, allocation_protocol_version TEXT
        )
        """,
        """
        CREATE TABLE st_dynamic_shadow_trial_plan (
            plan_id TEXT PRIMARY KEY, candidate_run_uid TEXT,
            candidate_receipt_hash TEXT, strategy_key TEXT,
            strategy_version TEXT, strategy_version_hash TEXT,
            execution_binding_hash TEXT, trade_date TEXT, stock_code TEXT,
            account_id TEXT, maximum_target_bp INTEGER,
            candidate_fact_hash TEXT, candidate_signal_json TEXT,
            candidate_signal_hash TEXT,
            plan_payload_json TEXT, plan_hash TEXT, plan_status TEXT,
            automatic_real_order_submission INTEGER DEFAULT 0,
            real_order_authority INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE st_dynamic_shadow_trial_chain (
            chain_id TEXT PRIMARY KEY, plan_id TEXT UNIQUE,
            source_intent_id TEXT, entry_order_id TEXT, entry_fill_id TEXT,
            forward_evidence_id TEXT, intent_fact_hash TEXT,
            risk_decision_fact_hash TEXT,
            entry_order_fact_hash TEXT, entry_fill_fact_hash TEXT,
            forward_evidence_fact_hash TEXT, exit_set_hash TEXT,
            exit_binding_count INTEGER, chain_payload_json TEXT,
            chain_hash TEXT, automatic_real_order_submission INTEGER DEFAULT 0,
            real_order_authority INTEGER DEFAULT 0, created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE st_strategy_registry (
            strategy_key TEXT PRIMARY KEY, current_version TEXT,
            current_status TEXT, enabled INTEGER
        )
        """,
        """
        CREATE TABLE st_strategy_version (
            strategy_key TEXT, version TEXT, version_hash TEXT,
            source_kind TEXT, PRIMARY KEY (strategy_key, version)
        )
        """,
        """
        CREATE TABLE st_strategy_industry_history (
            snapshot_id TEXT, trade_date TEXT, as_of_exclusive TEXT,
            stock_code TEXT, industry_name TEXT, industry_type TEXT,
            source_system TEXT, source_fact_id TEXT,
            source_effective_at TEXT, source_etl_sync_at TEXT,
            row_hash TEXT, PRIMARY KEY (snapshot_id, stock_code)
        )
        """,
        """
        CREATE TABLE st_trade_account_v2 (
            account_id TEXT PRIMARY KEY, status TEXT, cash_balance NUMERIC,
            real_trading_enabled INTEGER
        )
        """,
        """
        CREATE TABLE st_equity_daily_v2 (
            account_id TEXT, trade_date TEXT, total_equity NUMERIC,
            PRIMARY KEY (account_id, trade_date)
        )
        """,
        """
        CREATE TABLE st_reconciliation_v2 (
            account_id TEXT, trade_date TEXT, version INTEGER, status TEXT,
            PRIMARY KEY (account_id, trade_date, version)
        )
        """,
        """
        CREATE TABLE si_trade_calendar (
            trade_date TEXT PRIMARY KEY, trade_status INTEGER
        )
        """,
        """
        CREATE TABLE sm_stock_kline (
            stock_code TEXT, trade_date TEXT, close NUMERIC,
            k_type INTEGER, adjust_type INTEGER,
            PRIMARY KEY (stock_code, trade_date, k_type, adjust_type)
        )
        """,
        """
        CREATE TABLE st_position_lot_v2 (
            lot_id TEXT PRIMARY KEY, account_id TEXT, stock_code TEXT,
            remaining_quantity INTEGER, protective_stop NUMERIC
        )
        """,
        """
        CREATE TABLE st_execution_plan_v3 (
            execution_plan_id TEXT PRIMARY KEY, run_uid TEXT,
            account_id TEXT, trade_date TEXT, stock_code TEXT, side TEXT,
            quantity INTEGER, limit_price NUMERIC, state TEXT,
            reason_code TEXT, source TEXT, real_order_allowed INTEGER,
            idempotency_key TEXT, created_at TEXT, updated_at TEXT
        )
        """,
        """
        CREATE TABLE st_dynamic_shadow_trial_exit_binding (
            binding_id TEXT PRIMARY KEY, chain_id TEXT, allocation_id TEXT,
            exit_order_id TEXT, exit_fill_id TEXT, allocation_fact_hash TEXT,
            exit_order_fact_hash TEXT, exit_fill_fact_hash TEXT,
            binding_payload_json TEXT, binding_hash TEXT,
            real_order_authority INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """,
    )
    for statement in statements:
        connection.exec_driver_sql(statement)


def _candidate_receipt():
    raw_candidate = {
        "trade_date": "2026-08-21",
        "data_date": "2026-08-21",
        "stock_code": "600036",
        "strategy_key": "dynamic_alpha",
        "strategy_version": "v1",
        "strategy_version_hash": "1" * 64,
        "execution_binding_hash": "2" * 64,
        "adapter_artifact_sha256": "3" * 64,
        "cost_model_hash": "4" * 64,
        "signal_direction": "BUY",
        "signal_status": "BUY_READY",
        "score": 88.0,
    }
    output_payload = {
        "schema": "probiga.strategy-candidate-output.v1",
        "trade_date": "2026-08-21",
        "strategy_key": "dynamic_alpha",
        "strategy_version": "v1",
        "execution_binding_hash": "2" * 64,
        "candidates": [raw_candidate],
    }
    payload = {
        "schema": "probiga.strategy-candidate-run-receipt.v2",
        "strategy_key": "dynamic_alpha",
        "strategy_version": "v1",
        "strategy_version_hash": "1" * 64,
        "execution_binding_hash": "2" * 64,
        "adapter_artifact_sha256": "3" * 64,
        "cost_model_hash": "4" * 64,
        "trade_date": "2026-08-21",
        "adapter_key": "dynamic.alpha",
        "adapter_version": "1.0.0",
        "run_uid": "5" * 32,
        "completed_at": "2026-08-21T15:00:00+00:00",
        "status": "COMPLETED",
        "input_hash": "6" * 64,
        "output_hash": _digest(output_payload),
        "stable_result_hash": "8" * 64,
        "candidate_count": 1,
        "candidate_identity": ["600036"],
    }
    return {**payload, "receipt_hash": _digest(payload)}


def _insert_receipt(connection, receipt):
    connection.execute(text("""
        INSERT INTO st_strategy_adapter_run_receipt VALUES (
            :run_uid, :strategy_key, :strategy_version,
            :strategy_version_hash, :execution_binding_hash,
            :adapter_artifact_sha256, :cost_model_hash, :adapter_key,
            :adapter_version, :trade_date, :completed_at, :status,
            :input_hash, :output_hash, :stable_result_hash,
            :candidate_count, :candidate_identity_json, :receipt_json,
            :receipt_hash
        )
    """), {
        **receipt,
        "completed_at": "2026-08-21 15:00:00",
        "candidate_identity_json": json.dumps(receipt["candidate_identity"]),
        "receipt_json": json.dumps(receipt, sort_keys=True),
    })


def _raw_candidate():
    return {
        "trade_date": "2026-08-21",
        "data_date": "2026-08-21",
        "stock_code": "600036",
        "strategy_key": "dynamic_alpha",
        "strategy_version": "v1",
        "strategy_version_hash": "1" * 64,
        "execution_binding_hash": "2" * 64,
        "adapter_artifact_sha256": "3" * 64,
        "cost_model_hash": "4" * 64,
        "signal_direction": "BUY",
        "signal_status": "BUY_READY",
        "score": 88.0,
    }


def _candidate_receipt_for_candidates(candidates):
    candidates = [dict(item) for item in candidates]
    base = _candidate_receipt()
    payload = {
        key: value for key, value in base.items() if key != "receipt_hash"
    }
    payload["candidate_count"] = len(candidates)
    payload["candidate_identity"] = sorted(
        str(item["stock_code"]) for item in candidates
    )
    payload["output_hash"] = _digest({
        "schema": "probiga.strategy-candidate-output.v1",
        "trade_date": payload["trade_date"],
        "strategy_key": payload["strategy_key"],
        "strategy_version": payload["strategy_version"],
        "execution_binding_hash": payload["execution_binding_hash"],
        "candidates": candidates,
    })
    return {**payload, "receipt_hash": _digest(payload)}


def _persist_candidate_batch(connection, receipt):
    persist_strategy_adapter_candidate_facts(
        connection,
        candidate_receipt=receipt,
        candidates=[_raw_candidate()],
    )


def _insert_bootstrap_prerequisites(connection):
    connection.execute(text("""
        INSERT INTO st_strategy_registry VALUES
            ('dynamic_alpha', 'v1', 'SHADOW', 1)
    """))
    connection.execute(text("""
        INSERT INTO st_strategy_version VALUES
            ('dynamic_alpha', 'v1', :version_hash, 'runtime_registry')
    """), {"version_hash": "1" * 64})
    industry_payload = {
        "snapshot_id": "a" * 64,
        "trade_date": "2026-08-21",
        "as_of_exclusive": "2026-08-22T00:00:00",
        "stock_code": "600036",
        "industry_name": "银行",
        "industry_type": "CSRC_L1",
        "source_system": "QMT",
        "source_fact_id": "industry-600036-20260821",
        "source_effective_at": "2026-08-21T15:00:00",
        "source_etl_sync_at": "2026-08-21T16:00:00",
    }
    connection.execute(text("""
        INSERT INTO st_strategy_industry_history VALUES (
            :snapshot_id, :trade_date, :as_of_exclusive, :stock_code,
            :industry_name, :industry_type, :source_system, :source_fact_id,
            :source_effective_at, :source_etl_sync_at, :row_hash
        )
    """), {**industry_payload, "row_hash": _digest(industry_payload)})
    connection.execute(text("""
        INSERT INTO st_trade_account_v2 VALUES
            ('paper-main-v2', 'ACTIVE', 200000.00, 0)
    """))
    connection.execute(text("""
        INSERT INTO st_equity_daily_v2 VALUES
            ('paper-main-v2', '2026-08-21', 200000.00)
    """))
    connection.execute(text("""
        INSERT INTO st_reconciliation_v2 VALUES
            ('paper-main-v2', '2026-08-21', 1, 'PASS')
    """))
    connection.execute(text("""
        INSERT INTO si_trade_calendar VALUES
            ('2026-08-21', 1), ('2026-08-24', 1)
    """))
    connection.execute(text("""
        INSERT INTO sm_stock_kline VALUES
            ('600036', '2026-08-21', 10.00, 1, 0)
    """))


def _insert_existing_paper_round_trip(connection, plan):
    source_run_uid = str(plan["candidate_run_uid"])
    governance_payload = {
        "schema": "probiga.governance-paper-buy-receipt.v1",
        "governance_run_uid": "g" * 32,
        "trade_date": "2026-08-21",
        "build_commit_sha": "a" * 40,
        "decision_hash": "b" * 64,
        "paper_plan_hash": "c" * 64,
        "target_hash": "d" * 64,
        "stock_code": "600036",
        "strategy_key": "dynamic_alpha",
        "strategy_version": "v1",
        "strategy_version_hash": "1" * 64,
        "strategy_source_kind": "runtime_registry",
        "target_bp": 50,
        "new_buy_allowed": True,
        "exit_always_allowed": True,
        "real_order_authority": False,
    }
    governance = {
        **governance_payload,
        "receipt_hash": _digest(governance_payload),
    }
    ownership_hash = hashlib.sha256(
        (
            f"{source_run_uid}|forecast-1|600036|dynamic_alpha|v1"
        ).encode("utf-8")
    ).hexdigest()
    intent_evidence = {
        "run_uid": source_run_uid,
        "model_version": "v3-model",
        "signal_strategy_keys": ["dynamic_alpha"],
        "supporting_strategy_keys": ["dynamic_alpha"],
        "primary_strategy_key": "dynamic_alpha",
        "primary_strategy_version": "v1",
        "primary_forecast_id": "forecast-1",
        "sample_owner_role": "PRIMARY",
        "attribution_version": "V3_PRIMARY_FORECAST_SNAPSHOT_V1",
        "ownership_hash": ownership_hash,
        "strategy_governance": governance,
        "real_trading_enabled": False,
    }
    connection.execute(text("""
        INSERT INTO st_trade_intent_v2 (
            intent_id, account_id, decision_run_uid, strategy_version,
            stock_code, action, current_quantity, target_quantity,
            target_weight, earliest_at, expires_at, limit_price,
            worst_price, initial_stop, protective_stop,
            invalidation_condition, reason_code, evidence_json,
            intent_version, idempotency_key, created_at
        ) VALUES (
            'intent-buy', 'paper-main-v2', :source_run_uid,
            'v3-config', '600036',
            'BUY', 0, 100, 0.005, '2026-08-22 09:30:00',
            '2026-08-22 15:00:00', 10.00, 10.10, 9.00, 9.00,
            'thesis invalid', 'V3_VALIDATED_POSITIVE', :evidence_json, 1,
            'intent-key', '2026-08-21 16:00:00'
        )
    """), {
        "source_run_uid": source_run_uid,
        "evidence_json": json.dumps(intent_evidence, sort_keys=True),
    })
    connection.execute(text("""
        INSERT INTO st_risk_decision_v2 VALUES (
            'intent-buy', 'APPROVED', 100, 100, 100.00, 0.005,
            0.005, 0.005, 100.00, 198999.00, '{}', NULL,
            :decision_hash, '2026-08-21 16:00:00'
        )
    """), {"decision_hash": "f" * 64})
    connection.execute(text("""
        INSERT INTO st_order_v2 VALUES (
            'order-buy', 'paper-main-v2', 'intent-buy', '600036', 'BUY',
            'LIMIT', 10.00, 100, 100, 'FILLED', NULL,
            '2026-08-22 09:30:00', '2026-08-22 15:00:00', 'order-buy-key',
            '2026-08-21 16:00:00', '2026-08-22 09:31:00'
        )
    """))
    connection.execute(text("""
        INSERT INTO st_fill_v2 VALUES (
            'fill-buy', 'order-buy', 'paper-main-v2', '600036', 'BUY', 100,
            10.00, 1000.00, 1.00, -1001.00, 'quote-buy', 'match-buy',
            'fill-buy-key', '2026-08-22 09:31:00', '2026-08-22 09:31:00'
        )
    """))
    connection.execute(text("""
        INSERT INTO st_order_v2 VALUES (
            'order-sell', 'paper-main-v2', 'intent-sell', '600036', 'SELL',
            'LIMIT', 11.00, 100, 100, 'FILLED', NULL,
            '2026-08-29 09:30:00', '2026-08-29 15:00:00', 'order-sell-key',
            '2026-08-28 16:00:00', '2026-08-29 09:31:00'
        )
    """))
    connection.execute(text("""
        INSERT INTO st_fill_v2 VALUES (
            'fill-sell', 'order-sell', 'paper-main-v2', '600036', 'SELL', 100,
            11.00, 1100.00, 1.50, 1098.50, 'quote-sell', 'match-sell',
            'fill-sell-key', '2026-08-29 09:31:00', '2026-08-29 09:31:00'
        )
    """))
    connection.execute(text("""
        INSERT INTO st_forward_trade_evidence_v3 VALUES (
            'evidence-1', 'paper-main-v2', :source_run_uid, 'forecast-1',
            'intent-buy', '600036', 'dynamic_alpha',
            'v1', 'PRIMARY', 'VERIFIED_SNAPSHOT',
            'V3_PRIMARY_FORECAST_SNAPSHOT_V1', '["dynamic_alpha"]',
            :ownership_hash, 'EXECUTED_PAPER', 'PAPER_EXECUTED_LEDGER_V1',
            'order-buy', 'fill-buy', '2026-08-22',
            '2026-08-22 09:31:00', 100, 10.00, 1000.00, 1.00, 100,
            '["fill-sell"]', '["order-sell"]', '2026-08-29 09:31:00',
            11.00, 1100.00, 1.50, 97.50, 9.74025974, -2.0, 12.0,
            'TAKE_PROFIT', 'MATURED'
        )
    """), {
        "source_run_uid": source_run_uid,
        "ownership_hash": ownership_hash,
    })
    connection.execute(text("""
        INSERT INTO st_forward_exit_allocation_v3 VALUES (
            'allocation-1', 'evidence-1', 'ATTRIBUTED', 'paper-main-v2',
            '600036', 'fill-buy', 'fill-sell', 'order-sell', 0, 100,
            1100.00, 1.50, '2026-08-29 09:31:00',
            'PAPER_FIFO_EXIT_ALLOCATION_V1'
        )
    """))


def _create_named_plan(connection, index):
    """Create one independently replayable plan for binder scale tests."""

    stock_code = f"{600100 + index:06d}"
    candidate = {**_raw_candidate(), "stock_code": stock_code}
    receipt = _candidate_receipt_for_candidates([candidate])
    receipt_payload = {
        key: value for key, value in receipt.items() if key != "receipt_hash"
    }
    receipt_payload["run_uid"] = hashlib.sha256(
        f"scheduled-run:{index}".encode("utf-8")
    ).hexdigest()[:32]
    receipt_payload["stable_result_hash"] = hashlib.sha256(
        f"scheduled-result:{index}".encode("utf-8")
    ).hexdigest()
    receipt = {
        **receipt_payload,
        "receipt_hash": _digest(receipt_payload),
    }
    _insert_receipt(connection, receipt)
    persist_strategy_adapter_candidate_facts(
        connection,
        candidate_receipt=receipt,
        candidates=[candidate],
    )
    plan = create_dynamic_shadow_trial_plan(
        connection,
        strategy={
            "strategy_key": "dynamic_alpha",
            "current_version": "v1",
            "version_hash": "1" * 64,
            "source_kind": "runtime_registry",
            "current_status": "SHADOW",
            "enabled": True,
        },
        candidate_receipt=receipt,
        candidate_signal=candidate,
        maximum_target_bp=100,
    )
    return plan


def _insert_named_paper_round_trip(connection, plan, index):
    """Insert one unique, complete V2/V3 round trip for a named plan."""

    suffix = f"scale-{index}"
    code = str(plan["stock_code"])
    run_uid = str(plan["candidate_run_uid"])
    forecast_id = f"forecast-{suffix}"
    intent_id = f"intent-buy-{suffix}"
    buy_order_id = f"order-buy-{suffix}"
    buy_fill_id = f"fill-buy-{suffix}"
    sell_order_id = f"order-sell-{suffix}"
    sell_fill_id = f"fill-sell-{suffix}"
    evidence_id = f"evidence-{suffix}"
    allocation_id = f"allocation-{suffix}"
    governance_payload = {
        "schema": "probiga.governance-paper-buy-receipt.v1",
        "governance_run_uid": f"governance-{suffix}",
        "trade_date": str(plan["trade_date"]),
        "build_commit_sha": "a" * 40,
        "decision_hash": hashlib.sha256(
            f"decision:{suffix}".encode("utf-8")
        ).hexdigest(),
        "paper_plan_hash": hashlib.sha256(
            f"paper-plan:{suffix}".encode("utf-8")
        ).hexdigest(),
        "target_hash": hashlib.sha256(
            f"target:{suffix}".encode("utf-8")
        ).hexdigest(),
        "stock_code": code,
        "strategy_key": str(plan["strategy_key"]),
        "strategy_version": str(plan["strategy_version"]),
        "strategy_version_hash": str(plan["strategy_version_hash"]),
        "strategy_source_kind": "runtime_registry",
        "target_bp": 50,
        "new_buy_allowed": True,
        "exit_always_allowed": True,
        "real_order_authority": False,
    }
    governance = {
        **governance_payload,
        "receipt_hash": _digest(governance_payload),
    }
    ownership_hash = hashlib.sha256(
        (
            f"{run_uid}|{forecast_id}|{code}|"
            f"{plan['strategy_key']}|{plan['strategy_version']}"
        ).encode("utf-8")
    ).hexdigest()
    intent_evidence = {
        "run_uid": run_uid,
        "model_version": "v3-model",
        "signal_strategy_keys": [str(plan["strategy_key"])],
        "supporting_strategy_keys": [str(plan["strategy_key"])],
        "primary_strategy_key": str(plan["strategy_key"]),
        "primary_strategy_version": str(plan["strategy_version"]),
        "primary_forecast_id": forecast_id,
        "sample_owner_role": "PRIMARY",
        "attribution_status": "VERIFIED_SNAPSHOT",
        "attribution_version": "V3_PRIMARY_FORECAST_SNAPSHOT_V1",
        "ownership_hash": ownership_hash,
        "strategy_governance": governance,
        "real_trading_enabled": False,
    }
    connection.execute(text("""
        INSERT INTO st_trade_intent_v2 (
            intent_id, account_id, decision_run_uid, strategy_version,
            stock_code, action, current_quantity, target_quantity,
            target_weight, earliest_at, expires_at, limit_price,
            worst_price, initial_stop, protective_stop,
            invalidation_condition, reason_code, evidence_json,
            intent_version, idempotency_key, created_at
        ) VALUES (
            :intent_id, 'paper-main-v2', :run_uid, 'v3-config', :stock_code,
            'BUY', 0, 100, 0.005, '2026-08-22 09:30:00',
            '2026-08-22 15:00:00', 10.00, 10.10, 9.00, 9.00,
            'thesis invalid', 'V3_VALIDATED_POSITIVE', :evidence_json, 1,
            :idempotency_key, '2026-08-21 16:00:00'
        )
    """), {
        "intent_id": intent_id,
        "run_uid": run_uid,
        "stock_code": code,
        "evidence_json": json.dumps(intent_evidence, sort_keys=True),
        "idempotency_key": f"intent-key-{suffix}",
    })
    connection.execute(text("""
        INSERT INTO st_risk_decision_v2 VALUES (
            :intent_id, 'APPROVED', 100, 100, 100.00, 0.005,
            0.005, 0.005, 100.00, 198999.00, '{}', NULL,
            :decision_hash, '2026-08-21 16:00:00'
        )
    """), {
        "intent_id": intent_id,
        "decision_hash": hashlib.sha256(
            f"risk:{suffix}".encode("utf-8")
        ).hexdigest(),
    })
    connection.execute(text("""
        INSERT INTO st_order_v2 VALUES (
            :order_id, 'paper-main-v2', :intent_id, :stock_code, 'BUY',
            'LIMIT', 10.00, 100, 100, 'FILLED', NULL,
            '2026-08-22 09:30:00', '2026-08-22 15:00:00', :key,
            '2026-08-21 16:00:00', '2026-08-22 09:31:00'
        )
    """), {
        "order_id": buy_order_id, "intent_id": intent_id,
        "stock_code": code, "key": f"buy-order-key-{suffix}",
    })
    connection.execute(text("""
        INSERT INTO st_fill_v2 VALUES (
            :fill_id, :order_id, 'paper-main-v2', :stock_code, 'BUY', 100,
            10.00, 1000.00, 1.00, -1001.00, :quote, :match, :key,
            '2026-08-22 09:31:00', '2026-08-22 09:31:00'
        )
    """), {
        "fill_id": buy_fill_id, "order_id": buy_order_id,
        "stock_code": code, "quote": f"buy-quote-{suffix}",
        "match": f"buy-match-{suffix}", "key": f"buy-fill-key-{suffix}",
    })
    connection.execute(text("""
        INSERT INTO st_order_v2 VALUES (
            :order_id, 'paper-main-v2', :intent_id, :stock_code, 'SELL',
            'LIMIT', 11.00, 100, 100, 'FILLED', NULL,
            '2026-08-29 09:30:00', '2026-08-29 15:00:00', :key,
            '2026-08-28 16:00:00', '2026-08-29 09:31:00'
        )
    """), {
        "order_id": sell_order_id, "intent_id": f"intent-sell-{suffix}",
        "stock_code": code, "key": f"sell-order-key-{suffix}",
    })
    connection.execute(text("""
        INSERT INTO st_fill_v2 VALUES (
            :fill_id, :order_id, 'paper-main-v2', :stock_code, 'SELL', 100,
            11.00, 1100.00, 1.50, 1098.50, :quote, :match, :key,
            '2026-08-29 09:31:00', '2026-08-29 09:31:00'
        )
    """), {
        "fill_id": sell_fill_id, "order_id": sell_order_id,
        "stock_code": code, "quote": f"sell-quote-{suffix}",
        "match": f"sell-match-{suffix}", "key": f"sell-fill-key-{suffix}",
    })
    connection.execute(text("""
        INSERT INTO st_forward_trade_evidence_v3 VALUES (
            :evidence_id, 'paper-main-v2', :run_uid, :forecast_id,
            :intent_id, :stock_code, :strategy_key, :strategy_version,
            'PRIMARY', 'VERIFIED_SNAPSHOT',
            'V3_PRIMARY_FORECAST_SNAPSHOT_V1', :supporting_keys,
            :ownership_hash, 'EXECUTED_PAPER', 'PAPER_EXECUTED_LEDGER_V1',
            :entry_order_id, :entry_fill_id, '2026-08-22',
            '2026-08-22 09:31:00', 100, 10.00, 1000.00, 1.00, 100,
            :exit_fill_ids, :exit_order_ids, '2026-08-29 09:31:00',
            11.00, 1100.00, 1.50, 97.50, 9.74025974, -2.0, 12.0,
            'TAKE_PROFIT', 'MATURED'
        )
    """), {
        "evidence_id": evidence_id, "run_uid": run_uid,
        "forecast_id": forecast_id, "intent_id": intent_id,
        "stock_code": code, "strategy_key": str(plan["strategy_key"]),
        "strategy_version": str(plan["strategy_version"]),
        "supporting_keys": json.dumps([str(plan["strategy_key"])]),
        "ownership_hash": ownership_hash, "entry_order_id": buy_order_id,
        "entry_fill_id": buy_fill_id,
        "exit_fill_ids": json.dumps([sell_fill_id]),
        "exit_order_ids": json.dumps([sell_order_id]),
    })
    connection.execute(text("""
        INSERT INTO st_forward_exit_allocation_v3 VALUES (
            :allocation_id, :evidence_id, 'ATTRIBUTED', 'paper-main-v2',
            :stock_code, :entry_fill_id, :exit_fill_id, :exit_order_id,
            0, 100, 1100.00, 1.50, '2026-08-29 09:31:00',
            'PAPER_FIFO_EXIT_ALLOCATION_V1'
        )
    """), {
        "allocation_id": allocation_id, "evidence_id": evidence_id,
        "stock_code": code, "entry_fill_id": buy_fill_id,
        "exit_fill_id": sell_fill_id, "exit_order_id": sell_order_id,
    })
    return evidence_id


def test_dynamic_shadow_trial_reuses_exact_v2_v3_ledger_end_to_end():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        plan = create_dynamic_shadow_trial_plan(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
            candidate_signal=_raw_candidate(),
            maximum_target_bp=100,
        )
        assert plan["real_order_authority"] is False
        _insert_existing_paper_round_trip(connection, plan)
        completed = bind_dynamic_shadow_trial_to_existing_paper_evidence(
            connection,
            plan_id=plan["plan_id"],
            forward_evidence_id="evidence-1",
        )
        assert completed["status"] == "VERIFIED_MATURED_INTERNAL_PAPER_CHAIN"
        assert completed["exit_binding_count"] == 1
        assert completed["automatic_real_order_submission"] is False
        replay = verify_dynamic_shadow_trial(connection, plan["plan_id"])
        assert replay["chain_hash"] == completed["chain_hash"]
        public_replay = verify_dynamic_shadow_ledger_chain(
            {
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
            },
            candidate_receipt=receipt,
            connection=connection,
            plan_id=plan["plan_id"],
        )
        assert public_replay["chain_hash"] == completed["chain_hash"]
        readiness = dynamic_shadow_ledger_readiness(
            connection=connection,
            strategy_key="dynamic_alpha",
            strategy_version="v1",
            strategy_version_hash="1" * 64,
            execution_binding_hash="2" * 64,
        )
        assert readiness["funding_pipeline_ready"] is True
        assert readiness["verified_forward_evidence_ready"] is True
        assert readiness["verified_chain_count"] == 1
        assert readiness["invalid_chain_count"] == 0


def test_production_producers_create_bounded_plan_then_bind_matured_chain():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        produced = create_dynamic_shadow_trial_plans_from_candidate_facts(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
            maximum_target_bp=100,
        )
        assert produced["plan_count"] == 1
        assert produced["maximum_target_bp"] == 100
        assert produced["automatic_real_order_submission"] is False
        plan = verify_dynamic_shadow_trial_plan(
            connection,
            produced["plan_ids"][0],
        )
        assert plan["candidate_signal"]["candidate"] == _raw_candidate()
        _insert_existing_paper_round_trip(connection, plan)

    binding = bind_pending_dynamic_shadow_trials(engine)
    assert binding["status"] == "OK"
    assert binding["bound_plan_count"] == 1
    assert binding["automatic_real_order_submission"] is False
    assert binding["real_order_authority"] is False
    with engine.connect() as connection:
        verified = verify_dynamic_shadow_trial(
            connection,
            produced["plan_ids"][0],
        )
        assert verified["forward_evidence_id"] == "evidence-1"


def test_pending_binder_does_not_prefetch_plans_without_exact_evidence():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        _create_named_plan(connection, 0)

    selected = []

    def capture_select(_conn, _cursor, statement, _params, _context, _many):
        if statement.lstrip().upper().startswith("SELECT"):
            selected.append(" ".join(statement.lower().split()))

    event.listen(engine, "before_cursor_execute", capture_select)
    try:
        binding = bind_pending_dynamic_shadow_trials(engine)
    finally:
        event.remove(engine, "before_cursor_execute", capture_select)

    assert binding["status"] == "OK"
    assert binding["processed_plan_count"] == 0
    assert binding["bound_plan_count"] == 0
    assert binding["pending_plan_count"] == 1
    assert binding["unmatched_pending_plan_count"] == 1
    assert binding["remaining_unbound_plan_count"] == 1
    assert binding["still_pending_plan_ids"] == []
    assert len(selected) == 2
    for table_name in (
        "st_strategy_adapter_run_receipt",
        "st_strategy_adapter_candidate_fact",
        "st_trade_intent_v2",
        "st_risk_decision_v2",
        "st_order_v2",
        "st_fill_v2",
        "st_forward_exit_allocation_v3",
        "st_strategy_industry_history",
    ):
        assert all(table_name not in statement for statement in selected)


def test_pending_binder_caps_duplicate_exact_industry_rows_before_loading():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        plan = _create_named_plan(connection, 0)
        _insert_named_paper_round_trip(connection, plan, 0)
        connection.execute(text("""
            INSERT INTO st_strategy_industry_history VALUES (
                :snapshot_id, :trade_date, '2026-08-22 00:00:00',
                :stock_code, '银行', 'CSRC_L1', 'QMT', :source_fact_id,
                '2026-08-21 15:00:00', '2026-08-21 16:00:00', :row_hash
            )
        """), [
            {
                "snapshot_id": hashlib.sha256(
                    f"duplicate-snapshot:{index}".encode("utf-8")
                ).hexdigest(),
                "trade_date": str(plan["trade_date"]),
                "stock_code": str(plan["stock_code"]),
                "source_fact_id": f"duplicate-industry:{index}",
                "row_hash": hashlib.sha256(
                    f"duplicate-row:{index}".encode("utf-8")
                ).hexdigest(),
            }
            for index in range(2)
        ])
    selected = []

    def capture_select(_conn, _cursor, statement, _params, _context, _many):
        if statement.lstrip().upper().startswith("SELECT"):
            selected.append(" ".join(statement.lower().split()))

    event.listen(engine, "before_cursor_execute", capture_select)
    try:
        binding = bind_pending_dynamic_shadow_trials(engine)
    finally:
        event.remove(engine, "before_cursor_execute", capture_select)

    industry_selects = [
        statement for statement in selected
        if "from st_strategy_industry_history" in statement
    ]
    assert binding["status"] == "UNAVAILABLE_OR_INVALID"
    assert len(industry_selects) == 1
    assert industry_selects[0].startswith("select count(*)")
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_dynamic_shadow_trial_chain"
        )).scalar() == 0


def test_more_than_five_hundred_old_unmatched_plans_do_not_starve_mature_plan():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    stale_count = 501
    with engine.begin() as connection:
        _schema(connection)
        mature_plan = _create_named_plan(connection, 0)
        evidence_id = _insert_named_paper_round_trip(
            connection, mature_plan, 0,
        )
        connection.execute(text("""
            INSERT INTO st_dynamic_shadow_trial_plan (
                plan_id, candidate_run_uid, candidate_receipt_hash,
                strategy_key, strategy_version, strategy_version_hash,
                execution_binding_hash, trade_date, stock_code, account_id,
                maximum_target_bp, candidate_fact_hash,
                candidate_signal_json, candidate_signal_hash,
                plan_payload_json, plan_hash, plan_status,
                automatic_real_order_submission, real_order_authority,
                created_at
            ) VALUES (
                :plan_id, :candidate_run_uid, :candidate_receipt_hash,
                'stale_dynamic', 'v1', :strategy_version_hash,
                :execution_binding_hash, '2000-01-01', '000001',
                'paper-main-v2', 1, :candidate_fact_hash, '{}',
                :candidate_signal_hash, '{}', :plan_hash,
                'PLANNED_SHADOW_TRIAL', 0, 0, '2000-01-01 00:00:00'
            )
        """), [
            {
                "plan_id": hashlib.sha256(
                    f"stale-plan:{index}".encode("utf-8")
                ).hexdigest(),
                "candidate_run_uid": f"{index + 1000:032x}",
                "candidate_receipt_hash": hashlib.sha256(
                    f"stale-receipt:{index}".encode("utf-8")
                ).hexdigest(),
                "strategy_version_hash": "1" * 64,
                "execution_binding_hash": "2" * 64,
                "candidate_fact_hash": hashlib.sha256(
                    f"stale-fact:{index}".encode("utf-8")
                ).hexdigest(),
                "candidate_signal_hash": hashlib.sha256(
                    f"stale-signal:{index}".encode("utf-8")
                ).hexdigest(),
                "plan_hash": hashlib.sha256(
                    f"stale-payload:{index}".encode("utf-8")
                ).hexdigest(),
            }
            for index in range(stale_count)
        ])

    binding = bind_pending_dynamic_shadow_trials(engine)

    assert binding["status"] == "OK"
    assert binding["processed_plan_ids"] == [mature_plan["plan_id"]]
    assert binding["processed_plan_count"] == 1
    assert binding["bound_plan_count"] == 1
    assert binding["bound"][0]["forward_evidence_id"] == evidence_id
    assert binding["unmatched_pending_plan_count"] == stale_count
    assert binding["remaining_unbound_plan_count"] == stale_count
    assert binding["pending_plan_count"] == stale_count
    assert binding["still_pending_plan_ids"] == []


def test_pending_binder_select_count_is_constant_for_one_and_many_chains():
    def run_binding(chain_count):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with engine.begin() as connection:
            _schema(connection)
            for index in range(chain_count):
                plan = _create_named_plan(connection, index)
                _insert_named_paper_round_trip(connection, plan, index)
        selected = []

        def capture_select(_conn, _cursor, statement, _params, _context, _many):
            if statement.lstrip().upper().startswith("SELECT"):
                selected.append(" ".join(statement.lower().split()))

        event.listen(engine, "before_cursor_execute", capture_select)
        try:
            result = bind_pending_dynamic_shadow_trials(engine)
        finally:
            event.remove(engine, "before_cursor_execute", capture_select)
        return result, selected

    one, one_selects = run_binding(1)
    many, many_selects = run_binding(7)

    assert one["status"] == "OK"
    assert many["status"] == "OK"
    assert one["bound_plan_count"] == 1
    assert many["bound_plan_count"] == 7
    assert len(one_selects) == len(many_selects)
    assert len(one_selects) == 14


def test_pending_binder_savepoint_rolls_back_partial_chain_on_exit_failure():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        plan = _create_named_plan(connection, 0)
        _insert_named_paper_round_trip(connection, plan, 0)
        connection.exec_driver_sql("""
            CREATE TRIGGER reject_dynamic_exit_binding
            BEFORE INSERT ON st_dynamic_shadow_trial_exit_binding
            BEGIN
                SELECT RAISE(ABORT, 'injected exit binding failure');
            END
        """)

    binding = bind_pending_dynamic_shadow_trials(engine)

    assert binding["status"] == "INVALID"
    assert binding["processed_plan_count"] == 1
    assert binding["bound_plan_count"] == 0
    assert binding["rejected_plan_count"] == 1
    assert binding["pending_plan_count"] == 1
    assert binding["remaining_unbound_plan_count"] == 1
    assert binding["still_pending_plan_ids"] == [plan["plan_id"]]
    with engine.connect() as connection:
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_dynamic_shadow_trial_chain"
        )).scalar() == 0
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_dynamic_shadow_trial_exit_binding"
        )).scalar() == 0


@pytest.mark.parametrize("lifecycle", ["ACTIVE", "REDUCE"])
def test_bootstrap_plan_producer_rejects_non_shadow_lifecycle(lifecycle):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        with pytest.raises(
            DynamicShadowLedgerError,
            match="只有启用且处于影子观察",
        ):
            create_dynamic_shadow_trial_plan(
                connection,
                strategy={
                    "strategy_key": "dynamic_alpha",
                    "current_version": "v1",
                    "version_hash": "1" * 64,
                    "source_kind": "runtime_registry",
                    "current_status": lifecycle,
                    "enabled": True,
                },
                candidate_receipt=receipt,
                candidate_signal=_raw_candidate(),
                maximum_target_bp=100,
            )
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_dynamic_shadow_trial_plan"
        )).scalar() == 0


def test_bootstrap_shadow_lane_reaches_matured_readiness_from_zero_chain():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        _insert_bootstrap_prerequisites(connection)
        produced = create_dynamic_shadow_trial_plans_from_candidate_facts(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
            maximum_target_bp=100,
        )
        before = dynamic_shadow_ledger_readiness(
            connection=connection,
            strategy_key="dynamic_alpha",
            strategy_version="v1",
            strategy_version_hash="1" * 64,
            execution_binding_hash="2" * 64,
        )
        assert before["funding_pipeline_ready"] is False
        identity = ("dynamic_alpha", "v1", "1" * 64, "2" * 64)
        batch_before = batch_dynamic_shadow_ledger_readiness(
            connection, [identity],
        )[identity]
        assert batch_before["status"] == "VERIFIED_PENDING"
        bootstrap = materialize_dynamic_shadow_bootstrap_orders(
            connection,
            plan_ids=produced["plan_ids"],
        )
        assert bootstrap["paper_order_count"] == 1
        assert bootstrap["real_order_count"] == 0
        created = bootstrap["created"][0]
        assert created["maximum_target_bp"] == 100
        assert created["quantity"] == 100
        assert created["real_order_allowed"] is False
        assert created["industry_snapshot_id"] == "a" * 64
        order = dict(connection.execute(text("""
            SELECT o.*, i.strategy_version
            FROM st_order_v2 o
            JOIN st_trade_intent_v2 i ON i.intent_id=o.intent_id
            WHERE o.order_id=:order_id
        """), {"order_id": created["order_id"]}).mappings().one())
        gate = _execution_buy_gate_decision(
            connection,
            order=order,
            now=datetime(2026, 8, 24, 9, 31),
        )
        assert gate.allowed is True
        connection.execute(text("""
            UPDATE st_risk_decision_v2 SET approved_quantity=99
            WHERE intent_id=:intent_id
        """), {"intent_id": created["intent_id"]})
        tampered_gate = _execution_buy_gate_decision(
            connection,
            order=order,
            now=datetime(2026, 8, 24, 9, 31),
        )
        assert tampered_gate.allowed is False
        assert tampered_gate.reason_code == "DYNAMIC_SHADOW_BOOTSTRAP_INVALID"
        connection.execute(text("""
            UPDATE st_risk_decision_v2 SET approved_quantity=100
            WHERE intent_id=:intent_id
        """), {"intent_id": created["intent_id"]})
        execution_plan = connection.execute(text("""
            SELECT source, real_order_allowed
            FROM st_execution_plan_v3
            WHERE execution_plan_id=:execution_plan_id
        """), {
            "execution_plan_id": created["execution_plan_id"],
        }).mappings().one()
        assert execution_plan["source"] == "DYNAMIC_SHADOW_BOOTSTRAP"
        assert int(execution_plan["real_order_allowed"]) == 0

        connection.execute(text("""
            UPDATE st_order_v2
            SET status='FILLED', filled_quantity=quantity,
                waiting_reason=NULL, updated_at='2026-08-24 09:31:00'
            WHERE order_id=:order_id
        """), {"order_id": created["order_id"]})
        connection.execute(text("""
            INSERT INTO st_fill_v2 VALUES (
                'bootstrap-buy-fill', :order_id, 'paper-main-v2',
                '600036', 'BUY', 100, 10.00, 1000.00, 1.00, -1001.00,
                'bootstrap-buy-quote', 'bootstrap-buy-match',
                'bootstrap-buy-fill-key', '2026-08-24 09:31:00',
                '2026-08-24 09:31:00'
            )
        """), {"order_id": created["order_id"]})
        connection.execute(text("""
            INSERT INTO st_order_v2 VALUES (
                'bootstrap-sell-order', 'paper-main-v2', 'bootstrap-exit',
                '600036', 'SELL', 'LIMIT', 11.00, 100, 100, 'FILLED',
                NULL, '2026-08-31 09:30:00', '2026-08-31 14:45:00',
                'bootstrap-sell-order-key', '2026-08-30 16:00:00',
                '2026-08-31 09:31:00'
            )
        """))
        connection.execute(text("""
            INSERT INTO st_fill_v2 VALUES (
                'bootstrap-sell-fill', 'bootstrap-sell-order',
                'paper-main-v2', '600036', 'SELL', 100, 11.00, 1100.00,
                1.50, 1098.50, 'bootstrap-sell-quote',
                'bootstrap-sell-match', 'bootstrap-sell-fill-key',
                '2026-08-31 09:31:00', '2026-08-31 09:31:00'
            )
        """))
        intent = connection.execute(text("""
            SELECT evidence_json FROM st_trade_intent_v2
            WHERE intent_id=:intent_id
        """), {"intent_id": created["intent_id"]}).mappings().one()
        intent_evidence = json.loads(intent["evidence_json"])
        connection.execute(text("""
            INSERT INTO st_forward_trade_evidence_v3 VALUES (
                'bootstrap-evidence', 'paper-main-v2', :source_run_uid,
                :source_forecast_id, :source_intent_id, '600036',
                'dynamic_alpha', 'v1', 'PRIMARY', 'VERIFIED_SNAPSHOT',
                'V3_PRIMARY_FORECAST_SNAPSHOT_V1', '["dynamic_alpha"]',
                :ownership_hash, 'EXECUTED_PAPER',
                'PAPER_EXECUTED_LEDGER_V1', :entry_order_id,
                'bootstrap-buy-fill', '2026-08-24',
                '2026-08-24 09:31:00', 100, 10.00, 1000.00, 1.00, 100,
                '["bootstrap-sell-fill"]', '["bootstrap-sell-order"]',
                '2026-08-31 09:31:00', 11.00, 1100.00, 1.50, 97.50,
                9.74025974, -2.0, 12.0, 'TAKE_PROFIT', 'MATURED'
            )
        """), {
            "source_run_uid": receipt["run_uid"],
            "source_forecast_id": intent_evidence["primary_forecast_id"],
            "source_intent_id": created["intent_id"],
            "ownership_hash": intent_evidence["ownership_hash"],
            "entry_order_id": created["order_id"],
        })
        connection.execute(text("""
            INSERT INTO st_forward_exit_allocation_v3 VALUES (
                'bootstrap-allocation', 'bootstrap-evidence', 'ATTRIBUTED',
                'paper-main-v2', '600036', 'bootstrap-buy-fill',
                'bootstrap-sell-fill', 'bootstrap-sell-order', 0, 100,
                1100.00, 1.50, '2026-08-31 09:31:00',
                'PAPER_FIFO_EXIT_ALLOCATION_V1'
            )
        """))
    scheduled_binding = bind_pending_dynamic_shadow_trials(engine)
    assert scheduled_binding["status"] == "OK"
    assert scheduled_binding["bound_plan_count"] == 1
    assert scheduled_binding["bound"][0]["forward_evidence_id"] == (
        "bootstrap-evidence"
    )
    with engine.connect() as connection:
        verified = verify_dynamic_shadow_trial(
            connection,
            produced["plan_ids"][0],
        )
        assert verified["status"] == "VERIFIED_MATURED_INTERNAL_PAPER_CHAIN"
        after = dynamic_shadow_ledger_readiness(
            connection=connection,
            strategy_key="dynamic_alpha",
            strategy_version="v1",
            strategy_version_hash="1" * 64,
            execution_binding_hash="2" * 64,
        )
        assert after["funding_pipeline_ready"] is True
        assert after["verified_chain_count"] == 1
        batch_after = batch_dynamic_shadow_ledger_readiness(
            connection, [identity],
        )[identity]
        assert batch_after["status"] == "VERIFIED"
        assert batch_after["funding_pipeline_ready"] is True
        assert batch_after["real_order_authority"] is False


def test_invalid_first_twenty_plans_do_not_hide_later_valid_plan():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        _insert_bootstrap_prerequisites(connection)
        produced = create_dynamic_shadow_trial_plans_from_candidate_facts(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
        )
        result = materialize_dynamic_shadow_bootstrap_orders(
            connection,
            plan_ids=[
                *(f"missing-plan-{index}" for index in range(20)),
                produced["plan_ids"][0],
            ],
        )
        assert result["paper_order_count"] == 1
        assert result["new_paper_order_count"] == 1
        assert result["scanned_plan_count"] == 21
        assert result["deferred_plan_count"] == 0
        assert len(result["skipped"]) == 20
        assert result["created"][0]["plan_id"] == produced["plan_ids"][0]
        assert result["real_order_count"] == 0
        assert result["real_order_authority"] is False


def test_bootstrap_buy_is_not_created_without_exact_industry_history():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        _insert_bootstrap_prerequisites(connection)
        connection.execute(text("DELETE FROM st_strategy_industry_history"))
        produced = create_dynamic_shadow_trial_plans_from_candidate_facts(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
        )
        bootstrap = materialize_dynamic_shadow_bootstrap_orders(
            connection,
            plan_ids=produced["plan_ids"],
        )
        assert bootstrap["paper_order_count"] == 0
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_order_v2"
        )).scalar() == 0
        assert "行业" in bootstrap["skipped"][0]["reason"]


def test_bootstrap_risk_rejection_is_frozen_and_idempotently_non_executable():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        _insert_bootstrap_prerequisites(connection)
        connection.execute(text("""
            UPDATE st_trade_account_v2 SET cash_balance=0
            WHERE account_id='paper-main-v2'
        """))
        produced = create_dynamic_shadow_trial_plans_from_candidate_facts(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
        )
        first = materialize_dynamic_shadow_bootstrap_orders(
            connection,
            plan_ids=produced["plan_ids"],
        )
        assert first["paper_order_count"] == 0
        risk = connection.execute(text("""
            SELECT decision_status, first_failure
            FROM st_risk_decision_v2
        """)).mappings().one()
        assert risk["decision_status"] == "REJECTED"
        assert risk["first_failure"] == "CASH_AVAILABLE"
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_trade_intent_v2"
        )).scalar() == 1
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_order_v2"
        )).scalar() == 0

        replay = materialize_dynamic_shadow_bootstrap_orders(
            connection,
            plan_ids=produced["plan_ids"],
        )
        assert replay["paper_order_count"] == 0
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_trade_intent_v2"
        )).scalar() == 1
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_risk_decision_v2"
        )).scalar() == 1
        assert connection.execute(text(
            "SELECT COUNT(*) FROM st_order_v2"
        )).scalar() == 0


def test_dynamic_shadow_exit_is_never_blocked_by_bootstrap_buy_gate():
    decision = _execution_buy_gate_decision(
        None,
        order={"side": "SELL"},
        now=datetime(2026, 8, 24, 9, 31),
    )
    assert decision.allowed is True


def test_old_version_chain_never_qualifies_new_strategy_version():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        plan = create_dynamic_shadow_trial_plan(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
            candidate_signal=_raw_candidate(),
        )
        _insert_existing_paper_round_trip(connection, plan)
        bind_dynamic_shadow_trial_to_existing_paper_evidence(
            connection,
            plan_id=plan["plan_id"],
            forward_evidence_id="evidence-1",
        )
        old_version = dynamic_shadow_ledger_readiness(
            connection=connection,
            strategy_key="dynamic_alpha",
            strategy_version="v1",
            strategy_version_hash="1" * 64,
            execution_binding_hash="2" * 64,
        )
        new_version = dynamic_shadow_ledger_readiness(
            connection=connection,
            strategy_key="dynamic_alpha",
            strategy_version="v2",
            strategy_version_hash="9" * 64,
            execution_binding_hash="8" * 64,
        )
        assert old_version["funding_pipeline_ready"] is True
        assert old_version["verified_chain_count"] == 1
        assert new_version["status"] == "VERIFIED_EMPTY"
        assert new_version["funding_pipeline_ready"] is False
        assert new_version["verified_chain_count"] == 0


def test_cross_version_forward_evidence_cannot_bind_plan():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        plan = create_dynamic_shadow_trial_plan(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
            candidate_signal=_raw_candidate(),
        )
        _insert_existing_paper_round_trip(connection, plan)
        connection.execute(text("""
            UPDATE st_forward_trade_evidence_v3
            SET strategy_version='v0'
            WHERE evidence_id='evidence-1'
        """))
        with pytest.raises(
            DynamicShadowLedgerError,
            match="前向证据",
        ):
            bind_dynamic_shadow_trial_to_existing_paper_evidence(
                connection,
                plan_id=plan["plan_id"],
                forward_evidence_id="evidence-1",
            )


@pytest.mark.parametrize(
    ("column", "value"),
    (
        ("sample_owner_role", "SUPPORTING"),
        ("attribution_status", "LEGACY_VERSION_DERIVED"),
        ("attribution_version", "V3_PRIMARY_FORECAST_SNAPSHOT_V0"),
        ("ownership_hash", "0" * 64),
    ),
)
def test_forward_evidence_owner_snapshot_tamper_cannot_bind_plan(
    column,
    value,
):
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        plan = create_dynamic_shadow_trial_plan(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
            candidate_signal=_raw_candidate(),
        )
        _insert_existing_paper_round_trip(connection, plan)
        connection.execute(text(
            f"UPDATE st_forward_trade_evidence_v3 SET {column}=:value "
            "WHERE evidence_id='evidence-1'"
        ), {"value": value})
        with pytest.raises(DynamicShadowLedgerError):
            bind_dynamic_shadow_trial_to_existing_paper_evidence(
                connection,
                plan_id=plan["plan_id"],
                forward_evidence_id="evidence-1",
            )


def test_intent_frozen_primary_strategy_version_tamper_cannot_bind_plan():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        plan = create_dynamic_shadow_trial_plan(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
            candidate_signal=_raw_candidate(),
        )
        _insert_existing_paper_round_trip(connection, plan)
        evidence = json.loads(connection.execute(text("""
            SELECT evidence_json FROM st_trade_intent_v2
            WHERE intent_id='intent-buy'
        """)).scalar())
        evidence["primary_strategy_version"] = "v0"
        connection.execute(text("""
            UPDATE st_trade_intent_v2 SET evidence_json=:evidence_json
            WHERE intent_id='intent-buy'
        """), {"evidence_json": json.dumps(evidence, sort_keys=True)})
        with pytest.raises(DynamicShadowLedgerError):
            bind_dynamic_shadow_trial_to_existing_paper_evidence(
                connection,
                plan_id=plan["plan_id"],
                forward_evidence_id="evidence-1",
            )


def test_dynamic_shadow_trial_fails_closed_after_underlying_fill_tamper():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        plan = create_dynamic_shadow_trial_plan(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
            candidate_signal=_raw_candidate(),
        )
        _insert_existing_paper_round_trip(connection, plan)
        bind_dynamic_shadow_trial_to_existing_paper_evidence(
            connection,
            plan_id=plan["plan_id"],
            forward_evidence_id="evidence-1",
        )
        connection.execute(text(
            "UPDATE st_fill_v2 SET price=99 WHERE fill_id='fill-buy'"
        ))
        with pytest.raises(DynamicShadowLedgerError, match="哈希复算失败"):
            verify_dynamic_shadow_trial(connection, plan["plan_id"])
        readiness = dynamic_shadow_ledger_readiness(
            connection=connection,
            strategy_key="dynamic_alpha",
            strategy_version="v1",
            strategy_version_hash="1" * 64,
            execution_binding_hash="2" * 64,
        )
        assert readiness["funding_pipeline_ready"] is False
        assert readiness["verified_forward_evidence_ready"] is False
        assert readiness["invalid_chain_count"] == 1


def test_cross_candidate_run_forward_evidence_cannot_bind_plan():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        plan = create_dynamic_shadow_trial_plan(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
            candidate_signal=_raw_candidate(),
        )
        _insert_existing_paper_round_trip(connection, plan)
        other_run = "other-candidate-run"
        ownership_hash = hashlib.sha256(
            (
                f"{other_run}|forecast-1|600036|dynamic_alpha|v1"
            ).encode("utf-8")
        ).hexdigest()
        evidence = connection.execute(text("""
            SELECT evidence_json FROM st_trade_intent_v2
            WHERE intent_id='intent-buy'
        """)).mappings().one()
        payload = json.loads(evidence["evidence_json"])
        payload["run_uid"] = other_run
        payload["ownership_hash"] = ownership_hash
        connection.execute(text("""
            UPDATE st_trade_intent_v2
            SET decision_run_uid=:run_uid, evidence_json=:evidence_json
            WHERE intent_id='intent-buy'
        """), {
            "run_uid": other_run,
            "evidence_json": json.dumps(payload, sort_keys=True),
        })
        connection.execute(text("""
            UPDATE st_forward_trade_evidence_v3
            SET source_run_uid=:run_uid, ownership_hash=:ownership_hash
            WHERE evidence_id='evidence-1'
        """), {
            "run_uid": other_run,
            "ownership_hash": ownership_hash,
        })

        with pytest.raises(DynamicShadowLedgerError):
            bind_dynamic_shadow_trial_to_existing_paper_evidence(
                connection,
                plan_id=plan["plan_id"],
                forward_evidence_id="evidence-1",
            )


def test_dynamic_shadow_trial_rejects_forged_or_tampered_candidate_row():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        strategy = {
            "strategy_key": "dynamic_alpha",
            "current_version": "v1",
            "version_hash": "1" * 64,
            "source_kind": "runtime_registry",
            "current_status": "SHADOW",
            "enabled": True,
        }
        forged = {**_raw_candidate(), "caller_injected_rank": 1}
        with pytest.raises(DynamicShadowLedgerError, match="禁止注入或改写字段"):
            create_dynamic_shadow_trial_plan(
                connection,
                strategy=strategy,
                candidate_receipt=receipt,
                candidate_signal=forged,
            )
        plan = create_dynamic_shadow_trial_plan(
            connection,
            strategy=strategy,
            candidate_receipt=receipt,
            candidate_signal=_raw_candidate(),
        )
        tampered = {**_raw_candidate(), "score": 99.0}
        connection.execute(text("""
            UPDATE st_strategy_adapter_candidate_fact
            SET candidate_json=:candidate_json
            WHERE candidate_run_uid=:run_uid AND stock_code='600036'
        """), {
            "candidate_json": json.dumps(tampered, sort_keys=True),
            "run_uid": receipt["run_uid"],
        })
        with pytest.raises(DynamicShadowLedgerError, match="候选事实身份或哈希"):
            verify_dynamic_shadow_trial_plan(connection, plan["plan_id"])


def test_dynamic_shadow_schema_has_exact_foreign_keys_and_no_real_authority():
    sql = "\n".join(dynamic_shadow_ledger_ddl_statements()).lower()
    assert DYNAMIC_SHADOW_LEDGER_TABLE_NAMES == (
        "st_strategy_adapter_candidate_fact",
        "st_dynamic_shadow_trial_plan",
        "st_dynamic_shadow_trial_chain",
        "st_dynamic_shadow_trial_exit_binding",
    )
    for target in (
        "st_strategy_adapter_run_receipt (run_uid)",
        "st_strategy_adapter_run_receipt (receipt_hash)",
        "st_trade_intent_v2 (intent_id)",
        "st_order_v2 (order_id)",
        "st_fill_v2 (fill_id)",
        "st_forward_trade_evidence_v3 (evidence_id)",
        "st_forward_exit_allocation_v3 (allocation_id)",
    ):
        assert f"references {target}" in sql
    assert "check (account_id = 'paper-main-v2')" in sql
    assert "risk_decision_fact_hash char(64) not null" in sql
    assert "constraint fk_dynamic_shadow_chain_risk" in sql
    assert sql.count("real_order_authority tinyint(1) not null default 0") == 3
    assert "real_order_authority = 1" not in sql


def test_empty_shadow_tables_do_not_claim_funding_readiness():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    with engine.begin() as connection:
        _schema(connection)
        readiness = dynamic_shadow_ledger_readiness(
            connection=connection,
            strategy_key="dynamic_alpha",
            strategy_version="v1",
            strategy_version_hash="1" * 64,
            execution_binding_hash="2" * 64,
        )
        assert readiness["status"] == "VERIFIED_EMPTY"
        assert readiness["schema_readable"] is True
        assert readiness["shadow_trial_producer_ready"] is True
        assert readiness["funding_pipeline_ready"] is False
        assert readiness["verified_forward_evidence_ready"] is False


def test_batch_readiness_distinguishes_empty_pending_mature_and_invalid():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    identity = ("dynamic_alpha", "v1", "1" * 64, "2" * 64)
    with engine.begin() as connection:
        _schema(connection)
        connection.execute(text("""
            INSERT INTO st_strategy_registry VALUES
                ('dynamic_alpha', 'v1', 'SHADOW', 1)
        """))
        connection.execute(text("""
            INSERT INTO st_strategy_version VALUES
                ('dynamic_alpha', 'v1', :version_hash, 'runtime_registry')
        """), {"version_hash": "1" * 64})
        empty = batch_dynamic_shadow_ledger_readiness(
            connection, [identity],
        )[identity]
        assert empty["status"] == "VERIFIED_EMPTY"
        assert empty["shadow_trial_producer_ready"] is True
        assert empty["funding_pipeline_ready"] is False

        receipt = _candidate_receipt()
        _insert_receipt(connection, receipt)
        _persist_candidate_batch(connection, receipt)
        plan = create_dynamic_shadow_trial_plan(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
            candidate_signal=_raw_candidate(),
        )
        pending = batch_dynamic_shadow_ledger_readiness(
            connection, [identity],
        )[identity]
        assert pending["status"] == "VERIFIED_PENDING"
        assert pending["pending_plan_count"] == 1
        assert pending["funding_pipeline_ready"] is False

        _insert_existing_paper_round_trip(connection, plan)
        bind_dynamic_shadow_trial_to_existing_paper_evidence(
            connection,
            plan_id=plan["plan_id"],
            forward_evidence_id="evidence-1",
        )
        mature = batch_dynamic_shadow_ledger_readiness(
            connection, [identity],
        )[identity]
        assert mature["status"] == "VERIFIED"
        assert mature["verified_chain_count"] == 1
        assert mature["funding_pipeline_ready"] is True
        assert mature["real_order_authority"] is False
        assert mature["automatic_real_order_submission"] is False

        connection.execute(text(
            "UPDATE st_fill_v2 SET price=99 WHERE fill_id='fill-buy'"
        ))
        invalid = batch_dynamic_shadow_ledger_readiness(
            connection, [identity],
        )[identity]
        assert invalid["status"] == "INVALID"
        assert invalid["invalid_chain_count"] == 1
        assert invalid["funding_pipeline_ready"] is False
        assert invalid["shadow_trial_producer_ready"] is False


def test_batch_readiness_accepts_complete_mixed_action_candidate_receipt():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    identity = ("dynamic_alpha", "v1", "1" * 64, "2" * 64)
    buy = _raw_candidate()
    hold = {
        **_raw_candidate(),
        "stock_code": "600000",
        "signal_direction": "HOLD",
        "signal_status": "WATCH",
        "score": 55.0,
    }
    receipt = _candidate_receipt_for_candidates([buy, hold])

    with engine.begin() as connection:
        _schema(connection)
        connection.execute(text("""
            INSERT INTO st_strategy_registry VALUES
                ('dynamic_alpha', 'v1', 'SHADOW', 1)
        """))
        connection.execute(text("""
            INSERT INTO st_strategy_version VALUES
                ('dynamic_alpha', 'v1', :version_hash, 'runtime_registry')
        """), {"version_hash": "1" * 64})
        _insert_receipt(connection, receipt)
        persist_strategy_adapter_candidate_facts(
            connection,
            candidate_receipt=receipt,
            candidates=[buy, hold],
        )
        plan_set = create_dynamic_shadow_trial_plans_from_candidate_facts(
            connection,
            strategy={
                "strategy_key": "dynamic_alpha",
                "current_version": "v1",
                "version_hash": "1" * 64,
                "source_kind": "runtime_registry",
                "current_status": "SHADOW",
                "enabled": True,
            },
            candidate_receipt=receipt,
        )
        readiness = batch_dynamic_shadow_ledger_readiness(
            connection, [identity],
        )[identity]

    assert plan_set["candidate_fact_count"] == 2
    assert plan_set["eligible_candidate_count"] == 1
    assert plan_set["ineligible_candidate_count"] == 1
    assert plan_set["plan_count"] == 1
    assert readiness["status"] == "VERIFIED_PENDING"
    assert readiness["plan_count"] == 1
    assert readiness["pending_plan_count"] == 1
    assert readiness["invalid_chain_count"] == 0
    assert readiness["funding_pipeline_ready"] is False


def test_batch_readiness_query_count_is_constant_for_large_inventory():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    inventory_size = 750
    identities = []
    with engine.begin() as connection:
        _schema(connection)
        registry_rows = []
        version_rows = []
        plan_rows = []
        for index in range(inventory_size):
            strategy_key = f"dynamic_{index:04d}"
            version_hash = hashlib.sha256(
                f"version:{strategy_key}".encode("utf-8")
            ).hexdigest()
            binding_hash = hashlib.sha256(
                f"binding:{strategy_key}".encode("utf-8")
            ).hexdigest()
            identities.append((
                strategy_key, "v1", version_hash, binding_hash,
            ))
            registry_rows.append({
                "strategy_key": strategy_key,
                "current_version": "v1",
            })
            version_rows.append({
                "strategy_key": strategy_key,
                "version": "v1",
                "version_hash": version_hash,
            })
            plan_rows.append({
                "plan_id": hashlib.sha256(
                    f"plan:{strategy_key}".encode("utf-8")
                ).hexdigest(),
                "candidate_run_uid": f"{index:032x}",
                "candidate_receipt_hash": "3" * 64,
                "strategy_key": strategy_key,
                "strategy_version_hash": version_hash,
                "execution_binding_hash": binding_hash,
                "stock_code": f"{index:06d}"[-6:],
                "candidate_fact_hash": "4" * 64,
                "candidate_signal_hash": "5" * 64,
                "plan_hash": "6" * 64,
            })
        connection.execute(text("""
            INSERT INTO st_strategy_registry
            (strategy_key, current_version, current_status, enabled)
            VALUES (:strategy_key, :current_version, 'SHADOW', 1)
        """), registry_rows)
        connection.execute(text("""
            INSERT INTO st_strategy_version
            (strategy_key, version, version_hash, source_kind)
            VALUES (:strategy_key, :version, :version_hash,
                    'runtime_registry')
        """), version_rows)

        select_count = 0

        def count_selects(_conn, _cursor, statement, *_args):
            nonlocal select_count
            if statement.lstrip().upper().startswith("SELECT"):
                select_count += 1

        event.listen(engine, "before_cursor_execute", count_selects)
        try:
            readiness = batch_dynamic_shadow_ledger_readiness(
                connection, identities,
            )
        finally:
            event.remove(engine, "before_cursor_execute", count_selects)
        assert len(readiness) == inventory_size
        assert select_count == 3
        assert all(
            row["status"] == "VERIFIED_EMPTY"
            and row["shadow_trial_producer_ready"] is True
            and row["funding_pipeline_ready"] is False
            and row["real_order_authority"] is False
            for row in readiness.values()
        )

        # Exercise the non-empty relation path as well.  These deliberately
        # incomplete rows must all fail closed, but SQL count remains constant
        # rather than growing with the 750 exact strategy versions.
        connection.execute(text("""
            INSERT INTO st_dynamic_shadow_trial_plan (
                plan_id, candidate_run_uid, candidate_receipt_hash,
                strategy_key, strategy_version, strategy_version_hash,
                execution_binding_hash, trade_date, stock_code, account_id,
                maximum_target_bp, candidate_fact_hash,
                candidate_signal_json, candidate_signal_hash,
                plan_payload_json, plan_hash, plan_status,
                automatic_real_order_submission, real_order_authority
            ) VALUES (
                :plan_id, :candidate_run_uid, :candidate_receipt_hash,
                :strategy_key, 'v1', :strategy_version_hash,
                :execution_binding_hash, '2026-08-21', :stock_code,
                'paper-main-v2', 100, :candidate_fact_hash, '{}',
                :candidate_signal_hash, '{}', :plan_hash,
                'PLANNED_SHADOW_TRIAL', 0, 0
            )
        """), plan_rows)
        select_count = 0
        event.listen(engine, "before_cursor_execute", count_selects)
        try:
            invalid = batch_dynamic_shadow_ledger_readiness(
                connection, identities,
            )
        finally:
            event.remove(engine, "before_cursor_execute", count_selects)
        assert len(invalid) == inventory_size
        # Each prefetched relation now has its own authoritative COUNT plus a
        # count+1 bounded read, so truncation cannot hide historical rows.
        assert select_count == 29
        assert all(
            row["status"] == "INVALID"
            and row["funding_pipeline_ready"] is False
            and row["real_order_authority"] is False
            for row in invalid.values()
        )


def test_batch_readiness_historical_mode_never_hides_retired_version_plans():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    identity = ("dynamic_retired", "v1", "1" * 64, "2" * 64)
    with engine.begin() as connection:
        _schema(connection)
        connection.execute(text("""
            INSERT INTO st_strategy_registry
            (strategy_key, current_version, current_status, enabled)
            VALUES ('dynamic_retired', 'v2', 'SHADOW', 1)
        """))
        connection.execute(text("""
            INSERT INTO st_strategy_version
            (strategy_key, version, version_hash, source_kind)
            VALUES ('dynamic_retired', 'v1', :version_hash,
                    'runtime_registry')
        """), {"version_hash": identity[2]})
        connection.execute(text("""
            INSERT INTO st_dynamic_shadow_trial_plan (
                plan_id, candidate_run_uid, candidate_receipt_hash,
                strategy_key, strategy_version, strategy_version_hash,
                execution_binding_hash, trade_date, stock_code, account_id,
                maximum_target_bp, candidate_fact_hash,
                candidate_signal_json, candidate_signal_hash,
                plan_payload_json, plan_hash, plan_status,
                automatic_real_order_submission, real_order_authority
            ) VALUES (
                :plan_id, :run_uid, :receipt_hash,
                'dynamic_retired', 'v1', :version_hash,
                :binding_hash, '2026-08-20', '000001', 'paper-main-v2',
                100, :fact_hash, '{}', :signal_hash, '{}', :plan_hash,
                'PLANNED_SHADOW_TRIAL', 0, 0
            )
        """), {
            "plan_id": "3" * 64,
            "run_uid": "4" * 32,
            "receipt_hash": "5" * 64,
            "version_hash": identity[2],
            "binding_hash": identity[3],
            "fact_hash": "6" * 64,
            "signal_hash": "7" * 64,
            "plan_hash": "8" * 64,
        })

        current = batch_dynamic_shadow_ledger_readiness(
            connection, [identity]
        )[identity]
        historical = batch_dynamic_shadow_ledger_readiness(
            connection, [identity], include_historical=True
        )[identity]

    assert current["status"] == "VERIFIED_EMPTY"
    assert current["plan_count"] == 0
    assert historical["status"] == "INVALID"
    assert historical["plan_count"] == 1
    assert historical["invalid_chain_count"] == 1
    assert historical["real_order_authority"] is False


def test_aggregate_capability_uses_version_ledger_readiness(monkeypatch):
    from server.engine import strategy_execution_adapters as adapters
    from server.engine import strategy_governance as governance

    deployed = SimpleNamespace(
        adapter_key="dynamic.alpha",
        adapter_version="1.0.0",
        artifact_sha256="3" * 64,
        evaluator_types=frozenset({"dynamic_score"}),
    )
    monkeypatch.setattr(
        adapters,
        "_REGISTRY",
        {(deployed.adapter_key, deployed.adapter_version): deployed},
    )
    monkeypatch.setattr(adapters, "_REGISTRY_SEALED", True)
    monkeypatch.setattr(adapters, "_REGISTRY_SEAL_HASH", "a" * 64)
    monkeypatch.setattr(adapters, "_deployment_mode", lambda: "production")
    load_calls = 0

    def load_large_registry_once():
        nonlocal load_calls
        load_calls += 1
        return [{
            "strategy_key": f"dynamic_{index:04d}",
            "current_version": "v1",
            "version_hash": hashlib.sha256(
                f"version:{index}".encode("utf-8")
            ).hexdigest(),
            "source_kind": "runtime_registry",
            "execution_adapter": {
                "executable": True,
                "execution_binding_hash": hashlib.sha256(
                    f"binding:{index}".encode("utf-8")
                ).hexdigest(),
                "paper_chain_structure_ready": True,
                "funding_pipeline_ready": True,
                "funding_evidence_state": "MATURED",
                "funding_ledger_hash": "4" * 64,
                "verified_forward_chain_count": 1,
            },
        } for index in range(500)]

    monkeypatch.setattr(governance, "load_registry", load_large_registry_once)

    capability = adapters.strategy_execution_adapter_capabilities()
    assert load_calls == 1
    assert capability["funding_pipeline_ready"] is True
    assert capability["governance_paper_execution_ready"] is True
    assert capability["production_execution_ready"] is True
    assert capability["dynamic_version_count"] == 500
    assert capability["structure_ready_dynamic_version_count"] == 500
    assert capability["funding_ready_dynamic_version_count"] == 500
    assert capability["real_order_submission_enabled"] is False
