# -*- coding: utf-8 -*-

import inspect
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from unittest.mock import MagicMock, call, patch

import server.engine.sim_trade_engine as sim_module
from server.engine.sim_trade_engine import SimTradeEngine


class _Result:
    def __init__(self, rowcount=1, scalar_value=0):
        self.rowcount = rowcount
        self._scalar_value = scalar_value

    def scalar(self):
        return self._scalar_value

    def __iter__(self):
        return iter(())


class _Connection:
    def __init__(self, transaction=None, claim_rowcount=1):
        self.transaction = transaction
        self.claim_rowcount = claim_rowcount
        self.order_id = 0
        self.executed = []

    def execute(self, statement, params=None):
        sql = str(statement)
        params = dict(params or {})
        self.executed.append((sql, params))
        if "SET status = 'MATCHING'" in sql:
            self.order_id = int(params.get("id") or 0)
            return _Result(self.claim_rowcount)
        return _Result(1)


class _Transaction:
    def __init__(self, shared_lock=None, claim_rowcount=1):
        self.connection = _Connection(self, claim_rowcount=claim_rowcount)
        self.shared_lock = shared_lock
        self.has_lock = False
        self.committed = False
        self.rolled_back = False

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc, traceback):
        self.rolled_back = exc_type is not None
        self.committed = exc_type is None
        if self.has_lock:
            self.shared_lock.release()
            self.has_lock = False
        return False


class _Engine:
    def __init__(self, *, shared_lock=None, claim_rowcounts=None):
        self.shared_lock = shared_lock
        self.claim_rowcounts = list(claim_rowcounts or [])
        self.transactions = []

    def begin(self):
        rowcount = self.claim_rowcounts.pop(0) if self.claim_rowcounts else 1
        transaction = _Transaction(self.shared_lock, claim_rowcount=rowcount)
        self.transactions.append(transaction)
        return transaction


def _engine_without_init():
    return object.__new__(SimTradeEngine)


def _fresh_gate(code="000001", trade_date="2026-08-05", *, context_hash="ctx"):
    now = datetime.now().astimezone()
    return {
        "status": "ALLOW",
        "eligible": True,
        "ordinary_buy_eligible": True,
        "reason": "fresh facts",
        "context_hash": context_hash,
        "evaluated_at": (now - timedelta(seconds=1)).isoformat(),
        "valid_until": (now + timedelta(seconds=30)).isoformat(),
        "stock_code": code,
        "trade_date": trade_date,
    }


def _holding(**overrides):
    data = {
        "id": 31,
        "stock_code": "000001",
        "short_name": "Ping An Bank",
        "strategy_type": "short_term",
        "trade_mode": "live",
        "buy_price": 10.0,
        "buy_amount": 6000.0,
        "buy_shares": 600,
        "buy_date": "2026-08-04",
        "status": "holding",
        "profit": 0,
        "profit_rate": 0,
        "fee_total": 0,
        "exit_order_id": None,
        "ai_score": 80,
    }
    data.update(overrides)
    return data


class SimTradeAtomicExecutionTest(unittest.TestCase):
    def test_atomic_claim_is_compare_and_set_and_loser_skips(self):
        engine = _engine_without_init()
        db = _Engine(claim_rowcounts=[0])
        with patch.object(sim_module, "get_engine", return_value=db), patch.object(
            engine, "_match_buy_order"
        ) as match_buy:
            result = engine._claim_and_match_order(
                9,
                trade_mode="live",
                effective_trade_date="2026-08-05",
                price_info={"price": 10.0, "source": "live"},
                state={},
            )

        self.assertEqual(result["reason"], "already_claimed_or_terminal")
        match_buy.assert_not_called()
        sql, params = db.transactions[0].connection.executed[0]
        self.assertIn("status IN ('PENDING', 'PARTIAL')", sql)
        self.assertIn("SET status = 'MATCHING'", sql)
        self.assertEqual(params["id"], 9)

    def test_match_failure_escapes_transaction_and_rolls_back_claim(self):
        engine = _engine_without_init()
        db = _Engine()
        order = {
            "id": 10,
            "status": "MATCHING",
            "side": "BUY",
            "trade_mode": "live",
            "stock_code": "000001",
            "order_date": "2026-08-05",
        }
        with patch.object(sim_module, "get_engine", return_value=db), patch.object(
            sim_module, "_connection_rows", return_value=[order]
        ), patch.object(engine, "_acquire_buy_execution_lock"), patch.object(
            engine, "_transactional_buy_cash_state", return_value={}
        ), patch.object(
            engine, "_match_buy_order", side_effect=RuntimeError("flow insert failed")
        ):
            with self.assertRaisesRegex(RuntimeError, "flow insert failed"):
                engine._claim_and_match_order(
                    10,
                    trade_mode="live",
                    effective_trade_date="2026-08-05",
                    price_info={"price": 10.0, "source": "live"},
                    state={},
                )

        self.assertTrue(db.transactions[0].rolled_back)
        self.assertFalse(db.transactions[0].committed)

    def test_two_different_buy_orders_are_serialized_by_database_mutex_path(self):
        engine = _engine_without_init()
        shared_lock = threading.Lock()
        db = _Engine(shared_lock=shared_lock)
        active = 0
        max_active = 0
        guard = threading.Lock()

        def rows_for_connection(connection, sql, params=None):
            if "FROM st_sim_order o" in sql:
                return [{
                    "id": connection.order_id,
                    "status": "MATCHING",
                    "side": "BUY",
                    "trade_mode": "live",
                    "stock_code": f"{connection.order_id:06d}",
                    "order_date": "2026-08-05",
                }]
            return []

        def acquire_mutex(connection, **_kwargs):
            connection.transaction.shared_lock.acquire()
            connection.transaction.has_lock = True

        def match_buy(order, _price, _state, **kwargs):
            nonlocal active, max_active
            self.assertIs(kwargs["_connection"].transaction.connection, kwargs["_connection"])
            with guard:
                active += 1
                max_active = max(max_active, active)
            time.sleep(0.03)
            with guard:
                active -= 1
            return {"order_id": order["id"], "status": "filled"}

        with patch.object(sim_module, "get_engine", return_value=db), patch.object(
            sim_module, "_connection_rows", side_effect=rows_for_connection
        ), patch.object(
            engine, "_acquire_buy_execution_lock", side_effect=acquire_mutex
        ) as acquire, patch.object(
            engine, "_transactional_buy_cash_state", return_value={"cash_available_after_buffer": 100000}
        ), patch.object(engine, "_match_buy_order", side_effect=match_buy):
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = [
                    pool.submit(
                        engine._claim_and_match_order,
                        order_id,
                        trade_mode="live",
                        effective_trade_date="2026-08-05",
                        price_info={"price": 10.0, "source": "live"},
                        state={},
                    )
                    for order_id in (101, 202)
                ]
                results = [future.result() for future in futures]

        self.assertEqual(max_active, 1)
        self.assertEqual({result["order_id"] for result in results}, {101, 202})
        self.assertEqual(acquire.call_count, 2)

    def test_database_mutex_uses_existing_risk_row_and_for_update(self):
        engine = _engine_without_init()
        connection = _Connection()
        with patch.object(sim_module, "_connection_rows", return_value=[{"id": 1}]) as rows:
            engine._acquire_buy_execution_lock(
                connection,
                trade_mode="forward",
                trade_date="2026-08-05",
            )

        insert_sql = connection.executed[0][0]
        lock_sql = rows.call_args.args[1]
        self.assertIn("st_sim_risk_budget", insert_sql)
        self.assertIn("__EXECUTION_LOCK__", insert_sql)
        self.assertIn("ON DUPLICATE KEY UPDATE", insert_sql)
        self.assertIn("FOR UPDATE", lock_sql)

    @patch.object(sim_module, "_ensure_tables")
    @patch.object(sim_module, "evaluate_sim_buy_execution_gate")
    def test_buy_gate_rejects_cross_instrument_identity(self, evaluate_gate, _ensure):
        evaluate_gate.return_value = _fresh_gate(code="603221")
        signal = {
            "stock_code": "000001",
            "strategy_type": "short_term",
            "trade_mode": "live",
            "buy_date": "2026-08-05",
            "price": 10.0,
            "shares": 100,
            "execution_gate": _fresh_gate(code="000001"),
        }

        with self.assertRaisesRegex(ValueError, "execution-time gate"):
            _engine_without_init().execute_buy(signal)

    @patch.object(sim_module, "_ensure_tables")
    @patch.object(sim_module, "evaluate_sim_buy_execution_gate")
    def test_caller_gate_and_forged_ttl_never_authorize_buy(self, evaluate_gate, _ensure):
        evaluate_gate.return_value = {
            **_fresh_gate(),
            "status": "EXECUTION_BLOCKED",
            "eligible": False,
            "ordinary_buy_eligible": False,
            "reason": "fresh helper revoked eligibility",
        }
        forged = _fresh_gate()
        forged["valid_until"] = "2099-12-31T23:59:59+08:00"
        signal = {
            "stock_code": "000001",
            "strategy_type": "short_term",
            "trade_mode": "live",
            "buy_date": "2026-08-05",
            "price": 10.0,
            "shares": 100,
            "execution_gate": forged,
            "_execution_gate_authorization": object(),
        }

        with self.assertRaisesRegex(ValueError, "fresh helper revoked"):
            _engine_without_init().execute_buy(signal)

    def test_buy_fill_writes_position_flow_order_signal_on_claim_connection(self):
        engine = _engine_without_init()
        connection = _Connection()
        gate = _fresh_gate(context_hash="a" * 64)
        order = {
            "id": 44,
            "signal_id": 12,
            "stock_code": "000001",
            "short_name": "Ping An Bank",
            "strategy_type": "short_term",
            "trade_mode": "live",
            "order_date": "2026-08-05",
            "remaining_shares": 100,
            "requested_shares": 100,
            "filled_shares": 0,
            "execution_gate_hash": "a" * 64,
            "reason": "approved signal",
        }
        with patch.object(
            sim_module, "evaluate_sim_buy_execution_gate", return_value=gate
        ) as evaluate, patch.object(
            sim_module, "_connection_rows", return_value=[]
        ), patch.object(
            sim_module, "_connection_insert_get_id", side_effect=[501, 601]
        ) as insert_id:
            result = engine._match_buy_order(
                order,
                {"price": 10.0, "source": "live", "change_pct": 0},
                {"cash_available_after_buffer": 100000},
                effective_trade_date="2026-08-05",
                _connection=connection,
            )

        self.assertEqual(result["status"], "filled")
        self.assertEqual(result["position_id"], 501)
        self.assertEqual(evaluate.call_count, 2)
        self.assertTrue(all(item.args[0] is connection for item in insert_id.call_args_list))
        sql_text = "\n".join(sql for sql, _params in connection.executed)
        self.assertIn("UPDATE st_sim_order", sql_text)
        self.assertIn("UPDATE st_sim_signal", sql_text)
        self.assertIn("INSERT INTO st_sim_event", sql_text)

    def test_final_buy_recheck_rejects_allow_to_allow_context_change(self):
        engine = _engine_without_init()
        order = {
            "id": 45,
            "signal_id": 13,
            "stock_code": "000001",
            "short_name": "Ping An Bank",
            "strategy_type": "short_term",
            "trade_mode": "live",
            "order_date": "2026-08-05",
            "remaining_shares": 100,
            "requested_shares": 100,
            "filled_shares": 0,
            "execution_gate_hash": "a" * 64,
            "reason": "approved signal",
        }
        with patch.object(
            sim_module,
            "evaluate_sim_buy_execution_gate",
            side_effect=[
                _fresh_gate(context_hash="a" * 64),
                _fresh_gate(context_hash="b" * 64),
            ],
        ), patch.object(sim_module, "_connection_rows", return_value=[]), patch.object(
            sim_module, "_connection_insert_get_id"
        ) as insert_id:
            with self.assertRaisesRegex(ValueError, "context changed"):
                engine._match_buy_order(
                    order,
                    {"price": 10.0, "source": "live", "change_pct": 0},
                    {"cash_available_after_buffer": 100000},
                    effective_trade_date="2026-08-05",
                    _connection=_Connection(),
                )

        insert_id.assert_not_called()

    def test_duplicate_sell_order_returns_idempotently_without_second_mutation(self):
        engine = _engine_without_init()
        connection = _Connection()
        prior_flow = {
            "id": 91,
            "stock_code": "000001",
            "trade_mode": "live",
            "price": 9.8,
            "shares": 300,
            "amount": 2940,
            "fee": 10,
        }
        signal = {
            "position_id": 31,
            "order_id": 77,
            "stock_code": "000001",
            "strategy_type": "short_term",
            "trade_mode": "live",
            "sell_date": "2026-08-05",
            "sell_price": 9.8,
            "shares": 300,
        }
        with patch.object(sim_module, "_connection_rows", side_effect=[[_holding()], [prior_flow]]):
            result = engine._execute_sell_in_transaction(
                connection,
                signal,
                expected_stock_code="000001",
                expected_trade_date="2026-08-05",
                expected_trade_mode="live",
            )

        self.assertEqual(result["status"], "idempotent")
        self.assertEqual(result["flow_id"], 91)
        self.assertEqual(connection.executed, [])

    def test_partial_live_sell_without_order_id_fails_closed(self):
        engine = _engine_without_init()
        signal = {
            "position_id": 31,
            "stock_code": "000001",
            "strategy_type": "short_term",
            "trade_mode": "live",
            "sell_date": "2026-08-05",
            "sell_price": 9.8,
            "shares": 300,
        }
        with patch.object(sim_module, "_connection_rows", return_value=[_holding()]):
            with self.assertRaisesRegex(ValueError, "stable order_id"):
                engine._execute_sell_in_transaction(
                    _Connection(),
                    signal,
                    expected_stock_code="000001",
                    expected_trade_date="2026-08-05",
                    expected_trade_mode="live",
                )

    def test_live_sell_rechecks_t_plus_one_against_effective_trade_date(self):
        engine = _engine_without_init()
        signal = {
            "position_id": 31,
            "order_id": 88,
            "stock_code": "000001",
            "strategy_type": "short_term",
            "trade_mode": "live",
            "sell_date": "2026-08-04",
            "sell_price": 9.8,
            "shares": 600,
        }
        with patch.object(sim_module, "_connection_rows", side_effect=[[_holding()], []]):
            with self.assertRaisesRegex(ValueError, r"T\+1"):
                engine._execute_sell_in_transaction(
                    _Connection(),
                    signal,
                    expected_stock_code="000001",
                    expected_trade_date="2026-08-04",
                    expected_trade_mode="live",
                )

    def test_reduce_defaults_to_half_board_lots_and_small_position_waits(self):
        engine = _engine_without_init()
        base = {
            "position_id": 31,
            "stock_code": "000001",
            "short_name": "Ping An Bank",
            "strategy_type": "short_term",
            "trade_mode": "live",
            "sell_price": 9.8,
            "shares": 600,
            "reason": "dynamic_reduce",
            "holding_assessment": {"exit_intent": "REDUCE"},
        }
        with patch.object(sim_module, "_read_sql", return_value=[]), patch.object(
            engine, "_create_order", return_value=51
        ) as create_order, patch.object(engine, "_log_event"):
            created = engine._create_sell_order_from_signal(base, "2026-08-05")

        self.assertEqual(created["requested_shares"], 300)
        self.assertEqual(created["time_in_force"], "GTC")
        self.assertEqual(create_order.call_args.args[2], 300)
        self.assertEqual(create_order.call_args.kwargs["source_event"], "RISK_SELL_GTC_REDUCE")

        too_small = {**base, "shares": 100}
        waiting = engine._create_sell_order_from_signal(too_small, "2026-08-05")
        self.assertEqual(waiting["status"], "waiting")
        self.assertEqual(waiting["reason"], "reduce_below_board_lot")

    def test_cross_day_sell_match_uses_supplied_trade_date(self):
        engine = _engine_without_init()
        order = {
            "id": 70,
            "position_id": 31,
            "stock_code": "000001",
            "short_name": "Ping An Bank",
            "strategy_type": "short_term",
            "trade_mode": "live",
            "order_date": "2026-08-04",
            "remaining_shares": 100,
            "requested_shares": 100,
            "filled_shares": 0,
            "reason": "risk reduce",
        }
        holding = _holding(buy_shares=600, buy_date="2026-08-04")
        with patch.object(sim_module, "_connection_rows", return_value=[holding]), patch.object(
            engine,
            "_execute_sell_in_transaction",
            return_value={"status": "ok", "amount": 980, "fee": 5, "position_id": 31},
        ) as execute_sell, patch.object(engine, "_fill_order"), patch.object(engine, "_log_event"):
            result = engine._match_sell_order(
                order,
                {"price": 9.8, "source": "live", "change_pct": 0},
                effective_trade_date="2026-08-05",
                _connection=_Connection(),
            )

        self.assertEqual(result["status"], "filled")
        self.assertEqual(execute_sell.call_args.kwargs["expected_trade_date"], "2026-08-05")
        self.assertEqual(execute_sell.call_args.args[1]["sell_date"], "2026-08-05")

    def test_gtc_and_partial_terminal_invariants_are_present(self):
        expire_source = inspect.getsource(SimTradeEngine._expire_open_orders)
        close_source = inspect.getsource(SimTradeEngine._mark_order_closed)
        file_source = inspect.getsource(sim_module)

        self.assertIn("RISK_SELL_GTC%", expire_source)
        self.assertIn("remaining_shares = 0", expire_source)
        self.assertIn("PARTIAL_EXPIRED", expire_source)
        self.assertIn("PARTIAL_CANCELLED", close_source)
        self.assertIn("remaining_shares", close_source)
        self.assertEqual(file_source.count("def run_event_tick("), 1)

    @patch.object(sim_module, "_read_sql", return_value=[])
    @patch.object(sim_module, "_table_columns", return_value={"id"})
    def test_required_execution_schema_fails_closed(self, _columns, _read):
        with self.assertRaisesRegex(RuntimeError, "schema is incomplete"):
            sim_module._require_sim_execution_schema()

    def test_constructor_only_runs_read_only_schema_validation(self):
        with patch.object(sim_module, "_require_sim_execution_schema") as require, patch.object(
            sim_module, "_exec_sql"
        ) as write:
            SimTradeEngine()

        require.assert_called_once_with()
        write.assert_not_called()

    def test_constructor_fails_closed_when_schema_is_missing(self):
        with patch.object(
            sim_module,
            "_require_sim_execution_schema",
            side_effect=RuntimeError("sim execution schema is incomplete"),
        ), patch.object(sim_module, "_exec_sql") as write:
            with self.assertRaisesRegex(RuntimeError, "schema is incomplete"):
                SimTradeEngine()

        write.assert_not_called()

    def test_dashboard_get_reports_missing_schema_without_ddl(self):
        import server.api.routers.sim_trade as router_module

        with patch.object(
            sim_module,
            "_require_sim_execution_schema",
            side_effect=RuntimeError("sim execution schema is incomplete"),
        ), patch.object(sim_module, "_exec_sql") as engine_write, patch.object(
            router_module, "_exec_sql"
        ) as router_write:
            result = router_module.sim_trade_dashboard(trade_mode="live")

        self.assertIn("schema is incomplete", result["error"])
        engine_write.assert_not_called()
        router_write.assert_not_called()

    def test_migration_requires_explicit_schema_change_authorization(self):
        with patch.object(sim_module, "_exec_sql") as write, patch.object(
            sim_module, "_read_sql"
        ) as read:
            with self.assertRaisesRegex(PermissionError, "allow_schema_change=True"):
                sim_module.migrate_sim_trade_schema()

        write.assert_not_called()
        read.assert_not_called()

    def test_migration_failure_is_propagated(self):
        with patch.object(
            sim_module, "_exec_sql", side_effect=RuntimeError("ddl rejected")
        ):
            with self.assertRaisesRegex(RuntimeError, "ddl rejected"):
                sim_module.migrate_sim_trade_schema(allow_schema_change=True)

    def test_text_column_migration_is_replay_safe(self):
        with patch.object(
            sim_module, "_read_sql", return_value=[{"DATA_TYPE": "text"}]
        ), patch.object(sim_module, "_exec_sql") as write:
            sim_module._ensure_text_column("st_sim_position", "sell_reason")

        write.assert_not_called()

    def test_migration_cli_is_the_explicit_schema_write_entrypoint(self):
        import tools.migrate_sim_trade as migration_cli

        with patch.object(migration_cli, "load_project_env") as load_env, patch.object(
            migration_cli, "migrate_sim_trade_schema"
        ) as migrate:
            with self.assertRaises(SystemExit) as denied:
                migration_cli.main([])
            self.assertEqual(denied.exception.code, 2)
            load_env.assert_not_called()
            migrate.assert_not_called()

            self.assertEqual(
                migration_cli.main(["--allow-schema-change"]),
                0,
            )

        load_env.assert_called_once_with()
        migrate.assert_called_once_with(allow_schema_change=True)


if __name__ == "__main__":
    unittest.main()
