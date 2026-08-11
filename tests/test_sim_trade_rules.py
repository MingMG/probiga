# -*- coding: utf-8 -*-

import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

from server.engine.sim_trade_engine import (
    SIM_RISK_CONFIG,
    STRATEGY_CONFIG,
    SimTradeEngine,
    _holding_limit_reached,
    _is_trade_date,
    _is_buy_execution_time,
    _intraday_action_window,
    _previous_trade_date,
    _sina_symbol,
    build_buy_decision,
    build_sell_decision,
    fetch_recommended_candidates,
    recommended_candidate_summary,
)


def _candidate(**overrides):
    data = {
        "stock_code": "000001",
        "short_name": "Ping An Bank",
        "ai_score": 72,
        "short_term_score": 78,
        "long_term_score": 66,
        "fundamental": 68,
        "capital_score": 74,
        "technical": 70,
        "recommend_status": "ALLOW",
        "signal_status": "CONFIRM",
        "chase_risk_status": "ALLOW",
        "ordinary_buy_eligible": True,
        "event_risk_level": "LOW",
        "reason": "test summary",
        "sources": "trend",
    }
    data.update(overrides)
    return data


class SimTradeRuleTest(unittest.TestCase):
    def test_sim_risk_config_reserves_stock_txt_cash_buffer(self):
        self.assertEqual(SIM_RISK_CONFIG["max_total_position_pct"], 0.80)
        self.assertEqual(SIM_RISK_CONFIG["cash_buffer_pct"], 0.20)

    def test_sina_symbol_supports_sh_sz_and_bj(self):
        self.assertEqual(_sina_symbol("600519"), "sh600519")
        self.assertEqual(_sina_symbol("000001"), "sz000001")
        self.assertEqual(_sina_symbol("920001"), "bj920001")
        self.assertEqual(_sina_symbol("830799"), "bj830799")

    def test_ultra_short_requires_ai_short_capital_and_low_risk(self):
        result = build_buy_decision("ultra_short", _candidate())

        self.assertTrue(result["allowed"])
        self.assertEqual(result["analysis"]["ai_score"], 72)
        self.assertEqual(result["analysis"]["capital_score"], 74)

    def test_missing_chase_gate_fails_closed(self):
        candidate = _candidate()
        candidate.pop("chase_risk_status")
        candidate.pop("ordinary_buy_eligible")

        result = build_buy_decision("ultra_short", candidate)

        self.assertFalse(result["allowed"])
        self.assertIn("追高", result["reason"])

    def test_execution_blocked_chase_gate_cannot_be_overridden_by_scores(self):
        result = build_buy_decision(
            "ultra_short",
            _candidate(
                ai_score=99,
                short_term_score=99,
                capital_score=99,
                chase_risk_status="EXECUTION_BLOCKED",
                ordinary_buy_eligible=False,
            ),
        )

        self.assertFalse(result["allowed"])
        self.assertIn("追高", result["reason"])

    def test_missing_recommend_and_signal_statuses_fail_closed(self):
        candidate = _candidate()
        candidate.pop("recommend_status")
        candidate.pop("signal_status")

        result = build_buy_decision("ultra_short", candidate)

        self.assertFalse(result["allowed"])
        self.assertEqual(result["analysis"]["signal_status"], "WATCH")

    @patch("server.engine.sim_trade_engine._read_sql")
    @patch("server.engine.sim_trade_engine._table_columns")
    def test_candidate_query_refuses_legacy_table_without_chase_columns(
        self, table_columns, read_sql
    ):
        table_columns.return_value = {
            "stock_code",
            "pick_date",
            "recommend_status",
            "signal_status",
        }

        self.assertEqual(fetch_recommended_candidates("2026-08-04"), [])
        read_sql.assert_not_called()

    @patch("server.engine.sim_trade_engine._read_sql", return_value=[])
    @patch("server.engine.sim_trade_engine._table_columns")
    def test_candidate_query_requires_all_four_explicit_new_buy_gates(
        self, table_columns, read_sql
    ):
        table_columns.return_value = {
            "stock_code",
            "pick_date",
            "ai_score",
            "recommend_status",
            "signal_status",
            "chase_risk_status",
            "ordinary_buy_eligible",
        }

        fetch_recommended_candidates("2026-08-04")

        sql = read_sql.call_args.args[0]
        self.assertIn("recommend_status = 'ALLOW'", sql)
        self.assertIn("signal_status IN ('CONFIRM', 'BUY_READY')", sql)
        self.assertIn("chase_risk_status = 'ALLOW'", sql)
        self.assertIn("ordinary_buy_eligible = 1", sql)

    @patch("server.engine.sim_trade_engine._table_columns")
    @patch("server.engine.sim_trade_engine._read_sql")
    def test_candidate_summary_does_not_count_chase_blocked_row_as_allow(
        self, read_sql, table_columns
    ):
        table_columns.return_value = {
            "recommend_status",
            "signal_status",
            "chase_risk_status",
            "ordinary_buy_eligible",
        }
        read_sql.return_value = [
            {
                "recommend_status": "ALLOW",
                "signal_status": "CONFIRM",
                "chase_risk_status": "EXECUTION_BLOCKED",
                "ordinary_buy_eligible": 1,
                "cnt": 3,
            }
        ]

        result = recommended_candidate_summary("2026-08-04")

        self.assertEqual(result["total"], 3)
        self.assertEqual(result["allow_count"], 0)

    def test_score_above_70_is_not_enough_without_strategy_confirm(self):
        result = build_buy_decision(
            "ultra_short",
            _candidate(ai_score=80, short_term_score=80, capital_score=50),
        )

        self.assertFalse(result["allowed"])
        self.assertIn("50", result["reason"])

    def test_watch_signal_is_not_buy_ready(self):
        result = build_buy_decision(
            "ultra_short",
            _candidate(ai_score=80, short_term_score=82, capital_score=78, signal_status="WATCH"),
        )

        self.assertFalse(result["allowed"])
        self.assertIn("观察", result["reason"])

    def test_low_entry_score_blocks_buy(self):
        result = build_buy_decision(
            "ultra_short",
            _candidate(
                ai_score=86,
                final_trade_score=80,
                entry_score=42,
                short_term_score=82,
                capital_score=78,
                signal_status="CONFIRM",
            ),
        )

        self.assertFalse(result["allowed"])
        self.assertIn("42", result["reason"])

    def test_688_prefix_blocks_buy(self):
        result = build_buy_decision(
            "ultra_short",
            _candidate(stock_code="688001", expected_return_pct=12, risk_reward_ratio=4),
        )

        self.assertFalse(result["allowed"])
        self.assertIn("688", result["reason"])

    def test_non_main_a_share_code_blocks_buy(self):
        result = build_buy_decision(
            "ultra_short",
            _candidate(stock_code="830799", expected_return_pct=12, risk_reward_ratio=4),
        )

        self.assertFalse(result["allowed"])
        self.assertIn("沪深A股", result["reason"])

    def test_low_risk_reward_blocks_non_main_wave_buy(self):
        result = build_buy_decision(
            "ultra_short",
            _candidate(
                ai_score=86,
                final_trade_score=82,
                entry_score=75,
                short_term_score=82,
                capital_score=78,
                expected_return_pct=6,
                risk_reward_ratio=2.2,
                signal_status="CONFIRM",
            ),
        )

        self.assertFalse(result["allowed"])
        self.assertIn("盈亏比", result["reason"])

    def test_failed_sector_gate_blocks_buy(self):
        result = build_buy_decision(
            "ultra_short",
            _candidate(
                ai_score=86,
                final_trade_score=82,
                entry_score=75,
                short_term_score=82,
                capital_score=78,
                expected_return_pct=12,
                risk_reward_ratio=4,
                signal_status="CONFIRM",
                sector_gate_status="BLOCK",
            ),
        )

        self.assertFalse(result["allowed"])
        self.assertIn("板块", result["reason"])

    def test_short_term_requires_technical_confirm(self):
        result = build_buy_decision(
            "short_term",
            _candidate(ai_score=76, short_term_score=72, technical=40),
        )

        self.assertFalse(result["allowed"])
        self.assertIn("40", result["reason"])

    def test_main_wave_requires_buy_ready_signal(self):
        ready = build_buy_decision(
            "main_wave",
            _candidate(
                ai_score=66,
                final_trade_score=66,
                entry_score=60,
                main_wave_score=78,
                trend_hold_score=70,
                main_wave_signal="BUY_READY",
                signal_status="BUY_READY",
            ),
        )
        reduce = build_buy_decision(
            "main_wave",
            _candidate(
                ai_score=90,
                final_trade_score=90,
                entry_score=80,
                main_wave_score=88,
                trend_hold_score=50,
                main_wave_signal="REDUCE",
                signal_status="SELL_ALERT",
            ),
        )

        self.assertTrue(ready["allowed"])
        self.assertFalse(reduce["allowed"])
        self.assertIn("卖出提醒", reduce["reason"])

    def test_swing_blocks_risk_above_configured_level(self):
        result = build_buy_decision(
            "swing",
            _candidate(ai_score=76, long_term_score=70, fundamental=70, event_risk_level="MEDIUM"),
        )

        self.assertFalse(result["allowed"])
        self.assertIn("风险等级中", result["reason"])

    @patch("server.engine.sim_trade_engine._ensure_tables")
    @patch("server.engine.sim_trade_engine._is_trading_time", return_value=False)
    def test_live_buy_scan_skips_outside_trading_time(self, _mock_time, _mock_tables):
        engine = SimTradeEngine()

        self.assertEqual(engine.check_buy_signals("ultra_short"), [])

    @patch("server.engine.sim_trade_engine._ensure_tables")
    @patch("server.engine.sim_trade_engine._is_trading_time", return_value=False)
    def test_live_sell_scan_skips_outside_trading_time(self, _mock_time, _mock_tables):
        engine = SimTradeEngine()

        self.assertEqual(engine.check_sell_signals(), [])

    def test_buy_execution_uses_live_market_instead_of_fixed_windows(self):
        morning_entry = datetime(2026, 7, 3, 9, 40)
        midday_observe = datetime(2026, 7, 3, 10, 30)
        afternoon_entry = datetime(2026, 7, 3, 14, 27)
        exit_window = datetime(2026, 7, 3, 13, 20)
        t_window = datetime(2026, 7, 3, 14, 58)
        lunch_break = datetime(2026, 7, 3, 12, 0)
        after_close = datetime(2026, 7, 3, 15, 2)

        self.assertTrue(_is_buy_execution_time(morning_entry))
        self.assertTrue(_is_buy_execution_time(afternoon_entry))
        self.assertTrue(_is_buy_execution_time(midday_observe))
        self.assertTrue(_is_buy_execution_time(exit_window))
        self.assertTrue(_is_buy_execution_time(t_window))
        self.assertFalse(_is_buy_execution_time(lunch_break))
        self.assertFalse(_is_buy_execution_time(after_close))
        self.assertTrue(_intraday_action_window(morning_entry)["is_preferred_entry_window"])
        self.assertEqual(_intraday_action_window(exit_window)["action"], "exit")
        self.assertEqual(_intraday_action_window(t_window)["action"], "t")

    def test_strategy_holding_count_has_no_hard_cap(self):
        for cfg in STRATEGY_CONFIG.values():
            self.assertEqual(cfg["max_holding"], 0)
            self.assertFalse(_holding_limit_reached(cfg, 10_000))

    def test_main_wave_hold_decision_explains_all_thresholds(self):
        decision = build_sell_decision(
            "main_wave",
            10.0,
            9.276,
            "2026-07-02",
            as_of_date=date(2026, 7, 23),
            max_profit_rate=3.0,
        )

        self.assertFalse(decision["should_sell"])
        self.assertEqual(decision["action"], "HOLD")
        self.assertEqual(decision["holding_days"], 21)
        self.assertAlmostEqual(decision["distance_to_stop_loss_pct"], 2.76)
        self.assertIn("继续持有", decision["reason_detail"])
        self.assertIn("21/60天", decision["reason_detail"])

    def test_sell_decision_triggers_stop_loss(self):
        decision = build_sell_decision(
            "main_wave",
            10.0,
            8.99,
            "2026-07-02",
            as_of_date=date(2026, 7, 23),
        )

        self.assertTrue(decision["should_sell"])
        self.assertEqual(decision["reason"], "stop_loss")

    def test_trailing_stop_uses_peak_profit_even_after_current_profit_falls(self):
        decision = build_sell_decision(
            "main_wave",
            10.0,
            12.5,
            "2026-07-02",
            as_of_date=date(2026, 7, 23),
            max_profit_rate=40.0,
        )

        self.assertTrue(decision["should_sell"])
        self.assertEqual(decision["reason"], "trailing_stop")

    def test_dynamic_holding_exit_overrides_static_hold(self):
        decision = build_sell_decision(
            "main_wave",
            10.0,
            10.2,
            "2026-07-01",
            as_of_date=date(2026, 8, 4),
            holding_assessment={
                "exit_intent": "SELL",
                "reason": "critical announcement detected at cutoff",
            },
        )

        self.assertTrue(decision["should_sell"])
        self.assertEqual(decision["reason"], "dynamic_sell")
        self.assertIn("critical announcement", decision["reason_detail"])

    def test_t_plus_one_keeps_dynamic_exit_intent_pending(self):
        decision = build_sell_decision(
            "short_term",
            10.0,
            9.9,
            "2026-08-04",
            as_of_date=date(2026, 8, 4),
            holding_assessment={"exit_intent": "REDUCE", "reason": "trend invalidated"},
        )

        self.assertFalse(decision["should_sell"])
        self.assertTrue(decision["exit_intent"])
        self.assertEqual(decision["action"], "WAIT_EXECUTION")
        self.assertEqual(decision["pending_exit_reason"], "dynamic_reduce")

    def test_near_limit_down_keeps_stop_loss_exit_intent_pending(self):
        decision = build_sell_decision(
            "short_term",
            10.0,
            9.0,
            "2026-08-01",
            as_of_date=date(2026, 8, 4),
            near_limit_down=True,
            holding_assessment={"exit_intent": "WAIT_DATA", "reason": "news feed stale"},
        )

        self.assertFalse(decision["should_sell"])
        self.assertTrue(decision["exit_intent"])
        self.assertEqual(decision["action"], "WAIT_EXECUTION")
        self.assertEqual(decision["pending_exit_reason"], "stop_loss")

    def test_missing_dynamic_data_never_suppresses_static_stop(self):
        decision = build_sell_decision(
            "short_term",
            10.0,
            9.0,
            "2026-08-01",
            as_of_date=date(2026, 8, 4),
            holding_assessment={"exit_intent": "WAIT_DATA", "reason": "event source missing"},
        )

        self.assertTrue(decision["should_sell"])
        self.assertEqual(decision["reason"], "stop_loss")

    @patch("server.engine.sim_trade_engine._ensure_tables")
    def test_direct_live_execute_buy_without_fresh_gate_is_rejected(self, _mock_tables):
        engine = SimTradeEngine()

        with self.assertRaisesRegex(ValueError, "execution-time gate"):
            engine.execute_buy({
                "stock_code": "000001",
                "short_name": "Ping An Bank",
                "strategy_type": "short_term",
                "trade_mode": "live",
                "price": 10.0,
                "shares": 100,
            })

    @patch("server.engine.sim_trade_engine._ensure_tables")
    @patch("server.engine.sim_trade_engine.evaluate_sim_buy_execution_gate")
    def test_buy_order_revoked_before_fill_cancels_without_position_write(
        self, evaluate_gate, _mock_tables
    ):
        engine = SimTradeEngine()
        evaluate_gate.return_value = {
            "status": "EXECUTION_BLOCKED",
            "eligible": False,
            "ordinary_buy_eligible": False,
            "reason": "nine-board chase risk",
            "context_hash": "blocked",
        }
        order = {
            "id": 7,
            "signal_id": 3,
            "stock_code": "603221",
            "strategy_type": "ultra_short",
            "order_date": "2026-08-04",
            "remaining_shares": 100,
            "filled_shares": 0,
            "execution_gate_hash": "old-allow",
        }
        with patch.object(
            engine,
            "_cancel_buy_order_for_execution_gate",
            return_value={"order_id": 7, "status": "cancelled", "reason": "execution_gate"},
        ) as cancel_order, patch.object(engine, "execute_buy") as execute_buy:
            result = engine._match_buy_order(
                order,
                {"price": 10.0, "source": "live", "change_pct": 0},
                {"cash_available_after_buffer": 100_000},
            )

        self.assertEqual(result["status"], "cancelled")
        cancel_order.assert_called_once()
        execute_buy.assert_not_called()

    @patch("server.engine.sim_trade_engine._ensure_tables")
    @patch("server.engine.sim_trade_engine.evaluate_sim_buy_execution_gate")
    def test_changed_gate_context_requeues_unfilled_order_instead_of_stale_fill(
        self, evaluate_gate, _mock_tables
    ):
        engine = SimTradeEngine()
        now = datetime.now().astimezone()
        evaluate_gate.return_value = {
            "status": "ALLOW",
            "eligible": True,
            "ordinary_buy_eligible": True,
            "reason": "fresh facts",
            "context_hash": "new-context",
            "evaluated_at": (now - timedelta(seconds=1)).isoformat(),
            "valid_until": (now + timedelta(seconds=30)).isoformat(),
            "stock_code": "000001",
            "trade_date": "2026-08-04",
        }
        order = {
            "id": 8,
            "signal_id": 4,
            "stock_code": "000001",
            "strategy_type": "short_term",
            "order_date": "2026-08-04",
            "remaining_shares": 100,
            "filled_shares": 0,
            "execution_gate_hash": "old-context",
        }
        with patch.object(
            engine,
            "_cancel_buy_order_for_execution_gate",
            return_value={"order_id": 8, "status": "cancelled", "reason": "execution_gate"},
        ) as cancel_order, patch.object(engine, "execute_buy") as execute_buy:
            result = engine._match_buy_order(
                order,
                {"price": 10.0, "source": "live", "change_pct": 0},
                {"cash_available_after_buffer": 100_000},
            )

        self.assertEqual(result["status"], "cancelled")
        self.assertTrue(cancel_order.call_args.kwargs["requeue_if_unfilled"])
        execute_buy.assert_not_called()

    @patch("server.engine.sim_trade_engine._ensure_tables")
    def test_risk_budget_sizes_order_by_portfolio_constraints(self, _mock_tables):
        engine = SimTradeEngine()
        state = {
            "total_equity": 1_000_000,
            "total_available_for_position": 200_000,
            "cash_available_after_buffer": 180_000,
            "used_by_strategy": {"short_term": 120_000},
            "pending_by_strategy": {"short_term": 0},
            "used_by_stock": {},
            "pending_by_stock": {},
        }
        signal = {
            "stock_code": "000001",
            "strategy_type": "short_term",
            "event_risk_level": "LOW",
            "final_trade_score": 80,
            "stop_loss_price": 9.5,
        }

        budget = engine._risk_budget_for_signal(signal, 10.0, state)

        self.assertTrue(budget["allowed"])
        self.assertGreaterEqual(budget["shares"], 100)
        self.assertEqual(budget["shares"] % 100, 0)
        self.assertLessEqual(budget["amount"], 100_000)

    @patch("server.engine.sim_trade_engine._ensure_tables")
    def test_risk_budget_blocks_critical_risk(self, _mock_tables):
        engine = SimTradeEngine()
        state = {
            "total_equity": 1_000_000,
            "total_available_for_position": 200_000,
            "cash_available_after_buffer": 180_000,
            "used_by_strategy": {},
            "pending_by_strategy": {},
            "used_by_stock": {},
            "pending_by_stock": {},
        }
        signal = {
            "stock_code": "000001",
            "strategy_type": "short_term",
            "event_risk_level": "CRITICAL",
            "final_trade_score": 90,
        }

        budget = engine._risk_budget_for_signal(signal, 10.0, state)

        self.assertFalse(budget["allowed"])
        self.assertEqual(budget["shares"], 0)

    @patch(
        "server.engine.sim_trade_engine._read_sql",
        side_effect=[RuntimeError("calendar not available"), [{"trade_date": "2026-06-26"}]],
    )
    def test_previous_trade_date_falls_back_to_local_kline(self, _mock_read):
        self.assertEqual(_previous_trade_date("2026-06-29"), "2026-06-26")

    @patch(
        "server.engine.sim_trade_engine._read_sql",
        side_effect=[RuntimeError("calendar not available"), [{"ok": 1}]],
    )
    def test_is_trade_date_falls_back_to_local_kline(self, _mock_read):
        self.assertTrue(_is_trade_date("2026-06-26"))


if __name__ == "__main__":
    unittest.main()
