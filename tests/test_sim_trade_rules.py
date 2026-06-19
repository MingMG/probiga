# -*- coding: utf-8 -*-

import unittest
from unittest.mock import patch

from server.engine.sim_trade_engine import SimTradeEngine, _sina_symbol, build_buy_decision


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
        "event_risk_level": "LOW",
        "reason": "test summary",
        "sources": "trend",
    }
    data.update(overrides)
    return data


class SimTradeRuleTest(unittest.TestCase):
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
        self.assertIn("WATCH", result["reason"])

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
        self.assertIn("SELL_ALERT", reduce["reason"])

    def test_swing_blocks_risk_above_configured_level(self):
        result = build_buy_decision(
            "swing",
            _candidate(ai_score=76, long_term_score=70, fundamental=70, event_risk_level="MEDIUM"),
        )

        self.assertFalse(result["allowed"])
        self.assertIn("MEDIUM", result["reason"])

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


if __name__ == "__main__":
    unittest.main()
