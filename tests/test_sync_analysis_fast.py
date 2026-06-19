# -*- coding: utf-8 -*-
import unittest

from biz.analysis.sync_analysis_fast import (
    add_strategy_signals,
    build_strategy_trade_plan,
    choose_recommend_status,
    clamp_score,
    classify_notice_title,
    linear_score,
    select_primary_strategy,
)
import pandas as pd
from unittest.mock import patch


class SyncAnalysisFastTest(unittest.TestCase):
    def test_clamp_score_handles_invalid_values(self):
        self.assertEqual(clamp_score(120), 100.0)
        self.assertEqual(clamp_score(-5), 0.0)
        self.assertEqual(clamp_score(None), 50.0)

    def test_linear_score_maps_range(self):
        self.assertEqual(linear_score(5, 0, 10), 50.0)
        self.assertEqual(linear_score(20, 0, 10), 100.0)
        self.assertEqual(linear_score(None, 0, 10), 50.0)

    def test_notice_title_classification(self):
        result = classify_notice_title("关于公司被立案调查及股份回购计划的公告")
        self.assertGreater(result["critical"], 0)
        self.assertGreater(result["positive"], 0)

    def test_recommend_gate_blocks_st_name(self):
        status, reason = choose_recommend_status(
            stock_code="000001",
            short_name="*ST测试",
            ai_score=90,
            short_term_score=80,
            long_term_score=80,
            event_risk_level="LOW",
            amount=100_000_000,
            change_pct=2,
            min_score=60,
        )
        self.assertEqual(status, "BLOCK")
        self.assertIn("ST", reason)

    def test_recommend_gate_allows_clean_candidate(self):
        status, _ = choose_recommend_status(
            stock_code="300001",
            short_name="测试股份",
            ai_score=70,
            short_term_score=68,
            long_term_score=66,
            event_risk_level="LOW",
            amount=120_000_000,
            change_pct=3,
            min_score=62,
        )
        self.assertEqual(status, "ALLOW")

    def test_primary_strategy_prefers_highest_qualified_score(self):
        strategy = select_primary_strategy({
            "ultra_short_score": 82,
            "short_term_score": 74,
            "swing_score": 66,
        })
        self.assertEqual(strategy, "ultra_short")

    def test_trade_plan_outputs_price_band_and_risk_levels(self):
        plan = build_strategy_trade_plan({
            "close": 10.0,
            "ma5": 9.9,
            "ma10": 9.8,
            "ma20": 9.6,
            "ma60": 9.3,
            "amount": 120_000_000,
            "change_pct": 2.0,
            "market_mood_score": 55,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "quality_score": 80,
            "entry_score": 72,
            "final_trade_score": 77.6,
            "expected_return_pct": 14,
            "heat_overload_score": 70,
            "confidence_score": 80,
            "failure_penalty_score": 100,
            "ultra_short_score": 80,
            "short_term_score": 72,
            "swing_score": 68,
        }, "ultra_short")

        self.assertEqual(plan["signal_status"], "CONFIRM")
        self.assertLess(plan["entry_price_low"], plan["entry_price_high"])
        self.assertLess(plan["stop_loss_price"], plan["entry_price_low"])
        self.assertGreater(plan["take_profit_1"], plan["entry_price_high"])
        self.assertGreater(plan["take_profit_2"], plan["take_profit_1"])

    def test_trade_plan_blocks_small_expected_return(self):
        plan = build_strategy_trade_plan({
            "close": 10.0,
            "ma5": 9.9,
            "ma10": 9.8,
            "ma20": 9.6,
            "ma60": 9.3,
            "amount": 120_000_000,
            "change_pct": 1.0,
            "market_mood_score": 55,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "quality_score": 90,
            "entry_score": 80,
            "final_trade_score": 87,
            "expected_return_pct": 3.5,
            "heat_overload_score": 70,
            "confidence_score": 80,
            "failure_penalty_score": 100,
            "ultra_short_score": 82,
        }, "ultra_short")

        self.assertEqual(plan["signal_status"], "BLOCK")

    def test_main_wave_buy_ready_ignores_fixed_expected_return_cap(self):
        row = {
            "close": 20.0,
            "ma5": 19.5,
            "ma10": 18.8,
            "ma20": 17.2,
            "ma60": 16.5,
            "amount": 300_000_000,
            "change_pct": 4.0,
            "market_mood_score": 60,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "quality_score": 66,
            "entry_score": 62,
            "final_trade_score": 65,
            "expected_return_pct": 1.0,
            "heat_overload_score": 70,
            "confidence_score": 70,
            "failure_penalty_score": 100,
            "main_wave_score": 78,
            "trend_hold_score": 72,
            "main_wave_signal": "BUY_READY",
            "main_wave_reason": "主升浪放量突破",
            "trend_stop_price": 16.68,
        }

        plan = build_strategy_trade_plan(row, "main_wave")

        self.assertEqual(plan["signal_status"], "BUY_READY")
        self.assertIn("main-wave", plan["signal_reason"])

    def test_main_wave_extended_move_triggers_sell_alert(self):
        df = pd.DataFrame([{
            "stock_code": "603629",
            "ai_score": 80,
            "long_term_score": 70,
            "short_term_score": 82,
            "technical_score": 90,
            "capital_score": 88,
            "sentiment_score": 70,
            "event_score": 80,
            "risk_score": 75,
            "close": 211.94,
            "ma5": 205,
            "ma10": 183.26,
            "ma20": 162.51,
            "ma60": 95,
            "high_20": 216.88,
            "high_60": 216.88,
            "low_60": 51.3,
            "amount": 1_000_000_000,
            "amount_ma5": 850_000_000,
            "amount_ma20": 830_000_000,
            "change_pct": 4.92,
            "turnover_ratio": 8.0,
            "volatility_20": 6.0,
            "pct_20": 120,
            "dist_ma20": 30.4,
            "from_low_60": 313.1,
            "amount_ratio_5": 1.18,
            "amount_ratio_20": 1.2,
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
            "market_mood_score": 70,
        }])

        out = add_strategy_signals(df)
        row = out.iloc[0]

        self.assertGreaterEqual(row["main_wave_score"], 70)
        self.assertEqual(row["main_wave_signal"], "REDUCE")
        self.assertIn("高位扩张", row["main_wave_reason"])

    def test_run_batch_emits_progress_events(self):
        progress_events = []
        empty_df = pd.DataFrame()

        with patch("biz.analysis.sync_analysis_fast.load_kline_features", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.load_finance", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.load_flow_features", return_value=(empty_df, "2026-06-13")), \
             patch("biz.analysis.sync_analysis_fast.load_hot_rank", return_value=(empty_df, "2026-06-13")), \
             patch("biz.analysis.sync_analysis_fast.load_notice_features", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.load_confidence_features", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.load_recommendation_history", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.load_failure_features", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.load_sector_rotation_features", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.compute_market_mood", return_value=55.0), \
             patch("biz.analysis.sync_analysis_fast.compute_scores", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast._build_text_fields", return_value=empty_df), \
             patch("biz.analysis.sync_analysis_fast.build_analysis_rows", return_value=[]), \
             patch("biz.analysis.sync_analysis_fast.build_recommendation_rows", return_value=[]), \
             patch("biz.analysis.sync_analysis_fast.save_outputs"):
            from biz.analysis.sync_analysis_fast import run_batch

            stats = run_batch(
                engine=object(),
                trade_date="2026-06-13",
                progress_callback=progress_events.append,
            )

        self.assertEqual(stats.trade_date, "2026-06-13")
        self.assertTrue(progress_events)
        self.assertEqual(progress_events[0]["stage"], "load_kline")
        self.assertEqual(progress_events[-1]["stage"], "done")
        self.assertEqual(progress_events[-1]["analysis_count"], 0)


if __name__ == "__main__":
    unittest.main()
