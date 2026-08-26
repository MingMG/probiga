# -*- coding: utf-8 -*-
import unittest

from biz.analysis.sync_analysis_fast import (
    _load_sector_industry_memberships,
    add_strategy_signals,
    build_data_quality,
    build_recommendation_rows,
    build_strategy_trade_plan,
    choose_recommend_status,
    clamp_score,
    classify_notice_title,
    linear_score,
    load_flow_features,
    load_hot_rank,
    select_primary_strategy,
    validate_exact_daily_flow_coverage,
)
import pandas as pd
from unittest.mock import patch

from server.common.daily_stock_universe import DailyStockUniverse


class SyncAnalysisFastTest(unittest.TestCase):
    def test_flow_features_keep_only_exact_target_rows_per_stock(self):
        source = pd.DataFrame([
            {
                "stock_code": "1",
                "trade_date": "2026-08-25",
                "main_net_inflow": 10,
                "max_net_inflow": 1,
                "lg_net_inflow": 2,
                "mid_net_inflow": 3,
                "sm_net_inflow": 4,
            },
            {
                "stock_code": "1",
                "trade_date": "2026-08-26",
                "main_net_inflow": 20,
                "max_net_inflow": 2,
                "lg_net_inflow": 3,
                "mid_net_inflow": 4,
                "sm_net_inflow": 5,
            },
            {
                "stock_code": "2",
                "trade_date": "2026-08-25",
                "main_net_inflow": 30,
                "max_net_inflow": 3,
                "lg_net_inflow": 4,
                "mid_net_inflow": 5,
                "sm_net_inflow": 6,
            },
        ])
        with patch(
            "biz.analysis.sync_analysis_fast._recent_dates",
            return_value=["2026-08-26", "2026-08-25"],
        ), patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            return_value=source,
        ):
            frame, flow_date = load_flow_features(object(), "2026-08-26")

        self.assertEqual(flow_date, "2026-08-26")
        self.assertEqual(frame["stock_code"].tolist(), ["000001"])
        self.assertEqual(str(frame.iloc[0]["flow_trade_date"]), "2026-08-26")
        self.assertEqual(frame.iloc[0]["main_net_inflow_5d"], 30)

    def test_flow_quality_uses_each_stock_date_and_accepts_exact_zero(self):
        _, exact_flags = build_data_quality(
            {
                "flow_trade_date": "2026-08-26",
                "main_net_inflow": 0,
                "main_net_inflow_5d": 0,
            },
            trade_date="2026-08-26",
            flow_date="2026-08-26",
        )
        _, stale_flags = build_data_quality(
            {"flow_trade_date": "2026-08-25"},
            trade_date="2026-08-26",
            flow_date="2026-08-26",
        )
        _, missing_flags = build_data_quality(
            {"flow_trade_date": None},
            trade_date="2026-08-26",
            flow_date="2026-08-26",
        )

        self.assertNotIn("missing_flow", exact_flags)
        self.assertNotIn("stale_flow", exact_flags)
        self.assertIn("stale_flow", stale_flags)
        self.assertIn("missing_flow", missing_flags)

    def test_exact_flow_coverage_blocks_missing_traded_stock(self):
        universe = DailyStockUniverse(
            target_date="2026-08-26",
            catalog_batch_id="catalog",
            catalog_manifest_hash="a" * 64,
            catalog_member_set_hash="b" * 64,
            expected_codes=("000001", "000002"),
            expected_code_set_hash="c" * 64,
        )
        kline = pd.DataFrame([
            {"stock_code": "000001", "volume": 100, "amount": 1000},
            {"stock_code": "000002", "volume": 0, "amount": 0},
        ])
        with patch(
            "biz.analysis.sync_analysis_fast.load_daily_stock_universe",
            return_value=universe,
        ):
            with self.assertRaisesRegex(RuntimeError, "DATA_BLOCKED.*capital-flow"):
                validate_exact_daily_flow_coverage(
                    object(),
                    trade_date="2026-08-26",
                    kline=kline,
                    flow=pd.DataFrame({"stock_code": []}),
                )

    def test_hot_rank_never_falls_back_to_an_older_partition(self):
        observed = {}

        def read_sql(statement, _engine, params):
            observed["sql"] = str(statement)
            observed["params"] = dict(params)
            return pd.DataFrame()

        with patch("biz.analysis.sync_analysis_fast.pd.read_sql", side_effect=read_sql):
            frame, hot_date = load_hot_rank(object(), "2026-08-26")

        self.assertTrue(frame.empty)
        self.assertEqual(hot_date, "")
        self.assertIn("snapshot_date = :trade_date", observed["sql"])
        self.assertNotIn("snapshot_date <=", observed["sql"])
        self.assertEqual(observed["params"], {"trade_date": "2026-08-26"})

    def test_hot_rank_rejects_exact_date_single_source_rows(self):
        source = pd.DataFrame([
            {
                "stock_code": "1",
                "fused_rank": 1,
                "total_score": 100,
                "source_flag": "east_only",
            },
            {
                "stock_code": "2",
                "fused_rank": 2,
                "total_score": 99,
                "source_flag": "ths_only",
            },
        ])
        with patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            return_value=source,
        ):
            frame, hot_date = load_hot_rank(object(), "2026-08-26")

        self.assertTrue(frame.empty)
        self.assertEqual(hot_date, "")

    def test_hot_rank_keeps_only_exact_date_multi_source_consensus(self):
        source = pd.DataFrame([
            {
                "stock_code": "1",
                "fused_rank": 1,
                "total_score": 100,
                "source_flag": "east_only",
            },
            {
                "stock_code": "2",
                "fused_rank": 2,
                "total_score": 99,
                "source_flag": "east_ths",
            },
            {
                "stock_code": "3",
                "fused_rank": 3,
                "total_score": 98,
                "source_flag": "east_sina",
            },
        ])
        with patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            return_value=source,
        ):
            frame, hot_date = load_hot_rank(object(), "2026-08-26")

        self.assertEqual(hot_date, "2026-08-26")
        self.assertEqual(frame["stock_code"].tolist(), ["000003"])
        self.assertEqual(frame["source_flag"].tolist(), ["east_sina"])

    def test_sector_membership_prefers_complete_immutable_snapshot(self):
        run = pd.DataFrame([
            {
                "snapshot_date": "2026-08-11",
                "source": "QMT_LOCAL",
                "industry_relation_count": 2,
            }
        ])
        evidence = pd.DataFrame([{"relation_count": 2}])
        memberships = pd.DataFrame([
            {"stock_code": "000001", "industry_name": "Bank"},
            {"stock_code": "000002", "industry_name": "AI"},
        ])

        with patch(
            "biz.analysis.sync_analysis_fast._table_exists", return_value=True
        ), patch(
            "biz.analysis.sync_analysis_fast.pd.read_sql",
            side_effect=[run, evidence, memberships],
        ):
            result = _load_sector_industry_memberships(object(), "2026-08-11")

        self.assertEqual(len(result), 2)
        self.assertEqual(
            result.set_index("stock_code").loc["000002", "industry_name"],
            "AI",
        )

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

    def test_recommend_gate_suspends_missing_core_data(self):
        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="娴嬭瘯鑲′唤",
            ai_score=78,
            short_term_score=72,
            long_term_score=66,
            event_risk_level="LOW",
            amount=120_000_000,
            change_pct=3,
            min_score=62,
            data_quality_score=58,
            data_quality_flags=["missing_flow"],
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertTrue(reason)

    def test_recommend_gate_suspends_stale_flow_when_score_not_strong_enough(self):
        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="娴嬭瘯鑲′唤",
            ai_score=65,
            short_term_score=68,
            long_term_score=66,
            event_risk_level="LOW",
            amount=120_000_000,
            change_pct=3,
            min_score=62,
            data_quality_score=88,
            data_quality_flags=["stale_flow"],
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertTrue(reason)

    def test_recommend_gate_never_allows_stale_flow_even_with_high_score(self):
        status, reason = choose_recommend_status(
            stock_code="300001",
            short_name="测试股份",
            ai_score=99,
            short_term_score=98,
            long_term_score=97,
            event_risk_level="LOW",
            amount=120_000_000,
            change_pct=3,
            min_score=62,
            data_quality_score=88,
            data_quality_flags=["stale_flow"],
        )
        self.assertEqual(status, "SUSPENDED")
        self.assertIn("目标交易日", reason)

    def test_recommendation_rows_keep_soft_risk_in_observation_ledger(self):
        row = {
            "stock_code": "600001",
            "short_name": "candidate",
            "ai_score": 78,
            "long_term_score": 72,
            "short_term_score": 76,
            "quality_score": 79,
            "final_trade_score": 77,
            "entry_score": 68,
            "capital_score": 70,
            "main_wave_score": 65,
            "main_wave_signal": "WATCH",
            "signal_status": "WATCH",
            "recommend_status": "SUSPENDED",
            "recommend_reason": "keep observing",
            "event_risk_level": "LOW",
        }

        rows = build_recommendation_rows(
            pd.DataFrame([row]), "2026-08-11", top_n=80, min_score=62
        )

        self.assertEqual([item["stock_code"] for item in rows], ["600001"])
        self.assertEqual(rows[0]["recommend_status"], "SUSPENDED")
        self.assertEqual(rows[0]["signal_status"], "WATCH")

    def test_recommendation_rows_exclude_exit_signal(self):
        row = {
            "stock_code": "600001",
            "short_name": "candidate",
            "ai_score": 90,
            "long_term_score": 85,
            "short_term_score": 88,
            "quality_score": 92,
            "final_trade_score": 91,
            "entry_score": 86,
            "capital_score": 84,
            "main_wave_score": 82,
            "main_wave_signal": "SELL_ALERT",
            "signal_status": "WATCH",
            "recommend_status": "SUSPENDED",
            "recommend_reason": "exit",
            "event_risk_level": "LOW",
        }

        rows = build_recommendation_rows(
            pd.DataFrame([row]), "2026-08-11", top_n=80, min_score=62
        )

        self.assertEqual(rows, [])

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
             patch("biz.analysis.sync_analysis_fast.validate_exact_daily_flow_coverage"), \
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

    def test_strict_run_repairs_missing_qmt_kline_before_analysis(self):
        progress_events = []

        with patch("biz.analysis.sync_analysis_fast.previous_trade_date", return_value="2026-06-26"), \
             patch(
                 "biz.analysis.sync_analysis_fast.assert_trade_date_ready",
                 side_effect=[
                     RuntimeError("K-line latest date is 2026-06-25, earlier than required 2026-06-26"),
                     {
                         "trade_date": "2026-06-26",
                         "latest_kline_date": "2026-06-26",
                         "kline_count": 4200,
                         "expected_count": 5000,
                         "min_coverage": 0.8,
                     },
                 ],
             ) as ready_mock, \
             patch("biz.analysis.sync_analysis_fast.repair_missing_qmt_kline_for_trade_date") as repair_mock, \
             patch("biz.analysis.sync_analysis_fast._prepare_batch_outputs", return_value=([], [], 55.0, "2026-06-26", "2026-06-26")), \
             patch("biz.analysis.sync_analysis_fast.save_outputs"):
            from biz.analysis.sync_analysis_fast import run_batch

            stats = run_batch(
                engine=object(),
                trade_date="2026-06-26",
                strict_prev_trade_day=True,
                execution_time="2026-06-28 08:30:00",
                auto_repair_missing_kline=True,
                progress_callback=progress_events.append,
            )

        self.assertEqual(stats.trade_date, "2026-06-26")
        self.assertEqual(ready_mock.call_count, 2)
        repair_mock.assert_called_once_with("2026-06-26", progress_callback=progress_events.append)
        stages = [event["stage"] for event in progress_events]
        self.assertIn("strict_date_missing", stages)
        self.assertIn("strict_date_ready", stages)


if __name__ == "__main__":
    unittest.main()
