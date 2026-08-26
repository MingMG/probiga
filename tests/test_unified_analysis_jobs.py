# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch

from biz.analysis.sync_analysis_fast import BatchStats
from biz.analysis import sync_analysis_incremental, sync_analysis_result, sync_sim_trade


class UnifiedAnalysisJobsTest(unittest.TestCase):
    def test_sync_analysis_result_main_uses_full_batch_by_default(self):
        stats = BatchStats("2026-06-10", 10, 3, 55.0, "2026-06-10", "2026-06-10")
        with patch("sys.argv", ["sync_analysis_result"]), \
             patch("biz.analysis.sync_analysis_result.resolve_trade_date", return_value="2026-06-10"), \
             patch("biz.analysis.sync_analysis_result.get_engine", return_value=object()), \
             patch("biz.analysis.sync_analysis_result.run_batch", return_value=stats) as mock_run_batch, \
             patch("biz.analysis.sync_analysis_result.run_batch_for_codes") as mock_run_codes:
            code = sync_analysis_result.main()

        self.assertEqual(code, 0)
        mock_run_batch.assert_called_once()
        mock_run_codes.assert_not_called()

    def test_sync_analysis_result_main_uses_scoped_batch_for_single_code(self):
        stats = BatchStats("2026-06-10", 1, 1, 55.0, "2026-06-10", "2026-06-10")
        with patch("sys.argv", ["sync_analysis_result", "--code", "1"]), \
             patch("biz.analysis.sync_analysis_result.resolve_trade_date", return_value="2026-06-10"), \
             patch("biz.analysis.sync_analysis_result.get_engine", return_value=object()), \
             patch("biz.analysis.sync_analysis_result.run_batch_for_codes", return_value=stats) as mock_run_codes:
            code = sync_analysis_result.main()

        self.assertEqual(code, 0)
        mock_run_codes.assert_called_once()
        self.assertEqual(mock_run_codes.call_args.kwargs["stock_codes"], ["000001"])

    def test_incremental_main_uses_union_of_portfolio_and_recommendations(self):
        stats = BatchStats("2026-06-10", 2, 1, 55.0, "2026-06-10", "2026-06-10")
        with patch("biz.analysis.sync_analysis_incremental.get_portfolio_stocks", return_value=["000001"]), \
             patch("biz.analysis.sync_analysis_incremental.get_recommended_stocks", return_value=["000002", "000001"]), \
             patch("biz.analysis.sync_analysis_incremental.resolve_trade_date", return_value="2026-06-10"), \
             patch("biz.analysis.sync_analysis_incremental.get_engine", return_value=object()), \
             patch("biz.analysis.sync_analysis_incremental.run_batch_for_codes", return_value=stats) as mock_run_codes:
            code = sync_analysis_incremental.main()

        self.assertEqual(code, 0)
        self.assertEqual(mock_run_codes.call_args.kwargs["stock_codes"], ["000001", "000002"])

    def test_prepare_signals_requires_read_only_recommendation_prerequisite(self):
        with patch.dict(
            sync_sim_trade.os.environ,
            {"PROBIGA_DEPLOYMENT_MODE": "test"},
        ), patch("biz.analysis.sync_sim_trade._previous_trade_date", return_value="2026-06-26"), \
             patch(
                 "biz.analysis.sync_sim_trade.ensure_recommendations_for_signal_date",
                 return_value={
                     "status": "exists",
                     "signal_date": "2026-06-26",
                     "count": 3,
                     "read_only": True,
                 },
             ) as ensure_mock, \
             patch("biz.analysis.sync_sim_trade.SimTradeEngine") as engine_cls:
            engine_cls.return_value.prepare_signal_pool.return_value = {
                "status": "ok",
                "trade_date": "2026-06-29",
                "signal_date": "2026-06-26",
                "counts": {"total": 3},
            }

            out = sync_sim_trade.prepare_signals(
                trade_date="2026-06-29",
                ensure_recommendations=True,
            )

        ensure_mock.assert_called_once()
        self.assertEqual(out["recommendation_prerequisite"]["status"], "exists")
        self.assertTrue(out["recommendation_prerequisite"]["read_only"])
        self.assertNotIn("ensure_recommendations", out)
        engine_cls.return_value.prepare_signal_pool.assert_called_once_with(
            trade_date="2026-06-29",
            signal_date="2026-06-26",
            strict=True,
            reset=False,
        )


if __name__ == "__main__":
    unittest.main()
