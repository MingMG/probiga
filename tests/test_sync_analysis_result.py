# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch

import pandas as pd

from biz.analysis.sync_analysis_result import resolve_trade_date


class SyncAnalysisResultTest(unittest.TestCase):
    def test_resolve_trade_date_uses_latest_available_not_after_requested(self):
        frame = pd.DataFrame([{"d": "2026-06-13"}])
        with patch(
            "biz.analysis.sync_analysis_result.get_engine", return_value=object()
        ), patch("biz.analysis.sync_analysis_result.read_frame", return_value=frame):
            self.assertEqual(resolve_trade_date("2026-06-14"), "2026-06-13")

    def test_resolve_trade_date_raises_when_no_kline_data(self):
        frame = pd.DataFrame([{"d": None}])
        with patch(
            "biz.analysis.sync_analysis_result.get_engine", return_value=object()
        ), patch("biz.analysis.sync_analysis_result.read_frame", return_value=frame):
            with self.assertRaises(RuntimeError):
                resolve_trade_date("2026-06-14")


if __name__ == "__main__":
    unittest.main()
