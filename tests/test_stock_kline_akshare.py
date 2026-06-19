# -*- coding: utf-8 -*-
import unittest

import pandas as pd

from biz.stock_market.stock_kline_akshare import _klines_json_to_df, akshare_daily_to_sm_kline


class StockKlineAkshareTest(unittest.TestCase):
    def test_eastmoney_change_fields_are_preserved(self):
        raw = {
            "data": {
                "klines": [
                    "2026-06-09,422.79,440.87,448.50,415.00,39577,1726040855.00,8.08,6.36,26.37,4.40",
                    "2026-06-10,292.90,282.38,297.97,275.60,57894,1643631241.00,7.35,-7.26,-22.09,4.45",
                ]
            }
        }

        parsed = _klines_json_to_df(raw)
        converted = akshare_daily_to_sm_kline(parsed, "688167", 1, 0)

        latest = converted.iloc[-1]
        self.assertEqual(latest["trade_date"], "2026-06-10")
        self.assertAlmostEqual(latest["change_pct"], -7.26)
        self.assertAlmostEqual(latest["change"], -22.09)
        self.assertAlmostEqual(latest["pre_close"], 304.47)

    def test_missing_change_fields_fall_back_to_close_calculation(self):
        raw = pd.DataFrame(
            [
                {"date": "2026-06-09", "open": 10, "high": 11, "low": 9, "close": 10, "volume": 100, "amount": 1000},
                {"date": "2026-06-10", "open": 10, "high": 12, "low": 10, "close": 11, "volume": 120, "amount": 1300},
            ]
        )

        converted = akshare_daily_to_sm_kline(raw, "000001", 1, 0)

        latest = converted.iloc[-1]
        self.assertAlmostEqual(latest["change_pct"], 10.0)
        self.assertAlmostEqual(latest["change"], 1.0)

    def test_extreme_calculated_change_without_source_fields_is_suppressed(self):
        raw = pd.DataFrame(
            [
                {"date": "2026-06-09", "open": 34, "high": 36.3, "low": 33.6, "close": 36.01, "volume": 100, "amount": 1000},
                {"date": "2026-06-10", "open": 25.07, "high": 25.87, "low": 24.06, "close": 24.50, "volume": 120, "amount": 1300},
            ]
        )

        converted = akshare_daily_to_sm_kline(raw, "920026", 1, 0)

        latest = converted.iloc[-1]
        self.assertIsNone(latest["change_pct"])
        self.assertIsNone(latest["change"])


if __name__ == "__main__":
    unittest.main()
