# -*- coding: utf-8 -*-
import unittest

import pandas as pd

from integrations.myquant.bridge import to_gm_symbol, to_stock_code
from biz.stock_market.sync_stock_market import _myquant_daily_to_sm_kline


class MyQuantBridgeTest(unittest.TestCase):
    def test_symbol_mapping_supports_sh_sz_and_skips_bj(self):
        self.assertEqual(to_gm_symbol("600519"), "SHSE.600519")
        self.assertEqual(to_gm_symbol("000001"), "SZSE.000001")
        self.assertEqual(to_gm_symbol("300750"), "SZSE.300750")
        self.assertIsNone(to_gm_symbol("830799"))
        self.assertEqual(to_stock_code("SHSE.600519"), "600519")

    def test_daily_conversion_preserves_share_volume(self):
        raw = pd.DataFrame(
            [
                {
                    "symbol": "SHSE.600519",
                    "eob": "2026-06-12T00:00:00+08:00",
                    "open": 1271.18,
                    "high": 1295.0,
                    "low": 1265.01,
                    "close": 1291.91,
                    "pre_close": 1279.0,
                    "volume": 5049478,
                    "amount": 6477910214.0,
                }
            ]
        )

        converted = _myquant_daily_to_sm_kline(raw, {"600519": "贵州茅台"})

        self.assertEqual(len(converted), 1)
        row = converted.iloc[0]
        self.assertEqual(row["stock_code"], "600519")
        self.assertEqual(row["trade_date"], "2026-06-12")
        self.assertEqual(row["volume"], 5049478)
        self.assertAlmostEqual(row["change"], 12.91, places=2)
        self.assertAlmostEqual(row["change_pct"], 1.0094, places=3)


if __name__ == "__main__":
    unittest.main()
