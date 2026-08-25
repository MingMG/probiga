# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch

from server.engine.data_loader import StockDataLoader


class DataLoaderValuationHistoryTest(unittest.TestCase):
    @staticmethod
    def fake_read_sql(sql: str, params: dict | None = None) -> list[dict]:
        normalized = " ".join(sql.split())

        if "SELECT stock_code, short_name, exchange, list_date FROM si_all_code" in normalized:
            return [{"stock_code": "000001", "short_name": "测试股份", "exchange": "SZ", "list_date": "2010-01-01"}]
        if "SELECT stock_code, short_name FROM si_all_code WHERE stock_code = :c" in normalized:
            return [{"stock_code": "000001", "short_name": "测试股份"}]
        if "SELECT plate_name FROM si_stock_plate_east" in normalized:
            return []
        if "SELECT industry_name FROM si_industry_sw" in normalized:
            return []
        if "SELECT DISTINCT name FROM si_stock_concept_east" in normalized:
            return []
        if "SELECT close AS price, change_pct, open, high, low, volume, amount, turnover_ratio, pre_close FROM sm_stock_kline" in normalized:
            return [{
                "price": 30.0,
                "change_pct": 1.0,
                "open": 29.0,
                "high": 31.0,
                "low": 28.0,
                "volume": 100000,
                "amount": 3000000,
                "turnover_ratio": 2.0,
                "pre_close": 29.7,
            }]
        if "SELECT total_shares, limit_shares, list_a_shares FROM si_stock_shares" in normalized:
            return [{"total_shares": 100000000, "limit_shares": 0, "list_a_shares": 80000000}]
        if "SELECT AVG(volume) AS avg_vol FROM (" in normalized:
            return [{"avg_vol": 100000}]
        if "SELECT basic_eps, net_asset_ps, roe_wtd, roa_wtd, gross_margin, net_margin," in normalized:
            return [{
                "basic_eps": 3.0,
                "net_asset_ps": 6.0,
                "roe_wtd": 12.0,
                "roa_wtd": 6.0,
                "gross_margin": 35.0,
                "net_margin": 12.0,
                "total_rev": 100.0,
                "net_profit_attr_sh": 20.0,
                "total_rev_yoy_gr": 15.0,
                "net_profit_yoy_gr": 18.0,
                "report_date": "2026-03-31",
            }]
        if "SELECT basic_eps, net_asset_ps, roe_wtd, gross_margin, asset_liab_ratio," in normalized:
            return [{
                "basic_eps": 3.0,
                "net_asset_ps": 6.0,
                "roe_wtd": 12.0,
                "gross_margin": 35.0,
                "asset_liab_ratio": 45.0,
                "total_rev_yoy_gr": 15.0,
                "net_profit_yoy_gr": 18.0,
                "report_date": "2026-03-31",
            }]
        if "SELECT MAX(trade_date) AS d FROM sm_stock_capital_flow_daily" in normalized:
            return [{"d": "2026-06-10"}]
        if "FROM sm_stock_capital_flow_daily WHERE stock_code = :c AND trade_date = :ftd" in normalized:
            return []
        if "FROM sm_stock_capital_flow_daily WHERE stock_code = :c AND trade_date = :td" in normalized:
            return []
        if "SELECT COUNT(*) AS cnt, SUM(a_net_amount) AS inst_net_buy FROM st_a_list_daily" in normalized:
            return [{"cnt": 0, "inst_net_buy": 0}]
        if "FROM st_a_list_info" in normalized:
            return []
        if "SELECT report_date, report_type, basic_eps, net_asset_ps," in normalized:
            return []
        if "AS ref_close" in normalized and "FROM si_stock_finance f" in normalized:
            return [
                {"report_date": "2026-03-31", "basic_eps": 3.0, "net_asset_ps": 6.0, "ref_close": 33.0},
                {"report_date": "2025-12-31", "basic_eps": 2.0, "net_asset_ps": 5.0, "ref_close": 18.0},
                {"report_date": "2025-09-30", "basic_eps": 1.0, "net_asset_ps": 4.0, "ref_close": 8.0},
            ]
        if "SELECT trade_date, open, close, high, low, volume, change_pct FROM sm_stock_kline" in normalized:
            return []
        if "FROM si_notice_eastmoney" in normalized:
            return []
        if "FROM st_news_flash" in normalized:
            return []
        if "FROM st_hot_rank_fused" in normalized:
            return []
        if "FROM st_stock_lifting_last_month" in normalized:
            return []
        if "FROM st_mine_clearance_tdx" in normalized:
            return []
        if "FROM st_user_portfolio" in normalized:
            return []
        if "FROM si_stock_holder" in normalized:
            return []
        return []

    def test_load_full_data_uses_historical_prices_for_percentiles(self):
        loader = StockDataLoader()
        with patch("server.engine.data_loader._latest_kline_trade_date", return_value="2026-06-10"), \
             patch("server.engine.data_loader._read_sql", side_effect=self.fake_read_sql):
            result = loader.load_full_data("000001", trade_date="2026-06-10", use_realtime=False)

        valuation = result["valuation"]
        self.assertEqual(valuation["pe_ttm"], 10.0)
        self.assertEqual(valuation["pb"], 5.0)
        self.assertEqual(valuation["pe_percentile"], 66.7)
        self.assertEqual(valuation["pb_percentile"], 66.7)
        self.assertEqual(valuation["verdict"], "合理")

    def test_load_light_data_reuses_historical_percentiles(self):
        loader = StockDataLoader()
        with patch("server.engine.data_loader._latest_kline_trade_date", return_value="2026-06-10"), \
             patch("server.engine.data_loader._read_sql", side_effect=self.fake_read_sql):
            result = loader.load_light_data("000001", trade_date="2026-06-10", use_realtime=False)

        valuation = result["valuation"]
        self.assertEqual(valuation["pe_percentile"], 66.7)
        self.assertEqual(valuation["pb_percentile"], 66.7)
        self.assertEqual(valuation["verdict"], "合理")

    def test_strategy_context_never_reads_mutable_reference_inputs(self):
        forbidden = (
            "si_stock_plate_east",
            "si_industry_sw",
            "si_stock_concept_east",
            "si_stock_holder",
            "st_hot_rank_fused",
            "st_stock_lifting_last_month",
            "st_mine_clearance_tdx",
        )

        def strict_reader(sql: str, params: dict | None = None) -> list[dict]:
            normalized = " ".join(sql.split())
            for table in forbidden:
                if table in normalized:
                    raise AssertionError(f"strategy read leaked into {table}")
            return self.fake_read_sql(sql, params)

        finance = {
            "basic_eps": 3.0,
            "net_asset_ps": 6.0,
            "finance_report_date": "2026-03-31",
        }
        finance_evidence = {
            "pit_status": "AVAILABLE",
            "pit_reason": "",
            "manifest_hash": "f" * 64,
            "revision_ids": ["a" * 64],
            "content_hashes": ["b" * 64],
        }
        event_evidence = {
            "event_pit_status": "AVAILABLE",
            "event_pit_reason": "",
            "event_manifest_hash": "e" * 64,
            "event_revision_ids": [],
            "event_content_hashes": [],
        }
        loader = StockDataLoader()
        with patch(
            "server.engine.data_loader._latest_kline_trade_date",
            return_value="2026-06-10",
        ), patch(
            "server.engine.data_loader._read_sql", side_effect=strict_reader
        ), patch(
            "server.engine.data_loader._pit_finance_bundle",
            return_value=(finance, [finance], [8.0], [4.0], finance_evidence),
        ), patch(
            "server.engine.data_loader._pit_notice_bundle",
            return_value=([], event_evidence),
        ):
            result = loader.load_full_data(
                "000001",
                trade_date="2026-06-10",
                use_realtime=False,
                strategy_context=True,
                decision_at="2026-06-10 15:10:00",
                fact_cutoff_at="2026-06-10 15:00:00",
            )

        self.assertIsNone(result["industry"])
        self.assertEqual(result["concepts"], [])
        self.assertEqual(result["holder"], {})
        self.assertEqual(result["hot_rank"], {})
        self.assertIsNone(result["lifting"]["has_lifting_soon"])
        self.assertEqual(result["mine_clearance"]["pit_status"], "DATA_BLOCKED")
        self.assertEqual(
            result["strategy_reference_evidence"]["status"], "DATA_BLOCKED"
        )


if __name__ == "__main__":
    unittest.main()
