# -*- coding: utf-8 -*-
from decimal import Decimal
import unittest
from unittest.mock import MagicMock, patch

from server.api.routers import hot_data


class HotDataDetailHelperTest(unittest.TestCase):
    def test_normalize_db_value_handles_decimal_and_nan(self):
        self.assertEqual(hot_data._normalize_db_value(Decimal("12.34")), 12.34)
        self.assertIsNone(hot_data._normalize_db_value(float("nan")))

    def test_legacy_capital_view_restores_yuan_units(self):
        capital = {
            "today": {
                "main_net_inflow": 123.45,
                "max_net_inflow": 67.89,
                "lg_net_inflow": -12.0,
                "mid_net_inflow": 3.0,
                "sm_net_inflow": None,
                "data_source": "east",
            },
            "flow_3d": 456.78,
            "flow_5d": None,
            "flow_20d": -90.12,
            "dragon_tiger": {"count_20d": 1, "inst_net_buy": 8888},
        }

        out = hot_data._legacy_capital_view(capital)

        self.assertEqual(out["today"]["main_net_inflow"], 1234500.0)
        self.assertEqual(out["today"]["max_net_inflow"], 678900.0)
        self.assertEqual(out["today"]["lg_net_inflow"], -120000.0)
        self.assertEqual(out["flow_3d"], 4567800.0)
        self.assertEqual(out["flow_20d"], -901200.0)
        self.assertEqual(out["dragon_tiger"]["inst_net_buy"], 8888)

    def test_load_stock_detail_payload_uses_loader_and_mode_specific_dates(self):
        fake_loader = MagicMock()
        fake_loader.load_full_data.return_value = {
            "stock_code": "000001",
            "trade_date": "2026-06-13",
            "capital": {"today": {"main_net_inflow": 10.0}},
        }

        with patch("server.api.routers.hot_data.StockDataLoader", return_value=fake_loader), \
             patch("server.api.routers.hot_data._portfolio_close_trade_date", return_value="2026-06-13"):
            out = hot_data._load_stock_detail_payload("000001", mode="post_close")

        fake_loader.load_full_data.assert_called_once_with(
            "000001",
            trade_date="2026-06-13",
            use_realtime=False,
        )
        self.assertEqual(out["capital"]["today"]["main_net_inflow"], 100000.0)

    def test_aggregate_concept_multi_day_rows_without_pandas_dataframe(self):
        rows = [
            {
                "snapshot_date": "2026-06-14",
                "plate_type": 1,
                "rank": 3,
                "concept_code": "C001",
                "concept_name": "AI",
                "change_pct": 2.0,
                "hot_value": 100,
            },
            {
                "snapshot_date": "2026-06-13",
                "plate_type": 1,
                "rank": 5,
                "concept_code": "C001",
                "concept_name": "AI",
                "change_pct": 1.0,
                "hot_value": 80,
            },
            {
                "snapshot_date": "2026-06-14",
                "plate_type": 1,
                "rank": 1,
                "concept_code": "C002",
                "concept_name": "机器人",
                "change_pct": 5.0,
                "hot_value": 120,
            },
        ]

        out = hot_data._aggregate_concept_multi_day_rows(rows, days=3)

        self.assertEqual(out[0]["concept_code"], "C001")
        self.assertEqual(out[0]["appear_days"], 2)
        self.assertEqual(out[0]["avg_rank"], 4.0)
        self.assertEqual(out[0]["last_rank"], 3.0)
        self.assertEqual(out[0]["avg_hot_value"], 90.0)
        self.assertEqual(out[1]["concept_code"], "C002")
        self.assertEqual(out[1]["appear_pct"], 33.3)

    def test_portfolio_live_reuses_shared_snapshot(self):
        snapshot = {"data": [{"stock_code": "000001"}], "total": 1, "summary": {"holding_count": 1}}

        with patch("server.api.routers.hot_data._get_portfolio_snapshot", return_value={**snapshot, "live": True}) as snapshot_mock:
            out = hot_data.portfolio_live()

        snapshot_mock.assert_called_once_with(live_mode=True)
        self.assertTrue(out["live"])
        self.assertEqual(out["total"], 1)

    def test_monitor_resolve_trade_date_uses_latest_before_requested(self):
        with patch("server.api.routers.hot_data._read_sql", side_effect=[
            [{"d": "2026-06-13"}],
        ]):
            out = hot_data._monitor_resolve_trade_date("2026-06-14")

        self.assertEqual(out, "2026-06-13")

    def test_recommended_progress_uses_long_ttl(self):
        with patch("server.api.routers.hot_data._cache_get", return_value={"status": "done"}) as cache_get_mock, \
             patch("server.api.routers.hot_data._job_is_running", return_value=False):
            out = hot_data.recommended_stocks_progress()

        cache_get_mock.assert_called_once_with("rec_screen_progress", ttl_seconds=7200)
        self.assertEqual(out["status"], "done")


if __name__ == "__main__":
    unittest.main()
