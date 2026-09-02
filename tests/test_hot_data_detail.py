# -*- coding: utf-8 -*-
from datetime import date, datetime, timedelta
from decimal import Decimal
import inspect
import os
import threading
import unittest
from unittest.mock import MagicMock, call, patch

from server.api.routers import hot_data
from server.common.sql_reader import current_bound_sql_connection


class _FakeIntradayDatetime(datetime):
    @classmethod
    def now(cls):
        return cls(2026, 6, 26, 13, 30, 0)


class _FakeWeekendDatetime(datetime):
    @classmethod
    def now(cls):
        return cls(2026, 6, 28, 10, 0, 0)


class _FakePremarketDatetime(datetime):
    @classmethod
    def now(cls):
        return cls(2026, 7, 8, 8, 30, 0)


class _FakeLunchDatetime(datetime):
    @classmethod
    def now(cls):
        return cls(2026, 6, 26, 12, 10, 0)


class _FakeAfterCloseDatetime(datetime):
    @classmethod
    def now(cls):
        return cls(2026, 6, 26, 15, 20, 0)


class HotDataDetailHelperTest(unittest.TestCase):
    def _clear_hot_data_cache(self):
        with hot_data._cache_lock:
            hot_data._cache_store.clear()
            hot_data._portfolio_completed_force_requests.clear()
            hot_data._portfolio_snapshot_generation = 0
        with hot_data._fallback_lock:
            hot_data._fallback_events.clear()

    def test_normalize_db_value_handles_decimal_and_nan(self):
        self.assertEqual(hot_data._normalize_db_value(Decimal("12.34")), 12.34)
        self.assertIsNone(hot_data._normalize_db_value(float("nan")))

    def test_startup_news_counts_use_one_batch_query(self):
        rows = [
            {"stocks": '[{"code":"000001"},{"code":"600000"}]'},
            {"stocks": '[{"code":"000001"}]'},
            {"stocks": None},
        ]
        with patch("server.api.routers.hot_data._read_sql", return_value=rows) as read_sql:
            result = hot_data._news_counts_for_codes(
                ["1", "600000"],
                "2026-07-23",
            )

        self.assertEqual(result, {"000001": 2, "600000": 1})
        read_sql.assert_called_once()
        query = read_sql.call_args.args[0]
        self.assertIn("publish_time < DATE_ADD(:d, INTERVAL 1 DAY)", query)

    def test_portfolio_kline_quote_is_marked_closed(self):
        row = {}
        hot_data._portfolio_apply_kline_quote(
            row,
            {
                "close": 12.34,
                "change_pct": 1.23,
                "short_name": "示例股份",
                "trade_date": "2026-07-03",
            },
        )

        self.assertEqual(row["cur_price"], 12.34)
        self.assertEqual(row["quote_status"], "closed")
        self.assertEqual(row["quote_source"], "daily_kline")
        self.assertEqual(row["quote_trade_date"], "2026-07-03")

    def test_portfolio_live_quote_change_is_rebased_from_previous_close(self):
        row = {}
        hot_data._portfolio_apply_quote(
            row,
            {
                "price": 49.81,
                "change": 0,
                "change_pct": 0,
                "snapshot_at": "2026-07-06 10:00:00",
                "source": "qmt_live_table",
                "quote_status": "fresh",
            },
        )
        hot_data._portfolio_rebase_quote_change(
            row,
            {"trade_date": "2026-07-03", "close": 50.22, "pre_close": 49.30},
        )

        self.assertEqual(row["quote_prev_close"], 50.22)
        self.assertEqual(row["price_change"], -0.41)
        self.assertEqual(row["change_pct"], -0.82)

    def test_portfolio_post_close_prefers_today_current_snapshot_over_previous_kline(self):
        row = {}
        hot_data._portfolio_apply_snapshot_quote(
            row,
            portfolio_mode="post_close",
            close_trade_date="2026-07-23",
            kline={
                "trade_date": "2026-07-22",
                "close": 10.0,
                "pre_close": 9.5,
                "change_pct": 5.26,
            },
            closed_quote={
                "stock_code": "000001",
                "price": 11.0,
                "change": 1.0,
                "change_pct": 10.0,
                "snapshot_at": "2026-07-23 15:01:00",
                "source": "current_close_table",
                "quote_status": "closed",
            },
        )

        self.assertEqual(row["cur_price"], 11.0)
        self.assertEqual(row["quote_trade_date"], "2026-07-23")
        self.assertEqual(row["quote_source"], "current_close_table")
        self.assertEqual(row["quote_status"], "closed")
        self.assertEqual(row["change_pct"], 10.0)

    def test_portfolio_live_quote_keeps_provider_day_change_when_kline_is_stale(self):
        row = {}
        hot_data._portfolio_apply_snapshot_quote(
            row,
            portfolio_mode="post_close",
            close_trade_date="2026-08-26",
            expected_previous_trade_date="2026-08-25",
            kline={
                "trade_date": "2026-08-21",
                "close": 36.61,
                "pre_close": 33.28,
                "change_pct": 10.01,
            },
            closed_quote={
                "stock_code": "000603",
                "price": 37.16,
                "change": 0.89,
                "change_pct": 2.453819,
                "snapshot_at": "2026-08-26 15:00:00",
                "source": "current_close_table",
                "quote_status": "closed",
            },
        )

        self.assertEqual(row["quote_trade_date"], "2026-08-26")
        self.assertEqual(row["price_change"], 0.89)
        self.assertEqual(row["change_pct"], 2.453819)
        self.assertNotIn("quote_prev_close", row)

    def test_portfolio_close_receipt_requires_post_close_full_market_proof(self):
        engine = MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        connection.execute.return_value.mappings.return_value.first.return_value = {
            "receipt_id": "r1",
            "expected_count": 5205,
            "observed_count": 5205,
            "coverage": Decimal("1.0"),
            "source_generated_at": "2026-08-26 15:00:05",
            "published_at": "2026-08-26 15:00:09",
            "capture_mode": "OFF_SESSION_SNAPSHOT",
        }

        with patch(
            "server.api.routers.hot_data.get_current_engine",
            return_value=engine,
        ):
            receipt = hot_data._portfolio_verified_close_receipt("2026-08-26")

        self.assertEqual(receipt["receipt_id"], "r1")
        sql = str(connection.execute.call_args.args[0])
        params = connection.execute.call_args.args[1]
        self.assertIn("source_generated_at >= :close_start", sql)
        self.assertEqual(params["close_start"], datetime(2026, 8, 26, 15, 0))

        connection.execute.return_value.mappings.return_value.first.return_value = {
            **receipt,
            "source_generated_at": "2026-08-26 11:30:00",
        }
        with patch(
            "server.api.routers.hot_data.get_current_engine",
            return_value=engine,
        ):
            self.assertIsNone(
                hot_data._portfolio_verified_close_receipt("2026-08-26")
            )

    def test_portfolio_closed_quote_rejects_morning_snapshot_even_with_receipt(self):
        morning = {
            "stock_code": "000001",
            "short_name": "示例股份",
            "price": 10.5,
            "change": 0.5,
            "change_pct": 5.0,
            "volume": 100,
            "amount": 1000,
            "snapshot_at": "2026-08-26 11:30:00",
        }
        with patch(
            "server.api.routers.hot_data._portfolio_verified_close_receipt",
            return_value={"receipt_id": "r1"},
        ), patch(
            "server.api.routers.hot_data._read_sql",
            side_effect=[[morning], [morning]],
        ) as read_sql:
            result = hot_data._portfolio_closed_quotes_from_current_table(
                ["000001"], "2026-08-26"
            )

        self.assertEqual(result, {})
        self.assertIn(":td_close_start", read_sql.call_args_list[0].args[0])

    def test_portfolio_closed_quote_accepts_receipt_backed_close_snapshot(self):
        close = {
            "stock_code": "000001",
            "short_name": "示例股份",
            "price": 10.8,
            "change": 0.8,
            "change_pct": 8.0,
            "volume": 100,
            "amount": 1000,
            "snapshot_at": "2026-08-26 15:00:01",
        }
        with patch(
            "server.api.routers.hot_data._portfolio_verified_close_receipt",
            return_value={"receipt_id": "r1"},
        ), patch(
            "server.api.routers.hot_data._read_sql",
            return_value=[close],
        ):
            result = hot_data._portfolio_closed_quotes_from_current_table(
                ["000001"], "2026-08-26"
            )

        self.assertEqual(result["000001"]["quote_status"], "closed")
        self.assertEqual(result["000001"]["source"], "current_close_table")
        self.assertEqual(result["000001"]["price"], 10.8)

    def test_portfolio_concept_tags_bind_the_explicit_closed_trade_date(self):
        source = inspect.getsource(hot_data._build_portfolio_snapshot)

        self.assertIn("snapshot_date = :profile_date", source)
        self.assertIn('"profile_date": profile_date', source)
        self.assertNotIn("SELECT MAX(snapshot_date) FROM st_hot_rank_ths", source)
        self.assertIn('row["concept_tag_date"]', source)

    def test_hot_data_cache_evicts_least_recently_used_entry(self):
        self._clear_hot_data_cache()
        try:
            with patch("server.api.routers.hot_data.get_api_cache_config", return_value={"max_entries": 2}):
                hot_data._cache_set("a", 1)
                hot_data._cache_set("b", 2)
                self.assertEqual(hot_data._cache_get("a", ttl_seconds=60), 1)
                hot_data._cache_set("c", 3)

            self.assertEqual(hot_data._cache_get("a", ttl_seconds=60), 1)
            self.assertIsNone(hot_data._cache_get("b", ttl_seconds=60))
            self.assertEqual(hot_data._cache_get("c", ttl_seconds=60), 3)
        finally:
            self._clear_hot_data_cache()

    def test_hot_data_cache_drops_expired_entry(self):
        self._clear_hot_data_cache()
        try:
            hot_data._cache_set("expired", "value")

            self.assertIsNone(hot_data._cache_get("expired", ttl_seconds=0))
            with hot_data._cache_lock:
                self.assertNotIn("expired", hot_data._cache_store)
        finally:
            self._clear_hot_data_cache()

    def test_fallback_health_reports_recorded_contexts(self):
        self._clear_hot_data_cache()
        try:
            hot_data._record_fallback("unit-test", RuntimeError("boom"))
            hot_data._record_fallback("unit-test", RuntimeError("again"))

            payload = hot_data.fallback_health()

            self.assertEqual(payload["status"], "observed")
            self.assertEqual(payload["total_fallbacks"], 2)
            self.assertEqual(payload["contexts"][0]["context"], "unit-test")
            self.assertEqual(payload["contexts"][0]["count"], 2)
            self.assertIn("RuntimeError", payload["contexts"][0]["last_error"])
        finally:
            self._clear_hot_data_cache()

    def test_strategy_runtime_params_snapshot_merges_runtime_values(self):
        runtime_rows = [
            {
                "param_key": "min_risk_reward",
                "param_value": Decimal("3.25"),
                "source": "calibration",
                "effective_date": "2026-06-30",
                "updated_at": "2026-06-30 18:00:00",
                "metadata_json": '{"direction":"tighten"}',
            }
        ]
        calibration_rows = [{
            "calibration_date": "2026-06-30",
            "window_days": 90,
            "scope_type": "global",
            "scope_key": "all",
            "sample_count": 42,
            "avg_return_5d": Decimal("-1.2"),
            "win_rate_5d": Decimal("39.0"),
            "suggestion": "建议收紧",
        }]

        with patch("server.api.routers.hot_data._read_sql", side_effect=[runtime_rows, calibration_rows]):
            out = hot_data._strategy_runtime_params_snapshot("2026-06-30")

        self.assertEqual(out["params_map"]["min_risk_reward"], 3.25)
        self.assertIn("price_crosscheck_tolerance_pct", out["params_map"])
        runtime_item = next(item for item in out["params"] if item["param_key"] == "min_risk_reward")
        self.assertEqual(runtime_item["source"], "calibration")
        self.assertEqual(runtime_item["metadata"]["direction"], "tighten")

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

    def test_market_clock_uses_today_during_intraday_even_if_daily_data_lags(self):
        with patch("server.api.routers.hot_data.datetime", _FakeIntradayDatetime), patch(
            "server.api.routers.hot_data._read_sql",
            side_effect=[
                [{"d": "2026-06-26"}],
                [{"d": "2026-06-25"}],
                [{"d": "2026-06-25"}],
            ],
        ):
            out = hot_data.market_clock()

        self.assertEqual(out["phase"], "intraday")
        self.assertTrue(out["is_intraday"])
        self.assertEqual(out["active_trade_date"], "2026-06-26")
        self.assertEqual(out["recommendation_trade_date"], "2026-06-25")
        self.assertEqual(out["ui_trade_date"], "2026-06-26")
        self.assertEqual(out["latest_data_date"], "2026-06-25")

    def test_market_clock_uses_latest_trade_date_on_closed_day(self):
        with patch("server.api.routers.hot_data.datetime", _FakeWeekendDatetime), patch(
            "server.api.routers.hot_data._read_sql",
            side_effect=[
                [{"d": "2026-06-26"}],
                [{"d": "2026-06-26"}],
            ],
        ):
            out = hot_data.market_clock()

        self.assertEqual(out["phase"], "closed")
        self.assertFalse(out["is_trade_day"])
        self.assertEqual(out["expected_trade_date"], "2026-06-26")
        self.assertEqual(out["recommendation_trade_date"], "2026-06-26")
        self.assertEqual(out["ui_trade_date"], "2026-06-26")

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

    def test_load_stock_detail_payload_light_prefers_snapshot_market_data(self):
        def _fake_read_sql(sql, params=None):
            if "FROM si_all_code" in sql:
                return [{"stock_code": "000001", "short_name": "Ping An Bank", "exchange": "SZ", "list_date": "1991-04-03"}]
            if "FROM si_stock_plate_east" in sql:
                return [{"plate_name": "Bank"}]
            if "FROM si_stock_concept_east" in sql:
                return [{"name": "Finance"}]
            if "FROM sm_stock_snapshot" in sql:
                return [{
                    "price": 12.34,
                    "close": 12.34,
                    "change_pct": 1.23,
                    "open": 12.0,
                    "high": 12.5,
                    "low": 11.9,
                    "volume": 1000,
                    "amount": 1200000,
                    "turnover_ratio": 2.5,
                    "pre_close": 12.19,
                    "market_cap": 123456789.0,
                    "main_net_inflow": 100000.0,
                    "max_net_inflow": 50000.0,
                    "lg_net_inflow": 25000.0,
                    "mid_net_inflow": 12000.0,
                    "sm_net_inflow": -30000.0,
                }]
            if "FROM si_stock_shares" in sql:
                return [{"total_shares": 10000000, "list_a_shares": 8000000}]
            if "SELECT MAX(trade_date) AS d FROM sm_stock_capital_flow_daily" in sql:
                return [{"d": "2026-06-18"}]
            if "FROM sm_stock_capital_flow_daily WHERE stock_code = :c AND trade_date = :td" in sql:
                return [{"main_net_inflow": 100000.0, "max_net_inflow": 50000.0, "lg_net_inflow": 25000.0,
                         "mid_net_inflow": 12000.0, "sm_net_inflow": -30000.0, "data_source": "east"}]
            if "FROM si_stock_finance" in sql:
                return [{"basic_eps": 1.23, "net_asset_ps": 4.56, "report_date": "2026-03-31"}]
            if "FROM si_stock_holder" in sql:
                return [{"report_date": "2026-03-31", "holder_num": 1000, "holder_num_change": -20, "holder_num_ratio": -2.0}]
            return []

        with patch("server.api.routers.hot_data._portfolio_close_trade_date", return_value="2026-06-21"), \
             patch("server.api.routers.hot_data._latest_date_not_after", return_value="2026-05-29") as latest_mock, \
             patch("server.api.routers.hot_data._read_sql", side_effect=_fake_read_sql):
            out = hot_data._load_stock_detail_payload("1", mode="post_close", light=True)

        latest_mock.assert_called_once_with("sm_stock_snapshot", "2026-06-21")
        self.assertEqual(out["trade_date"], "2026-05-29")
        self.assertEqual(out["requested_trade_date"], "2026-06-21")
        self.assertEqual(out["quote_trade_date"], "2026-05-29")
        self.assertEqual(out["flow_trade_date"], "2026-06-18")
        self.assertEqual(out["quote_source"], "snapshot")
        self.assertEqual(out["detail_source"], "snapshot_light")
        self.assertEqual(out["market"]["price"], 12.34)
        self.assertEqual(out["market"]["market_cap"], 123456789.0)
        self.assertEqual(out["basic"]["short_name"], "Ping An Bank")

    def test_load_latest_analysis_snapshot_uses_latest_available_row(self):
        row = {
            "stock_code": "000001",
            "stock_name": "Ping An Bank",
            "analysis_date": "2026-06-13",
            "strengths": "[\"trend\"]",
            "risks": "[\"volatility\"]",
            "event_risk_detail": "[{\"title\": \"notice\"}]",
            "data_quality_flags": "[\"stale_flow\"]",
            "recommend_status": "ALLOW",
        }

        with patch("server.api.routers.hot_data._read_sql", return_value=[row]) as read_sql_mock:
            out = hot_data._load_latest_analysis_snapshot("000001", trade_date="2026-06-14")

        sql, params = read_sql_mock.call_args.args
        self.assertIn("analysis_date <= :trade_date", sql)
        self.assertEqual(params["stock_code"], "000001")
        self.assertEqual(params["trade_date"], "2026-06-14")
        self.assertEqual(out["short_name"], "Ping An Bank")
        self.assertEqual(out["analysis_date"], "2026-06-13")
        self.assertEqual(out["strengths"], ["trend"])
        self.assertEqual(out["risks"], ["volatility"])
        self.assertEqual(out["event_risk_detail"], [{"title": "notice"}])
        self.assertEqual(out["data_quality_flags"], ["stale_flow"])

    def test_load_latest_recommendation_snapshot_exposes_trade_score(self):
        columns = {
            "stock_code", "short_name", "pick_date", "ai_score", "final_trade_score",
            "quality_score", "entry_score", "short_term_score", "long_term_score",
            "capital_score", "technical", "signal_status", "recommend_status",
            "recommend_reason", "primary_strategy", "model_version", "last_check_time",
            "created_at",
        }
        row = {
            "stock_code": "000001",
            "short_name": "Ping An",
            "pick_date": "2026-06-13",
            "ai_score": 70,
            "final_trade_score": 77,
            "quality_score": 72,
            "entry_score": 80,
        }

        with patch("server.api.routers.hot_data._table_columns", return_value=columns), \
             patch("server.api.routers.hot_data._read_sql", return_value=[row]) as read_mock:
            out = hot_data._load_latest_recommendation_snapshot("000001", trade_date="2026-06-14")

        sql = read_mock.call_args.args[0]
        self.assertIn("st_recommended_stocks", sql)
        self.assertIn("pick_date <= :trade_date", sql)
        self.assertEqual(out["recommendation_score"], 77.0)
        self.assertEqual(out["recommendation_score_source"], "final_trade_score")

    def test_stock_detail_includes_analysis_snapshot(self):
        payload = {
            "basic": {
                "short_name": "Ping An Bank",
                "exchange": "SZ",
                "list_date": "1991-04-03",
            },
            "market": {"price": 12.34},
            "capital": {},
            "finance": {},
            "valuation": {},
            "technical": {},
            "news": {},
            "holder": {},
            "holding": None,
            "industry": "Bank",
            "concepts": ["Finance"],
            "trade_date": "2026-06-13",
            "requested_trade_date": "2026-06-21",
            "quote_trade_date": "2026-06-13",
            "flow_trade_date": "2026-06-12",
            "quote_source": "snapshot",
            "detail_source": "snapshot_light",
            "hot_rank": {"fused_rank": 5},
        }
        snapshot = {
            "stock_code": "000001",
            "analysis_date": "2026-06-10",
            "recommend_status": "ALLOW",
            "summary": "summary",
        }

        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._cache_set") as cache_set_mock, \
             patch("server.api.routers.hot_data._portfolio_market_mode", return_value="post_close"), \
             patch("server.api.routers.hot_data._stock_detail_portfolio_context", return_value={}), \
             patch("server.api.routers.hot_data._load_stock_detail_payload", return_value=payload) as payload_mock, \
             patch("server.api.routers.hot_data._load_latest_analysis_snapshot", return_value=snapshot) as snapshot_mock, \
             patch("server.api.routers.hot_data._load_latest_recommendation_snapshot", return_value={"final_trade_score": 77.0, "ai_score": 70.0}) as rec_mock, \
             patch("server.api.routers.hot_data._generate_ai_analysis", return_value={"score": 88, "analysis_date": "2026-06-10"}) as analysis_mock:
            out = hot_data.stock_detail("1")

        payload_mock.assert_called_once_with("000001", mode="post_close", light=True)
        snapshot_mock.assert_called_once_with("000001", trade_date="2026-06-13")
        rec_mock.assert_called_once_with("000001", trade_date="2026-06-13")
        analysis_mock.assert_called_once()
        self.assertEqual(analysis_mock.call_args.kwargs["analysis_snapshot"], snapshot)
        self.assertTrue(analysis_mock.call_args.kwargs["prefer_snapshot"])
        self.assertEqual(out["stock_code"], "000001")
        self.assertEqual(out["analysis_snapshot"]["recommend_status"], "ALLOW")
        self.assertEqual(out["recommendation_snapshot"]["final_trade_score"], 77.0)
        self.assertEqual(out["ai_analysis"]["score"], 88)
        self.assertEqual(out["requested_trade_date"], "2026-06-21")
        self.assertEqual(out["quote_trade_date"], "2026-06-13")
        self.assertEqual(out["flow_trade_date"], "2026-06-12")
        self.assertEqual(out["analysis_trade_date"], "2026-06-10")
        self.assertEqual(out["quote_source"], "snapshot")
        self.assertEqual(out["detail_source"], "snapshot_light")
        self.assertTrue(out["quote_is_stale"])
        self.assertTrue(out["flow_is_stale"])
        self.assertTrue(out["analysis_is_stale"])
        cache_set_mock.assert_called_once()

    def test_stock_detail_overlays_current_watchlist_truth_and_holding_strategy(self):
        payload = {
            "basic": {
                "short_name": "旧名称",
                "exchange": "SH",
                "list_date": "2021-01-01",
            },
            "market": {
                "price": 28.0,
                "change_pct": 1.0,
                "open": 27.5,
                "high": 28.5,
                "low": 27.0,
                "turnover_ratio": 3.0,
                "total_shares": 1000,
                "float_shares": 800,
            },
            "capital": {"today": {"main_net_inflow": 1.0}},
            "finance": {"latest": {"basic_eps": 2.0, "net_asset_ps": 10.0}},
            "valuation": {"pe_ttm": 14.0, "pb": 2.8},
            "technical": {"ma20": 29.0},
            "news": {},
            "holder": {},
            "holding": None,
            "industry": "旧行业",
            "concepts": [],
            "trade_date": "2026-08-11",
            "requested_trade_date": "2026-08-26",
            "quote_trade_date": "2026-08-11",
            "flow_trade_date": "2026-08-11",
            "quote_source": "snapshot",
            "detail_source": "snapshot_light",
            "hot_rank": {},
        }
        watch_analysis = {
            "operation_advice": "控仓",
            "trend": "偏弱",
            "funds": "流出",
        }
        holding_strategy = {
            "stock_code": "601606",
            "action": "立即卖出",
            "exit_intent": "SELL",
            "reason": "latest price 30.6300 invalidated MA20 trend stop 32.4402",
            "shares": 1200,
            "sellable_shares": 1200,
            "execution_authority": "ADVISORY_ONLY",
        }
        context = {
            "row": {
                "stock_code": "601606",
                "display_name": "长城军工",
                "industry_name": "国防军工",
                "cur_price": 30.63,
                "change_pct": -2.89,
                "quote_prev_close": 31.54,
                "quote_volume": 298048,
                "quote_amount": 909134200,
                "quote_trade_date": "2026-08-26",
                "quote_snapshot_at": "2026-08-26 15:00:04",
                "quote_source": "current_close_table",
                "quote_status": "closed",
                "shares": 1200,
                "cost_price": 31.467,
                "position_date": "2026-08-24",
                "main_net_inflow": -50415295,
                "max_net_inflow": -100,
                "lg_net_inflow": -200,
                "mid_net_inflow": 300,
                "sm_net_inflow": 400,
                "flow_trade_date": "2026-08-26",
                "flow_latest_time": "2026-08-26 14:51:00",
                "flow_source": "qmt",
                "flow_status": "closed",
                "watch_analysis": watch_analysis,
            },
            "snapshot_stale": False,
            "holding_strategy": holding_strategy,
            "holding_strategy_context": {
                "status": "ok",
                "execution_authority": "ADVISORY_ONLY",
            },
        }
        analysis_snapshot = {"analysis_date": "2026-08-06"}

        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._cache_set"), \
             patch("server.api.routers.hot_data._portfolio_market_mode", return_value="post_close"), \
             patch("server.api.routers.hot_data._stock_detail_portfolio_context", return_value=context), \
             patch("server.api.routers.hot_data._load_stock_detail_payload", return_value=payload), \
             patch("server.api.routers.hot_data._load_latest_analysis_snapshot", return_value=analysis_snapshot), \
             patch("server.api.routers.hot_data._load_latest_recommendation_snapshot", return_value={}), \
             patch("server.api.routers.hot_data._generate_ai_analysis", return_value={"analysis_date": "2026-08-06"}) as analysis_mock:
            out = hot_data.stock_detail("601606")

        self.assertTrue(out["watchlist_member"])
        self.assertEqual(out["short_name"], "长城军工")
        self.assertEqual(out["market"]["price"], 30.63)
        self.assertEqual(out["market"]["change_pct"], -2.89)
        self.assertEqual(out["market"]["pre_close"], 31.54)
        self.assertIsNone(out["market"]["open"])
        self.assertEqual(out["market"]["market_cap"], 30630.0)
        self.assertEqual(out["valuation"]["pe_ttm"], 15.31)
        self.assertEqual(out["valuation"]["pb"], 3.06)
        self.assertEqual(out["capital"]["today"]["main_net_inflow"], -50415295)
        self.assertEqual(out["quote_trade_date"], "2026-08-26")
        self.assertEqual(out["flow_trade_date"], "2026-08-26")
        self.assertEqual(out["quote_source"], "current_close_table")
        self.assertEqual(out["holding"]["shares"], 1200)
        self.assertEqual(out["watch_analysis"], watch_analysis)
        self.assertEqual(out["holding_strategy"]["action"], "立即卖出")
        self.assertFalse(out["quote_is_stale"])
        self.assertFalse(out["flow_is_stale"])
        self.assertTrue(out["analysis_is_stale"])
        self.assertTrue(out["technical_is_stale"])
        self.assertIn("portfolio_snapshot_overlay", out["detail_source"])
        self.assertEqual(analysis_mock.call_args.args[2]["price"], 30.63)
        self.assertEqual(analysis_mock.call_args.args[9]["shares"], 1200)

    def test_stock_detail_portfolio_context_matches_live_row_and_strategy(self):
        snapshot = {
            "snapshot_stale": False,
            "data": [
                {"stock_code": "000001", "shares": 0},
                {
                    "stock_code": "601606",
                    "shares": 1200,
                    "quote_status": "closed",
                },
            ],
        }
        strategy_payload = {
            "status": "ok",
            "trade_date": "2026-08-26",
            "execution_authority": "ADVISORY_ONLY",
            "data": [
                {"stock_code": "601606", "action": "立即卖出"},
                {"stock_code": "002165", "action": "等待数据"},
            ],
        }

        with patch(
            "server.api.routers.hot_data._get_portfolio_snapshot",
            return_value=snapshot,
        ) as snapshot_mock, patch(
            "server.api.routers.hot_data._cache_get",
            return_value=None,
        ), patch(
            "server.api.routers.hot_data._cache_set",
        ) as cache_set_mock, patch(
            "server.api.routers.hot_data.portfolio_holding_strategy",
            return_value=strategy_payload,
        ) as strategy_mock:
            out = hot_data._stock_detail_portfolio_context("601606")

        snapshot_mock.assert_called_once_with(live_mode=True)
        strategy_mock.assert_called_once_with("")
        cache_set_mock.assert_called_once_with(
            "stock_detail_holding_strategy_current",
            strategy_payload,
        )
        self.assertEqual(out["row"]["shares"], 1200)
        self.assertEqual(out["holding_strategy"]["action"], "立即卖出")
        self.assertEqual(
            out["holding_strategy_context"]["execution_authority"],
            "ADVISORY_ONLY",
        )

    def test_stock_detail_portfolio_context_failure_is_explicit_and_local(self):
        with patch(
            "server.api.routers.hot_data._get_portfolio_snapshot",
            side_effect=RuntimeError("temporary snapshot failure"),
        ):
            context = hot_data._stock_detail_portfolio_context("601606")

        out = hot_data._apply_stock_detail_portfolio_context(
            {
                "market": {"price": 30.0},
                "holding_strategy": {"action": "旧动作"},
                "watch_analysis": {"operation_advice": "旧建议"},
            },
            context,
        )

        self.assertEqual(
            context,
            {
                "unavailable": True,
                "reason_code": "PORTFOLIO_CONTEXT_UNAVAILABLE",
            },
        )
        self.assertIsNone(out["watchlist_member"])
        self.assertIsNone(out["holding"])
        self.assertIsNone(out["holding_strategy"])
        self.assertEqual(out["watch_analysis"], {})
        self.assertTrue(out["quote_is_stale"])
        self.assertEqual(
            out["holding_strategy_context"]["reason_code"],
            "PORTFOLIO_CONTEXT_UNAVAILABLE",
        )

    def test_stock_detail_portfolio_context_reuses_shared_strategy_cache(self):
        cached_strategy = {
            "status": "ok",
            "execution_authority": "ADVISORY_ONLY",
            "data": [
                {"stock_code": "601606", "action": "立即卖出"},
            ],
        }
        with patch(
            "server.api.routers.hot_data._get_portfolio_snapshot",
            return_value={
                "data": [
                    {
                        "stock_code": "601606",
                        "shares": 1200,
                        "quote_status": "closed",
                    },
                ],
            },
        ), patch(
            "server.api.routers.hot_data._cache_get",
            return_value=cached_strategy,
        ) as cache_get_mock, patch(
            "server.api.routers.hot_data.portfolio_holding_strategy",
        ) as strategy_mock:
            context = hot_data._stock_detail_portfolio_context("601606")

        cache_get_mock.assert_called_once()
        strategy_mock.assert_not_called()
        self.assertEqual(context["holding_strategy"]["action"], "立即卖出")

    def test_stock_detail_portfolio_context_blocks_strategy_for_stale_quote(self):
        with patch(
            "server.api.routers.hot_data._get_portfolio_snapshot",
            return_value={
                "snapshot_stale": True,
                "data": [
                    {
                        "stock_code": "601606",
                        "shares": 1200,
                        "cur_price": 30.63,
                        "quote_status": "stale",
                    },
                ],
            },
        ), patch(
            "server.api.routers.hot_data._cache_get",
        ) as cache_get_mock, patch(
            "server.api.routers.hot_data.portfolio_holding_strategy",
        ) as strategy_mock:
            context = hot_data._stock_detail_portfolio_context("601606")

        cache_get_mock.assert_not_called()
        strategy_mock.assert_not_called()
        self.assertIsNone(context["holding_strategy"])
        self.assertEqual(
            context["holding_strategy_context"]["reason_code"],
            "PORTFOLIO_QUOTE_UNVERIFIED",
        )

    def test_stock_detail_portfolio_context_blocks_previous_close_strategy(self):
        with patch(
            "server.api.routers.hot_data._get_portfolio_snapshot",
            return_value={
                "snapshot_stale": False,
                "data": [
                    {
                        "stock_code": "601606",
                        "shares": 1200,
                        "cur_price": 30.63,
                        "quote_status": "previous_close",
                    },
                ],
            },
        ), patch(
            "server.api.routers.hot_data._cache_get",
        ) as cache_get_mock, patch(
            "server.api.routers.hot_data.portfolio_holding_strategy",
        ) as strategy_mock:
            context = hot_data._stock_detail_portfolio_context("601606")

        cache_get_mock.assert_not_called()
        strategy_mock.assert_not_called()
        self.assertIsNone(context["holding_strategy"])
        self.assertEqual(
            context["holding_strategy_context"]["reason_code"],
            "PORTFOLIO_QUOTE_UNVERIFIED",
        )

    def test_stock_detail_overlay_derives_previous_close_from_current_quote(self):
        out = hot_data._apply_stock_detail_portfolio_context(
            {
                "market": {"price": 9.0, "pre_close": 8.8},
                "date": "2026-08-25",
                "quote_trade_date": "2026-08-25",
            },
            {
                "row": {
                    "stock_code": "000001",
                    "shares": 0,
                    "cur_price": 10.2,
                    "price_change": 0.2,
                    "change_pct": 2.0,
                    "quote_trade_date": "2026-08-26",
                    "quote_status": "closed",
                },
            },
        )

        self.assertEqual(out["market"]["pre_close"], 10.0)
        self.assertEqual(out["date"], "2026-08-26")

        reapplied = hot_data._apply_stock_detail_portfolio_context(
            out,
            {
                "row": {
                    "stock_code": "000001",
                    "shares": 0,
                    "cur_price": 10.2,
                    "price_change": 0.2,
                    "change_pct": 2.0,
                    "quote_trade_date": "2026-08-26",
                    "quote_status": "closed",
                },
            },
        )
        self.assertEqual(
            reapplied["detail_source"].count("portfolio_snapshot_overlay+"),
            1,
        )

    def test_stock_detail_overlay_does_not_replace_newer_same_day_quote_or_industry(self):
        out = hot_data._apply_stock_detail_portfolio_context(
            {
                "market": {"price": 12.0},
                "industry": "权威行业",
                "quote_trade_date": "2026-08-26",
                "quote_snapshot_at": "2026-08-26 15:00:05",
            },
            {
                "row": {
                    "stock_code": "000001",
                    "shares": 0,
                    "cur_price": 11.0,
                    "industry_name": "旧快照行业",
                    "quote_trade_date": "2026-08-26",
                    "quote_snapshot_at": "2026-08-26 14:59:59",
                    "quote_status": "closed",
                },
                "snapshot_stale": False,
            },
        )

        self.assertEqual(out["market"]["price"], 12.0)
        self.assertEqual(out["industry"], "权威行业")

    def test_stock_detail_overlay_does_not_invent_previous_close_without_evidence(self):
        out = hot_data._apply_stock_detail_portfolio_context(
            {
                "market": {"price": 9.0, "pre_close": 8.8},
                "quote_trade_date": "2026-08-25",
            },
            {
                "row": {
                    "stock_code": "000001",
                    "shares": 0,
                    "cur_price": "10.2",
                    "price_change": "invalid",
                    "quote_prev_close": None,
                    "change_pct": None,
                    "quote_trade_date": "2026-08-26",
                    "quote_status": "closed",
                },
                "snapshot_stale": False,
            },
        )

        self.assertEqual(out["market"]["price"], 10.2)
        self.assertIsNone(out["market"]["pre_close"])

    def test_stock_detail_portfolio_context_rejects_snapshot_error(self):
        with patch(
            "server.api.routers.hot_data._get_portfolio_snapshot",
            return_value={
                "data": [],
                "error": "database unavailable",
                "retryable": True,
            },
        ), patch(
            "server.api.routers.hot_data.portfolio_holding_strategy",
        ) as strategy_mock:
            context = hot_data._stock_detail_portfolio_context("601606")

        self.assertEqual(
            context,
            {
                "unavailable": True,
                "reason_code": "PORTFOLIO_SNAPSHOT_UNAVAILABLE",
            },
        )
        strategy_mock.assert_not_called()

    def test_stock_detail_watchlist_nonholding_gets_watch_advice_without_fake_position(self):
        payload = {
            "basic": {"short_name": "测试股"},
            "market": {"price": 10.0},
            "trade_date": "2026-08-11",
            "requested_trade_date": "2026-08-26",
            "quote_trade_date": "2026-08-11",
            "flow_trade_date": "2026-08-11",
        }
        context = {
            "row": {
                "stock_code": "000001",
                "shares": 0,
                "cur_price": 11.0,
                "quote_trade_date": "2026-08-26",
                "quote_status": "closed",
                "watch_analysis": {"operation_advice": "关注"},
            },
            "holding_strategy": None,
        }

        out = hot_data._apply_stock_detail_portfolio_context(payload, context)

        self.assertTrue(out["watchlist_member"])
        self.assertIsNone(out["holding"])
        self.assertIsNone(out["holding_strategy"])
        self.assertEqual(out["watch_analysis"]["operation_advice"], "关注")
        self.assertEqual(out["market"]["price"], 11.0)

    def test_portfolio_analyze_exposes_ai_metadata(self):
        payload = {
            "basic": {"short_name": "Ping An Bank"},
            "market": {"price": 12.34, "change_pct": 1.2},
            "capital": {},
            "finance": {},
            "valuation": {},
            "technical": {"support": 12.0, "resistance": 12.8},
            "industry": "Bank",
            "concepts": ["Finance"],
            "holding": None,
            "trade_date": "2026-06-13",
            "requested_trade_date": "2026-06-21",
            "quote_trade_date": "2026-06-13",
            "flow_trade_date": "2026-06-12",
            "quote_source": "snapshot",
            "detail_source": "snapshot_light",
            "hot_rank": {"fused_rank": 5},
        }
        snapshot = {"analysis_date": "2026-06-10", "recommend_status": "ALLOW"}
        ai_result = {
            "conclusion": "analysis text",
            "source": "deepseek",
            "analysis_date": "2026-06-10",
            "scores": {"short_term_score": 74.0},
            "score": 70.0,
            "action": "关注",
            "action_reason": "trend acceptable",
            "recommend_status": "ALLOW",
            "event_risk_level": "LOW",
        }

        with patch("server.api.routers.hot_data._portfolio_market_mode", return_value="post_close"), \
             patch("server.api.routers.hot_data._load_stock_detail_payload", return_value=payload) as payload_mock, \
             patch("server.api.routers.hot_data._load_latest_analysis_snapshot", return_value=snapshot), \
             patch("server.api.routers.hot_data._load_latest_recommendation_snapshot", return_value={"final_trade_score": 77.0, "ai_score": 70.0}), \
             patch("server.api.routers.hot_data._generate_ai_analysis", return_value=ai_result) as analysis_mock:
            out = hot_data.portfolio_analyze("1")

        payload_mock.assert_called_once_with("000001", mode="post_close", light=True)
        self.assertTrue(analysis_mock.call_args.kwargs["prefer_snapshot"])
        self.assertEqual(out["stock_code"], "000001")
        self.assertEqual(out["analysis"], "analysis text")
        self.assertEqual(out["ai_source"], "deepseek")
        self.assertEqual(out["ai_analysis_date"], "2026-06-10")
        self.assertEqual(out["ai_score"], 70.0)
        self.assertEqual(out["ai_action"], "关注")
        self.assertEqual(out["ai_action_reason"], "trend acceptable")
        self.assertEqual(out["ai_recommend_status"], "ALLOW")
        self.assertEqual(out["ai_event_risk_level"], "LOW")
        self.assertEqual(out["recommendation_snapshot"]["final_trade_score"], 77.0)
        self.assertEqual(out["requested_trade_date"], "2026-06-21")
        self.assertEqual(out["quote_trade_date"], "2026-06-13")
        self.assertEqual(out["flow_trade_date"], "2026-06-12")
        self.assertEqual(out["quote_source"], "snapshot")
        self.assertEqual(out["detail_source"], "snapshot_light")
        self.assertTrue(out["quote_is_stale"])
        self.assertTrue(out["flow_is_stale"])
        self.assertTrue(out["analysis_is_stale"])

    def test_mainforce_analysis_returns_cached_payload(self):
        cached = {"stock_code": "000001", "score": 77.0}

        with patch("server.api.routers.hot_data._cache_get", return_value=cached) as cache_get_mock, \
             patch("server.api.routers.hot_data._compute_mainforce_behavior_fast") as compute_mock:
            out = hot_data.mainforce_analysis("000001", "2026-06-13")

        cache_get_mock.assert_called_once_with("mainforce_analysis_000001_2026-06-13", ttl_seconds=300)
        compute_mock.assert_not_called()
        self.assertEqual(out, cached)

    def test_mainforce_analysis_caches_computed_payload(self):
        result = {"stock_code": "000001", "score": 61.5}

        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._compute_mainforce_behavior_fast", return_value=result) as compute_mock, \
             patch("server.api.routers.hot_data._cache_set") as cache_set_mock:
            out = hot_data.mainforce_analysis("000001", "2026-06-13")

        compute_mock.assert_called_once_with("000001", "2026-06-13")
        cache_set_mock.assert_called_once_with("mainforce_analysis_000001_2026-06-13", result)
        self.assertEqual(out, result)

    def test_mainforce_analysis_uses_latest_market_analysis_date_by_default(self):
        result = {"stock_code": "000001", "score": 61.5}

        with patch("server.api.routers.hot_data._latest_market_analysis_date", return_value="2026-05-30") as latest_mock, \
             patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._compute_mainforce_behavior_fast", return_value=result) as compute_mock, \
             patch("server.api.routers.hot_data._cache_set"):
            out = hot_data.mainforce_analysis("000001")

        latest_mock.assert_called_once_with()
        compute_mock.assert_called_once_with("000001", "2026-05-30")
        self.assertEqual(out, result)

    def test_recommended_stocks_returns_cached_latest_payload(self):
        cached = {"date": "2026-06-13", "data": [{"stock_code": "000001"}], "total": 1}

        with patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=False), \
             patch("server.api.routers.hot_data._cache_get", return_value=cached) as cache_get_mock, \
             patch("server.api.routers.hot_data._recommended_stocks_v2") as query_mock:
            out = hot_data.recommended_stocks("", "main_wave", "WATCH")

        cache_get_mock.assert_called_once_with(
            "recommended_stocks_latest_main_wave_WATCH",
            ttl_seconds=300,
        )
        query_mock.assert_not_called()
        self.assertEqual(out, cached)

    def test_recommended_stocks_caches_query_result(self):
        result = {"date": "2026-06-13", "data": [{"stock_code": "000001"}], "total": 1}

        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._recommended_stocks_v2", return_value=result) as query_mock, \
             patch("server.api.routers.hot_data._cache_set") as cache_set_mock:
            out = hot_data.recommended_stocks("", "", "")

        query_mock.assert_called_once_with("", "", "")
        cache_set_mock.assert_called_once_with("recommended_stocks_latest_all_all", result)
        self.assertEqual(out, result)

    def test_recommended_stocks_explicit_date_bypasses_process_local_cache(self):
        stale = {"date": "2026-06-13", "data": [], "total": 0}
        current = {
            "date": "2026-06-13",
            "data": [{"stock_code": "000001"}],
            "total": 1,
        }

        with patch(
            "server.api.routers.hot_data._cache_get", return_value=stale
        ) as cache_get, patch(
            "server.api.routers.hot_data._cache_set"
        ) as cache_set, patch(
            "server.api.routers.hot_data._recommended_stocks_v2",
            return_value=current,
        ) as query_mock:
            out = hot_data.recommended_stocks("2026-06-13", "", "")

        cache_get.assert_not_called()
        cache_set.assert_not_called()
        query_mock.assert_called_once_with("2026-06-13", "", "")
        self.assertEqual(out, current)

    def test_recommended_stocks_exact_identity_bypasses_stale_cache(self):
        run_uid = "a" * 32
        build_sha = "b" * 40
        pool_sha256 = "c" * 64
        result = {
            "date": "2026-06-13",
            "data": [],
            "total": 0,
            "identity_verified": True,
            "data_status": "READY",
            "run_uid": run_uid,
            "build_sha": build_sha,
            "canonical_pool_sha256": pool_sha256,
        }

        with patch("server.api.routers.hot_data._cache_get") as cache_get, \
             patch("server.api.routers.hot_data._cache_set") as cache_set, \
             patch(
                 "server.api.routers.hot_data._recommended_stocks_v2",
                 return_value=result,
             ) as query_mock:
            out = hot_data.recommended_stocks(
                trade_date="2026-06-13",
                strategy="",
                signal_status="",
                expected_run_uid=run_uid,
                expected_build_sha=build_sha,
                expected_pool_sha256=pool_sha256,
            )

        cache_get.assert_not_called()
        cache_set.assert_not_called()
        query_mock.assert_called_once_with(
            "2026-06-13",
            "",
            "",
            expected_run_uid=run_uid,
            expected_build_sha=build_sha,
            expected_pool_sha256=pool_sha256,
        )
        self.assertEqual(out, result)

    def test_recommended_stocks_exact_empty_pool_is_run_build_hash_bound(self):
        run_uid = "a" * 32
        build_sha = "b" * 40
        pool_sha256 = "c" * 64
        history = {
            "run_uid": run_uid,
            "trade_date": "2026-07-08",
            "status": "done",
            "total": 5205,
            "passed": 0,
            "executable_count": 0,
            "canonical_pool_sha256": pool_sha256,
            "build_sha": build_sha,
            "published_at": "2026-07-08 22:20:00",
        }
        manifest = {
            "analysis_count": 5205,
            "recommendation_count": 0,
            "executable_count": 0,
            "canonical_pool_sha256": pool_sha256,
            "publisher_run_uids": [],
            "publication_statuses": [],
            "live_gate_alignment": True,
        }
        columns = {
            "stock_code",
            "pick_date",
            "ai_score",
            "publisher_run_uid",
            "publication_status",
        }
        publication_connection = MagicMock()
        publication_engine = MagicMock()
        publication_engine.connect.return_value.__enter__.return_value = (
            publication_connection
        )
        bound_connections = []
        sql_results = [[], [], [history]]

        def snapshot_sql(*_args, **_kwargs):
            bound_connections.append(current_bound_sql_connection())
            return sql_results.pop(0)

        with patch(
            "server.api.routers.hot_data._table_columns",
            return_value=columns,
        ), patch(
            "server.api.routers.hot_data._read_sql",
            side_effect=snapshot_sql,
        ) as read_sql_mock, patch(
            "server.api.routers.hot_data._recommended_data_freshness",
            return_value={"status": "ready"},
        ), patch(
            "server.api.routers.hot_data._recommendation_theme_coverage",
            return_value={},
        ), patch(
            "server.api.routers.hot_data._read_recommended_pool_manifest",
            return_value=manifest,
        ) as manifest_mock, patch(
            "server.api.routers.hot_data.get_engine",
            return_value=publication_engine,
        ):
            out = hot_data._recommended_stocks_v2(
                "2026-07-08",
                expected_run_uid=run_uid,
                expected_build_sha=build_sha,
                expected_pool_sha256=pool_sha256,
            )

        query_sql, query_params = read_sql_mock.call_args_list[0].args
        self.assertIn("r.publication_status = 'ACTIVE'", query_sql)
        self.assertIn("r.publisher_run_uid = :expected_run_uid", query_sql)
        self.assertEqual(query_params["expected_run_uid"], run_uid)
        self.assertEqual(out["total"], 0)
        self.assertTrue(out["identity_verified"])
        self.assertEqual(out["data_status"], "READY")
        self.assertEqual(out["publication_status"], "VERIFIED_EMPTY")
        self.assertEqual(out["run_uid"], run_uid)
        self.assertEqual(out["build_sha"], build_sha)
        self.assertEqual(out["canonical_pool_sha256"], pool_sha256)
        self.assertTrue(bound_connections)
        self.assertTrue(all(
            connection is publication_connection
            for connection in bound_connections
        ))
        manifest_mock.assert_called_once_with(
            "2026-07-08",
            connection=publication_connection,
        )

    def test_recommended_pool_contract_rejects_another_active_run(self):
        with patch(
            "server.api.routers.hot_data._read_sql",
            return_value=[{
                "publisher_run_uid": "d" * 32,
                "publication_status": "ACTIVE",
                "active_count": 1,
            }],
        ):
            with self.assertRaisesRegex(RuntimeError, "publisher run identity differs"):
                hot_data._recommended_pool_publication_contract(
                    columns={"publisher_run_uid", "publication_status"},
                    trade_date="2026-07-08",
                    rows=[],
                    expected_run_uid="a" * 32,
                    expected_build_sha="b" * 40,
                    expected_pool_sha256="c" * 64,
                )

    def test_recommended_pool_contract_rejects_equal_count_content_tamper(self):
        run_uid = "a" * 32
        build_sha = "b" * 40
        pool_sha256 = "c" * 64
        active_group = {
            "publisher_run_uid": run_uid,
            "publication_status": "ACTIVE",
            "active_count": 1,
        }
        history = {
            "run_uid": run_uid,
            "trade_date": "2026-07-08",
            "status": "done",
            "total": 5205,
            "passed": 1,
            "executable_count": 0,
            "canonical_pool_sha256": pool_sha256,
            "build_sha": build_sha,
            "published_at": "2026-07-08 22:20:00",
        }
        tampered_manifest = {
            "analysis_count": 5205,
            "recommendation_count": 1,
            "executable_count": 0,
            "canonical_pool_sha256": "d" * 64,
            "publisher_run_uids": [run_uid],
            "publication_statuses": ["ACTIVE"],
            "live_gate_alignment": True,
        }
        rows = [{
            "stock_code": "000002",
            "publisher_run_uid": run_uid,
            "publication_status": "ACTIVE",
        }]

        with patch(
            "server.api.routers.hot_data._read_sql",
            side_effect=[[active_group], [history]],
        ), patch(
            "server.api.routers.hot_data._read_recommended_pool_manifest",
            return_value=tampered_manifest,
        ):
            with self.assertRaisesRegex(
                RuntimeError, "immutable publication differs"
            ):
                hot_data._recommended_pool_publication_contract(
                    columns={"publisher_run_uid", "publication_status"},
                    trade_date="2026-07-08",
                    rows=rows,
                    expected_run_uid=run_uid,
                    expected_build_sha=build_sha,
                    expected_pool_sha256=pool_sha256,
                    connection=MagicMock(),
                )

    def test_recommended_stocks_exact_identity_failure_is_data_blocked(self):
        with patch(
            "server.api.routers.hot_data._recommended_stocks_v2",
            side_effect=RuntimeError("ticket pool immutable publication differs"),
        ):
            out = hot_data.recommended_stocks(
                trade_date="2026-07-08",
                strategy="",
                signal_status="",
                expected_run_uid="a" * 32,
                expected_build_sha="b" * 40,
                expected_pool_sha256="c" * 64,
            )

        self.assertEqual(out["data_status"], "DATA_BLOCKED")
        self.assertEqual(
            out["reason_code"],
            "TICKET_POOL_PUBLICATION_IDENTITY_MISMATCH",
        )
        self.assertFalse(out["identity_verified"])
        self.assertEqual(out["data"], [])

    def test_recommended_stocks_explicit_date_does_not_fallback_to_previous_pick_date(self):
        publication_engine = MagicMock()
        publication_connection = MagicMock()
        publication_engine.connect.return_value.__enter__.return_value = publication_connection
        with patch("server.api.routers.hot_data._table_columns", return_value={"stock_code", "pick_date", "ai_score"}), \
             patch("server.api.routers.hot_data.get_engine", return_value=publication_engine), \
             patch("server.api.routers.hot_data._read_sql", return_value=[]) as read_sql_mock, \
             patch("server.api.routers.hot_data._latest_date_not_after", return_value="2026-07-06") as latest_not_after_mock, \
             patch("server.api.routers.hot_data._latest_date", return_value="2026-07-06") as latest_date_mock, \
             patch("server.api.routers.hot_data._recommended_data_freshness", return_value={"status": "missing", "is_missing_date": True}):
            out = hot_data._recommended_stocks_v2("2026-07-08", "", "")

        self.assertEqual(out["date"], "2026-07-08")
        self.assertEqual(out["total"], 0)
        latest_not_after_mock.assert_not_called()
        latest_date_mock.assert_not_called()
        sql, params = read_sql_mock.call_args.args
        self.assertIn("r.pick_date = :d", sql)
        self.assertIn("'BLOCK' AS `recommend_status`", sql)
        self.assertIn("'DATA_BLOCKED' AS `chase_risk_status`", sql)
        self.assertIn("0 AS `ordinary_buy_eligible`", sql)
        self.assertEqual(params["d"], "2026-07-08")

    def test_recommended_data_freshness_flags_fallback_and_stale_sources(self):
        latest = {
            ("sm_stock_kline", "trade_date"): "2026-06-27",
            ("sm_stock_snapshot", "trade_date"): "2026-06-28",
            ("sm_stock_capital_flow_daily", "trade_date"): "2026-06-26",
            ("st_hot_rank_fused", "snapshot_date"): "2026-06-28",
        }
        with patch("server.api.routers.hot_data._latest_date", side_effect=lambda table, col="trade_date": latest[(table, col)]), \
             patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=True):
            out = hot_data._recommended_data_freshness(
                requested_date="2026-06-28",
                result_date="2026-06-28",
                live_quote_count=3,
                total=10,
            )

        self.assertEqual(out["status"], "stale")
        self.assertIn("K线", out["stale_sources"])
        self.assertIn("资金流", out["stale_sources"])
        self.assertEqual(out["quote_mode"], "live")
        self.assertTrue(out["is_intraday"])

        with patch("server.api.routers.hot_data._latest_date", return_value="2026-06-26"), \
             patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=False):
            fallback = hot_data._recommended_data_freshness(
                requested_date="2026-06-28",
                result_date="2026-06-26",
                total=2,
            )

        self.assertEqual(fallback["status"], "fallback")
        self.assertTrue(fallback["is_fallback_date"])

    def test_recommended_data_freshness_marks_missing_explicit_date(self):
        with patch("server.api.routers.hot_data._latest_date", return_value="2026-07-08"), \
             patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=True):
            out = hot_data._recommended_data_freshness(
                requested_date="2026-07-08",
                result_date="2026-07-08",
                total=0,
            )

        self.assertEqual(out["status"], "missing")
        self.assertEqual(out["status_label"], "目标日推荐未生成")
        self.assertTrue(out["is_missing_date"])
        self.assertFalse(out["is_fallback_date"])

    def test_latest_date_not_after_uses_nearest_available_row(self):
        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._read_sql", return_value=[{"d": "2026-06-17"}]) as read_sql_mock, \
             patch("server.api.routers.hot_data._cache_set") as cache_set_mock:
            out = hot_data._latest_date_not_after("st_recommended_stocks", "2026-06-21", "pick_date")

        sql, params = read_sql_mock.call_args.args
        self.assertIn("`pick_date` <= :d", sql)
        self.assertEqual(params["d"], "2026-06-21")
        cache_set_mock.assert_called_once_with(
            "latest_date_not_after_st_recommended_stocks_pick_date_2026-06-21",
            "2026-06-17",
        )
        self.assertEqual(out, "2026-06-17")

    def test_recommendation_gate_summary_uses_target_date_without_strict_check(self):
        read_rows = [
            [{"rec_count": 80, "latest_created_at": "2026-06-30 23:48:27", "actionable_count": 80}],
            [{"analysis_count": 5191, "latest_updated_at": "2026-06-30 23:48:20"}],
            [{"news_count": 0, "latest_news_time": None}],
        ]

        with patch("server.api.routers.hot_data.get_engine", return_value=object()), \
             patch("biz.analysis.sync_analysis_fast.previous_trade_date") as previous_mock, \
             patch("biz.analysis.sync_analysis_fast.assert_trade_date_ready") as ready_mock, \
             patch("server.api.routers.hot_data._read_sql", side_effect=read_rows) as read_mock:
            out = hot_data._recommendation_gate_status(
                execution_time="2026-07-01 08:30:00",
                target_trade_date="2026-06-30",
                check_readiness=False,
            )

        previous_mock.assert_not_called()
        ready_mock.assert_not_called()
        self.assertEqual(out["expected_trade_date"], "2026-06-30")
        self.assertEqual(out["target_source"], "request")
        self.assertTrue(out["ready"])
        self.assertEqual(out["ready_source"], "existing_recommendation")
        self.assertFalse(out["strict_ok"])
        self.assertTrue(out["readiness"]["skipped"])
        self.assertEqual(out["recommendation"]["count"], 80)
        actionable_sql = read_mock.call_args_list[0].args[0]
        self.assertIn("recommend_status = 'ALLOW'", actionable_sql)
        self.assertIn("chase_risk_status = 'ALLOW'", actionable_sql)
        self.assertIn("ordinary_buy_eligible = 1", actionable_sql)
        self.assertIn(
            "signal_status, '') IN ('BUY_READY', 'CONFIRM')",
            actionable_sql,
        )
        self.assertNotIn(
            "('BUY_READY', 'CONFIRM', 'ALLOW')",
            actionable_sql,
        )

    def test_recommended_stocks_gate_passes_target_and_readiness_mode(self):
        with patch("server.api.routers.hot_data._recommendation_gate_status", return_value={"status": "ok"}) as gate_mock:
            out = hot_data.recommended_stocks_gate(
                execution_time="2026-07-01 08:30:00",
                min_kline_coverage=0.8,
                target_trade_date="2026-06-30",
                check_readiness=False,
            )

        self.assertEqual(out["status"], "ok")
        gate_mock.assert_called_once_with(
            execution_time="2026-07-01 08:30:00",
            min_kline_coverage=0.8,
            target_trade_date="2026-06-30",
            check_readiness=False,
        )

    def test_generate_ai_analysis_falls_back_to_snapshot_without_api_key(self):
        snapshot = {
            "analysis_date": "2026-06-13",
            "recommend_status": "ALLOW",
            "recommend_reason": "评分达标，风险可控",
            "recommendation": "可进入候选池，等待盘中确认。",
            "summary": "短线和长线评分都处于中上水平。",
            "short_term_score": 72.0,
            "long_term_score": 68.0,
            "event_risk_score": 88.0,
            "event_risk_level": "LOW",
            "data_quality_score": 90.0,
            "data_quality_flags": ["stale_flow"],
            "strengths": ["技术趋势较强"],
            "risks": ["短期波动偏大"],
        }

        with patch.dict("os.environ", {}, clear=True), \
             patch("server.api.routers.hot_data._read_dotenv_key", return_value=""):
            out = hot_data._generate_ai_analysis(
                "000001",
                "Ping An Bank",
                {"price": 12.34},
                {},
                {},
                {},
                {"support": 12.0, "resistance": 12.8},
                "Bank",
                ["Finance"],
                holding=None,
                trade_date="2026-06-13",
                analysis_snapshot=snapshot,
            )

        self.assertEqual(out["source"], "snapshot_fallback")
        self.assertEqual(out["analysis_date"], "2026-06-13")
        self.assertEqual(out["action"], "关注")
        self.assertEqual(out["action_reason"], "评分达标，风险可控")
        self.assertEqual(out["score"], 70.0)
        self.assertIn("统一分析快照回退", out["conclusion"])
        self.assertIn("技术趋势较强", out["conclusion"])
        self.assertIn("数据标记 stale_flow", out["conclusion"])

    def test_generate_ai_analysis_merges_snapshot_fields_when_ai_succeeds(self):
        snapshot = {
            "analysis_date": "2026-06-13",
            "recommend_status": "ALLOW",
            "recommend_reason": "trend and risk acceptable",
            "recommendation": "watch for confirmation",
            "summary": "snapshot summary",
            "short_term_score": 74.0,
            "long_term_score": 66.0,
            "event_risk_score": 82.0,
            "event_risk_level": "LOW",
            "data_quality_score": 88.0,
            "strengths": ["trend"],
            "risks": ["volatility"],
        }

        class _Resp:
            def json(self):
                return {"choices": [{"message": {"content": "AI generated conclusion"}}]}

        with patch.dict("os.environ", {"DEEPSEEK_API_KEY": "test-key"}, clear=True), \
             patch("httpx.post", return_value=_Resp()) as post_mock:
            out = hot_data._generate_ai_analysis(
                "000001",
                "Ping An Bank",
                {"price": 12.34},
                {},
                {},
                {},
                {"support": 12.0, "resistance": 12.8},
                "Bank",
                ["Finance"],
                holding=None,
                trade_date="2026-06-13",
                analysis_snapshot=snapshot,
            )

        post_mock.assert_called_once()
        self.assertEqual(out["source"], "deepseek")
        self.assertEqual(out["analysis_date"], "2026-06-13")
        self.assertEqual(out["score"], 70.0)
        self.assertEqual(out["action"], "关注")
        self.assertEqual(out["action_reason"], "trend and risk acceptable")
        self.assertEqual(out["recommend_status"], "ALLOW")
        self.assertEqual(out["scores"]["short_term_score"], 74.0)
        self.assertEqual(out["conclusion"], "AI generated conclusion")

    def test_generate_ai_analysis_prefers_snapshot_when_requested(self):
        snapshot = {
            "analysis_date": "2026-06-13",
            "recommend_status": "ALLOW",
            "recommend_reason": "use snapshot first",
            "recommendation": "watch for confirmation",
            "summary": "snapshot summary",
            "short_term_score": 74.0,
            "long_term_score": 66.0,
            "event_risk_score": 82.0,
            "event_risk_level": "LOW",
            "data_quality_score": 88.0,
        }

        with patch.dict("os.environ", {"DEEPSEEK_API_KEY": "test-key"}, clear=True), \
             patch("httpx.post") as post_mock:
            out = hot_data._generate_ai_analysis(
                "000001",
                "Ping An Bank",
                {"price": 12.34},
                {},
                {},
                {},
                {"support": 12.0, "resistance": 12.8},
                "Bank",
                ["Finance"],
                holding=None,
                trade_date="2026-06-13",
                analysis_snapshot=snapshot,
                prefer_snapshot=True,
            )

        post_mock.assert_not_called()
        self.assertEqual(out["source"], "snapshot_fallback")
        self.assertEqual(out["analysis_date"], "2026-06-13")
        self.assertEqual(out["score"], 70.0)
        self.assertEqual(out["action_reason"], "use snapshot first")

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

    def test_portfolio_codes_returns_lightweight_deduped_watchlist(self):
        with patch("server.api.routers.hot_data._read_sql", return_value=[
            {"stock_code": "1"},
            {"stock_code": "000001"},
            {"stock_code": "600522"},
            {"stock_code": "600000' OR 1=1"},
        ]):
            out = hot_data.portfolio_codes()

        self.assertEqual(out, {
            "data": [{"stock_code": "000001"}, {"stock_code": "600522"}],
            "total": 2,
        })

    def test_portfolio_stock_code_query_uses_bound_safe_values(self):
        clean, placeholders, params = hot_data._portfolio_stock_code_query([
            "1",
            "600522",
            "600000' OR 1=1",
        ])

        self.assertEqual(clean, ["000001", "600522"])
        self.assertEqual(placeholders, ":portfolio_code_0, :portfolio_code_1")
        self.assertEqual(params, {
            "portfolio_code_0": "000001",
            "portfolio_code_1": "600522",
        })
        self.assertNotIn("600522", placeholders)

    def test_portfolio_mutations_reject_invalid_code_before_database_work(self):
        with patch("server.api.routers.hot_data._ensure_portfolio_position_columns") as ensure_columns, \
             patch("server.api.routers.hot_data._ensure_portfolio_trans_log_table") as ensure_log, \
             patch("server.api.routers.hot_data._read_sql") as read_sql, \
             patch("server.api.routers.hot_data._exec_sql") as exec_sql:
            out = hot_data.portfolio_add(hot_data.PortfolioAdd(stock_code="1"))

        self.assertEqual(out["status"], "error")
        self.assertIn("exactly 6 digits", out["error"])
        ensure_columns.assert_not_called()
        ensure_log.assert_not_called()
        read_sql.assert_not_called()
        exec_sql.assert_not_called()

        with patch("server.api.routers.hot_data._exec_sql") as exec_sql:
            out = hot_data.portfolio_reorder(hot_data.PortfolioReorder(
                codes=["000001", "600000' OR 1=1"],
            ))

        self.assertEqual(out["status"], "error")
        exec_sql.assert_not_called()

    def test_duplicate_watchlist_add_preserves_recorded_position(self):
        existing = {
            "stock_code": "002409",
            "short_name": "雅克科技",
            "cost_price": 147,
            "shares": 100,
            "position_date": "2026-08-07",
            "sort_order": 7,
        }
        with patch("server.api.routers.hot_data._ensure_portfolio_position_columns"), \
             patch("server.api.routers.hot_data._ensure_portfolio_trans_log_table"), \
             patch("server.api.routers.hot_data._read_sql", side_effect=[[existing], [{"short_name": "雅克科技"}]]), \
             patch("server.api.routers.hot_data._exec_sql") as exec_sql, \
             patch("server.api.routers.hot_data._invalidate_portfolio_snapshot_cache"):
            out = hot_data.portfolio_add(hot_data.PortfolioAdd(
                stock_code="002409",
                watchlist_only=True,
            ))

        self.assertEqual(out["status"], "ok")
        self.assertTrue(out["position_preserved"])
        self.assertEqual(out["cost_price"], 147)
        self.assertEqual(out["shares"], 100)
        self.assertEqual(out["position_date"], "2026-08-07")
        self.assertFalse(any("INSERT INTO st_user_portfolio" in call.args[0] for call in exec_sql.call_args_list))

    def test_active_holding_cannot_be_removed_without_sell_record(self):
        with patch("server.api.routers.hot_data._read_sql", return_value=[{"shares": 100}]), \
             patch("server.api.routers.hot_data._exec_sql") as exec_sql:
            out = hot_data.portfolio_remove("002409")

        self.assertEqual(out["status"], "error")
        self.assertIn("请先记录卖出", out["error"])
        exec_sql.assert_not_called()

    def test_holding_strategy_tracks_watchlist_position_outside_candidate_pool(self):
        snapshot = {"data": [
            {
                "stock_code": "002409",
                "display_name": "雅克科技",
                "cost_price": 147,
                "shares": 100,
                "position_date": "2026-08-07",
            },
            {"stock_code": "000001", "shares": 0},
        ]}
        decision = {
            "stock_code": "002409",
            "trade_date": "2026-08-17",
            "knowledge_cutoff": "2026-08-17T15:10:00+08:00",
            "exit_intent": "SELL",
            "reason": "persisted strategy signal is SELL_ALERT",
            "evidence": {
                "recommendation": {"signal_status": "SELL_ALERT"},
                "price": {"latest_price": 157.99},
                "thresholds": {"trend_stop_price": 151},
            },
        }
        with patch("server.api.routers.hot_data._get_portfolio_snapshot", return_value=snapshot), \
             patch("server.api.routers.hot_data.get_engine", return_value=object()), \
             patch("server.api.routers.hot_data.get_kline_engine", return_value="kline-engine"), \
             patch("server.api.routers.hot_data.get_current_engine", return_value="quote-engine"), \
             patch("server.api.routers.hot_data.evaluate_watchlist_holding_exit_at_cutoff", return_value=decision) as evaluate:
            out = hot_data.portfolio_holding_strategy("2026-08-17")

        self.assertEqual(out["summary"]["holding_count"], 1)
        self.assertEqual(out["summary"]["sell_count"], 1)
        self.assertEqual(out["data"][0]["stock_code"], "002409")
        self.assertEqual(out["data"][0]["action"], "立即卖出")
        self.assertEqual(out["execution_authority"], "ADVISORY_ONLY")
        self.assertEqual(out["market_context"]["market_action"], "WAIT_DATA")
        evaluate.assert_called_once()
        self.assertEqual(
            evaluate.call_args.kwargs["market_context"]["status"],
            "BLOCKED",
        )
        self.assertEqual(evaluate.call_args.kwargs["price_engine"], "kline-engine")
        self.assertEqual(evaluate.call_args.kwargs["quote_engine"], "quote-engine")

    def test_latest_canonical_holding_view_accepts_stored_late_market_evidence(self):
        snapshot = {"data": [{
            "stock_code": "002165",
            "display_name": "红宝丽",
            "cost_price": 7.60,
            "shares": 4800,
            "position_date": "2026-08-20",
        }]}
        governance = {
            "snapshot": {"_bridge_is_latest": True},
            "run": {
                "run_uid": "a" * 32,
                "trade_date": "2026-08-28",
                "requested_as_of": "2026-08-28",
                "decision_at": None,
                "status": "COMPLETED",
                "dominant_regime": "HIGH_RANGE",
                "regime": {
                    "quality_status": "PASS",
                    "risk_asset_cap": 0.5,
                },
                "target_count": 0,
            },
        }
        decision = {
            "stock_code": "002165",
            "trade_date": "2026-08-28",
            "exit_intent": "HOLD",
            "reason": "current canonical daily price is verified",
            "evidence": {
                "price": {
                    "latest_price": 8.37,
                    "price_trade_date": "2026-08-28",
                },
                "thresholds": {},
            },
        }
        with patch(
            "server.trading_v3.repository.TradingV3Repository.latest_run_metadata",
            return_value=None,
        ), patch(
            "server.api.routers.hot_data.canonical_governance_decision",
            return_value=governance,
        ) as canonical_bridge, patch(
            "server.api.routers.hot_data._get_portfolio_snapshot",
            return_value=snapshot,
        ), patch(
            "server.api.routers.hot_data.get_engine", return_value=object()
        ), patch(
            "server.api.routers.hot_data.get_kline_engine",
            return_value="kline-engine",
        ), patch(
            "server.api.routers.hot_data.get_current_engine",
            return_value="quote-engine",
        ), patch(
            "server.api.routers.hot_data.evaluate_watchlist_holding_exit_at_cutoff",
            return_value=decision,
        ) as evaluate:
            out = hot_data.portfolio_holding_strategy("2026-08-28")

        cutoff = str(evaluate.call_args.args[3])
        self.assertNotEqual(cutoff, "2026-08-28T23:59:59+08:00")
        self.assertEqual(out["knowledge_cutoff"], cutoff)
        self.assertEqual(out["data"][0]["latest_price"], 8.37)
        self.assertEqual(out["data"][0]["action"], "继续持有")
        canonical_bridge.assert_called_once_with(
            date(2026, 8, 28), latest_as_of=True
        )

    def test_portfolio_live_force_rebuilds_shared_snapshot(self):
        snapshot = {"data": [{"stock_code": "000001"}], "total": 1, "summary": {"holding_count": 1}}

        with patch("server.api.routers.hot_data._get_portfolio_snapshot", return_value={**snapshot, "live": True}) as snapshot_mock:
            out = hot_data.portfolio_live(force=True)

        snapshot_mock.assert_called_once_with(live_mode=True, force_live=True)
        self.assertTrue(out["live"])
        self.assertEqual(out["total"], 1)

        with patch("server.api.routers.hot_data._get_portfolio_snapshot", return_value={**snapshot, "live": True}) as snapshot_mock:
            hot_data.portfolio_live(force=True, refresh_id="browser-refresh-1")

        snapshot_mock.assert_called_once_with(
            live_mode=True,
            force_live=True,
            force_request_id="browser-refresh-1",
        )

    def test_live_quotes_from_current_table_can_return_stale_rows(self):
        snapshot_at = (_FakeIntradayDatetime.now() - timedelta(seconds=120)).strftime("%Y-%m-%d %H:%M:%S")
        with patch("server.api.routers.hot_data.datetime", _FakeIntradayDatetime), patch("server.api.routers.hot_data._read_sql", return_value=[{
            "stock_code": "000001",
            "short_name": "Ping An",
            "price": 10.5,
            "change": 0.1,
            "change_pct": 0.96,
            "volume": 1000,
            "amount": 2000,
            "snapshot_at": snapshot_at,
        }]):
            out = hot_data._live_quotes_from_current_table(
                ["000001"],
                max_age_seconds=20,
                allow_stale=True,
                max_stale_age_seconds=300,
            )

        self.assertEqual(out["000001"]["quote_status"], "stale")
        self.assertGreaterEqual(out["000001"]["quote_age_seconds"], 100)

    def test_capital_flow_realtime_prefers_qmt_minute_flow(self):
        qmt_rows = [{
            "stock_code": "000001",
            "trade_time": "2026-06-28 10:01:00",
            "main_net_inflow": 1000.0,
            "max_net_inflow": 800.0,
            "lg_net_inflow": 200.0,
            "mid_net_inflow": -100.0,
            "sm_net_inflow": -900.0,
            "data_source": "gj_qmt",
        }]
        with patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=True), \
             patch("server.api.routers.hot_data._portfolio_refresh_qmt_min_flow", return_value={"status": "success", "rows": 1}) as refresh_mock, \
             patch("server.api.routers.hot_data._read_sql", return_value=qmt_rows), \
             patch("server.api.routers.hot_data._portfolio_time_age_seconds", return_value=12):
            out = hot_data.capital_flow_realtime("1")

        refresh_mock.assert_called_once_with(["000001"], force=True)
        self.assertEqual(out["source"], "qmt_min_flow")
        self.assertEqual(out["flow_status"], "fresh")
        self.assertEqual(out["latest"]["main_net_inflow"], 1000.0)

    def test_portfolio_min_flow_summary_diffs_cumulative_rows(self):
        trade_date = datetime.now().strftime("%Y-%m-%d")
        rows = [
            {"stock_code": "000001", "trade_time": f"{trade_date} 10:14:00", "main_net_inflow": 100_000_000.0},
            {"stock_code": "000001", "trade_time": f"{trade_date} 10:15:00", "main_net_inflow": 110_000_000.0},
            {"stock_code": "000001", "trade_time": f"{trade_date} 10:19:00", "main_net_inflow": 140_000_000.0},
            {"stock_code": "000001", "trade_time": f"{trade_date} 10:20:00", "main_net_inflow": 150_000_000.0},
        ]
        with patch("server.api.routers.hot_data._read_sql", return_value=rows) as read_mock, \
             patch("server.api.routers.hot_data._portfolio_time_age_seconds", return_value=12):
            out = hot_data._portfolio_min_flow_summary(
                ["000001"], trade_date=trade_date, market_mode="intraday",
            )

        item = out["000001"]
        self.assertEqual(item["flow_status"], "fresh")
        self.assertEqual(item["main_net_inflow"], 150_000_000.0)
        self.assertEqual(item["flow_1m"], 10_000_000.0)
        self.assertEqual(item["flow_5m"], 40_000_000.0)
        self.assertEqual(item["flow_attitude"], "strong_in")
        sql, params = read_mock.call_args.args
        self.assertNotIn("SUM(CASE", sql)
        self.assertEqual(params["flow_date"], trade_date)

    def test_portfolio_min_flow_summary_does_not_publish_stale_attitude(self):
        trade_date = datetime.now().strftime("%Y-%m-%d")
        rows = [
            {"stock_code": "000001", "trade_time": f"{trade_date} 10:15:00", "main_net_inflow": -100_000_000.0},
            {"stock_code": "000001", "trade_time": f"{trade_date} 10:20:00", "main_net_inflow": -180_000_000.0},
        ]
        with patch("server.api.routers.hot_data._read_sql", return_value=rows), \
             patch("server.api.routers.hot_data._portfolio_time_age_seconds", return_value=600):
            out = hot_data._portfolio_min_flow_summary(
                ["000001"], trade_date=trade_date, market_mode="intraday",
            )

        self.assertEqual(out["000001"]["flow_status"], "stale")
        self.assertEqual(out["000001"]["flow_attitude"], "")
        self.assertEqual(out["000001"]["flow_attitude_basis"], "")

    def test_portfolio_min_flow_summary_labels_fresh_flow_while_baseline_builds(self):
        trade_date = datetime.now().strftime("%Y-%m-%d")
        rows = [{
            "stock_code": "000001",
            "trade_time": f"{trade_date} 09:30:00",
            "main_net_inflow": 12_500_000.0,
        }]
        with patch("server.api.routers.hot_data._read_sql", return_value=rows), \
             patch("server.api.routers.hot_data._portfolio_time_age_seconds", return_value=12):
            out = hot_data._portfolio_min_flow_summary(
                ["000001"], trade_date=trade_date, market_mode="intraday",
            )

        item = out["000001"]
        self.assertEqual(item["flow_status"], "fresh")
        self.assertEqual(item["main_net_inflow"], 12_500_000.0)
        self.assertIsNone(item["flow_5m"])
        self.assertEqual(item["flow_attitude"], "neutral")
        self.assertEqual(item["flow_attitude_label"], "基线建立中")
        self.assertEqual(item["flow_attitude_basis"], "minute_current_fresh")

    def test_portfolio_min_flow_summary_keeps_target_day_close(self):
        rows = [
            {"stock_code": "000001", "trade_time": "2026-08-06 14:55:00", "main_net_inflow": 100_000_000.0},
            {"stock_code": "000001", "trade_time": "2026-08-06 15:00:00", "main_net_inflow": 125_000_000.0},
        ]
        with patch("server.api.routers.hot_data._read_sql", return_value=rows):
            out = hot_data._portfolio_min_flow_summary(
                ["000001"], trade_date="2026-08-06", market_mode="post_close",
            )

        self.assertEqual(out["000001"]["flow_status"], "closed")
        self.assertEqual(out["000001"]["main_net_inflow"], 125_000_000.0)
        self.assertEqual(out["000001"]["flow_attitude_basis"], "minute_day_close")

    def test_read_sql_routes_pure_capital_flow_to_minute_engine(self):
        flow_engine = object()
        with patch("server.api.routers.hot_data.get_minute_engine", return_value=flow_engine) as minute_engine_mock, \
             patch("server.api.routers.hot_data.get_engine") as primary_engine_mock, \
             patch("server.api.routers.hot_data.get_kline_engine") as kline_engine_mock, \
             patch("server.api.routers.hot_data.read_sql_rows", return_value=[{"ok": 1}]) as read_mock:
            out = hot_data._read_sql(
                "SELECT stock_code FROM sm_stock_capital_flow_daily WHERE trade_date = :d",
                {"d": "2026-08-10"},
            )

        self.assertEqual(out, [{"ok": 1}])
        minute_engine_mock.assert_called_once_with()
        primary_engine_mock.assert_not_called()
        kline_engine_mock.assert_not_called()
        self.assertIs(read_mock.call_args.args[0], flow_engine)

    def test_portfolio_qmt_flow_refresh_writes_to_minute_engine(self):
        flow_engine = MagicMock()
        frame = hot_data.pd.DataFrame([{
            "stock_code": "000001",
            "trade_time": "2026-08-10 10:00:00",
            "main_net_inflow": 12_000_000.0,
            "max_net_inflow": 8_000_000.0,
            "lg_net_inflow": 4_000_000.0,
            "mid_net_inflow": -1_000_000.0,
            "sm_net_inflow": -11_000_000.0,
        }])

        with patch("integrations.qmt.bridge.flow_min", return_value=frame), \
             patch("integrations.qmt.backend.to_qmt_symbol", return_value="000001.SZ"), \
             patch("server.api.routers.hot_data.get_minute_engine", return_value=flow_engine), \
             patch("server.api.routers.hot_data.get_engine") as primary_engine_mock, \
             patch("server.api.routers.hot_data.write_frame") as write_mock:
            out = hot_data._portfolio_refresh_qmt_min_flow(["000001"], force=True)

        self.assertEqual(out["status"], "success")
        self.assertEqual(out["rows"], 1)
        primary_engine_mock.assert_not_called()
        flow_engine.begin.assert_called_once_with()
        self.assertIs(write_mock.call_args.args[2], flow_engine)

    def test_portfolio_snapshot_hides_stale_flow_values_and_attitude(self):
        position = {
            "id": 1,
            "stock_code": "000001",
            "shares": 0,
            "cost_price": 0,
            "sort_order": 1,
        }
        kline = {
            "stock_code": "000001",
            "trade_date": "2026-08-10",
            "close": 10.0,
            "pre_close": 9.8,
            "change_pct": 2.04,
            "short_name": "Ping An",
        }
        stale_flow = {
            "000001": {
                "main_net_inflow": 180_000_000.0,
                "max_net_inflow": 80_000_000.0,
                "lg_net_inflow": 60_000_000.0,
                "mid_net_inflow": 10_000_000.0,
                "sm_net_inflow": -150_000_000.0,
                "flow_1m": 10_000_000.0,
                "flow_5m": 50_000_000.0,
                "flow_15m": 90_000_000.0,
                "flow_status": "stale",
                "flow_trade_date": "2026-08-09",
                "flow_latest_time": "2026-08-09 15:00:00",
                "flow_source": "old_snapshot",
                "flow_attitude": "strong_in",
                "flow_attitude_label": "Strong inflow",
                "flow_attitude_ratio": 18.0,
                "flow_attitude_basis": "minute_5m",
            },
        }

        def read_rows(sql, params=None):
            if "FROM st_user_portfolio" in sql:
                return [dict(position)]
            if "FROM sm_stock_kline" in sql:
                return [dict(kline)]
            return []

        with patch("server.api.routers.hot_data._read_sql", side_effect=read_rows), \
             patch("server.api.routers.hot_data._portfolio_market_mode", return_value="post_close"), \
             patch("server.api.routers.hot_data._portfolio_close_trade_date", return_value="2026-08-10"), \
             patch("server.api.routers.hot_data._portfolio_closed_quotes_from_current_table", return_value={}), \
             patch("server.api.routers.hot_data._portfolio_min_flow_summary", return_value=stale_flow):
            out = hot_data._build_portfolio_snapshot()

        item = out["data"][0]
        self.assertEqual(item["expected_flow_date"], "2026-08-10")
        self.assertEqual(item["flow_status"], "stale")
        self.assertEqual(item["flow_trade_date"], "2026-08-09")
        for field in (
            "main_net_inflow", "max_net_inflow", "lg_net_inflow",
            "mid_net_inflow", "sm_net_inflow", "flow_1m", "flow_5m", "flow_15m",
        ):
            self.assertIsNone(item[field])
        self.assertEqual(item["flow_attitude"], "")
        self.assertEqual(item["flow_attitude_label"], "")
        self.assertIsNone(item["flow_attitude_ratio"])
        self.assertEqual(item["watch_analysis"]["funds"], "暂无")

    def test_portfolio_daily_flow_attitude_returns_neutral_label_for_zero_flow(self):
        out = hot_data._portfolio_daily_flow_attitude(0)

        self.assertEqual(out["level"], "neutral")
        self.assertTrue(out["label"])

    def test_portfolio_refresh_prices_submits_registered_public_quorum_task(self):
        launched = {
            "accepted": True,
            "status": "running",
            "job_id": "a" * 32,
        }
        with patch(
            "server.api.routers.hot_data._is_monitor_trading_time",
            return_value=True,
        ), patch(
            "server.api.routers.hot_data._launch_registered_scheduler_task",
            return_value=launched,
        ) as launch_mock:
            out = hot_data.portfolio_refresh_prices()

        self.assertTrue(out["accepted"])
        self.assertEqual(out["state"], "running")
        self.assertIn("已提交", out["message"])
        launch_mock.assert_called_once_with(
            task_type="portfolio_quote_refresh",
            expected_script_path="tools/run_portfolio_quote_refresh.py",
            script_args_override="--force",
        )

    def test_portfolio_refresh_prices_off_hours_forces_public_quorum_task(self):
        launched = {
            "accepted": True,
            "status": "running",
            "job_id": "c" * 32,
        }
        with patch(
            "server.api.routers.hot_data._is_monitor_trading_time",
            return_value=False,
        ), patch(
            "server.api.routers.hot_data._launch_registered_scheduler_task",
            return_value=launched,
        ) as launch_mock:
            out = hot_data.portfolio_refresh_prices()

        self.assertTrue(out["accepted"])
        self.assertEqual(out["state"], "running")
        self.assertEqual(out["job_id"], "c" * 32)
        launch_mock.assert_called_once_with(
            task_type="portfolio_quote_refresh",
            expected_script_path="tools/run_portfolio_quote_refresh.py",
            script_args_override="--force",
        )

    def test_portfolio_refresh_status_uses_scheduler_audit_receipt(self):
        with patch(
            "server.api.routers.hot_data._read_sql",
            side_effect=[
                [{"id": 17}],
                [{
                    "run_uid": "b" * 32,
                    "status": "running",
                    "run_at": "2026-08-25 09:31:00",
                    "finished_at": None,
                    "output": "",
                }],
            ],
        ) as read_mock:
            out = hot_data.portfolio_refresh_prices_status("b" * 32)

        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["state"]["state"], "running")
        self.assertEqual(out["state"]["job_id"], "b" * 32)
        history_sql = read_mock.call_args_list[1].args[0]
        history_params = read_mock.call_args_list[1].args[1]
        self.assertIn("run_uid=:run_uid", history_sql)
        self.assertEqual(history_params["run_uid"], "b" * 32)

    def test_portfolio_live_quote_read_does_not_enter_qmt_on_cache_miss(self):
        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._cache_set"), \
             patch("server.api.routers.hot_data._live_quotes_from_current_table", return_value={}), \
             patch("server.api.routers.hot_data._live_quotes_from_portfolio_public_table", return_value={}), \
             patch("server.api.routers.hot_data._live_quotes_from_public_quote_table", return_value={}), \
             patch("tools.sync_qmt_realtime.sync_qmt_realtime") as sync_mock:
            out = hot_data._portfolio_fetch_live_quotes(["000001"])

        self.assertEqual(out, {})
        sync_mock.assert_not_called()

    def test_portfolio_force_read_never_calls_remote_market_writers(self):
        fresh_quote = {
            "000001": {
                "stock_code": "000001",
                "price": 12.34,
                "quote_status": "fresh",
            }
        }
        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._cache_set"), \
             patch("server.api.routers.hot_data._live_quotes_from_current_table", return_value=fresh_quote), \
             patch("server.api.routers.hot_data._live_quotes_from_portfolio_public_table", return_value={}), \
             patch("server.api.routers.hot_data._live_quotes_from_public_quote_table", return_value={}), \
             patch("tools.run_big_qmt_bridge.sync_big_qmt_realtime", side_effect=RuntimeError("QMT unavailable")) as qmt_mock, \
             patch("tools.sync_market_realtime.sync_market_realtime") as sina_mock:
            out = hot_data._portfolio_fetch_live_quotes(["000001"], force=True)

        qmt_mock.assert_not_called()
        sina_mock.assert_not_called()
        self.assertEqual(out, fresh_quote)

    def test_portfolio_live_quotes_prefer_public_quorum_and_exclude_qmt(self):
        qmt_quote = {
            "stock_code": "600000",
            "price": 8.01,
            "source": "qmt_live_table",
            "is_qmt": True,
            "quote_status": "fresh",
        }
        quorum_quote = {
            "stock_code": "000001",
            "price": 12.35,
            "source": "portfolio_public_quote_quorum",
            "is_qmt": False,
            "quote_status": "fresh",
        }
        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._cache_set"), \
             patch(
                 "server.api.routers.hot_data._live_quotes_from_current_table",
                 return_value={"600000": qmt_quote},
             ) as current_mock, \
             patch(
                 "server.api.routers.hot_data._live_quotes_from_portfolio_public_table",
                 return_value={"000001": quorum_quote},
             ) as public_mock, \
             patch(
                 "server.api.routers.hot_data._live_quotes_from_public_quote_table",
                 return_value={},
             ):
            out = hot_data._portfolio_fetch_live_quotes(["000001", "600000"])

        self.assertIs(out["000001"], quorum_quote)
        self.assertNotIn("600000", out)
        public_mock.assert_called_once_with(
            ["000001", "600000"],
            max_age_seconds=hot_data.PORTFOLIO_LIVE_FRESH_SECONDS,
        )
        self.assertEqual(
            current_mock.call_args_list,
            [
                call(
                    ["600000"],
                    max_age_seconds=hot_data.PORTFOLIO_LIVE_FRESH_SECONDS,
                ),
                call(
                    ["600000"],
                    max_age_seconds=hot_data.PORTFOLIO_LIVE_FRESH_SECONDS,
                    allow_stale=True,
                    max_stale_age_seconds=hot_data.PORTFOLIO_LIVE_STALE_SECONDS,
                ),
            ],
        )

    def test_portfolio_closed_quotes_use_public_quorum_without_qmt(self):
        qmt_quote = {
            "stock_code": "000001",
            "price": 12.34,
            "source": "current_close_table",
            "quote_status": "closed",
        }
        quorum_quote = {
            "stock_code": "000001",
            "price": 12.35,
            "source": "portfolio_public_quote_quorum",
            "quote_status": "closed",
        }
        with patch(
            "server.api.routers.hot_data._portfolio_closed_quotes_from_current_table",
            return_value={"000001": qmt_quote},
        ) as qmt_mock, patch(
            "server.api.routers.hot_data._live_quotes_from_portfolio_public_table",
            return_value={"000001": quorum_quote},
        ) as public_mock, patch(
            "server.api.routers.hot_data._live_quotes_from_public_quote_table",
            return_value={},
        ):
            out = hot_data._portfolio_fetch_closed_quotes(
                ["000001", "600000"],
                date.today().isoformat(),
            )

        self.assertIs(out["000001"], quorum_quote)
        self.assertNotIn("600000", out)
        qmt_mock.assert_not_called()
        public_mock.assert_called_once_with(
            ["000001", "600000"],
            max_age_seconds=hot_data.PORTFOLIO_LIVE_FRESH_SECONDS,
        )

    def test_portfolio_public_quote_reader_requires_current_two_source_rows(self):
        quote = {
            "stock_code": "000001",
            "short_name": "平安银行",
            "price": 10.2,
            "pre_close": 10.0,
            "change_pct": 2.0,
            "volume": 1234,
            "amount": 5678,
            "quote_at": "2026-06-26 13:29:30",
            "source_provider": "PUBLIC_PORTFOLIO_QUORUM_V1",
            "source_count": 2,
            "provider_mask": "sina,tencent",
        }
        with patch("server.api.routers.hot_data.datetime", _FakeIntradayDatetime), \
             patch("server.api.routers.hot_data._read_sql", return_value=[quote]) as read_mock:
            out = hot_data._live_quotes_from_portfolio_public_table(
                ["1"],
                max_age_seconds=90,
            )

        self.assertEqual(out["000001"]["price"], 10.2)
        self.assertEqual(out["000001"]["change"], 0.2)
        self.assertEqual(
            out["000001"]["source"],
            "portfolio_public_quote_quorum",
        )
        sql = read_mock.call_args.args[0]
        self.assertIn("st_portfolio_public_quote_v1", sql)
        self.assertIn("source_count >= 2", sql)
        self.assertIn("quality_status = 'PASS'", sql)

    def test_portfolio_public_quote_reader_accepts_verified_same_day_close(self):
        quote = {
            "stock_code": "000001",
            "short_name": "平安银行",
            "price": 10.2,
            "pre_close": 10.0,
            "change_pct": 2.0,
            "volume": 1234,
            "amount": 5678,
            "quote_at": "2026-06-26 15:00:03",
            "source_provider": "PUBLIC_PORTFOLIO_QUORUM_V1",
            "source_count": 2,
            "provider_mask": "sina,tencent",
        }
        with patch("server.api.routers.hot_data.datetime", _FakeAfterCloseDatetime), \
             patch("server.api.routers.hot_data._read_sql", return_value=[quote]) as read_mock:
            out = hot_data._live_quotes_from_portfolio_public_table(
                ["1"],
                max_age_seconds=90,
            )

        self.assertEqual(out["000001"]["price"], 10.2)
        self.assertEqual(out["000001"]["quote_status"], "closed")
        params = read_mock.call_args.args[1]
        self.assertEqual(params["accept_same_day_close"], 1)
        self.assertEqual(params["close_start"], _FakeAfterCloseDatetime(2026, 6, 26, 15, 0, 0))

    def test_public_quote_reader_requires_recent_passed_two_source_batch(self):
        receipt = {"batch_id": "batch-1", "quote_at": "2026-06-26 13:29:30"}
        quote = {
            "stock_code": "000001",
            "short_name": "平安银行",
            "price": 10.2,
            "pre_close": 10.0,
            "change_pct": 2.0,
            "volume": 1234,
            "amount": 5678,
            "quote_at": "2026-06-26 13:29:30",
            "source_provider": "PUBLIC_QUOTE_QUORUM_V1",
            "source_count": 2,
            "provider_mask": "sina,tencent",
        }
        with patch("server.api.routers.hot_data.datetime", _FakeIntradayDatetime), \
             patch(
                 "server.api.routers.hot_data._read_sql",
                 side_effect=[[receipt], [quote]],
             ) as read_mock:
            out = hot_data._live_quotes_from_public_quote_table(
                ["1"],
                max_age_seconds=90,
            )

        self.assertEqual(out["000001"]["price"], 10.2)
        self.assertEqual(out["000001"]["change"], 0.2)
        self.assertEqual(out["000001"]["source"], "public_quote_quorum")
        self.assertEqual(out["000001"]["source_count"], 2)
        self.assertEqual(out["000001"]["quote_status"], "fresh")
        receipt_sql = read_mock.call_args_list[0].args[0]
        quote_sql = read_mock.call_args_list[1].args[0]
        self.assertIn("quality_status = 'PASS'", receipt_sql)
        self.assertIn("source_count >= 2", quote_sql)
        self.assertIn("quality_status = 'PASS'", quote_sql)

    def test_portfolio_live_snapshot_has_positive_intraday_ttl(self):
        with patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=True):
            self.assertEqual(hot_data._portfolio_snapshot_ttl_seconds(True), 3)

    def test_portfolio_snapshot_returns_stale_value_while_builder_is_busy(self):
        self._clear_hot_data_cache()
        stale = {"data": [{"stock_code": "000001"}], "total": 1, "summary": {}}
        try:
            with hot_data._cache_lock:
                hot_data._cache_store["portfolio_snapshot"] = (0.0, stale)
            hot_data._portfolio_snapshot_build_lock.acquire()
            try:
                with patch("server.api.routers.hot_data._cache_now", return_value=10.0), \
                     patch("server.api.routers.hot_data._portfolio_snapshot_ttl_seconds", return_value=3), \
                     patch("server.api.routers.hot_data._build_portfolio_snapshot") as build_mock:
                    out = hot_data._get_portfolio_snapshot(live_mode=True)
            finally:
                hot_data._portfolio_snapshot_build_lock.release()

            build_mock.assert_not_called()
            self.assertEqual(out["total"], 1)
            self.assertTrue(out["snapshot_stale"])
            self.assertTrue(out["snapshot_refreshing"])
        finally:
            self._clear_hot_data_cache()

    def test_forced_portfolio_snapshot_reports_retryable_contention(self):
        self._clear_hot_data_cache()
        stale = {"data": [{"stock_code": "000001"}], "total": 1, "summary": {}}
        try:
            with hot_data._cache_lock:
                hot_data._cache_store["portfolio_snapshot"] = (0.0, stale)
            hot_data._portfolio_snapshot_build_lock.acquire()
            try:
                with patch("server.api.routers.hot_data._cache_now", return_value=10.0), \
                     patch("server.api.routers.hot_data._build_portfolio_snapshot") as build_mock:
                    out = hot_data._get_portfolio_snapshot(live_mode=True, force_live=True)
            finally:
                hot_data._portfolio_snapshot_build_lock.release()

            build_mock.assert_not_called()
            self.assertTrue(out["retryable"])
            self.assertTrue(out["force"])
            self.assertIn("already in progress", out["error"])
        finally:
            self._clear_hot_data_cache()

    def test_completed_force_request_is_reused_by_browser_retry(self):
        self._clear_hot_data_cache()
        result = {"data": [{"stock_code": "000001"}], "total": 1, "summary": {}}
        try:
            with patch(
                "server.api.routers.hot_data._build_portfolio_snapshot",
                return_value=result,
            ) as build_mock:
                first = hot_data._get_portfolio_snapshot(
                    live_mode=True,
                    force_live=True,
                    force_request_id="same-browser-refresh",
                )
                second = hot_data._get_portfolio_snapshot(
                    live_mode=True,
                    force_live=True,
                    force_request_id="same-browser-refresh",
                )

            build_mock.assert_called_once_with(force_live=True)
            self.assertEqual(first["total"], 1)
            self.assertEqual(second["total"], 1)
            self.assertTrue(second["force_reused"])
        finally:
            self._clear_hot_data_cache()

    def test_force_waiter_rechecks_same_request_after_winning_build_lock(self):
        self._clear_hot_data_cache()
        result = {"data": [{"stock_code": "000001"}], "total": 1, "summary": {}}
        first_check_done = threading.Event()
        outputs = []
        errors = []
        original_completed = hot_data._portfolio_completed_force_result
        lock_held = False

        def observed_completed(force_request_id):
            completed = original_completed(force_request_id)
            first_check_done.set()
            return completed

        def run_waiter():
            try:
                outputs.append(hot_data._get_portfolio_snapshot(
                    live_mode=True,
                    force_live=True,
                    force_request_id="force-finishes-during-wait",
                ))
            except Exception as exc:  # pragma: no cover - assertion reports it
                errors.append(exc)

        try:
            hot_data._portfolio_snapshot_build_lock.acquire()
            lock_held = True
            with patch(
                "server.api.routers.hot_data._portfolio_completed_force_result",
                side_effect=observed_completed,
            ), patch(
                "server.api.routers.hot_data._build_portfolio_snapshot",
            ) as build_mock:
                waiter = threading.Thread(target=run_waiter)
                waiter.start()
                self.assertTrue(first_check_done.wait(timeout=1.0))
                self.assertTrue(hot_data._portfolio_snapshot_cache_publish(
                    result,
                    generation=hot_data._portfolio_snapshot_generation,
                    force_request_id="force-finishes-during-wait",
                ))
                hot_data._portfolio_snapshot_build_lock.release()
                lock_held = False
                waiter.join(timeout=2.0)

            self.assertFalse(waiter.is_alive())
            self.assertEqual(errors, [])
            build_mock.assert_not_called()
            self.assertTrue(outputs[0]["force_reused"])
        finally:
            if lock_held:
                hot_data._portfolio_snapshot_build_lock.release()
            self._clear_hot_data_cache()

    def test_force_build_error_with_stale_data_remains_retryable(self):
        self._clear_hot_data_cache()
        stale = {"data": [{"stock_code": "OLD"}], "total": 1, "summary": {}}
        fresh = {"data": [{"stock_code": "NEW"}], "total": 1, "summary": {}}
        try:
            with hot_data._cache_lock:
                hot_data._cache_store["portfolio_snapshot"] = (0.0, stale)
            with patch(
                "server.api.routers.hot_data._build_portfolio_snapshot",
                side_effect=[RuntimeError("temporary database outage"), fresh],
            ) as build_mock:
                failed = hot_data._get_portfolio_snapshot(
                    live_mode=True,
                    force_live=True,
                    force_request_id="retry-after-error",
                )
                recovered = hot_data._get_portfolio_snapshot(
                    live_mode=True,
                    force_live=True,
                    force_request_id="retry-after-error",
                )

            self.assertIn("temporary database outage", failed["error"])
            self.assertEqual(failed["data"][0]["stock_code"], "OLD")
            self.assertEqual(recovered["data"][0]["stock_code"], "NEW")
            self.assertEqual(build_mock.call_count, 2)
        finally:
            self._clear_hot_data_cache()

    def test_cached_snapshot_error_has_short_negative_ttl(self):
        self._clear_hot_data_cache()
        failure = {"data": [], "total": 0, "error": "database unavailable"}
        fresh = {"data": [{"stock_code": "000001"}], "total": 1, "summary": {}}
        try:
            with hot_data._cache_lock:
                hot_data._cache_store["portfolio_snapshot"] = (0.0, failure)
            with patch("server.api.routers.hot_data._cache_now", return_value=10.0), \
                 patch("server.api.routers.hot_data._portfolio_snapshot_ttl_seconds", return_value=60), \
                 patch(
                     "server.api.routers.hot_data._build_portfolio_snapshot",
                     return_value=fresh,
                 ) as build_mock:
                out = hot_data._get_portfolio_snapshot(live_mode=False)

            build_mock.assert_called_once_with(force_live=False)
            self.assertEqual(out["total"], 1)
        finally:
            self._clear_hot_data_cache()

    def test_portfolio_mutation_prevents_inflight_snapshot_from_repopulating_cache(self):
        self._clear_hot_data_cache()

        def build_then_mutate(*, force_live=False):
            hot_data._invalidate_portfolio_snapshot_cache()
            return {"data": [{"stock_code": "OLD"}], "total": 1, "summary": {}}

        try:
            with patch(
                "server.api.routers.hot_data._build_portfolio_snapshot",
                side_effect=build_then_mutate,
            ):
                out = hot_data._get_portfolio_snapshot(live_mode=True)

            self.assertTrue(out["retryable"])
            self.assertTrue(out["snapshot_superseded"])
            with hot_data._cache_lock:
                self.assertNotIn("portfolio_snapshot", hot_data._cache_store)
        finally:
            self._clear_hot_data_cache()

    def test_cold_portfolio_contention_has_a_bounded_wait(self):
        self._clear_hot_data_cache()
        try:
            hot_data._portfolio_snapshot_build_lock.acquire()
            try:
                with patch("server.api.routers.hot_data._build_portfolio_snapshot") as build_mock:
                    out = hot_data._get_portfolio_snapshot(live_mode=True)
            finally:
                hot_data._portfolio_snapshot_build_lock.release()

            build_mock.assert_not_called()
            self.assertTrue(out["retryable"])
            self.assertIn("already in progress", out["error"])
        finally:
            self._clear_hot_data_cache()

    def test_portfolio_snapshot_keeps_last_good_value_on_build_error(self):
        self._clear_hot_data_cache()
        stale = {"data": [{"stock_code": "000001"}], "total": 1, "summary": {}}
        try:
            with hot_data._cache_lock:
                hot_data._cache_store["portfolio_snapshot"] = (0.0, stale)
            with patch("server.api.routers.hot_data._cache_now", return_value=10.0), \
                 patch("server.api.routers.hot_data._portfolio_snapshot_ttl_seconds", return_value=3), \
                 patch(
                     "server.api.routers.hot_data._build_portfolio_snapshot",
                     side_effect=RuntimeError("temporary database outage"),
                 ):
                out = hot_data._get_portfolio_snapshot(live_mode=True)

            self.assertEqual(out["total"], 1)
            self.assertTrue(out["snapshot_stale"])
            self.assertIn("temporary database outage", out["snapshot_error"])
        finally:
            self._clear_hot_data_cache()

    def test_stock_detail_uses_short_intraday_cache(self):
        cached = {"stock_code": "000001", "cached": True}
        with patch("server.api.routers.hot_data._portfolio_market_mode", return_value="intraday"), \
             patch("server.api.routers.hot_data._stock_detail_portfolio_context", return_value={}), \
             patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=True), \
             patch("server.api.routers.hot_data._cache_get", return_value=cached) as cache_get_mock:
            out = hot_data.stock_detail("000001")

        self.assertEqual(out["stock_code"], cached["stock_code"])
        self.assertTrue(out["cached"])
        self.assertFalse(out["watchlist_member"])
        cache_get_mock.assert_called_once_with("stock_detail_000001_intraday", ttl_seconds=12)

    def test_recommended_stocks_latest_uses_bounded_intraday_cache(self):
        cached = {"data": [], "total": 0}
        with patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=True), \
             patch("server.api.routers.hot_data._cache_get", return_value=cached) as cache_get_mock:
            out = hot_data.recommended_stocks(trade_date="", strategy="", signal_status="")

        self.assertIs(out, cached)
        cache_get_mock.assert_called_once_with(
            "recommended_stocks_latest_all_all",
            ttl_seconds=30,
        )

    def test_a_list_info_returns_daily_summary(self):
        summary_row = {
            "stock_code": "000001",
            "short_name": "平安银行",
            "a_buy_amount": 300_000_000,
            "a_sell_amount": 120_000_000,
            "a_net_amount": 180_000_000,
            "change_cpt": 10.02,
            "reason": "日涨幅偏离值达7%",
        }
        detail_rows = [{
            "operate_name": "机构专用",
            "a_buy_amount": 80_000_000,
            "a_sell_amount": 10_000_000,
            "a_net_amount": 70_000_000,
        }]
        with patch("server.api.routers.hot_data._read_sql", side_effect=[[summary_row], detail_rows]):
            out = hot_data.a_list_info("2026-06-28", "000001")

        self.assertEqual(out["summary"]["a_net_amount"], 180_000_000)
        self.assertEqual(out["data"][0]["operate_name"], "机构专用")

    def test_a_list_info_builds_summary_when_daily_row_missing(self):
        detail_rows = [
            {"operate_name": "A", "a_buy_amount": 100.0, "a_sell_amount": 20.0, "reason": "x"},
            {"operate_name": "B", "a_buy_amount": 10.0, "a_sell_amount": 60.0, "reason": "x"},
        ]
        with patch("server.api.routers.hot_data._read_sql", side_effect=[[], detail_rows]):
            out = hot_data.a_list_info("2026-06-28", "000001")

        self.assertEqual(out["summary"]["a_buy_amount"], 110.0)
        self.assertEqual(out["summary"]["a_sell_amount"], 80.0)
        self.assertEqual(out["summary"]["a_net_amount"], 30.0)

    def test_portfolio_watch_analysis_flags_outflow_risk(self):
        row = {
            "stock_code": "000001",
            "cur_price": 10.0,
            "change_pct": -3.2,
            "shares": 1000,
            "is_holding": True,
            "main_net_inflow": -120_000_000,
            "quote_trade_date": "2026-06-26",
            "flow_trade_date": "2026-06-26",
            "flow_status": "closed",
            "flow_attitude_basis": "daily_close",
        }

        out = hot_data._portfolio_build_watch_analysis(row)

        self.assertEqual(out["funds"], "强出")
        self.assertEqual(out["funds_level"], "strong_out")
        self.assertEqual(out["operation_advice"], "控仓")
        self.assertIn("资金外流", out["risk_tip"])
        self.assertEqual(out["drawdown_guard"]["level"], "LOW")

    def test_portfolio_watch_analysis_flags_stop_loss_guard(self):
        row = {
            "stock_code": "000001",
            "cur_price": 9.1,
            "cost_price": 10.0,
            "profit_pct": -9.0,
            "change_pct": -3.5,
            "shares": 1000,
            "is_holding": True,
            "main_net_inflow": -60_000_000,
        }

        out = hot_data._portfolio_build_watch_analysis(row)

        self.assertEqual(out["drawdown_guard"]["level"], "HIGH")
        self.assertEqual(out["drawdown_guard"]["action"], "止损复核")
        self.assertEqual(out["drawdown_guard"]["stop_loss_line"], 9.5)

    def test_portfolio_watch_analysis_flags_profit_protection_guard(self):
        row = {
            "stock_code": "000001",
            "cur_price": 12.0,
            "cost_price": 9.5,
            "profit_pct": 26.3,
            "change_pct": -4.2,
            "shares": 1000,
            "is_holding": True,
            "main_net_inflow": -10_000_000,
        }

        out = hot_data._portfolio_build_watch_analysis(row)

        self.assertEqual(out["drawdown_guard"]["level"], "HIGH")
        self.assertEqual(out["drawdown_guard"]["action"], "止盈保护")
        self.assertIn("保护利润", out["drawdown_guard"]["reason"])

    def test_portfolio_watch_analysis_uses_fresh_minute_flow(self):
        row = {
            "stock_code": "000001",
            "cur_price": 10.0,
            "change_pct": 2.2,
            "shares": 1000,
            "is_holding": True,
            "main_net_inflow": -120_000_000,
            "flow_status": "fresh",
            "flow_attitude": "strong_in",
            "flow_attitude_label": "强进",
            "flow_attitude_ratio": 12.5,
            "flow_5m": 25_000_000,
        }

        out = hot_data._portfolio_build_watch_analysis(row)

        self.assertEqual(out["funds"], "强进")
        self.assertEqual(out["funds_level"], "strong_in")
        self.assertEqual(out["funds_source"], "minute_5m_fresh")
        funds_evidence = next(item for item in out["evidence"] if item["label"] == "资金")
        self.assertIn("2500.0万", funds_evidence["value"])
        self.assertNotIn("-1.20亿", funds_evidence["value"])

    def test_portfolio_watch_analysis_ignores_stale_minute_flow(self):
        row = {
            "stock_code": "000001",
            "cur_price": 10.0,
            "change_pct": -2.5,
            "shares": 1000,
            "is_holding": True,
            "main_net_inflow": -120_000_000,
            "flow_status": "stale",
            "flow_attitude": "strong_in",
            "flow_attitude_label": "强进",
            "flow_attitude_ratio": 12.5,
        }

        out = hot_data._portfolio_build_watch_analysis(row)

        self.assertEqual(out["funds"], "暂无")
        self.assertEqual(out["funds_level"], "neutral")
        self.assertEqual(out["funds_source"], "unavailable")
        self.assertIn("当日资金滞后", out["risk_tip"])

    def test_mainforce_fast_analysis_exposes_distribution_evidence(self):
        flow_rows = [
            {"trade_date": "2026-06-26", "main_net_inflow": -20_000_000, "sm_net_inflow": 10_000_000},
            {"trade_date": "2026-06-25", "main_net_inflow": -30_000_000, "sm_net_inflow": 12_000_000},
            {"trade_date": "2026-06-24", "main_net_inflow": -25_000_000, "sm_net_inflow": 9_000_000},
            {"trade_date": "2026-06-23", "main_net_inflow": -18_000_000, "sm_net_inflow": 8_000_000},
            {"trade_date": "2026-06-22", "main_net_inflow": -15_000_000, "sm_net_inflow": 7_000_000},
        ]
        snapshot = {
            "stock_code": "000001",
            "short_name": "平安银行",
            "trade_date": "2026-06-26",
            "price": 12.0,
            "change_pct": 4.5,
            "change_3d": 8.0,
            "change_5d": 22.0,
            "change_10d": 30.0,
            "turnover_ratio": 9.2,
            "amount": 1_000_000_000,
            "main_net_inflow": -50_000_000,
        }
        with patch("server.api.routers.hot_data._latest_date_not_after", return_value="2026-06-26"), patch(
            "server.api.routers.hot_data._read_sql",
            side_effect=[
                [{"short_name": "平安银行"}],
                [snapshot],
                flow_rows,
                [{"holder_num_ratio": 8.0}],
                [{"cnt": 1, "inst_net_buy": -30_000_000}],
            ],
        ):
            out = hot_data._compute_mainforce_behavior_fast("000001", "2026-06-26")

        evidence_text = " ".join(str(item["text"]) for item in out["evidence"])
        evidence_dirs = {item["direction"] for item in out["evidence"]}
        self.assertIn("出货", evidence_dirs)
        self.assertIn("散户流入", evidence_text)
        self.assertIn("机构净卖出", evidence_text)

    def test_mainforce_fast_analysis_falls_back_to_daily_kline_when_snapshot_lags(self):
        kline_rows = []
        for idx in range(11):
            day = 27 + idx
            date_text = f"2026-06-{day:02d}" if day <= 30 else f"2026-07-{day - 30:02d}"
            kline_rows.append({
                "trade_date": date_text,
                "close": 10.0 + idx,
                "amount": 200_000_000 + idx * 10_000_000,
                "change_pct": 1.0 + idx / 10,
                "turnover_ratio": 3.0,
            })
        flow_rows = [{"trade_date": "2026-07-07", "main_net_inflow": 20_000_000, "sm_net_inflow": -5_000_000}]

        with patch("server.api.routers.hot_data._latest_market_analysis_date", return_value="2026-07-07"), \
             patch(
                 "server.api.routers.hot_data._read_sql",
                 side_effect=[
                     [{"short_name": "平安银行"}],
                     [],
                     list(reversed(kline_rows)),
                     flow_rows,
                     [],
                     [{"cnt": 0, "inst_net_buy": 0}],
                 ],
             ):
            out = hot_data._compute_mainforce_behavior_fast("000001", "2026-07-07")

        self.assertEqual(out["trade_date"], "2026-07-07")
        self.assertEqual(out["source"], "daily_kline_fast")
        self.assertIn("日K线", out["note"])

    def test_sector_heat_matrix_falls_back_to_latest_available_date(self):
        side_effect = [
            [{"d": "2026-06-26"}],
            [],
            [],
            [{"snapshot_date": "2026-06-26", "concept_name": "汽车", "hot_value": 100.0, "plate_type": 3}],
            [],
            [{"trade_date": "2026-06-26"}],
        ]
        with patch("server.api.routers.hot_data._read_sql", side_effect=side_effect):
            out = hot_data.sector_heat_matrix(end_date="2026-06-28", days=3)

        self.assertTrue(out["fallback"])
        self.assertEqual(out["requested_date"], "2026-06-28")
        self.assertEqual(out["date"], "2026-06-26")
        self.assertEqual(out["dates"], ["2026-06-26"])
        self.assertIn("东财一级行业", out["groups"])

    def test_style_switch_signal_detects_risk_off_from_sentiment_and_news(self):
        sentiment = {
            "theme_analysis": {"rotation_score": 72, "phase": "高轮动", "phase_desc": "主线切换加快"},
            "style_analysis": {"bias": "大盘占优", "bias_desc": "资金避险偏好核心资产"},
            "capital_analysis": {"flow_style": "主力资金净流出", "recent_trend": "连续流出"},
        }
        news_rows = [
            {"title": "监管政策落地 市场波动加大", "content": "证监会 监管 政策", "subjects": [{"name": "金融监管"}]},
            {"title": "外部冲突扰动升级", "content": "冲突 制裁 汇率", "subjects": [{"name": "国际局势"}]},
            {"title": "稳增长政策会议释放积极信号", "content": "财政 政策 会议", "subjects": [{"name": "稳增长"}]},
        ]

        out = hot_data._style_switch_signal_from_inputs(sentiment, news_rows)

        self.assertEqual(out["status"], "risk_off")
        self.assertGreaterEqual(out["risk_off_score"], 65)
        self.assertGreaterEqual(out["switch_score"], 65)
        self.assertIn("银行", out["defensive_sectors"])
        self.assertTrue(any("风险" in item or "监管" in item for item in out["evidence"]))

    def test_sector_movement_prefers_qmt_plate_map(self):
        side_effect = [
            [{"sa": "2026-06-28 10:00:00"}],
            [{"sa": None}],
            [
                {"stock_code": "000001", "short_name": "A", "now_pct": 2.0, "now_amt": 1000, "prev_pct": None},
                {"stock_code": "000002", "short_name": "B", "now_pct": 4.0, "now_amt": 2000, "prev_pct": None},
            ],
            [
                {"stock_code": "000001", "plate_name": "Power"},
                {"stock_code": "000002", "plate_name": "Power"},
            ],
        ]

        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._cache_set"), \
             patch("server.api.routers.hot_data._read_sql", side_effect=side_effect):
            out = hot_data.sector_movement("industry")

        self.assertEqual(out["mapping_source"], ["qmt_industry"])
        self.assertEqual(out["sectors"][0]["name"], "Power")
        self.assertEqual(out["sectors"][0]["stock_count"], 2)

    def test_sector_rotation_signal_detects_switching(self):
        out = hot_data._sector_rotation_signal(
            rising_sectors=[{"name": "电力", "momentum": 8.5}],
            falling_sectors=[{"name": "半导体", "momentum": -7.2}],
            flow_in_top=[{"name": "电力", "main_net_inflow": 1_200_000_000}],
            flow_out_top=[{"name": "半导体", "main_net_inflow": -900_000_000}],
            flow_snapshot_at="2026-06-28 10:30:00",
        )

        self.assertEqual(out["status"], "switching")
        self.assertIn("半导体", out["summary"])
        self.assertIn("电力", out["summary"])
        self.assertEqual(out["aligned_in"], ["电力"])
        self.assertEqual(out["aligned_out"], ["半导体"])

    def test_sector_rotation_signal_marks_outflow_risk(self):
        out = hot_data._sector_rotation_signal(
            rising_sectors=[],
            falling_sectors=[{"name": "科技", "momentum": -9.0}],
            flow_in_top=[],
            flow_out_top=[{"name": "科技", "main_net_inflow": -1_500_000_000}],
        )

        self.assertEqual(out["status"], "outflow")
        self.assertEqual(out["risk_level"], "high")
        self.assertIn("控制回撤", out["action"])

    def test_sector_rotation_uses_market_live_cache(self):
        cached = {"trade_date": "2026-06-28", "cached": True}
        with patch("server.api.routers.hot_data._market_live_cache_ttl", return_value=30), \
             patch("server.api.routers.hot_data._cache_get", return_value=cached) as cache_get_mock:
            out = hot_data.sector_rotation("2026-06-28", days=10)

        self.assertIs(out, cached)
        cache_get_mock.assert_called_once_with("sector_rotation_2026-06-28_10", ttl_seconds=30)

    def test_monitor_resolve_trade_date_uses_latest_before_requested(self):
        with patch("server.api.routers.hot_data._table_columns", return_value=set()), \
             patch("server.api.routers.hot_data._read_sql", side_effect=[
                 [{"d": "2026-06-13"}],
             ]):
            out = hot_data._monitor_resolve_trade_date("2026-06-14")

        self.assertEqual(out, "2026-06-13")

    def test_monitor_resolve_trade_date_prefers_fresh_kline_over_stale_overview(self):
        with patch("server.api.routers.hot_data._table_columns", return_value={"trade_date"}), \
             patch("server.api.routers.hot_data._read_sql", side_effect=[
                 [{"d": "2026-06-26"}],
                 [{"d": "2026-06-30"}],
             ]):
            out = hot_data._monitor_resolve_trade_date("2026-06-30")

        self.assertEqual(out, "2026-06-30")

    def test_monitor_history_trade_dates_prefers_fresh_kline_over_stale_overview(self):
        kline_rows = [{"trade_date": "2026-06-29"}, {"trade_date": "2026-06-30"}]
        overview_rows = [{"trade_date": "2026-06-25"}, {"trade_date": "2026-06-26"}]
        with patch("server.api.routers.hot_data._table_columns", return_value={"trade_date"}), \
             patch("server.api.routers.hot_data._read_sql", side_effect=[kline_rows, overview_rows]):
            out = hot_data._monitor_history_trade_dates("2026-06-30", limit=2)

        self.assertEqual(out, ["2026-06-29", "2026-06-30"])

    def test_realtime_overview_uses_verified_full_current_table(self):
        engine = MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        connection.execute.return_value.mappings.return_value.first.return_value = {
            "expected_count": 5538,
            "observed_count": 5538,
            "coverage": Decimal("1.0"),
            "published_at": "2026-08-06 10:20:10",
            "source_generated_at": "2026-08-06 10:20:00",
            "capture_mode": "LIVE_FORWARD",
        }
        aggregate = {
            "up_cnt": 2800,
            "down_cnt": 2600,
            "sideline_cnt": 2400,
            "total": 5538,
            "total_amount": 2_000_000_000_000,
            "small_up_cnt": 1200,
            "small_total": 2300,
            "small_avg_chg": 0.6,
            "data_time": "2026-08-06 09:15:00",
            "today_count": 5538,
        }

        with patch("server.api.routers.hot_data.get_current_engine", return_value=engine), \
             patch("server.api.routers.hot_data._read_sql", return_value=[aggregate]) as read_mock:
            out = hot_data._get_realtime_overview()

        self.assertEqual(out["total"], 5538)
        self.assertEqual(out["data_source"], "sm_stock_current")
        self.assertEqual(out["data_time"], "2026-08-06 10:20:10")
        current_sql = read_mock.call_args.args[0]
        self.assertIn("FROM sm_stock_current", current_sql)
        self.assertNotIn("snapshot_at >=", current_sql)
        receipt_sql = str(connection.execute.call_args.args[0])
        self.assertIn("source_provider = 'gj_big_qmt_inner'", receipt_sql)
        self.assertIn("INTERVAL 90 SECOND", receipt_sql)

    def test_realtime_overview_rejects_unverified_or_wrong_day_current_table(self):
        engine = MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        connection.execute.return_value.mappings.return_value.first.return_value = None
        current = {
            "total": 5538,
            "today_count": 5538,
            "data_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        archive = {"total": 50, "today_count": 50, "data_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        with patch("server.api.routers.hot_data.get_current_engine", return_value=engine), \
             patch("server.api.routers.hot_data._read_sql", side_effect=[[current], [archive]]):
            self.assertIsNone(hot_data._get_realtime_overview())

        connection.execute.return_value.mappings.return_value.first.return_value = {
            "expected_count": 5538,
            "observed_count": 5538,
            "coverage": Decimal("1.0"),
            "published_at": datetime.now(),
            "source_generated_at": datetime.now(),
            "capture_mode": "OFF_SESSION_SNAPSHOT",
        }
        wrong_day = {"total": 5538, "today_count": 0, "data_time": "2026-08-05 15:00:00"}
        with patch("server.api.routers.hot_data.get_current_engine", return_value=engine), \
             patch("server.api.routers.hot_data._read_sql", side_effect=[[wrong_day], [archive]]):
            self.assertIsNone(hot_data._get_realtime_overview(allow_close=True))

    def test_monitor_realtime_snapshot_is_labelled_as_today(self):
        daily_row = {
            "up_cnt": 2400,
            "down_cnt": 2800,
            "sideline_cnt": 2100,
            "total": 5400,
            "total_amount": 1_500_000_000_000,
            "small_up_cnt": 900,
            "small_total": 2200,
            "small_avg_chg": -0.2,
        }
        realtime_row = {
            "up_cnt": 3000,
            "down_cnt": 2300,
            "sideline_cnt": 2000,
            "total": 5500,
            "total_amount": 1_800_000_000_000,
            "small_up_cnt": 1300,
            "small_total": 2300,
            "small_avg_chg": 0.8,
            "data_time": "2026-06-26 13:29:55",
            "data_source": "sm_stock_current",
        }
        overview_map = {
            "2026-06-24": dict(daily_row),
            "2026-06-25": dict(daily_row),
        }
        with patch("server.api.routers.hot_data.datetime", _FakeIntradayDatetime), \
             patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._cache_set"), \
             patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=True), \
             patch("server.api.routers.hot_data._get_realtime_overview", return_value=realtime_row), \
             patch("server.api.routers.hot_data._monitor_resolve_trade_date", return_value="2026-06-25"), \
             patch("server.api.routers.hot_data._monitor_history_trade_dates", return_value=["2026-06-24", "2026-06-25"]), \
             patch("server.api.routers.hot_data._monitor_overview_map", return_value=overview_map), \
             patch("server.api.routers.hot_data._monitor_hot_rows_map", return_value={}), \
             patch("server.api.routers.hot_data._monitor_index_price_map", return_value={}), \
             patch("server.api.routers.hot_data._read_sql", return_value=[]):
            out = hot_data.monitor_data("2026-06-26")

        self.assertEqual(out["trade_date"], "2026-06-26")
        self.assertTrue(out["is_realtime"])
        self.assertEqual(out["total_count"], 5500)
        self.assertEqual(out["flat_count"], 200)
        self.assertEqual(out["data_time"], "2026-06-26 13:29:55")
        self.assertEqual(out["history"]["dates"][-1], "06-26")

    def test_monitor_keeps_verified_current_snapshot_during_lunch_pause(self):
        daily_row = {
            "up_cnt": 2400, "down_cnt": 2800, "sideline_cnt": 2100,
            "total": 5400, "total_amount": 1_500_000_000_000,
            "small_up_cnt": 900, "small_total": 2200, "small_avg_chg": -0.2,
        }
        current_row = {
            "up_cnt": 2900, "down_cnt": 2400, "sideline_cnt": 2050,
            "total": 5500, "total_amount": 1_700_000_000_000,
            "small_up_cnt": 1250, "small_total": 2300, "small_avg_chg": 0.5,
            "data_time": "2026-06-26 11:30:05", "data_source": "sm_stock_current",
        }
        with patch("server.api.routers.hot_data.datetime", _FakeLunchDatetime), \
             patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._cache_set"), \
             patch("server.api.routers.hot_data._portfolio_is_trading_day", return_value=True), \
             patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=False), \
             patch("server.api.routers.hot_data._get_realtime_overview", return_value=current_row) as current_mock, \
             patch("server.api.routers.hot_data._monitor_resolve_trade_date", return_value="2026-06-25"), \
             patch("server.api.routers.hot_data._monitor_history_trade_dates", return_value=["2026-06-24", "2026-06-25"]), \
             patch("server.api.routers.hot_data._monitor_overview_map", return_value={"2026-06-24": daily_row, "2026-06-25": daily_row}), \
             patch("server.api.routers.hot_data._monitor_hot_rows_map", return_value={}), \
             patch("server.api.routers.hot_data._monitor_index_price_map", return_value={}):
            out = hot_data.monitor_data("2026-06-26")

        current_mock.assert_called_once_with(allow_close=True)
        self.assertEqual(out["trade_date"], "2026-06-26")
        self.assertFalse(out["is_realtime"])
        self.assertEqual(out["freshness_status"], "paused")
        self.assertEqual(out["data_time"], "2026-06-26 11:30:05")

    def test_monitor_hot_rows_map_prefers_qmt_plate_aggregate_for_current_date(self):
        fallback_rows = [
            {"snapshot_date": "2026-06-30", "plate_type": 1, "concept_name": "FallbackConcept", "hot_value": 999},
            {"snapshot_date": "2026-06-30", "plate_type": 3, "concept_name": "FallbackIndustry", "hot_value": 999},
        ]
        kline_rows = [
            {"stock_code": "000001", "trade_date": "2026-06-30", "amount": 100_000_000, "change_pct": 2.0},
            {"stock_code": "000002", "trade_date": "2026-06-30", "amount": 300_000_000, "change_pct": 4.0},
        ]
        plate_rows = [
            {"stock_code": "000001", "plate_type": "\u6982\u5ff5", "plate_name": "QmtConcept"},
            {"stock_code": "000002", "plate_type": "\u6982\u5ff5", "plate_name": "QmtConcept"},
            {"stock_code": "000001", "plate_type": "\u884c\u4e1a", "plate_name": "QmtIndustry"},
            {"stock_code": "000002", "plate_type": "\u884c\u4e1a", "plate_name": "QmtIndustry"},
        ]
        with patch("server.api.routers.hot_data._read_sql", side_effect=[fallback_rows, kline_rows, plate_rows]):
            out = hot_data._monitor_hot_rows_map(["2026-06-30"])

        concept = out[("2026-06-30", 1)][0]
        industry = out[("2026-06-30", 3)][0]
        self.assertEqual(concept["concept_name"], "QmtConcept")
        self.assertEqual(industry["concept_name"], "QmtIndustry")
        self.assertEqual(concept["data_source"], "qmt_plate_aggregate")
        self.assertEqual(concept["hot_value"], 4.0)
        self.assertEqual(concept["change_pct"], 3.5)

    def test_recommended_progress_uses_long_ttl(self):
        with patch("server.api.routers.hot_data._recommended_run_history_reconcile_scheduler_terminal"), \
             patch("server.api.routers.hot_data._recommended_history_progress", return_value=None), \
             patch("server.api.routers.hot_data._cache_get", return_value={"status": "done"}) as cache_get_mock, \
             patch("server.api.routers.hot_data._job_is_running", return_value=False):
            out = hot_data.recommended_stocks_progress()

        cache_get_mock.assert_called_once_with("rec_screen_progress", ttl_seconds=7200)
        self.assertEqual(out["status"], "done")

    def test_recommended_progress_prefers_running_history_over_queued_cache(self):
        cached = {"status": "queued", "percent": 5, "run_uid": "run-1", "step": "queued"}
        history = {
            "status": "running",
            "percent": 32,
            "run_uid": "run-1",
            "step": "loading",
            "is_running": True,
        }
        with patch("server.api.routers.hot_data._recommended_run_history_reconcile_scheduler_terminal"), \
             patch("server.api.routers.hot_data._cache_get", return_value=cached), \
             patch("server.api.routers.hot_data._recommended_history_progress", return_value=history), \
             patch("server.api.routers.hot_data._job_is_running", return_value=False):
            out = hot_data.recommended_stocks_progress()

        self.assertEqual(out["status"], "running")
        self.assertEqual(out["percent"], 32)
        self.assertEqual(out["step"], "loading")

    def test_legacy_recommendation_worker_paths_are_physically_removed(self):
        self.assertFalse(hasattr(hot_data, "_start_recommended_offline_process"))
        self.assertFalse(hasattr(hot_data, "_run_recommended_batch_in_process"))
        self.assertFalse(hasattr(hot_data, "_queued_recommended_run_response"))
        self.assertFalse(hasattr(hot_data, "_start_recommended_queue_worker"))

    def test_recommended_run_context_auto_intraday_uses_current_trade_date(self):
        with patch("server.api.routers.hot_data._market_clock_trade_date_from_calendar", return_value="2026-07-08"), \
             patch("server.api.routers.hot_data.get_engine", return_value=object()):
            out = hot_data._resolve_recommended_run_context(
                trade_date="2026-07-06",
                execution_time="2026-07-08 10:20:00",
                refresh_realtime=False,
                date_policy="auto",
            )

        self.assertEqual(out["trade_date"], "2026-07-08")
        self.assertEqual(out["date_source"], "intraday_current")
        self.assertFalse(out["strict_prev_trade_day"])
        self.assertTrue(out["refresh_realtime"])
        self.assertTrue(out["use_intraday_current"])
        self.assertEqual(out["data_cutoff_time"], "2026-07-08 10:20:00")

    def test_recommended_run_context_auto_closed_uses_previous_trade_day(self):
        with patch("server.api.routers.hot_data.datetime", _FakePremarketDatetime), \
             patch("server.api.routers.hot_data._market_clock_trade_date_from_calendar", return_value="2026-07-08"), \
             patch("server.api.routers.hot_data.get_engine", return_value=object()), \
             patch("biz.analysis.sync_analysis_fast.previous_trade_date", return_value="2026-07-07") as previous_mock:
            out = hot_data._resolve_recommended_run_context(
                trade_date="2026-07-08",
                refresh_realtime=True,
                date_policy="auto",
            )

        self.assertEqual(out["trade_date"], "2026-07-07")
        self.assertEqual(out["date_source"], "previous_trade_day")
        self.assertTrue(out["strict_prev_trade_day"])
        self.assertFalse(out["refresh_realtime"])
        self.assertFalse(out["use_intraday_current"])
        previous_mock.assert_called_once()

    def test_old_inline_env_cannot_restore_retired_recommendation_paths(self):
        with patch.dict(
            os.environ,
            {"PROBIGA_ALLOW_INLINE_AI_RECOMMENDATION_RUN": "1"},
        ), patch(
            "server.api.routers.hot_data._submit_manual_recommended_stocks",
            return_value={"accepted": False, "status": "strict_gate_blocked"},
        ) as submit_mock:
            out = hot_data.run_recommended_stocks(
                trade_date="2026-07-06",
                execution_time="2026-07-08 10:20:00",
            )

        self.assertEqual(out["status"], "strict_gate_blocked")
        submit_mock.assert_called_once()

    def test_recommended_run_history_endpoint_returns_rows(self):
        rows = [{
            "run_uid": "abc",
            "trade_date": "2026-06-28",
            "status": "done",
            "passed": 12,
            "total": 100,
        }]
        with patch("server.api.routers.hot_data._recommended_run_history_reconcile_scheduler_terminal"), \
             patch("server.api.routers.hot_data._ensure_recommended_run_history_table"), \
             patch("server.api.routers.hot_data._read_sql", return_value=rows):
            out = hot_data.recommended_stocks_run_history(limit=5)

        self.assertEqual(out["total"], 1)
        self.assertEqual(out["status"], "READY")
        self.assertEqual(out["data"][0]["run_uid"], "abc")

    def test_recommended_run_history_endpoint_never_masks_schema_failure_as_empty(self):
        with patch(
            "server.api.routers.hot_data._recommended_run_history_reconcile_scheduler_terminal"
        ), patch(
            "server.api.routers.hot_data._ensure_recommended_run_history_table",
            side_effect=RuntimeError("missing physical column"),
        ):
            out = hot_data.recommended_stocks_run_history(limit=5)

        self.assertEqual(out["status"], "DATA_UNAVAILABLE")
        self.assertEqual(out["error_code"], "recommended_run_history_unavailable")
        self.assertEqual(out["data"], [])
        self.assertNotIn("missing physical column", str(out))

    def test_recommended_run_history_start_and_finish_write_sql(self):
        build_sha = "c" * 40
        terminal_row = [{
            "run_uid": "d" * 32,
            "status": "done",
            "scheduler_job_id": None,
            "build_sha": build_sha,
            "trigger_source": "scheduled",
            "finished_at": "2026-06-28 09:01:00",
        }]
        with patch("server.api.routers.hot_data._ensure_recommended_run_history_table"), \
             patch("server.api.routers.hot_data._exec_sql", return_value=1) as exec_mock, \
             patch("server.api.routers.hot_data._read_sql", return_value=terminal_row), \
             patch("server.api.routers.hot_data._recommendation_build_sha", return_value=build_sha):
            run_uid = hot_data._recommended_run_history_start(
                trade_date="2026-06-28",
                min_score=50,
                top_n=80,
                strict_prev_trade_day=True,
                execution_time="2026-06-28 09:00:00",
                message="start",
                run_uid="d" * 32,
            )
            hot_data._recommended_run_history_finish(run_uid, status="done", payload={
                "total": 100,
                "passed": 12,
                "message": "ok",
            })

        self.assertTrue(run_uid)
        self.assertEqual(exec_mock.call_count, 2)
        self.assertEqual(exec_mock.call_args_list[0].args[1]["trade_date"], "2026-06-28")
        self.assertEqual(exec_mock.call_args_list[1].args[1]["status"], "done")

    def test_startup_screen_applies_requested_range_volume_and_full_a_share_universe(self):
        captured = {}

        def fake_read(sql, params=None):
            if "FROM sm_stock_kline t" in sql and "t.volume > base.avg_vol" in sql:
                captured["sql"] = sql
                captured["params"] = dict(params or {})
                return [{"stock_code": "920001", "short_name": "北证样本", "change_pct": 5.0}]
            return []

        with patch("server.api.routers.hot_data._latest_date_not_after", return_value="2026-08-07"), \
             patch("server.api.routers.hot_data._read_sql", side_effect=fake_read), \
             patch("server.api.routers.hot_data._compute_indicators", return_value={}), \
             patch("server.api.routers.hot_data._news_counts_for_codes", return_value={}):
            out = hot_data.screen_stocks(
                mode="startup",
                trade_date="2026-08-07",
                top=123,
                min_change=1.2,
                max_change=8.5,
                vol_boost=1.7,
            )

        self.assertEqual(out["total"], 1)
        self.assertIn("^(00|30|60|68|92)[0-9]{4}$", captured["sql"])
        self.assertIn("t.volume > base.avg_vol * :vboost", captured["sql"])
        self.assertIn("t.change_pct >= :cmin AND t.change_pct <= :cmax", captured["sql"])
        self.assertEqual(captured["params"]["vboost"], 1.7)
        self.assertEqual(captured["params"]["cmin"], 1.2)
        self.assertEqual(captured["params"]["cmax"], 8.5)
        self.assertEqual(captured["params"]["lim"], 123)

    def test_lhb_screen_exposes_net_buy_and_net_sell_direction(self):
        with patch("server.api.routers.hot_data._latest_date_not_after", return_value="2026-08-07"), \
             patch(
                 "server.api.routers.hot_data._read_sql",
                 side_effect=[
                     [{"exists": 1}],
                     [
                         {"stock_code": "600001", "a_net_amount": 100},
                         {"stock_code": "600002", "a_net_amount": -50},
                     ],
                 ],
             ):
            out = hot_data.screen_stocks(mode="lhb", trade_date="2026-08-07", top=20)

        self.assertEqual([row["lhb_direction"] for row in out["data"]], ["NET_BUY", "NET_SELL"])
        self.assertEqual([row["lhb_direction_score"] for row in out["data"]], [100.0, 0.0])


if __name__ == "__main__":
    unittest.main()
