# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from decimal import Decimal
import os
import unittest
from unittest.mock import MagicMock, patch

from server.api.routers import hot_data


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


class HotDataDetailHelperTest(unittest.TestCase):
    def _clear_hot_data_cache(self):
        with hot_data._cache_lock:
            hot_data._cache_store.clear()
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

    def test_recommended_stocks_returns_cached_payload(self):
        cached = {"date": "2026-06-13", "data": [{"stock_code": "000001"}], "total": 1}

        with patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=False), \
             patch("server.api.routers.hot_data._cache_get", return_value=cached) as cache_get_mock, \
             patch("server.api.routers.hot_data._recommended_stocks_v2") as query_mock:
            out = hot_data.recommended_stocks("2026-06-13", "main_wave", "WATCH")

        cache_get_mock.assert_called_once_with(
            "recommended_stocks_2026-06-13_main_wave_WATCH",
            ttl_seconds=300,
        )
        query_mock.assert_not_called()
        self.assertEqual(out, cached)

    def test_recommended_stocks_caches_query_result(self):
        result = {"date": "2026-06-13", "data": [{"stock_code": "000001"}], "total": 1}

        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._recommended_stocks_v2", return_value=result) as query_mock, \
             patch("server.api.routers.hot_data._cache_set") as cache_set_mock:
            out = hot_data.recommended_stocks("2026-06-13", "", "")

        query_mock.assert_called_once_with("2026-06-13", "", "")
        cache_set_mock.assert_called_once_with("recommended_stocks_2026-06-13_all_all", result)
        self.assertEqual(out, result)

    def test_recommended_stocks_explicit_date_does_not_fallback_to_previous_pick_date(self):
        with patch("server.api.routers.hot_data._table_columns", return_value={"stock_code", "pick_date", "ai_score"}), \
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

    def test_portfolio_live_force_rebuilds_shared_snapshot(self):
        snapshot = {"data": [{"stock_code": "000001"}], "total": 1, "summary": {"holding_count": 1}}

        with patch("server.api.routers.hot_data._get_portfolio_snapshot", return_value={**snapshot, "live": True}) as snapshot_mock:
            out = hot_data.portfolio_live(force=True)

        snapshot_mock.assert_called_once_with(live_mode=True, force_live=True)
        self.assertTrue(out["live"])
        self.assertEqual(out["total"], 1)

    def test_live_quotes_from_current_table_can_return_stale_rows(self):
        snapshot_at = (hot_data.datetime.now() - timedelta(seconds=120)).strftime("%Y-%m-%d %H:%M:%S")
        with patch("server.api.routers.hot_data._read_sql", return_value=[{
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

    def test_portfolio_daily_flow_attitude_returns_neutral_label_for_zero_flow(self):
        out = hot_data._portfolio_daily_flow_attitude(0)

        self.assertEqual(out["level"], "neutral")
        self.assertTrue(out["label"])

    def test_portfolio_refresh_prices_queues_qmt_without_blocking(self):
        with patch("server.api.routers.hot_data._read_sql", return_value=[{"stock_code": "000001"}]), \
             patch("server.api.routers.hot_data._queue_portfolio_qmt_refresh", return_value={
                 "state": "queued", "refreshed": 0,
             }) as queue_mock:
            out = hot_data.portfolio_refresh_prices()

        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["state"], "queued")
        self.assertEqual(out["refreshed"], 0)
        queue_mock.assert_called_once_with(["000001"])

    def test_portfolio_refresh_degraded_state_explains_coverage(self):
        quotes = {
            "000001": {"quote_status": "fresh"},
            "000002": {"quote_status": "stale"},
        }
        with hot_data._portfolio_qmt_refresh_lock:
            hot_data._portfolio_qmt_refresh_thread = None
            hot_data._portfolio_qmt_refresh_state = {
                "state": "idle",
                "started_at": "",
                "finished_at": "",
                "requested": 0,
                "refreshed": 0,
                "stale": 0,
                "missing": 0,
                "error": "",
            }

        with patch("server.api.routers.hot_data._portfolio_fetch_live_quotes", return_value=quotes) as fetch, \
             patch("server.api.routers.hot_data._invalidate_portfolio_snapshot_cache"):
            hot_data._queue_portfolio_qmt_refresh(["000001", "000002", "000003"])
            hot_data._portfolio_qmt_refresh_thread.join(timeout=1)

        fetch.assert_called_once_with(
            ["000001", "000002", "000003"],
            force=True,
            allow_remote=True,
        )
        with hot_data._portfolio_qmt_refresh_lock:
            state = dict(hot_data._portfolio_qmt_refresh_state)
        self.assertEqual(state["state"], "degraded")
        self.assertEqual(state["requested"], 3)
        self.assertEqual(state["refreshed"], 1)
        self.assertEqual(state["stale"], 1)
        self.assertEqual(state["missing"], 1)
        self.assertEqual(
            state["error"],
            "fresh quote coverage 1/3; stale=1; missing=1",
        )

    def test_portfolio_live_quote_read_does_not_enter_qmt_on_cache_miss(self):
        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._cache_set"), \
             patch("server.api.routers.hot_data._live_quotes_from_current_table", return_value={}), \
             patch("tools.sync_qmt_realtime.sync_qmt_realtime") as sync_mock:
            out = hot_data._portfolio_fetch_live_quotes(["000001"])

        self.assertEqual(out, {})
        sync_mock.assert_not_called()

    def test_portfolio_live_quote_falls_back_to_sina_when_big_qmt_fails(self):
        fresh_quote = {
            "000001": {
                "stock_code": "000001",
                "price": 12.34,
                "quote_status": "fresh",
            }
        }
        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._cache_set"), \
             patch("server.api.routers.hot_data._live_quotes_from_current_table", side_effect=[{}, fresh_quote]), \
             patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=True), \
             patch("integrations.registry.resolve_source", return_value="bigqmt"), \
             patch("server.api.routers.hot_data.get_current_engine") as current_engine_mock, \
             patch("tools.run_big_qmt_bridge.sync_big_qmt_realtime", side_effect=RuntimeError("QMT unavailable")) as qmt_mock, \
             patch("tools.sync_market_realtime.sync_market_realtime") as sina_mock:
            out = hot_data._portfolio_fetch_live_quotes(["000001"], force=True, allow_remote=True)

        qmt_mock.assert_called_once()
        current_engine = current_engine_mock.return_value
        sina_mock.assert_called_once_with(
            engine=current_engine,
            codes=["000001"],
            source="sina",
            archive_snapshot=False,
            run_rt_ddl=False,
            skip_closed=False,
            min_coverage=0.0,
            replace_scope="subset",
        )
        self.assertEqual(out, fresh_quote)

    def test_portfolio_live_snapshot_has_positive_intraday_ttl(self):
        with patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=True):
            self.assertEqual(hot_data._portfolio_snapshot_ttl_seconds(True), 1)

    def test_stock_detail_uses_short_intraday_cache(self):
        cached = {"stock_code": "000001", "cached": True}
        with patch("server.api.routers.hot_data._portfolio_market_mode", return_value="intraday"), \
             patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=True), \
             patch("server.api.routers.hot_data._cache_get", return_value=cached) as cache_get_mock:
            out = hot_data.stock_detail("000001")

        self.assertIs(out, cached)
        cache_get_mock.assert_called_once_with("stock_detail_000001_intraday", ttl_seconds=12)

    def test_recommended_stocks_uses_bounded_intraday_cache(self):
        cached = {"data": [], "total": 0}
        with patch("server.api.routers.hot_data._is_monitor_trading_time", return_value=True), \
             patch("server.api.routers.hot_data._cache_get", return_value=cached) as cache_get_mock:
            out = hot_data.recommended_stocks(trade_date="2026-06-28", strategy="", signal_status="")

        self.assertIs(out, cached)
        cache_get_mock.assert_called_once_with(
            "recommended_stocks_2026-06-28_all_all",
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
        with patch("server.api.routers.hot_data._cache_get", return_value={"status": "done"}) as cache_get_mock, \
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
        with patch("server.api.routers.hot_data._recommended_run_history_expire_stale"), \
             patch("server.api.routers.hot_data._cache_get", return_value=cached), \
             patch("server.api.routers.hot_data._recommended_history_progress", return_value=history), \
             patch("server.api.routers.hot_data._job_is_running", return_value=False):
            out = hot_data.recommended_stocks_progress()

        self.assertEqual(out["status"], "running")
        self.assertEqual(out["percent"], 32)
        self.assertEqual(out["step"], "loading")

    def test_recommended_offline_and_thread_paths_preserve_exact_intraday_cutoff(self):
        from biz.analysis.sync_analysis_fast import BatchStats

        cutoff = "2026-07-08 10:20:00"
        with patch(
            "server.api.routers.hot_data.get_engine", return_value=object()
        ), patch(
            "server.api.routers.hot_data.build_child_env", return_value={}
        ), patch(
            "server.api.scheduler_runtime.start_detached_python_job",
            return_value={"pid": 123},
        ) as start_mock:
            hot_data._start_recommended_offline_process(
                run_uid="run-1",
                trade_date="2026-07-08",
                min_score=62,
                top_n=80,
                strict_prev_trade_day=False,
                execution_time=cutoff,
                min_kline_coverage=0.8,
                auto_repair_missing_kline=False,
                refresh_realtime=True,
                use_intraday_current=True,
            )

        cmd = start_mock.call_args.kwargs["cmd"]
        self.assertIn("--use-intraday-current", cmd)
        self.assertEqual(cmd[cmd.index("--execution-time") + 1], cutoff)

        stats = BatchStats("2026-07-08", 1, 1, 50.0, "2026-07-08", "2026-07-08")
        with patch(
            "biz.analysis.sync_analysis_fast.run_batch", return_value=stats
        ) as run_mock:
            hot_data._run_recommended_batch_in_process(
                engine=object(),
                trade_date="2026-07-08",
                top_n=80,
                min_score=62,
                progress_callback=None,
                strict_prev_trade_day=False,
                execution_time=cutoff,
                min_kline_coverage=0.8,
                auto_repair_missing_kline=False,
                use_intraday_current=True,
            )

        self.assertEqual(run_mock.call_args.kwargs["execution_time"], cutoff)
        self.assertTrue(run_mock.call_args.kwargs["use_intraday_current"])

    def test_recommended_queued_response_autostarts_worker(self):
        with patch("server.api.routers.hot_data._recommended_run_history_start", return_value="run-1"), \
             patch("server.api.routers.hot_data._recommended_run_history_update") as update_mock, \
             patch("server.api.routers.hot_data._web_ai_recommendation_queue_autostart_enabled", return_value=True), \
             patch("server.api.routers.hot_data._start_recommended_queue_worker", return_value={"pid": 123}) as start_mock, \
             patch("server.api.routers.hot_data._cache_set") as cache_mock:
            out = hot_data._queued_recommended_run_response(
                trade_date="2026-07-06",
                min_score=50,
                top_n=80,
                strict_prev_trade_day=False,
                execution_time="2026-07-08 08:52:59",
                refresh_realtime=True,
            )

        self.assertEqual(out["status"], "queued")
        self.assertEqual(out["worker"], {"pid": 123})
        self.assertTrue(out["progress"]["is_running"])
        start_mock.assert_called_once_with(run_uid="run-1", refresh_realtime=True)
        update_mock.assert_called()
        cache_mock.assert_called_once()

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

    def test_recommended_run_queues_even_when_old_inline_env_enabled(self):
        run_context = {
            "trade_date": "2026-07-08",
            "strict_prev_trade_day": False,
            "execution_time": "2026-07-08 10:20:00",
            "refresh_realtime": True,
            "use_intraday_current": True,
            "date_policy": "auto",
            "date_source": "intraday_current",
        }
        with patch.dict(os.environ, {"PROBIGA_ALLOW_INLINE_AI_RECOMMENDATION_RUN": "1"}), \
             patch("server.api.routers.hot_data._resolve_recommended_run_context", return_value=run_context), \
             patch("server.api.routers.hot_data._recommended_run_history_expire_stale"), \
             patch("server.api.routers.hot_data._active_recommended_run", return_value=None), \
             patch("server.api.routers.hot_data._web_ai_recommendation_queue_enabled", return_value=True), \
             patch("server.api.routers.hot_data._queued_recommended_run_response", return_value={"status": "queued"}) as queue_mock:
            out = hot_data.run_recommended_stocks(
                trade_date="2026-07-06",
                execution_time="2026-07-08 10:20:00",
            )

        self.assertEqual(out["status"], "queued")
        queue_mock.assert_called_once()
        self.assertEqual(queue_mock.call_args.kwargs["trade_date"], "2026-07-08")
        self.assertTrue(queue_mock.call_args.kwargs["refresh_realtime"])
        self.assertEqual(queue_mock.call_args.kwargs["run_context"], run_context)

    def test_queued_recommended_worker_autostart_is_throttled(self):
        progress = {
            "status": "queued",
            "run_uid": "run-1",
            "trade_date": "2026-07-09",
            "execution_time": "2026-07-09 10:20:00",
            "strict_prev_trade_day": False,
        }
        with patch("server.api.routers.hot_data._web_ai_recommendation_queue_autostart_enabled", return_value=True), \
             patch("server.api.routers.hot_data._cache_get", return_value=None) as cache_get_mock, \
             patch("server.api.routers.hot_data._cache_set") as cache_set_mock, \
             patch("server.api.routers.hot_data._start_recommended_queue_worker", return_value={"pid": 123}) as start_mock, \
             patch("server.api.routers.hot_data._recommended_run_history_update") as update_mock:
            hot_data._maybe_autostart_queued_recommended_worker(progress)

        cache_get_mock.assert_called_once()
        cache_set_mock.assert_called_once()
        start_mock.assert_called_once_with(run_uid="run-1", refresh_realtime=True)
        update_mock.assert_called_once()
        self.assertEqual(progress["worker"], {"pid": 123})

    def test_recommended_run_history_endpoint_returns_rows(self):
        rows = [{
            "run_uid": "abc",
            "trade_date": "2026-06-28",
            "status": "done",
            "passed": 12,
            "total": 100,
        }]
        with patch("server.api.routers.hot_data._ensure_recommended_run_history_table"), \
             patch("server.api.routers.hot_data._read_sql", return_value=rows):
            out = hot_data.recommended_stocks_run_history(limit=5)

        self.assertEqual(out["total"], 1)
        self.assertEqual(out["data"][0]["run_uid"], "abc")

    def test_recommended_run_history_start_and_finish_write_sql(self):
        with patch("server.api.routers.hot_data._ensure_recommended_run_history_table"), \
             patch("server.api.routers.hot_data._exec_sql") as exec_mock:
            run_uid = hot_data._recommended_run_history_start(
                trade_date="2026-06-28",
                min_score=50,
                top_n=80,
                strict_prev_trade_day=True,
                execution_time="2026-06-28 09:00:00",
                message="start",
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


if __name__ == "__main__":
    unittest.main()
