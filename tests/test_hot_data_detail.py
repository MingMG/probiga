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
             patch("server.api.routers.hot_data._generate_ai_analysis", return_value={"score": 88, "analysis_date": "2026-06-10"}) as analysis_mock:
            out = hot_data.stock_detail("1")

        payload_mock.assert_called_once_with("000001", mode="post_close", light=True)
        snapshot_mock.assert_called_once_with("000001", trade_date="2026-06-13")
        analysis_mock.assert_called_once()
        self.assertEqual(analysis_mock.call_args.kwargs["analysis_snapshot"], snapshot)
        self.assertTrue(analysis_mock.call_args.kwargs["prefer_snapshot"])
        self.assertEqual(out["stock_code"], "000001")
        self.assertEqual(out["analysis_snapshot"]["recommend_status"], "ALLOW")
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

    def test_mainforce_analysis_uses_snapshot_latest_date_by_default(self):
        result = {"stock_code": "000001", "score": 61.5}

        with patch("server.api.routers.hot_data._latest_date", return_value="2026-05-29") as latest_mock, \
             patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._compute_mainforce_behavior_fast", return_value=result) as compute_mock, \
             patch("server.api.routers.hot_data._cache_set"):
            out = hot_data.mainforce_analysis("000001")

        latest_mock.assert_called_once_with("sm_stock_snapshot")
        compute_mock.assert_called_once_with("000001", "2026-05-29")
        self.assertEqual(out, result)

    def test_recommended_stocks_returns_cached_payload(self):
        cached = {"date": "2026-06-13", "data": [{"stock_code": "000001"}], "total": 1}

        with patch("server.api.routers.hot_data._cache_get", return_value=cached) as cache_get_mock, \
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

    def test_latest_date_not_after_uses_nearest_available_row(self):
        with patch("server.api.routers.hot_data._cache_get", return_value=None), \
             patch("server.api.routers.hot_data._read_sql", return_value=[{"d": "2026-06-17"}]) as read_sql_mock, \
             patch("server.api.routers.hot_data._cache_set") as cache_set_mock:
            out = hot_data._latest_date_not_after("st_recommended_stocks", "2026-06-21", "pick_date")

        sql, params = read_sql_mock.call_args.args
        self.assertIn("pick_date <= :d", sql)
        self.assertEqual(params["d"], "2026-06-21")
        cache_set_mock.assert_called_once_with(
            "latest_date_not_after_st_recommended_stocks_pick_date_2026-06-21",
            "2026-06-17",
        )
        self.assertEqual(out, "2026-06-17")

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
        }

        out = hot_data._portfolio_build_watch_analysis(row)

        self.assertEqual(out["funds"], "强出")
        self.assertEqual(out["funds_level"], "strong_out")
        self.assertEqual(out["operation_advice"], "控仓")
        self.assertIn("资金外流", out["risk_tip"])

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
