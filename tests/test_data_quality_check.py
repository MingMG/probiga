# -*- coding: utf-8 -*-
import unittest
from datetime import datetime
from unittest.mock import patch

from tools import data_quality_check
from tools.data_quality_check import (
    CheckResult,
    _date_lag_days,
    _expected_intraday_stock_count,
    _scheduler_bad_tasks,
    _scheduler_health_status,
    check_concept_data_freshness,
    check_analysis_outputs,
    check_index_data_freshness,
    check_intraday_foundation,
    check_intraday_scheduler_health,
    check_latest_trade_date_freshness,
    check_realtime_freshness,
    check_schema_collation,
    _status,
    expected_completed_trade_date,
    check_kline_integrity,
    check_recent_kline_calendar_completeness,
    expected_intraday_date,
    expected_scheduled_trade_date,
    intraday_readiness,
    is_intraday_session,
    run_checks,
)


class DataQualityCheckTest(unittest.TestCase):
    def test_status_helper(self):
        self.assertEqual(_status(True), "PASS")
        self.assertEqual(_status(True, warn=True), "WARN")
        self.assertEqual(_status(False), "FAIL")

    def test_date_lag_days(self):
        self.assertEqual(_date_lag_days("2026-06-10", "2026-06-12"), 2)
        self.assertEqual(_date_lag_days("", "2026-06-12"), 9999)

    def test_completed_trade_date_uses_previous_day_before_daily_ready_time(self):
        with patch("tools.data_quality_check._scalar", return_value="2026-06-12") as scalar:
            actual = expected_completed_trade_date(
                object(),
                now=datetime(2026, 6, 15, 11, 30),
            )

        self.assertEqual(actual, "2026-06-12")
        self.assertIn("trade_date < :today", scalar.call_args.args[1])

    def test_completed_trade_date_uses_today_after_daily_ready_time(self):
        with patch("tools.data_quality_check._scalar", return_value="2026-06-15") as scalar:
            actual = expected_completed_trade_date(
                object(),
                now=datetime(2026, 6, 15, 15, 20),
            )

        self.assertEqual(actual, "2026-06-15")
        self.assertIn("trade_date <= :today", scalar.call_args.args[1])

    def test_daily_freshness_accepts_previous_trade_day_intraday(self):
        with patch(
            "tools.data_quality_check.expected_completed_trade_date",
            return_value="2026-06-12",
        ):
            result = check_latest_trade_date_freshness(
                object(),
                "2026-06-12",
                now=datetime(2026, 6, 15, 11, 30),
            )

        self.assertEqual(result.status, "PASS")
    def test_latest_trade_date_ahead_of_conservative_expectation_passes(self):
        with patch(
            "tools.data_quality_check.expected_completed_trade_date",
            return_value="2026-08-07",
        ):
            result = check_latest_trade_date_freshness(
                object(),
                "2026-08-10",
                now=datetime(2026, 8, 10, 19, 0),
            )

        self.assertEqual(result.status, "PASS")
        self.assertEqual(result.details["lag_calendar_days"], -3)
        self.assertEqual(result.details["ahead_calendar_days"], 3)
        self.assertIn("领先", result.message)

    def test_latest_trade_date_behind_expectation_fails(self):
        with patch(
            "tools.data_quality_check.expected_completed_trade_date",
            return_value="2026-08-10",
        ):
            result = check_latest_trade_date_freshness(
                object(),
                "2026-08-07",
                now=datetime(2026, 8, 10, 19, 0),
            )

        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.details["lag_calendar_days"], 3)

    def test_check_result_as_dict(self):
        result = CheckResult("sample", "PASS", "ok", {"x": 1})
        self.assertEqual(result.as_dict(), {
            "name": "sample",
            "status": "PASS",
            "message": "ok",
            "details": {"x": 1},
        })

    def test_scheduler_health_status_warns_on_bad_tasks(self):
        self.assertEqual(_scheduler_health_status([]), "PASS")
        self.assertEqual(_scheduler_health_status([{"task_name": "x"}]), "WARN")

    def test_scheduler_bad_tasks_excludes_quality_check_tasks(self):
        captured = {}

        def fake_rows(engine, sql, params=None):
            captured["sql"] = sql
            captured["params"] = params or {}
            return []

        with patch("tools.data_quality_check._rows", side_effect=fake_rows):
            self.assertEqual(_scheduler_bad_tasks(object()), [])

        self.assertIn("task_type NOT IN", captured["sql"])
        values = set(captured["params"].values())
        self.assertIn("quality_check_pre", values)
        self.assertIn("quality_check_post", values)
        self.assertIn("intraday_quality_check", values)

    def test_scheduler_bad_tasks_detects_overdue_successful_cron(self):
        rows = [{
            "task_name": "daily index sync",
            "task_type": "index_daily",
            "script_path": "tools/sync.py",
            "cron_time": "04:50:00",
            "interval_minutes": 0,
            "last_run_status": "success",
            "last_run_at": datetime(2026, 8, 9, 4, 51),
            "last_triggered_at": datetime(2026, 8, 9, 4, 50),
            "last_run_output": "ok",
            "age_minutes": 0,
        }]
        with patch("tools.data_quality_check._rows", return_value=rows), \
             patch("tools.data_quality_check.is_intraday_session", return_value=False):
            bad = _scheduler_bad_tasks(object(), now=datetime(2026, 8, 10, 5, 20))

        self.assertEqual(len(bad), 1)
        self.assertEqual(bad[0]["issue"], "overdue_cron")
        self.assertNotIn("last_run_output", bad[0])

    def test_scheduler_bad_tasks_accepts_successful_cron_completed_today(self):
        rows = [{
            "task_name": "daily index sync",
            "task_type": "index_daily",
            "script_path": "tools/sync.py",
            "cron_time": "04:50:00",
            "interval_minutes": 0,
            "last_run_status": "success",
            "last_run_at": datetime(2026, 8, 10, 4, 55),
            "last_triggered_at": datetime(2026, 8, 10, 4, 50),
            "last_run_output": "ok",
            "age_minutes": 0,
        }]
        with patch("tools.data_quality_check._rows", return_value=rows), \
             patch("tools.data_quality_check.is_intraday_session", return_value=False):
            bad = _scheduler_bad_tasks(object(), now=datetime(2026, 8, 10, 5, 20))

        self.assertEqual(bad, [])

    def test_scheduler_bad_tasks_detects_never_completed_interval_task(self):
        rows = [{
            "task_name": "realtime quotes",
            "task_type": "intraday_realtime",
            "script_path": "tools/realtime.py",
            "cron_time": None,
            "interval_minutes": 1,
            "last_run_status": None,
            "last_run_at": None,
            "last_triggered_at": None,
            "last_run_output": None,
            "age_minutes": None,
        }]
        with patch("tools.data_quality_check._rows", return_value=rows), \
             patch("tools.data_quality_check.is_intraday_session", return_value=True):
            bad = _scheduler_bad_tasks(object(), now=datetime(2026, 8, 10, 10, 0))

        self.assertEqual(bad[0]["issue"], "never_completed")

    def test_kline_integrity_warns_on_extreme_change_only(self):
        with patch("tools.data_quality_check._row", return_value={
            "total": 1,
            "null_ohlc": 0,
            "bad_ohlc": 0,
            "nonpositive_price": 0,
            "extreme_change": 1,
        }):
            result = check_kline_integrity(object(), "2026-07-01")

        self.assertEqual(result.status, "WARN")

    def test_kline_integrity_fails_on_hard_price_errors(self):
        with patch("tools.data_quality_check._row", return_value={
            "total": 1,
            "null_ohlc": 1,
            "bad_ohlc": 0,
            "nonpositive_price": 0,
            "extreme_change": 0,
        }):
            result = check_kline_integrity(object(), "2026-07-01")

        self.assertEqual(result.status, "FAIL")

    def test_recent_kline_calendar_completeness_fails_on_missing_open_day(self):
        with patch("tools.data_quality_check._rows", side_effect=[
            [
                {"trade_date": "2026-07-23"},
                {"trade_date": "2026-07-22"},
                {"trade_date": "2026-07-21"},
            ],
            [
                {"trade_date": "2026-07-22", "stock_count": 5200},
                {"trade_date": "2026-07-23", "stock_count": 5200},
            ],
        ]):
            result = check_recent_kline_calendar_completeness(
                object(),
                "2026-07-23",
                lookback=3,
            )

        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.details["missing_dates"], ["2026-07-21"])

    def test_recent_kline_calendar_completeness_passes_full_range(self):
        with patch("tools.data_quality_check._rows", side_effect=[
            [
                {"trade_date": "2026-07-23"},
                {"trade_date": "2026-07-22"},
            ],
            [
                {"trade_date": "2026-07-22", "stock_count": 5200},
                {"trade_date": "2026-07-23", "stock_count": 5200},
            ],
        ]):
            result = check_recent_kline_calendar_completeness(
                object(),
                "2026-07-23",
                lookback=2,
            )

        self.assertEqual(result.status, "PASS")

    def test_expected_intraday_date_uses_today_on_trade_day(self):
        class Now:
            hour = 10
            minute = 0

            @staticmethod
            def date():
                class Day:
                    @staticmethod
                    def isoformat():
                        return "2026-06-15"
                return Day()

        with patch("tools.data_quality_check.datetime") as mock_datetime, \
             patch("tools.data_quality_check._scalar", return_value=1):
            mock_datetime.now.return_value = Now()

            self.assertEqual(expected_intraday_date(object(), "2026-06-12"), "2026-06-15")

    def test_expected_intraday_date_falls_back_when_market_closed(self):
        class Now:
            hour = 10
            minute = 0

            @staticmethod
            def date():
                class Day:
                    @staticmethod
                    def isoformat():
                        return "2026-06-13"
                return Day()

        with patch("tools.data_quality_check.datetime") as mock_datetime, \
             patch("tools.data_quality_check._scalar", return_value=0):
            mock_datetime.now.return_value = Now()

            self.assertEqual(expected_intraday_date(object(), "2026-06-12"), "2026-06-12")

    def test_scheduled_dataset_uses_previous_trade_day_before_ready_time(self):
        with patch("tools.data_quality_check._scalar", return_value="2026-08-10") as scalar:
            result = expected_scheduled_trade_date(
                object(),
                "2026-08-10",
                ready_time="16:30",
                now=datetime(2026, 8, 11, 13, 50),
            )

        self.assertEqual(result, "2026-08-10")
        self.assertIn("trade_date < :today", scalar.call_args.args[1])

    def test_scheduled_dataset_requires_today_after_ready_time(self):
        with patch("tools.data_quality_check._scalar", return_value="2026-08-11") as scalar:
            result = expected_scheduled_trade_date(
                object(),
                "2026-08-10",
                ready_time="16:30",
                now=datetime(2026, 8, 11, 16, 31),
            )

        self.assertEqual(result, "2026-08-11")
        self.assertIn("trade_date <= :today", scalar.call_args.args[1])

    def test_is_intraday_session_requires_open_trade_day_and_time(self):
        class Now:
            hour = 10
            minute = 0

            @staticmethod
            def date():
                class Day:
                    @staticmethod
                    def isoformat():
                        return "2026-06-15"
                return Day()

        with patch("tools.data_quality_check._scalar", return_value=1):
            self.assertTrue(is_intraday_session(object(), Now()))
        with patch("tools.data_quality_check._scalar", return_value=0):
            self.assertFalse(is_intraday_session(object(), Now()))

    def test_intraday_readiness_closed_does_not_require_checks(self):
        with patch("tools.data_quality_check.latest_trade_date", return_value="2026-06-12"), \
             patch("tools.data_quality_check.expected_intraday_date", return_value="2026-06-12"), \
             patch("tools.data_quality_check.is_intraday_session", return_value=False), \
             patch("tools.data_quality_check.next_trade_date", return_value="2026-06-15"):
            result = intraday_readiness(object())

        self.assertEqual(result["status"], "CLOSED")
        self.assertFalse(result["allow_realtime_trading"])
        self.assertEqual(result["checks"], [])

    def test_intraday_readiness_not_ready_when_checks_warn(self):
        with patch("tools.data_quality_check.latest_trade_date", return_value="2026-06-15"), \
             patch("tools.data_quality_check.expected_intraday_date", return_value="2026-06-15"), \
             patch("tools.data_quality_check.is_intraday_session", return_value=True), \
             patch("tools.data_quality_check.next_trade_date", return_value="2026-06-16"), \
             patch("tools.data_quality_check.check_required_tables", return_value=CheckResult("required_tables", "PASS", "ok")), \
             patch("tools.data_quality_check.check_realtime_freshness", return_value=CheckResult("realtime_snapshot", "WARN", "stale")), \
             patch("tools.data_quality_check.check_intraday_foundation", return_value=CheckResult("intraday_foundation", "PASS", "ok")), \
             patch("tools.data_quality_check.check_intraday_scheduler_health", return_value=CheckResult("intraday_scheduler_health", "PASS", "ok")):
            result = intraday_readiness(object())

        self.assertEqual(result["status"], "NOT_READY")
        self.assertFalse(result["allow_realtime_trading"])

    def test_realtime_freshness_uses_indexable_snapshot_range(self):
        captured = {}

        def fake_row(_engine, sql, params=None):
            captured["sql"] = sql
            captured["params"] = params or {}
            return {
                "latest_snapshot": "2026-07-24 10:30:00",
                "stock_count": 5526,
            }

        with patch("tools.data_quality_check._table_exists", return_value=True), \
             patch("tools.data_quality_check.expected_intraday_date", return_value="2026-07-24"), \
             patch("tools.data_quality_check._row", side_effect=fake_row):
            result = check_realtime_freshness(object(), "2026-07-23")

        self.assertEqual(result.status, "PASS")
        self.assertNotIn("DATE(snapshot_at)", captured["sql"])
        self.assertIn("snapshot_at >= :day_start", captured["sql"])
        self.assertIn("day_end", captured["params"])

    def test_intraday_denominator_uses_latest_unadjusted_daily_universe(self):
        with patch("tools.data_quality_check._row", return_value={
            "trade_date": "2026-08-07",
            "stock_count": 5281,
        }), patch("tools.data_quality_check._scalar") as scalar:
            result = _expected_intraday_stock_count(object())

        self.assertEqual(result, (5281, "latest_unadjusted_daily_kline", "2026-08-07"))
        scalar.assert_not_called()

    def test_intraday_foundation_counts_only_current_target_day(self):
        captured_current_sql = []

        def fake_row(_engine, sql, params=None):
            if "FROM sm_stock_minute" in sql:
                return {"latest_date": "2026-08-10", "stock_count": 5200, "row_count": 5200}
            if "FROM sm_stock_current" in sql:
                captured_current_sql.append(sql)
                return {"latest_snapshot": datetime(2026, 8, 10, 10, 0), "stock_count": 5200, "row_count": 5200}
            if "FROM sm_stock_capital_flow_min" in sql:
                return {"latest_time": datetime(2026, 8, 10, 10, 0), "stock_count": 5200, "row_count": 5200}
            raise AssertionError(sql)

        with patch("tools.data_quality_check._table_exists", return_value=True), \
             patch("tools.data_quality_check._expected_intraday_stock_count", return_value=(5200, "latest_unadjusted_daily_kline", "2026-08-07")), \
             patch("tools.data_quality_check.expected_intraday_date", return_value="2026-08-10"), \
             patch("tools.data_quality_check.is_intraday_session", return_value=True), \
             patch("tools.data_quality_check._row", side_effect=fake_row), \
             patch("tools.data_quality_check._scalar", side_effect=[
                 datetime(2026, 8, 10, 10, 0),
                 datetime(2026, 8, 10, 10, 0),
                 datetime(2026, 8, 10, 10, 0),
                 5200,
             ]):
            result = check_intraday_foundation(object(), "2026-08-07")

        self.assertEqual(result.status, "PASS")
        self.assertEqual(result.details["expected_stock_count_source"], "latest_unadjusted_daily_kline")
        self.assertIn("snapshot_at >= :day_start", captured_current_sql[0])

    def test_index_freshness_reports_each_stale_dataset(self):
        with patch("tools.data_quality_check._table_exists", return_value=True), \
             patch("tools.data_quality_check.expected_scheduled_trade_date", return_value="2026-08-10"), \
             patch("tools.data_quality_check._row", return_value={"row_count": 0, "index_count": 0}), \
             patch("tools.data_quality_check._latest_day_count", side_effect=[
                 {"latest_date": "2026-08-07", "entity_count": 562, "row_count": 562},
                 {"latest_date": "2026-08-06", "entity_count": 562, "row_count": 1000},
                 {"latest_date": "2026-08-03", "entity_count": 562, "row_count": 562},
             ]):
            result = check_index_data_freshness(object(), "2026-08-07")

        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.details["failures"], [
            "index_constituent", "index_current", "index_minute", "index_kline",
        ])

    def test_concept_freshness_requires_reference_relative_coverage(self):
        with patch("tools.data_quality_check._table_exists", return_value=True), \
             patch("tools.data_quality_check.expected_scheduled_trade_date", return_value="2026-08-10"), \
             patch("tools.data_quality_check._row", return_value={
                 "concept_count": 1289,
                 "constituent_count": 46924,
                 "latest_sync": "2026-08-10 15:00:00",
             }), \
             patch("tools.data_quality_check._latest_day_count", side_effect=[
                 {"latest_date": "2026-08-10", "entity_count": 504, "row_count": 504},
                 {"latest_date": "2026-08-10", "entity_count": 1288, "row_count": 1288},
             ]), \
             patch("tools.data_quality_check._scalar", side_effect=["2026-08-10 15:00:00", 1289]):
            result = check_concept_data_freshness(object(), "2026-08-10")

        self.assertEqual(result.status, "FAIL")
        self.assertEqual(result.details["failures"], ["concept_current"])
        self.assertEqual(result.details["minimum_reference_coverage"], 0.8)
        self.assertLess(result.details["current"]["reference_coverage"], 0.4)
        self.assertGreater(result.details["kline"]["reference_coverage"], 0.99)

    def test_analysis_outputs_reports_completed_zero_candidates_from_statuses(self):
        def fake_row(_engine, sql, params=None):
            if "FROM stock_analysis_result" in sql:
                return {
                    "analysis_date": "2026-08-10",
                    "analysis_count": 5280,
                    "status_count": 5280,
                    "allow_count": 0,
                    "expected_count": 5280,
                }
            if "FROM st_recommended_stocks" in sql:
                return {
                    "latest_date": "2026-08-03",
                    "current_count": 0,
                    "latest_count": 80,
                }
            if "FROM st_recommended_run_history" in sql:
                return {}
            raise AssertionError(sql)

        with patch("tools.data_quality_check._row", side_effect=fake_row), \
             patch("tools.data_quality_check._table_exists", return_value=True):
            result = check_analysis_outputs(object(), "2026-08-10")

        self.assertEqual(result.status, "PASS")
        self.assertIn("已运行，0 候选", result.message)
        self.assertNotIn("推荐池 2026-08-03", result.message)
        self.assertEqual(result.details["recommend_date"], "2026-08-10")
        self.assertEqual(result.details["recommend_count"], 0)
        self.assertEqual(result.details["recommendation_state"], "completed_zero")
        self.assertEqual(
            result.details["recommendation_evidence"],
            "stock_analysis_result.recommend_status",
        )

    def test_analysis_outputs_prefers_completed_zero_run_history(self):
        rows = iter([
            {
                "analysis_date": "2026-08-10",
                "analysis_count": 5280,
                "status_count": 5200,
                "allow_count": 3,
                "expected_count": 5280,
            },
            {"latest_date": "2026-08-03", "current_count": 0, "latest_count": 80},
            {
                "status": "done",
                "total": 5280,
                "passed": 0,
                "finished_at": "2026-08-10 15:30:00",
            },
        ])
        with patch("tools.data_quality_check._row", side_effect=lambda *args, **kwargs: next(rows)), \
             patch("tools.data_quality_check._table_exists", return_value=True):
            result = check_analysis_outputs(object(), "2026-08-10")

        self.assertEqual(result.status, "PASS")
        self.assertEqual(result.details["recommendation_state"], "completed_zero")
        self.assertEqual(
            result.details["recommendation_evidence"],
            "st_recommended_run_history",
        )

    def test_analysis_outputs_rejects_empty_completed_run_history(self):
        rows = iter([
            {
                "analysis_date": "2026-08-10",
                "analysis_count": 5280,
                "status_count": 5280,
                "allow_count": 3,
                "expected_count": 5280,
            },
            {"latest_date": "2026-08-03", "current_count": 0, "latest_count": 80},
            {
                "status": "done",
                "total": 0,
                "passed": 0,
                "finished_at": "2026-08-10 15:30:00",
            },
        ])
        with patch("tools.data_quality_check._row", side_effect=lambda *args, **kwargs: next(rows)), \
             patch("tools.data_quality_check._table_exists", return_value=True):
            result = check_analysis_outputs(object(), "2026-08-10")

        self.assertEqual(result.status, "WARN")
        self.assertEqual(result.details["recommendation_state"], "incomplete_run_history")

    def test_analysis_outputs_does_not_infer_zero_from_partial_analysis(self):
        rows = iter([
            {
                "analysis_date": "2026-08-10",
                "analysis_count": 1000,
                "status_count": 1000,
                "allow_count": 0,
                "expected_count": 5280,
            },
            {"latest_date": "2026-08-03", "current_count": 0, "latest_count": 80},
            {},
        ])
        with patch("tools.data_quality_check._row", side_effect=lambda *args, **kwargs: next(rows)), \
             patch("tools.data_quality_check._table_exists", return_value=True):
            result = check_analysis_outputs(object(), "2026-08-10")

        self.assertEqual(result.status, "WARN")
        self.assertEqual(result.details["recommendation_state"], "not_confirmed")
        self.assertLess(result.details["analysis_coverage"], 0.2)

    def test_schema_collation_includes_portfolio_current_join_columns(self):
        captured = {}
        targets = {
            "si_all_code.stock_code",
            "sm_stock_kline.stock_code",
            "sm_stock_capital_flow_daily.stock_code",
            "st_user_portfolio.stock_code",
            "sm_stock_current.stock_code",
            "stock_analysis_result.stock_code",
            "st_recommended_stocks.stock_code",
        }

        def fake_rows(_engine, sql, params=None):
            captured["params"] = params or {}
            return [{"column_key": target, "COLLATION_NAME": "utf8mb4_unicode_ci"} for target in targets]

        with patch("tools.data_quality_check._rows", side_effect=fake_rows):
            result = check_schema_collation(object())

        self.assertEqual(result.status, "PASS")
        self.assertIn("st_user_portfolio.stock_code", captured["params"]["targets"])
        self.assertIn("sm_stock_current.stock_code", captured["params"]["targets"])

    def test_intraday_scheduler_health_filters_to_intraday_tasks(self):
        captured = {}

        def fake_bad_tasks(engine, *, task_types=None):
            captured["task_types"] = task_types
            return []

        with patch("tools.data_quality_check._table_exists", return_value=True), \
             patch("tools.data_quality_check._scheduler_bad_tasks", side_effect=fake_bad_tasks):
            result = check_intraday_scheduler_health(object())

        self.assertEqual(result.status, "PASS")
        self.assertIn("intraday_minute_kline", captured["task_types"])
        self.assertNotIn("qmt_local_history_2026", captured["task_types"])

    def test_run_checks_stops_when_required_tables_missing(self):
        missing = CheckResult("required_tables", "FAIL", "缺少核心表", {"missing": ["sm_stock_kline"]})
        with patch("tools.data_quality_check.check_required_tables", return_value=missing):
            report = run_checks(object())
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["checks"], [missing.as_dict()])

    def test_main_uses_batch_engine_for_report(self):
        engine = object()
        report = {
            "status": "PASS",
            "trade_date": "2026-07-01",
            "generated_at": "2026-07-01T18:00:00",
            "checks": [],
        }

        with patch.object(data_quality_check.sys, "argv", ["data_quality_check.py", "--json"]), \
             patch("tools.data_quality_check.create_batch_engine", return_value=engine) as create_batch_engine, \
             patch("tools.data_quality_check.run_checks", return_value=report) as run_checks_mock:
            self.assertEqual(data_quality_check.main(), 0)

        create_batch_engine.assert_called_once_with()
        run_checks_mock.assert_called_once_with(engine, None, include_realtime=False)


if __name__ == "__main__":
    unittest.main()
