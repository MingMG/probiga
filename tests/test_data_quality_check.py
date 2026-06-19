# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch

from tools.data_quality_check import (
    CheckResult,
    _date_lag_days,
    _scheduler_health_status,
    _status,
    expected_intraday_date,
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

    def test_expected_intraday_date_uses_today_on_trade_day(self):
        with patch("tools.data_quality_check.date") as mock_date, \
             patch("tools.data_quality_check._scalar", return_value=1):
            mock_date.today.return_value.isoformat.return_value = "2026-06-15"

            self.assertEqual(expected_intraday_date(object(), "2026-06-12"), "2026-06-15")

    def test_expected_intraday_date_falls_back_when_market_closed(self):
        with patch("tools.data_quality_check.date") as mock_date, \
             patch("tools.data_quality_check._scalar", return_value=0):
            mock_date.today.return_value.isoformat.return_value = "2026-06-13"

            self.assertEqual(expected_intraday_date(object(), "2026-06-12"), "2026-06-12")

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
             patch("tools.data_quality_check.check_scheduler_health", return_value=CheckResult("scheduler_health", "PASS", "ok")):
            result = intraday_readiness(object())

        self.assertEqual(result["status"], "NOT_READY")
        self.assertFalse(result["allow_realtime_trading"])

    def test_run_checks_stops_when_required_tables_missing(self):
        missing = CheckResult("required_tables", "FAIL", "缺少核心表", {"missing": ["sm_stock_kline"]})
        with patch("tools.data_quality_check.check_required_tables", return_value=missing):
            report = run_checks(object())
        self.assertEqual(report["status"], "FAIL")
        self.assertEqual(report["checks"], [missing.as_dict()])


if __name__ == "__main__":
    unittest.main()
