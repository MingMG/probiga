# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch

from server.common.scheduler_validation import TASK_OUTPUT_REQUIREMENTS
from tools import check_and_fix_scheduled_tasks, ensure_quality_gate
from tools.ensure_quality_gate import TASKS, _task_payload, run


class QualityGateTaskTest(unittest.TestCase):
    def test_task_payload_only_uses_existing_scheduler_columns(self):
        task = {
            "task_name": "quality",
            "task_type": "quality_check",
            "group_name": "system",
            "script_path": "tools/data_quality_check.py",
            "script_args": "--json",
            "cron_time": "08:45",
            "interval_minutes": 0,
            "enabled": 1,
            "sort_order": 10,
            "description": "check",
            "ignored": "x",
        }
        columns = {"task_name", "script_path", "cron_time", "enabled"}

        self.assertEqual(
            _task_payload(task, columns),
            {
                "task_name": "quality",
                "script_path": "tools/data_quality_check.py",
                "cron_time": "08:45",
                "enabled": 1,
            },
        )

    def test_0908_theme_forecast_task_is_enabled_and_strictly_validated(self):
        task = next(
            item for item in TASKS
            if item.get("task_type") == "analysis_premarket_external"
        )
        self.assertEqual(task["cron_time"], "09:07")
        self.assertEqual(task["enabled"], 1)
        self.assertIn("--theme-forecast", task["script_args"])
        self.assertIn("--push-theme-forecast", task["script_args"])

        requirements = TASK_OUTPUT_REQUIREMENTS["analysis_premarket_external"]
        forecast = next(
            item for item in requirements
            if item.table == "st_premarket_theme_forecast_run"
        )
        self.assertEqual(forecast.date_col, "session_date")
        self.assertEqual(forecast.target, "run_date")
        self.assertIn("delivery_status = 'SUCCESS'", forecast.where_sql)

    def test_task_type_filter_only_upserts_requested_task(self):
        actions = []
        with patch(
            "tools.ensure_quality_gate.ensure_scheduler_columns",
            side_effect=lambda _engine: None,
        ), patch(
            "tools.ensure_quality_gate.upsert_task",
            side_effect=lambda _engine, task: actions.append(task["task_type"]) or "updated",
        ):
            result = run(object(), task_types={"analysis_premarket_external"})

        self.assertEqual(actions, ["analysis_premarket_external"])
        self.assertEqual(len(result), 1)

    def test_intraday_alert_task_definitions_are_exact_and_consistent(self):
        authoritative = {
            item["task_type"]: item for item in TASKS
        }["intraday_market_alert"]
        legacy = {
            item[1]: {
                "script_path": item[2],
                "script_args": item[3],
                "cron_time": item[4],
                "sort_order": item[5],
                "interval_minutes": int(item[6]) if len(item) > 6 else 0,
            }
            for item in check_and_fix_scheduled_tasks.OPTIONAL_TASKS
        }["intraday_market_alert"]
        expected = {
            "script_path": "tools/run_intraday_market_alert.py",
            "script_args": "--mode shadow --json",
            "cron_time": "09:25",
            "sort_order": 95,
            "interval_minutes": 1,
        }
        self.assertEqual({key: authoritative[key] for key in expected}, expected)
        self.assertEqual(legacy, expected)
        self.assertEqual(authoritative["enabled"], 1)
        self.assertEqual(authoritative["date_param"], "")

    def test_default_run_does_not_install_opt_in_intraday_alert(self):
        actions = []
        with patch.object(
            ensure_quality_gate,
            "ensure_scheduler_columns",
        ), patch.object(
            ensure_quality_gate,
            "upsert_task",
            side_effect=lambda _engine, task: actions.append(task["task_type"]) or "updated",
        ):
            ensure_quality_gate.run(object())

        self.assertNotIn("intraday_market_alert", actions)

    def test_intraday_alert_explicit_scope_writes_requested_runtime_mode(self):
        installed = []
        with patch.object(
            ensure_quality_gate,
            "ensure_scheduler_columns",
        ), patch.object(
            ensure_quality_gate,
            "upsert_task",
            side_effect=lambda _engine, task: installed.append(dict(task)) or "updated",
        ):
            result = ensure_quality_gate.run(
                object(),
                task_types=ensure_quality_gate.INTRADAY_ALERT_TASK_TYPES,
                intraday_alert_mode="live",
            )

        self.assertEqual(len(installed), 1)
        self.assertEqual(installed[0]["task_type"], "intraday_market_alert")
        self.assertEqual(installed[0]["script_args"], "--mode live --json")
        self.assertEqual(result, {"Intraday key market event alerts": "updated"})


if __name__ == "__main__":
    unittest.main()
