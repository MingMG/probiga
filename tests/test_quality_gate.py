# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch

from server.common.scheduler_validation import TASK_OUTPUT_REQUIREMENTS
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


if __name__ == "__main__":
    unittest.main()
