# -*- coding: utf-8 -*-
import unittest

from tools.ensure_quality_gate import _task_payload


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


if __name__ == "__main__":
    unittest.main()
