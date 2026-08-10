# -*- coding: utf-8 -*-
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from datetime import datetime

from server.api import scheduler_runtime
from server.common.scheduler_script_policy import SchedulerScriptPolicyError


class SchedulerRuntimeTest(unittest.TestCase):
    def tearDown(self):
        scheduler_runtime._scheduler_thread = None
        scheduler_runtime._task_semaphore = None
        scheduler_runtime._running_procs.clear()
        scheduler_runtime._running_task_ids.clear()

    def test_start_embedded_scheduler_skips_when_disabled(self):
        with patch("server.api.scheduler_runtime.get_scheduler_runtime_config", return_value={
            "embedded_enabled": False,
            "max_concurrent_tasks": 1,
            "poll_seconds": 60,
        }), patch("server.api.scheduler_runtime.threading.Thread") as thread_cls:
            self.assertIsNone(scheduler_runtime.start_embedded_scheduler())

        thread_cls.assert_not_called()

    def test_start_embedded_scheduler_starts_only_once(self):
        fake_thread = MagicMock()
        fake_thread.is_alive.return_value = True
        with patch("server.api.scheduler_runtime.get_scheduler_runtime_config", return_value={
            "embedded_enabled": True,
            "max_concurrent_tasks": 1,
            "poll_seconds": 60,
        }), patch("server.api.scheduler_runtime.threading.Thread", return_value=fake_thread) as thread_cls:
            first = scheduler_runtime.start_embedded_scheduler()
            second = scheduler_runtime.start_embedded_scheduler()

        self.assertIs(first, fake_thread)
        self.assertIs(second, fake_thread)
        thread_cls.assert_called_once_with(
            target=scheduler_runtime._check_and_run_tasks,
            daemon=True,
            name="scheduler-daemon",
        )
        fake_thread.start.assert_called_once()

    def test_scheduler_runtime_info_reports_runtime_limits(self):
        fake_thread = MagicMock()
        fake_thread.is_alive.return_value = True
        scheduler_runtime._scheduler_thread = fake_thread
        with patch("server.api.scheduler_runtime.get_scheduler_runtime_config", return_value={
            "embedded_enabled": True,
            "max_concurrent_tasks": 2,
            "poll_seconds": 45,
        }), patch("server.api.scheduler_runtime.get_api_mysql_pool_config", return_value={
            "pool_size": 3,
            "max_overflow": 1,
            "pool_recycle": 1800,
        }):
            self.assertEqual(
                scheduler_runtime.scheduler_runtime_info(),
                {
                    "embedded_scheduler_enabled": True,
                    "embedded_scheduler_running": True,
                    "scheduler_max_concurrent_tasks": 2,
                    "scheduler_poll_seconds": 45,
                    "api_mysql_pool_size": 3,
                    "api_mysql_max_overflow": 1,
                    "api_mysql_pool_recycle": 1800,
                },
            )

    def test_run_task_fails_closed_when_script_is_not_clean_git_content(self):
        engine = MagicMock()
        conn = engine.begin.return_value.__enter__.return_value
        row = {
            "id": 12,
            "task_name": "capital flow",
            "script_path": "tools/crawl_realtime_batch.py",
            "script_args": "--only flow",
            "date_param": "",
        }

        with patch(
            "server.api.scheduler_runtime.resolve_scheduler_script",
            side_effect=SchedulerScriptPolicyError("not clean"),
        ), patch("server.api.scheduler_runtime.subprocess.Popen") as popen:
            scheduler_runtime._run_task(row, Path("/opt/ProBigA-current"), engine)

        popen.assert_not_called()
        params = conn.execute.call_args.args[1]
        self.assertEqual(params["id"], 12)
        self.assertIn("SCHEDULER_SCRIPT_BLOCKED", params["o"])

    def test_linux_scheduler_delegates_all_qmt_launchers_to_windows(self):
        for task_type in (
            "all_code",
            "concept_constituent_east",
            "stock_kline",
            "intraday_realtime",
        ):
            self.assertTrue(
                scheduler_runtime._should_delegate_to_windows_qmt_bridge(
                    {"task_type": task_type}, platform_name="posix"
                )
            )
            self.assertFalse(
                scheduler_runtime._should_delegate_to_windows_qmt_bridge(
                    {"task_type": task_type}, platform_name="nt"
                )
            )

    def test_intraday_interval_tasks_do_not_run_before_market_window(self):
        row = {"task_type": "intraday_minute_flow"}

        self.assertTrue(
            scheduler_runtime._should_skip_outside_intraday_window(
                row, datetime(2026, 8, 11, 7, 30)
            )
        )
        self.assertFalse(
            scheduler_runtime._should_skip_outside_intraday_window(
                row, datetime(2026, 8, 11, 10, 0)
            )
        )


if __name__ == "__main__":
    unittest.main()
