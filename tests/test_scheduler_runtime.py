# -*- coding: utf-8 -*-
import unittest
from unittest.mock import MagicMock, patch

from server.api import scheduler_runtime


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


if __name__ == "__main__":
    unittest.main()
