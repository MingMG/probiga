# -*- coding: utf-8 -*-
import json
import threading
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
from pathlib import Path

from server.api import scheduler_runtime
from server.api.routers import scheduler as scheduler_router
from server.common.scheduler_validation import SchedulerValidationResult


class SchedulerRuntimeTest(unittest.TestCase):
    def tearDown(self):
        scheduler_runtime._scheduler_thread = None
        scheduler_runtime._scheduler_stop_event = None
        scheduler_runtime._task_semaphore = None
        scheduler_runtime._fast_lane_semaphore = None
        scheduler_runtime._alert_lane_semaphore = None
        scheduler_runtime._delivery_lane_semaphore = None
        scheduler_runtime._running_procs.clear()
        scheduler_runtime._running_task_ids.clear()
        scheduler_runtime._fast_lane_running_task_ids.clear()
        scheduler_runtime._alert_lane_running_task_ids.clear()
        scheduler_runtime._delivery_lane_running_task_ids.clear()
        scheduler_runtime._task_history_ready_engines.clear()
        scheduler_runtime._history_cleanup_next_at = 0.0
        scheduler_router._quality_cache.clear()

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
        thread_cls.assert_called_once()
        thread_kwargs = thread_cls.call_args.kwargs
        self.assertEqual(thread_kwargs["target"], scheduler_runtime._check_and_run_tasks)
        self.assertEqual(thread_kwargs["args"][0], "embedded")
        self.assertIs(thread_kwargs["args"][1], scheduler_runtime._scheduler_stop_event)
        self.assertTrue(thread_kwargs["daemon"])
        self.assertEqual(thread_kwargs["name"], "scheduler-daemon")
        fake_thread.start.assert_called_once()

    def test_stop_embedded_scheduler_signals_and_joins_thread(self):
        fake_thread = MagicMock()
        fake_thread.is_alive.side_effect = [True, False]
        stop_event = threading.Event()
        scheduler_runtime._scheduler_thread = fake_thread
        scheduler_runtime._scheduler_stop_event = stop_event

        scheduler_runtime.stop_embedded_scheduler(timeout_seconds=0.2)

        self.assertTrue(stop_event.is_set())
        fake_thread.join.assert_called_once_with(timeout=0.2)
        self.assertIsNone(scheduler_runtime._scheduler_thread)
        self.assertIsNone(scheduler_runtime._scheduler_stop_event)

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
                    "scheduler_alert_lane_tasks": 1,
                    "scheduler_delivery_lane_tasks": 1,
                    "scheduler_poll_seconds": 45,
                    "api_mysql_pool_size": 3,
                    "api_mysql_max_overflow": 1,
                    "api_mysql_pool_recycle": 1800,
                },
            )

    def test_sim_trade_uses_dedicated_fast_lane(self):
        self.assertTrue(scheduler_runtime._uses_fast_lane({"task_type": "sim_trade"}))
        self.assertFalse(scheduler_runtime._uses_fast_lane({"task_type": "stock_minute"}))

    def test_intraday_alert_uses_independent_single_worker_lane(self):
        alert = {"task_type": "intraday_market_alert"}
        general = {"task_type": "stock_minute"}
        fast = {"task_type": "sim_trade"}

        self.assertTrue(scheduler_runtime._uses_alert_lane(alert))
        self.assertFalse(scheduler_runtime._uses_fast_lane(alert))
        semaphore = scheduler_runtime._task_lane_semaphore(alert)
        self.assertIs(semaphore, scheduler_runtime._get_alert_lane_semaphore())
        self.assertIsNot(semaphore, scheduler_runtime._task_lane_semaphore(general))
        self.assertIsNot(semaphore, scheduler_runtime._task_lane_semaphore(fast))

        scheduler_runtime._running_task_ids.add(604)
        scheduler_runtime._alert_lane_running_task_ids.add(604)
        self.assertEqual(len(scheduler_runtime._alert_lane_running_task_ids), 1)

    def test_intraday_alert_is_linux_owned_and_market_hours_only(self):
        row = {
            "task_type": "intraday_market_alert",
            "script_path": "tools/run_intraday_market_alert.py",
            "cron_time": "09:25",
        }
        self.assertFalse(
            scheduler_runtime._should_skip_task_for_host(row, platform_name="posix")
        )
        self.assertTrue(
            scheduler_runtime._should_skip_task_for_host(row, platform_name="nt")
        )
        self.assertTrue(
            scheduler_runtime._should_skip_outside_intraday_window(
                row,
                datetime(2026, 8, 12, 9, 24),
            )
        )
        self.assertFalse(
            scheduler_runtime._should_skip_outside_intraday_window(
                row,
                datetime(2026, 8, 12, 9, 25),
            )
        )
        self.assertTrue(
            scheduler_runtime._should_skip_outside_intraday_window(
                row,
                datetime(2026, 8, 12, 15, 11),
            )
        )
        with patch("server.api.scheduler_runtime._is_trade_day", return_value=False):
            self.assertTrue(
                scheduler_runtime._should_skip_non_trading_day(row, MagicMock())
            )

    def test_user_deliverables_use_dedicated_delivery_lane(self):
        for task_type in ("news_daily", "daily_review", "evening_review"):
            with self.subTest(task_type=task_type):
                row = {"task_type": task_type}
                self.assertTrue(scheduler_runtime._uses_delivery_lane(row))
                self.assertFalse(scheduler_runtime._uses_fast_lane(row))
        self.assertFalse(
            scheduler_runtime._uses_delivery_lane({"task_type": "stock_minute"})
        )

    def test_delivery_lane_semaphore_is_independent_from_general_and_fast_lanes(self):
        delivery = scheduler_runtime._task_lane_semaphore({"task_type": "news_daily"})
        general = scheduler_runtime._task_lane_semaphore({"task_type": "stock_minute"})
        fast = scheduler_runtime._task_lane_semaphore({"task_type": "sim_trade"})

        self.assertIs(delivery, scheduler_runtime._get_delivery_lane_semaphore())
        self.assertIsNot(delivery, general)
        self.assertIsNot(delivery, fast)

    def test_delivery_lane_still_has_capacity_when_general_worker_is_full(self):
        scheduler_runtime._running_task_ids.add(501)

        self.assertFalse(
            scheduler_runtime._scheduler_lane_has_capacity(
                {"task_type": "stock_minute"},
                max_general_tasks=1,
            )
        )
        self.assertTrue(
            scheduler_runtime._scheduler_lane_has_capacity(
                {"task_type": "news_daily"},
                max_general_tasks=1,
            )
        )

        scheduler_runtime._running_task_ids.add(502)
        scheduler_runtime._delivery_lane_running_task_ids.add(502)
        self.assertFalse(
            scheduler_runtime._scheduler_lane_has_capacity(
                {"task_type": "evening_review"},
                max_general_tasks=1,
            )
        )

    def test_intraday_capital_flow_fast_is_latency_sensitive_and_windows_owned(self):
        row = {"task_type": "intraday_capital_flow_fast"}

        self.assertTrue(scheduler_runtime._uses_fast_lane(row))
        self.assertFalse(
            scheduler_runtime._should_skip_outside_intraday_window(
                row,
                datetime(2026, 8, 11, 9, 31),
            )
        )
        self.assertTrue(
            scheduler_runtime._should_skip_outside_intraday_window(
                row,
                datetime(2026, 8, 11, 15, 11),
            )
        )
        self.assertTrue(
            scheduler_runtime._should_delegate_to_windows_qmt_bridge(
                row,
                platform_name="posix",
            )
        )
        self.assertFalse(
            scheduler_runtime._should_delegate_to_windows_qmt_bridge(
                row,
                platform_name="nt",
            )
        )

    def test_qmt_desktop_tasks_are_delegated_only_on_non_windows_host(self):
        for task_type in (
            "etf_forward_daily",
            "fetch_hot_rank_xq",
            "index_current",
            "index_kline",
            "index_minute",
            "intraday_minute_kline",
            "intraday_realtime",
            "qmt_membership_snapshot",
            "stock_kline",
        ):
            row = {"task_type": task_type}
            self.assertTrue(
                scheduler_runtime._should_delegate_to_windows_qmt_bridge(
                    row,
                    platform_name="posix",
                )
            )
            self.assertFalse(
                scheduler_runtime._should_delegate_to_windows_qmt_bridge(
                    row,
                    platform_name="nt",
                )
            )
        self.assertFalse(
            scheduler_runtime._should_delegate_to_windows_qmt_bridge(
                {"task_type": "trading_v2_level1_validation"},
                platform_name="posix",
            )
        )

    def test_scheduler_host_ownership_prevents_cross_host_execution(self):
        qmt_task = {"task_type": "stock_kline"}
        linux_task = {"task_type": "analysis_fast"}
        self.assertTrue(
            scheduler_runtime._should_skip_task_for_host(qmt_task, platform_name="posix")
        )
        self.assertFalse(
            scheduler_runtime._should_skip_task_for_host(linux_task, platform_name="posix")
        )
        self.assertFalse(
            scheduler_runtime._should_skip_task_for_host(qmt_task, platform_name="nt")
        )
        self.assertTrue(
            scheduler_runtime._should_skip_task_for_host(linux_task, platform_name="nt")
        )

    def test_market_tasks_skip_on_non_trading_day(self):
        row = {
            "task_type": "stock_minute",
            "script_path": "tools/run_single_table.py",
            "script_args": "sm_stock_minute",
        }
        with patch("server.api.scheduler_runtime._is_trade_day", return_value=False):
            self.assertTrue(scheduler_runtime._should_skip_non_trading_day(row, MagicMock()))

    def test_qmt_membership_snapshot_skips_on_non_trading_day(self):
        row = {
            "task_type": "qmt_membership_snapshot",
            "script_path": "tools/sync_bigqmt_reference.py",
            "script_args": "--apply --force-reference-refresh --json",
        }
        with patch("server.api.scheduler_runtime._is_trade_day", return_value=False):
            self.assertTrue(
                scheduler_runtime._should_skip_non_trading_day(
                    row,
                    MagicMock(),
                )
            )

    def test_news_tasks_do_not_skip_on_non_trading_day(self):
        row = {
            "task_type": "news_sync",
            "script_path": "biz/news/sync_news.py",
            "script_args": "--pages 2 --mode realtime",
        }
        with patch("server.api.scheduler_runtime._is_trade_day", return_value=False):
            self.assertFalse(scheduler_runtime._should_skip_non_trading_day(row, MagicMock()))

    def test_daily_user_deliverables_skip_on_non_trading_day(self):
        with patch("server.api.scheduler_runtime._is_trade_day", return_value=False):
            for task_type in ("news_daily", "daily_review", "evening_review"):
                with self.subTest(task_type=task_type):
                    self.assertTrue(
                        scheduler_runtime._should_skip_non_trading_day(
                            {"task_type": task_type, "script_path": "", "script_args": ""},
                            MagicMock(),
                        )
                    )

    def test_missing_trade_calendar_row_keeps_user_deliverable_due(self):
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = None
        row = {"task_type": "news_daily", "script_path": "", "script_args": ""}

        self.assertIsNone(
            scheduler_runtime._is_trade_day(engine, datetime(2026, 8, 12).date())
        )
        self.assertIsNone(
            scheduler_runtime._should_skip_non_trading_day(
                row,
                engine,
                datetime(2026, 8, 12, 8, 30),
            )
        )

    def test_trade_calendar_failure_keeps_user_deliverable_due(self):
        engine = MagicMock()
        engine.connect.side_effect = RuntimeError("calendar unavailable")
        row = {"task_type": "daily_review", "script_path": "", "script_args": ""}

        self.assertIsNone(
            scheduler_runtime._should_skip_non_trading_day(
                row,
                engine,
                datetime(2026, 8, 12, 18, 0),
            )
        )

    def test_explicit_closed_calendar_row_skips_user_deliverable(self):
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = 0

        self.assertTrue(
            scheduler_runtime._should_skip_non_trading_day(
                {"task_type": "evening_review", "script_path": "", "script_args": ""},
                engine,
                datetime(2026, 8, 15, 20, 0),
            )
        )

    def test_unknown_trade_calendar_status_keeps_user_deliverable_due(self):
        engine = MagicMock()
        engine.connect.return_value.__enter__.return_value.execute.return_value.scalar.return_value = 2

        self.assertIsNone(
            scheduler_runtime._should_skip_non_trading_day(
                {"task_type": "news_daily", "script_path": "", "script_args": ""},
                engine,
                datetime(2026, 8, 12, 8, 30),
            )
        )

    def test_counterfactual_drain_runs_on_non_trading_day(self):
        row = {
            "task_type": "trading_v3_counterfactual_audit",
            "script_path": "tools/run_trading_v3_counterfactual.py",
            "script_args": "--limit 10000 --max-batches 10",
        }
        with patch(
            "server.api.scheduler_runtime._is_trade_day",
            return_value=False,
        ):
            self.assertFalse(
                scheduler_runtime._should_skip_non_trading_day(
                    row,
                    MagicMock(),
                )
            )

    def test_briefing_tasks_do_not_receive_default_date_arg(self):
        today = "2026-07-01"
        early = {"task_type": "news_daily", "script_args": "", "date_param": ""}
        evening = {"task_type": "evening_review", "script_args": "", "date_param": ""}

        self.assertEqual(
            scheduler_runtime._build_task_args(early, "biz/early_briefing/generate.py", today),
            [],
        )
        self.assertEqual(
            scheduler_runtime._build_task_args(evening, "biz/evening_review/generate.py", today),
            [],
        )

    def test_latest_trade_date_tasks_do_not_receive_default_date_arg(self):
        today = "2026-07-01"
        snapshot = {"task_type": "stock_snapshot_daily", "script_args": "", "date_param": ""}
        overview = {"task_type": "market_overview_daily", "script_args": "", "date_param": ""}

        self.assertEqual(
            scheduler_runtime._build_task_args(snapshot, "biz/stock_market/sync_stock_snapshot.py", today),
            [],
        )
        self.assertEqual(
            scheduler_runtime._build_task_args(overview, "tools/refresh_market_overview_daily.py", today),
            [],
        )

    def test_intraday_alert_keeps_explicit_mode_without_default_date_arg(self):
        row = {
            "task_type": "intraday_market_alert",
            "script_args": "--mode shadow --json",
            "date_param": "",
        }
        self.assertEqual(
            scheduler_runtime._build_task_args(
                row,
                "tools/run_intraday_market_alert.py",
                "2026-08-12",
            ),
            ["--mode", "shadow", "--json"],
        )

    def test_regular_tasks_receive_default_date_arg(self):
        row = {"task_type": "daily_review", "script_args": "", "date_param": ""}

        self.assertEqual(
            scheduler_runtime._build_task_args(row, "biz/review/generate.py", "2026-07-01"),
            ["2026-07-01"],
        )

    def test_v2_runtime_tasks_do_not_receive_unsupported_default_date_arg(self):
        today = "2026-07-27"
        tasks = {
            "trading_v2_intraday_activation": "tools/run_trading_v2_intraday_activation.py",
            "trading_v2_job_worker": "tools/run_trading_v2_job_worker.py",
            "trading_v2_level1_validation": "tools/validate_trading_v2_level1.py",
            "trading_v2_paper_tick": "tools/run_trading_v2_paper_tick.py",
            "trading_v2_reconciliation": "tools/run_trading_v2_reconciliation.py",
            "trading_v2_strategy_health": "tools/run_trading_v2_health.py",
        }

        for task_type, script_path in tasks.items():
            row = {
                "task_type": task_type,
                "script_args": "",
                "date_param": "",
            }
            self.assertEqual(
                scheduler_runtime._build_task_args(row, script_path, today),
                [],
            )

    def test_script_args_and_date_param_are_combined(self):
        row = {"task_type": "stock_minute", "script_args": "sm_stock_minute", "date_param": "2026-07-01"}

        self.assertEqual(
            scheduler_runtime._build_task_args(row, "tools/run_single_table.py", "2026-07-05"),
            ["sm_stock_minute", "2026-07-01"],
        )

    def test_date_param_range_is_split_for_scheduler_args(self):
        row = {"task_type": "capital_flow", "script_args": "--repair", "date_param": "2026-07-01:2026-07-03"}

        self.assertEqual(
            scheduler_runtime._build_task_args(row, "tools/backfill_capital_flow.py", "2026-07-05"),
            ["--repair", "2026-07-01", "2026-07-03"],
        )

    def test_run_single_table_with_only_table_arg_receives_default_date(self):
        row = {"task_type": "all_code", "script_args": "si_all_code", "date_param": ""}

        self.assertEqual(
            scheduler_runtime._build_task_args(row, "tools/run_single_table.py", "2026-07-05"),
            ["si_all_code", "2026-07-05"],
        )

    def test_task_timeout_uses_interval_for_fast_tasks(self):
        row = {"task_type": "intraday_realtime", "script_path": "tools/live.py", "interval_minutes": 5}

        self.assertEqual(scheduler_runtime._task_timeout_minutes(row), 20)

    def test_task_timeout_allows_long_market_sync(self):
        row = {"task_type": "stock_minute", "script_path": "tools/run_single_table.py", "interval_minutes": 0}

        self.assertEqual(
            scheduler_runtime._task_timeout_minutes(row),
            scheduler_runtime.LONG_TASK_TIMEOUT_MINUTES,
        )

    def test_task_timeout_allows_long_qmt_gap_repair(self):
        row = {
            "task_type": "qmt_local_gap_repair_execute",
            "script_path": "tools/backfill_guojin_qmt_local_history.py",
            "interval_minutes": 0,
        }

        self.assertEqual(
            scheduler_runtime._task_timeout_minutes(row),
            scheduler_runtime.LONG_TASK_TIMEOUT_MINUTES,
        )

    def test_scheduler_prioritizes_older_interval_task_over_realtime_poll(self):
        now = datetime(2026, 7, 20, 10, 0, 0)
        realtime = {
            "id": 39,
            "task_type": "intraday_realtime",
            "interval_minutes": 1,
            "last_triggered_at": "2026-07-20 09:59:20",
        }
        minute = {
            "id": 41,
            "task_type": "intraday_minute_kline",
            "interval_minutes": 15,
            "last_triggered_at": "2026-07-20 09:40:00",
        }

        ordered = sorted(
            [realtime, minute],
            key=lambda row: scheduler_runtime._scheduler_task_sort_key(row, now=now),
        )

        self.assertEqual(ordered[0]["task_type"], "intraday_minute_kline")

    def test_scheduler_keeps_not_due_tasks_after_due_tasks(self):
        now = datetime(2026, 7, 20, 10, 0, 0)
        due = {
            "id": 41,
            "task_type": "intraday_minute_kline",
            "interval_minutes": 15,
            "last_triggered_at": "2026-07-20 09:40:00",
        }
        not_due = {
            "id": 39,
            "task_type": "intraday_realtime",
            "interval_minutes": 1,
            "last_triggered_at": "2026-07-20 09:59:30",
        }

        ordered = sorted(
            [not_due, due],
            key=lambda row: scheduler_runtime._scheduler_task_sort_key(row, now=now),
        )

        self.assertEqual(ordered[0]["task_type"], "intraday_minute_kline")
        self.assertEqual(ordered[1]["task_type"], "intraday_realtime")

    def test_cleanup_stale_running_task_after_scheduler_restart(self):
        started_at = datetime.now() - timedelta(minutes=2)
        original_started_at = scheduler_runtime._scheduler_started_at
        original_running_procs = scheduler_runtime._running_procs
        original_running_task_ids = scheduler_runtime._running_task_ids
        scheduler_runtime._scheduler_started_at = datetime.now()
        scheduler_runtime._running_procs = {}
        scheduler_runtime._running_task_ids = {39}
        updates = []

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def execute(self, *_args, **_kwargs):
                class Result:
                    def mappings(self):
                        return self

                    def all(self):
                        return [{
                            "id": 39,
                            "task_name": "盘中实时行情同步",
                            "task_type": "intraday_realtime",
                            "script_path": "tools/sync_qmt_primary.py",
                            "interval_minutes": 1,
                            "last_run_at": started_at,
                            "last_triggered_at": started_at,
                        }]

                return Result()

        class FakeEngine:
            def connect(self):
                return FakeConnection()

        original_update = scheduler_runtime.update_scheduler_task
        scheduler_runtime.update_scheduler_task = lambda *args: updates.append(args)
        remaining_ids = None
        try:
            with patch(
                "server.api.scheduler_runtime._should_skip_task_for_host",
                return_value=False,
            ):
                cleaned = scheduler_runtime._cleanup_stale_running_tasks(FakeEngine())
            remaining_ids = set(scheduler_runtime._running_task_ids)
        finally:
            scheduler_runtime.update_scheduler_task = original_update
            scheduler_runtime._scheduler_started_at = original_started_at
            scheduler_runtime._running_procs = original_running_procs
            scheduler_runtime._running_task_ids = original_running_task_ids

        self.assertEqual(cleaned, 1)
        self.assertEqual(updates[0][2]["last_run_status"], "failed")
        self.assertIn("服务重启", updates[0][2]["last_run_output"])
        self.assertNotIn(39, remaining_ids)

    def test_cleanup_does_not_touch_task_owned_by_other_host(self):
        started_at = datetime.now() - timedelta(hours=2)
        scheduler_runtime._running_procs.clear()
        updates = []

        class FakeConnection:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def execute(self, *_args, **_kwargs):
                class Result:
                    def mappings(self):
                        return self

                    def all(self):
                        return [{
                            "id": 42,
                            "task_name": "Windows-owned minute sync",
                            "task_type": "intraday_minute_kline",
                            "script_path": "tools/sync_qmt_primary.py",
                            "interval_minutes": 15,
                            "last_run_at": started_at,
                            "last_triggered_at": started_at,
                        }]

                return Result()

        class FakeEngine:
            def connect(self):
                return FakeConnection()

        with patch(
            "server.api.scheduler_runtime._should_skip_task_for_host",
            return_value=True,
        ), patch(
            "server.api.scheduler_runtime.update_scheduler_task",
            side_effect=lambda *args: updates.append(args),
        ):
            cleaned = scheduler_runtime._cleanup_stale_running_tasks(FakeEngine())

        self.assertEqual(cleaned, 0)
        self.assertEqual(updates, [])

    def test_startup_cleanup_uses_host_aware_cleanup(self):
        engine = MagicMock()
        with patch("server.api.scheduler_runtime.get_engine", return_value=engine), patch(
            "server.api.scheduler_runtime._cleanup_stale_running_tasks",
            return_value=0,
        ) as cleanup:
            scheduler_runtime._catchup_on_startup()

        cleanup.assert_called_once_with(engine)

    def test_cron_catchup_allows_only_recent_missed_tasks(self):
        now = datetime(2026, 7, 6, 9, 26, 30)
        startup = now - timedelta(seconds=30)

        self.assertTrue(
            scheduler_runtime._cron_catchup_allowed(now=now, cron_time="09:25", startup_time=startup)
        )

    def test_cron_catchup_skips_old_missed_tasks(self):
        now = datetime(2026, 7, 6, 21, 22, 30)
        startup = now - timedelta(seconds=30)

        self.assertFalse(
            scheduler_runtime._cron_catchup_allowed(now=now, cron_time="09:25", startup_time=startup)
        )

    def test_cron_catchup_skips_when_scheduler_has_been_running(self):
        now = datetime(2026, 7, 6, 9, 26, 30)
        startup = now - timedelta(minutes=10)

        self.assertFalse(
            scheduler_runtime._cron_catchup_allowed(now=now, cron_time="09:25", startup_time=startup)
        )

    def test_critical_cron_catchup_allows_missed_morning_ai_task(self):
        row = {"task_type": "analysis_morning_strict", "last_triggered_at": "2026-07-07 08:30:00"}
        now = datetime(2026, 7, 8, 11, 12, 0)

        self.assertTrue(
            scheduler_runtime._critical_cron_catchup_allowed(row, now=now, cron_time="08:30")
        )

    def test_critical_cron_catchup_skips_already_triggered_today(self):
        row = {"task_type": "analysis_morning_strict", "last_triggered_at": "2026-07-08 08:30:00"}
        now = datetime(2026, 7, 8, 11, 12, 0)

        self.assertFalse(
            scheduler_runtime._critical_cron_catchup_allowed(row, now=now, cron_time="08:30")
        )

    def test_critical_cron_catchup_retries_failed_task_after_backoff(self):
        row = {
            "task_type": "stock_kline",
            "last_triggered_at": "2026-07-28 15:05:00",
            "last_run_at": "2026-07-28 15:06:00",
            "last_run_status": "failed",
        }
        now = datetime(2026, 7, 28, 15, 25, 0)

        self.assertTrue(
            scheduler_runtime._critical_cron_catchup_allowed(
                row,
                now=now,
                cron_time="15:05",
            )
        )

    def test_stock_kline_and_v3_close_can_catch_up_after_busy_slot(self):
        now = datetime(2026, 7, 28, 19, 0, 0)
        for task_type, cron_time in (
            ("stock_kline", "15:05"),
            ("trading_v3_close_decision", "16:05"),
        ):
            with self.subTest(task_type=task_type):
                self.assertTrue(
                    scheduler_runtime._critical_cron_catchup_allowed(
                        {
                            "task_type": task_type,
                            "last_triggered_at": "2026-07-27 16:05:00",
                        },
                        now=now,
                        cron_time=cron_time,
                    )
                )

    def test_critical_cron_market_overview_and_capital_flow_can_catch_up_after_busy_slot(self):
        now = datetime(2026, 8, 6, 20, 0, 0)
        for task_type, cron_time in (
            ("capital_flow", "17:30"),
            ("capital_flow_batch_fast", "15:20"),
            ("market_overview_daily", "18:20"),
        ):
            with self.subTest(task_type=task_type):
                self.assertTrue(
                    scheduler_runtime._critical_cron_catchup_allowed(
                        {
                            "task_type": task_type,
                            "last_triggered_at": "2026-08-05 18:20:00",
                        },
                        now=now,
                        cron_time=cron_time,
                    )
                )

    def test_critical_cron_catchup_allows_missed_daily_recommendation_task(self):
        row = {"task_type": "analysis_fast", "last_triggered_at": "2026-07-07 18:50:00"}
        now = datetime(2026, 7, 8, 21, 0, 0)

        self.assertTrue(
            scheduler_runtime._critical_cron_catchup_allowed(row, now=now, cron_time="18:50")
        )

    def test_critical_cron_catchup_recovers_missed_signal_prepare_same_day(self):
        row = {"task_type": "sim_trade_signal_prepare", "last_triggered_at": "2026-07-22 09:20:00"}
        now = datetime(2026, 7, 23, 14, 10, 0)

        self.assertTrue(
            scheduler_runtime._critical_cron_catchup_allowed(row, now=now, cron_time="09:20")
        )

    def test_critical_cron_catchup_recovers_missed_v2_premarket_plan(self):
        row = {
            "task_type": "trading_v2_premarket_decision",
            "last_triggered_at": "2026-07-24 09:20:00",
        }
        now = datetime(2026, 7, 27, 9, 22, 0)

        self.assertTrue(
            scheduler_runtime._critical_cron_catchup_allowed(
                row,
                now=now,
                cron_time="09:20",
            )
        )

    def test_v2_intraday_activation_uses_latency_sensitive_lane(self):
        row = {
            "task_type": "trading_v2_intraday_activation",
            "script_path": "tools/run_trading_v2_intraday_activation.py",
        }

        self.assertTrue(scheduler_runtime._uses_fast_lane(row))
        self.assertFalse(
            scheduler_runtime._should_skip_outside_intraday_window(
                row,
                datetime(2026, 7, 27, 10, 30),
            )
        )
        self.assertTrue(
            scheduler_runtime._should_skip_outside_intraday_window(
                row,
                datetime(2026, 7, 27, 21, 0),
            )
        )

    def test_overdue_cron_allows_daily_review_for_rest_of_same_day(self):
        now = datetime(2026, 7, 8, 23, 59, 0)
        startup = now - timedelta(seconds=30)
        row = {"task_type": "daily_review", "last_triggered_at": "2026-07-07 18:00:00"}

        self.assertTrue(
            scheduler_runtime._overdue_cron_allowed(
                row,
                now=now,
                cron_time="18:00",
                startup_time=startup,
            )
        )

    def test_user_deliverables_catch_up_after_busy_slot_same_day(self):
        now = datetime(2026, 8, 12, 23, 59, 0)

        for task_type, cron_time in (
            ("daily_review", "18:00"),
            ("evening_review", "20:00"),
        ):
            with self.subTest(task_type=task_type):
                self.assertTrue(
                    scheduler_runtime._critical_cron_catchup_allowed(
                        {
                            "task_type": task_type,
                            "last_triggered_at": "2026-08-11 20:00:00",
                            "last_run_status": "success",
                        },
                        now=now,
                        cron_time=cron_time,
                    )
                )

    def test_early_briefing_catches_up_only_during_the_morning(self):
        row = {
            "task_type": "news_daily",
            "last_triggered_at": "2026-08-11 08:30:00",
            "last_run_status": "success",
        }

        self.assertTrue(
            scheduler_runtime._critical_cron_catchup_allowed(
                row,
                now=datetime(2026, 8, 12, 11, 59, 0),
                cron_time="08:30",
            )
        )
        self.assertFalse(
            scheduler_runtime._critical_cron_catchup_allowed(
                row,
                now=datetime(2026, 8, 12, 12, 1, 0),
                cron_time="08:30",
            )
        )

    def test_user_deliverable_failed_run_retries_after_completion_backoff(self):
        row = {
            "task_type": "news_daily",
            "last_triggered_at": "2026-08-12 08:30:00",
            "last_run_at": "2026-08-12 08:30:00",
            "last_run_duration": 17 * 60,
            "last_run_status": "failed",
        }

        self.assertFalse(
            scheduler_runtime._critical_cron_catchup_allowed(
                row,
                now=datetime(2026, 8, 12, 9, 1, 0),
                cron_time="08:30",
            )
        )
        self.assertTrue(
            scheduler_runtime._critical_cron_catchup_allowed(
                row,
                now=datetime(2026, 8, 12, 9, 2, 0),
                cron_time="08:30",
            )
        )

    def test_overdue_cron_allows_recent_restart_catchup(self):
        now = datetime(2026, 7, 8, 9, 26, 0)
        startup = now - timedelta(seconds=30)
        row = {"task_type": "intraday_realtime", "last_triggered_at": "2026-07-07 09:25:00"}

        self.assertTrue(
            scheduler_runtime._overdue_cron_allowed(
                row,
                now=now,
                cron_time="09:25",
                startup_time=startup,
            )
        )

    def test_cron_due_runs_missed_task_after_scheduler_restart(self):
        row = {
            "cron_time": "08:30",
            "last_triggered_at": "2026-07-07 08:30:00",
            "last_run_status": "success",
        }

        self.assertTrue(scheduler_runtime._cron_due(row, now=datetime(2026, 7, 8, 10, 0)))

    def test_cron_due_waits_until_scheduled_time(self):
        row = {"cron_time": "08:30", "last_triggered_at": "2026-07-07 08:30:00"}

        self.assertFalse(scheduler_runtime._cron_due(row, now=datetime(2026, 7, 8, 8, 29)))

    def test_cron_due_retries_failed_task_after_backoff(self):
        row = {
            "cron_time": "08:30",
            "last_triggered_at": "2026-07-08 08:30:00",
            "last_run_at": "2026-07-08 08:40:00",
            "last_run_status": "failed",
        }

        self.assertFalse(scheduler_runtime._cron_due(row, now=datetime(2026, 7, 8, 8, 54)))
        self.assertTrue(scheduler_runtime._cron_due(row, now=datetime(2026, 7, 8, 8, 55)))

    def test_cron_due_does_not_repeat_successful_task_same_day(self):
        row = {
            "cron_time": "08:30",
            "last_triggered_at": "2026-07-08 08:30:00",
            "last_run_status": "success",
        }

        self.assertFalse(scheduler_runtime._cron_due(row, now=datetime(2026, 7, 8, 10, 0)))

    def test_intraday_task_skips_outside_trading_window(self):
        row = {"task_type": "intraday_realtime", "script_path": "tools/crawl_realtime_batch.py"}

        self.assertTrue(
            scheduler_runtime._should_skip_outside_intraday_window(row, datetime(2026, 7, 6, 21, 22))
        )

    def test_intraday_task_runs_inside_trading_window(self):
        row = {"task_type": "intraday_realtime", "script_path": "tools/crawl_realtime_batch.py"}

        self.assertFalse(
            scheduler_runtime._should_skip_outside_intraday_window(row, datetime(2026, 7, 6, 10, 30))
        )

    def test_post_close_stock_minute_is_not_misclassified_by_shared_script_path(self):
        row = {"task_type": "stock_minute", "script_path": "tools/crawl_minute_kline.py"}

        self.assertFalse(
            scheduler_runtime._should_skip_outside_intraday_window(row, datetime(2026, 7, 6, 15, 30))
        )

    def test_legacy_intraday_row_without_type_still_uses_script_path(self):
        row = {"task_type": "", "script_path": "tools/crawl_minute_kline.py"}

        self.assertTrue(
            scheduler_runtime._should_skip_outside_intraday_window(row, datetime(2026, 7, 6, 21, 22))
        )

    def test_claim_task_run_marks_running_atomically(self):
        conn = MagicMock()
        conn.execute.return_value.rowcount = 1
        ctx = MagicMock()
        ctx.__enter__.return_value = conn
        engine = MagicMock()
        engine.begin.return_value = ctx

        claimed = scheduler_runtime._claim_task_run({"id": 12}, engine)

        self.assertTrue(claimed)
        sql = str(conn.execute.call_args.args[0])
        params = conn.execute.call_args.args[1]
        self.assertIn("last_run_status='running'", sql)
        self.assertIn("last_run_status <> 'running'", sql)
        self.assertEqual(params["id"], 12)

    def test_claim_task_run_returns_false_when_already_claimed(self):
        conn = MagicMock()
        conn.execute.return_value.rowcount = 0
        ctx = MagicMock()
        ctx.__enter__.return_value = conn
        engine = MagicMock()
        engine.begin.return_value = ctx

        self.assertFalse(scheduler_runtime._claim_task_run({"id": 12}, engine))

    def test_run_task_marks_missing_script_failed(self):
        engine = MagicMock()
        row = {"id": 7, "task_name": "missing", "script_path": "tools/missing.py"}

        with patch(
            "server.api.scheduler_runtime.resolve_scheduler_script",
            return_value=scheduler_runtime.Path(
                "E:/definitely_missing_probiga_root/tools/missing.py"
            ),
        ), patch("server.api.scheduler_runtime.update_scheduler_task") as update_task:
            scheduler_runtime._run_task(row, scheduler_runtime.Path("E:/definitely_missing_probiga_root"), engine)

        update_task.assert_called_once()
        self.assertIs(update_task.call_args.args[0], engine)
        self.assertEqual(update_task.call_args.args[1], 7)
        values = update_task.call_args.args[2]
        self.assertEqual(values["last_run_status"], "failed")
        self.assertEqual(values["last_run_duration"], 0)
        self.assertIn("missing.py", values["last_run_output"])

    def test_run_task_finishes_history_when_script_policy_rejects(self):
        engine = MagicMock()
        row = {
            "id": 70,
            "task_name": "unsafe",
            "task_type": "daily_review",
            "script_path": "../outside.py",
        }

        with patch(
            "server.api.scheduler_runtime._task_history_start",
            return_value="run-70",
        ), patch(
            "server.api.scheduler_runtime._task_history_finish"
        ) as history_finish, patch(
            "server.api.scheduler_runtime.resolve_scheduler_script",
            side_effect=scheduler_runtime.SchedulerScriptPolicyError("outside root"),
        ), patch(
            "server.api.scheduler_runtime.update_scheduler_task"
        ) as update_task:
            scheduler_runtime._run_task(row, Path("E:/fake"), engine)

        final_values = update_task.call_args.args[2]
        self.assertEqual(final_values["last_run_status"], "failed")
        history_finish.assert_called_once()
        self.assertEqual(history_finish.call_args.kwargs["status"], "failed")
        self.assertEqual(history_finish.call_args.kwargs["exit_code"], 126)
        self.assertIn("SCHEDULER_SCRIPT_BLOCKED", history_finish.call_args.kwargs["output"])

    def test_history_output_redacts_secrets_before_truncating(self):
        output = (
            "prefix "
            + "x" * 5100
            + " password=hunter2 token:abc123 "
            + "Authorization: Bearer secret-token "
            + "mysql://alice:dbpass@db.example/probiga "
            + "https://example.test/?api_key=query-secret"
        )

        redacted = scheduler_runtime._redact_history_output(output)

        self.assertLessEqual(len(redacted), scheduler_runtime._HISTORY_OUTPUT_LIMIT)
        for secret in ("hunter2", "abc123", "secret-token", "dbpass", "query-secret"):
            self.assertNotIn(secret, redacted)
        self.assertIn("[REDACTED]", redacted)

    def test_history_start_records_metadata_without_task_arguments(self):
        engine = MagicMock()
        conn = MagicMock()
        ctx = MagicMock()
        ctx.__enter__.return_value = conn
        engine.begin.return_value = ctx
        row = {
            "id": 74,
            "task_name": "early briefing",
            "task_type": "news_daily",
            "script_args": "--token should-not-be-stored",
            "_trigger_source": "scheduled",
        }

        with patch("server.api.scheduler_runtime._ensure_task_history_table"):
            run_uid = scheduler_runtime._task_history_start(
                engine,
                row,
                run_uid="fixed-run-74",
            )

        self.assertEqual(run_uid, "fixed-run-74")
        params = conn.execute.call_args.args[1]
        self.assertEqual(params["task_id"], 74)
        self.assertEqual(params["task_type"], "news_daily")
        self.assertEqual(params["trigger_source"], "scheduled")
        self.assertNotIn("script_args", params)
        self.assertNotIn("should-not-be-stored", str(params))

    def test_legacy_history_upgrade_adds_required_indexes(self):
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__.return_value = conn
        columns_result = MagicMock()
        columns_result.fetchall.return_value = [
            (name,)
            for name in (
                "id",
                "run_uid",
                "task_id",
                "task_name",
                "task_type",
                "run_at",
                "finished_at",
                "status",
                "duration",
                "exit_code",
                "output",
                "host_name",
                "scheduler_instance_id",
                "trigger_source",
            )
        ]
        index_result = MagicMock()
        index_result.mappings.return_value.all.return_value = [
            {"Key_name": "PRIMARY", "Non_unique": 0, "Seq_in_index": 1, "Column_name": "id"}
        ]
        conn.execute.side_effect = [MagicMock(), columns_result, index_result, MagicMock(), MagicMock()]

        scheduler_runtime._ensure_task_history_table(engine)

        statements = [str(call.args[0]) for call in conn.execute.call_args_list]
        self.assertTrue(any("ADD UNIQUE INDEX" in sql and "`run_uid`" in sql for sql in statements))
        self.assertTrue(
            any("ADD INDEX" in sql and "`task_id`, `run_at`" in sql for sql in statements)
        )

    def test_history_upgrade_does_not_duplicate_equivalent_indexes(self):
        engine = MagicMock()
        conn = MagicMock()
        engine.begin.return_value.__enter__.return_value = conn
        columns_result = MagicMock()
        columns_result.fetchall.return_value = [
            (name,)
            for name in (
                "id",
                "run_uid",
                "task_id",
                "task_name",
                "task_type",
                "run_at",
                "finished_at",
                "status",
                "duration",
                "exit_code",
                "output",
                "host_name",
                "scheduler_instance_id",
                "trigger_source",
            )
        ]
        index_result = MagicMock()
        index_result.mappings.return_value.all.return_value = [
            {"Key_name": "run_uid_custom", "Non_unique": 0, "Seq_in_index": 1, "Column_name": "run_uid"},
            {"Key_name": "task_run_custom", "Non_unique": 1, "Seq_in_index": 1, "Column_name": "task_id"},
            {"Key_name": "task_run_custom", "Non_unique": 1, "Seq_in_index": 2, "Column_name": "run_at"},
        ]
        conn.execute.side_effect = [MagicMock(), columns_result, index_result]

        scheduler_runtime._ensure_task_history_table(engine)

        statements = [str(call.args[0]) for call in conn.execute.call_args_list]
        self.assertFalse(any("ADD UNIQUE INDEX" in sql for sql in statements))
        self.assertFalse(any("ADD INDEX" in sql for sql in statements))

    def test_history_retention_cleanup_is_bounded_and_throttled(self):
        engine = MagicMock()
        read_conn = MagicMock()
        table_result = MagicMock()
        table_result.fetchall.return_value = [
            ("st_scheduled_task_history",),
            ("sys_wecom_delivery_receipt",),
        ]
        read_conn.execute.return_value = table_result
        engine.connect.return_value.__enter__.return_value = read_conn
        delete_conn = MagicMock()
        delete_conn.execute.side_effect = [
            MagicMock(rowcount=12),
            MagicMock(rowcount=7),
        ]
        engine.begin.return_value.__enter__.return_value = delete_conn

        first = scheduler_runtime._maybe_cleanup_history(engine, monotonic_now=100.0)
        second = scheduler_runtime._maybe_cleanup_history(engine, monotonic_now=101.0)

        self.assertEqual(
            first,
            {"st_scheduled_task_history": 12, "sys_wecom_delivery_receipt": 7},
        )
        self.assertEqual(second, {})
        self.assertEqual(delete_conn.execute.call_count, 2)
        for call in delete_conn.execute.call_args_list:
            sql = str(call.args[0])
            self.assertIn(f"INTERVAL {scheduler_runtime.HISTORY_RETENTION_DAYS} DAY", sql)
            self.assertIn(f"LIMIT {scheduler_runtime.HISTORY_CLEANUP_BATCH_SIZE}", sql)

    def test_history_cleanup_failure_is_throttled(self):
        engine = MagicMock()
        engine.connect.side_effect = RuntimeError("database unavailable")

        self.assertEqual(
            scheduler_runtime._maybe_cleanup_history(engine, monotonic_now=200.0),
            {},
        )
        self.assertEqual(
            scheduler_runtime._maybe_cleanup_history(engine, monotonic_now=201.0),
            {},
        )
        self.assertEqual(engine.connect.call_count, 1)

    def test_history_finish_persists_redacted_terminal_summary(self):
        engine = MagicMock()
        conn = MagicMock()
        ctx = MagicMock()
        ctx.__enter__.return_value = conn
        engine.begin.return_value = ctx

        scheduler_runtime._task_history_finish(
            engine,
            "fixed-run-75",
            status="failed",
            duration=22,
            exit_code=1,
            output="request failed token=private-token",
        )

        params = conn.execute.call_args.args[1]
        self.assertEqual(params["run_uid"], "fixed-run-75")
        self.assertEqual(params["status"], "failed")
        self.assertEqual(params["duration"], 22)
        self.assertEqual(params["exit_code"], 1)
        self.assertNotIn("private-token", params["output"])
        self.assertIn("[REDACTED]", params["output"])

    def test_run_task_finishes_history_on_success(self):
        engine = MagicMock()
        row = {
            "id": 71,
            "task_name": "early briefing",
            "task_type": "news_daily",
            "script_path": "biz/early_briefing/generate.py",
            "script_args": "",
            "date_param": "",
            "interval_minutes": 0,
        }
        fake_proc = MagicMock()
        fake_proc.communicate.return_value = ("sent", "")
        fake_proc.returncode = 0

        with patch(
            "server.api.scheduler_runtime._task_history_start",
            return_value="run-71",
        ) as history_start, patch(
            "server.api.scheduler_runtime._task_history_finish"
        ) as history_finish, patch(
            "server.api.scheduler_runtime.resolve_scheduler_script",
            return_value=Path("E:/fake/generate.py"),
        ), patch.object(
            Path,
            "exists",
            return_value=True,
        ), patch(
            "server.api.scheduler_runtime.build_child_env",
            return_value={},
        ), patch(
            "server.api.scheduler_runtime._build_task_args",
            return_value=[],
        ), patch(
            "server.api.scheduler_runtime.subprocess.Popen",
            return_value=fake_proc,
        ), patch(
            "server.api.scheduler_runtime.update_scheduler_task"
        ):
            scheduler_runtime._run_task(row, Path("E:/fake"), engine)

        history_start.assert_called_once_with(engine, row, run_uid=None)
        history_finish.assert_called_once()
        self.assertEqual(history_finish.call_args.args[:2], (engine, "run-71"))
        self.assertEqual(history_finish.call_args.kwargs["status"], "success")
        self.assertEqual(history_finish.call_args.kwargs["exit_code"], 0)
        self.assertIn("sent", history_finish.call_args.kwargs["output"])

    def test_run_task_finishes_history_before_timeout_return(self):
        engine = MagicMock()
        row = {
            "id": 72,
            "task_name": "evening briefing",
            "task_type": "evening_review",
            "script_path": "biz/evening_review/generate.py",
            "script_args": "",
            "date_param": "",
            "interval_minutes": 0,
        }
        fake_proc = MagicMock()
        fake_proc.communicate.side_effect = [
            scheduler_runtime.subprocess.TimeoutExpired("cmd", 60),
            ("partial", "timed out"),
        ]
        fake_proc.returncode = -9

        with patch(
            "server.api.scheduler_runtime._task_history_start",
            return_value="run-72",
        ), patch(
            "server.api.scheduler_runtime._task_history_finish"
        ) as history_finish, patch(
            "server.api.scheduler_runtime.resolve_scheduler_script",
            return_value=Path("E:/fake/generate.py"),
        ), patch.object(
            Path,
            "exists",
            return_value=True,
        ), patch(
            "server.api.scheduler_runtime.build_child_env",
            return_value={},
        ), patch(
            "server.api.scheduler_runtime._build_task_args",
            return_value=[],
        ), patch(
            "server.api.scheduler_runtime.subprocess.Popen",
            return_value=fake_proc,
        ), patch(
            "server.api.scheduler_runtime.update_scheduler_task"
        ):
            scheduler_runtime._run_task(row, Path("E:/fake"), engine)

        history_finish.assert_called_once()
        self.assertEqual(history_finish.call_args.kwargs["status"], "timeout")
        self.assertEqual(history_finish.call_args.kwargs["exit_code"], -9)
        self.assertIn("partial", history_finish.call_args.kwargs["output"])

    def test_run_task_finishes_history_when_setup_raises(self):
        engine = MagicMock()
        row = {
            "id": 73,
            "task_name": "daily review",
            "task_type": "daily_review",
            "script_path": "biz/review/generate.py",
            "script_args": "",
            "date_param": "",
            "interval_minutes": 0,
        }

        with patch(
            "server.api.scheduler_runtime._task_history_start",
            return_value="run-73",
        ), patch(
            "server.api.scheduler_runtime._task_history_finish"
        ) as history_finish, patch(
            "server.api.scheduler_runtime.resolve_scheduler_script",
            side_effect=RuntimeError("resolver unavailable"),
        ), patch(
            "server.api.scheduler_runtime.update_scheduler_task"
        ) as update_task:
            scheduler_runtime._run_task(row, Path("E:/fake"), engine)

        final_values = update_task.call_args.args[2]
        self.assertEqual(final_values["last_run_status"], "failed")
        history_finish.assert_called_once()
        self.assertEqual(history_finish.call_args.kwargs["status"], "failed")
        self.assertIsNone(history_finish.call_args.kwargs["exit_code"])
        self.assertIn("resolver unavailable", history_finish.call_args.kwargs["output"])

    def test_run_task_marks_success_failed_when_data_validation_fails(self):
        engine = MagicMock()
        row = {
            "id": 7,
            "task_name": "daily kline",
            "task_type": "stock_kline",
            "script_path": "tools/job.py",
            "script_args": "",
            "date_param": "",
            "interval_minutes": 0,
        }
        fake_proc = MagicMock()
        fake_proc.communicate.return_value = ("ok", "")
        fake_proc.returncode = 0

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            script = root / "tools" / "job.py"
            script.parent.mkdir(parents=True)
            script.write_text("print('ok')", encoding="utf-8")
            with patch("server.api.scheduler_runtime.update_scheduler_task") as update_task, patch(
                "server.api.scheduler_runtime.resolve_scheduler_script",
                return_value=script,
            ), patch(
                "server.api.scheduler_runtime.build_child_env",
                return_value={"MYSQL_URL": "mysql://example"},
            ), patch(
                "server.api.scheduler_runtime._build_task_args",
                return_value=[],
            ), patch("server.api.scheduler_runtime.subprocess.Popen", return_value=fake_proc), patch(
                "server.api.scheduler_runtime.validate_scheduler_task_result",
                return_value=SchedulerValidationResult(checked=True, ok=False, message="sm_stock_kline: only 0 rows"),
            ):
                scheduler_runtime._run_task(row, root, engine)

        final_values = update_task.call_args_list[-1].args[2]
        self.assertEqual(final_values["last_run_status"], "failed")
        self.assertIn("DATA_VALIDATION_FAILED: sm_stock_kline: only 0 rows", final_values["last_run_output"])

    def test_run_task_preserves_nonzero_level1_block_state(self):
        engine = MagicMock()
        row = {
            "id": 67,
            "task_name": "Level1 continuity",
            "task_type": "trading_v2_level1_validation",
            "script_path": "tools/validate_trading_v2_level1.py",
            "script_args": "",
            "date_param": "",
            "interval_minutes": 0,
        }
        fake_proc = MagicMock()
        fake_proc.communicate.return_value = (
            json.dumps({
                "status": "BLOCK",
                "consecutive_trade_days": 0,
                "evidence": {"details": "x" * 6000},
            }),
            "",
        )
        fake_proc.returncode = 3

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            script = root / "tools" / "validate_trading_v2_level1.py"
            script.parent.mkdir(parents=True)
            script.write_text("print('BLOCK')", encoding="utf-8")
            with patch(
                "server.api.scheduler_runtime.update_scheduler_task"
            ) as update_task, patch(
                "server.api.scheduler_runtime.resolve_scheduler_script",
                return_value=script,
            ), patch(
                "server.api.scheduler_runtime.build_child_env",
                return_value={"MYSQL_URL": "mysql://example"},
            ), patch(
                "server.api.scheduler_runtime._build_task_args",
                return_value=[],
            ), patch(
                "server.api.scheduler_runtime.subprocess.Popen",
                return_value=fake_proc,
            ), patch(
                "server.api.scheduler_runtime.validate_scheduler_task_result"
            ) as validate_result:
                scheduler_runtime._run_task(row, root, engine)

        final_values = update_task.call_args_list[-1].args[2]
        self.assertEqual(final_values["last_run_status"], "blocked")
        validate_result.assert_not_called()

    def test_concept_source_block_is_not_recorded_as_success_or_failure(self):
        from server.common.scheduler_validation import scheduler_output_status

        status = scheduler_output_status(
            {"task_type": "concept_constituent_east"},
            json.dumps({
                "status": "BLOCK",
                "reason": "external_concept_source_unavailable",
            }),
        )

        self.assertEqual(status, "blocked")

    def test_scheduler_quality_uses_cache_unless_forced(self):
        reports = [
            {"status": "PASS", "trade_date": "2026-07-01", "checks": []},
            {"status": "WARN", "trade_date": "2026-07-01", "checks": []},
        ]
        with patch("server.api.routers.scheduler.get_engine", return_value=object()), patch(
            "tools.data_quality_check.run_checks",
            side_effect=reports,
        ) as run_checks:
            first = scheduler_router.scheduler_quality(trade_date="2026-07-01", fast=False)
            second = scheduler_router.scheduler_quality(trade_date="2026-07-01", fast=False)
            forced = scheduler_router.scheduler_quality(trade_date="2026-07-01", force=True, fast=False)

        self.assertFalse(first["cached"])
        self.assertTrue(second["cached"])
        self.assertEqual(second["status"], "PASS")
        self.assertFalse(forced["cached"])
        self.assertEqual(forced["status"], "WARN")
        self.assertEqual(run_checks.call_count, 2)

    def test_scheduler_quality_defaults_to_fast_mode(self):
        with patch("server.api.routers.scheduler.get_engine", return_value=object()), patch(
            "server.api.routers.scheduler._scheduler_quality_fast",
            return_value={"status": "PASS", "trade_date": "2026-07-01", "checks": [], "mode": "fast"},
        ) as fast_quality, patch("tools.data_quality_check.run_checks") as run_checks:
            payload = scheduler_router.scheduler_quality(trade_date="2026-07-01")

        self.assertEqual(payload["mode"], "fast")
        self.assertFalse(payload["cached"])
        fast_quality.assert_called_once()
        run_checks.assert_not_called()

    def test_scheduler_tasks_payload_reports_restart_safe_runtime(self):
        with patch("server.api.routers.scheduler._read_sql", return_value=[]), patch(
            "server.api.routers.scheduler.scheduler_runtime_info",
            return_value={
                "embedded_scheduler_enabled": False,
                "embedded_scheduler_running": False,
                "scheduler_max_concurrent_tasks": 1,
                "scheduler_poll_seconds": 60,
                "api_mysql_pool_size": 2,
                "api_mysql_max_overflow": 1,
                "api_mysql_pool_recycle": 1800,
            },
        ), patch(
            "server.api.routers.scheduler.read_scheduler_heartbeat",
            return_value={
                "instance_id": "host-123",
                "mode": "standalone",
                "host_name": "host",
                "pid": 123,
                "heartbeat_at": "2026-06-28 09:31:00",
                "heartbeat_age_seconds": 10,
            },
        ):
            payload = scheduler_router.list_tasks()

        self.assertEqual(payload["data"], [])
        self.assertTrue(payload["runtime"]["standalone_scheduler_online"])
        self.assertTrue(payload["runtime"]["api_restart_safe"])
        self.assertIn("重启 API 服务不会中断", payload["runtime"]["status_text"])

    def test_scheduler_tasks_payload_warns_when_embedded_scheduler_runs(self):
        with patch("server.api.routers.scheduler._read_sql", return_value=[]), patch(
            "server.api.routers.scheduler.scheduler_runtime_info",
            return_value={
                "embedded_scheduler_enabled": True,
                "embedded_scheduler_running": True,
                "scheduler_max_concurrent_tasks": 1,
                "scheduler_poll_seconds": 60,
                "api_mysql_pool_size": 2,
                "api_mysql_max_overflow": 1,
                "api_mysql_pool_recycle": 1800,
            },
        ), patch("server.api.routers.scheduler.read_scheduler_heartbeat", return_value=None):
            payload = scheduler_router.list_tasks()

        self.assertFalse(payload["runtime"]["api_restart_safe"])
        self.assertIn("重启 API 会中断", payload["runtime"]["status_text"])


if __name__ == "__main__":
    unittest.main()


def test_non_trading_day_skips_all_intraday_and_postmarket_heavy_tasks():
    task_types = {
        "intraday_realtime",
        "intraday_quality_check",
        "qmt_intraday_realtime",
        "sim_trade",
        "sim_trade_signal_prepare",
        "capital_flow_batch_fast",
        "stock_minute_flow",
        "stock_snapshot_daily",
        "market_overview_daily",
        "analysis_fast",
        "trading_v2_level1_validation",
    }
    with patch("server.api.scheduler_runtime._is_trade_day", return_value=False):
        for task_type in task_types:
            assert scheduler_runtime._should_skip_non_trading_day(
                {"task_type": task_type, "script_path": "", "script_args": ""},
                object(),
                datetime(2026, 7, 18, 10, 0),
            )
