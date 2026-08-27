# -*- coding: utf-8 -*-
import hashlib
import inspect
import json
import threading
import tempfile
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from server.api import scheduler_runtime
from server.api.routers import scheduler as scheduler_router
from server.common import release_data_readiness_contract as readiness_contract
from server.common.scheduler_validation import SchedulerValidationResult


def _governance_not_ready_payload() -> dict:
    return {
        "status": "blocked",
        "orchestration_status": "NOT_READY",
        "reason_code": "INPUT_NOT_READY",
        "error_class": "NOT_READY",
        "retryable": True,
        "input_ready": False,
        "reason": "权威交易日数据尚未就绪",
        "blocking_stage": "input_readiness",
        "target_trade_date": "2026-08-21",
        "requested_trade_date": "",
        "input_trade_date": "2026-08-20",
        "allocations": [{
            "target_type": "CASH",
            "simulated_weight_pct": 100.0,
            "real_order_authority": False,
        }],
        "automatic_real_order_submission": False,
        "real_order_authority": False,
    }


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
        scheduler_runtime._running_history_uids.clear()
        scheduler_runtime._stop_pending_task_ids.clear()
        scheduler_runtime._stop_requested_task_ids.clear()
        scheduler_runtime._timeout_pending_task_ids.clear()
        scheduler_runtime._timeout_requested_task_ids.clear()
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

    def test_linux_scheduler_heartbeat_binds_build_and_executor_role(self):
        engine = MagicMock()
        connection = engine.begin.return_value.__enter__.return_value
        build_sha = "a" * 40
        with patch(
            "server.api.scheduler_runtime.get_scheduler_runtime_config",
            return_value={"poll_seconds": 30, "max_concurrent_tasks": 2},
        ), patch(
            "server.api.scheduler_runtime.os.name", "posix"
        ), patch.dict(
            "server.api.scheduler_runtime.os.environ",
            {"PROBIGA_BUILD_COMMIT_SHA": build_sha},
            clear=False,
        ):
            scheduler_runtime._write_scheduler_heartbeat(engine, "standalone")

        sql = str(connection.execute.call_args.args[0])
        params = connection.execute.call_args.args[1]
        self.assertIn("build_sha", sql)
        self.assertIn("executor_role", sql)
        self.assertEqual(params["build_sha"], build_sha)
        self.assertEqual(params["executor_role"], "linux_standalone")
        self.assertNotIn("CREATE TABLE", sql.upper())

    def test_standalone_dispatch_authority_rejects_duplicate_fresh_executors(self):
        engine = MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        build_sha = "a" * 40
        common = {
            "mode": "standalone",
            "host_name": "scheduler-host",
            "build_sha": build_sha,
            "executor_role": "linux_standalone",
            "started_at": datetime(2026, 8, 25, 9, 0),
            "heartbeat_at": datetime(2026, 8, 25, 9, 1),
            "heartbeat_age_seconds": 1,
            "poll_seconds": 30,
            "max_concurrent_tasks": 2,
        }
        connection.execute.return_value.mappings.return_value = [
            {
                **common,
                "instance_id": "scheduler-host-123",
                "pid": 123,
            },
            {
                **common,
                "instance_id": "scheduler-host-456",
                "pid": 456,
            },
        ]
        with patch(
            "server.api.scheduler_runtime.os.name", "posix"
        ), patch(
            "server.api.scheduler_runtime.os.getpid", return_value=123
        ), patch(
            "server.api.scheduler_runtime.gethostname",
            return_value="scheduler-host",
        ), patch.dict(
            "server.api.scheduler_runtime.os.environ",
            {"PROBIGA_BUILD_COMMIT_SHA": build_sha},
            clear=False,
        ):
            passed, detail = scheduler_runtime._standalone_heartbeat_allows_dispatch(
                engine,
                "standalone",
            )

        self.assertFalse(passed)
        self.assertIn("fresh_heartbeat_not_unique", detail["errors"])
        self.assertEqual(detail["fresh_row_count"], 2)

    def test_standalone_heartbeat_write_failure_prevents_all_dispatch(self):
        stop_event = MagicMock()
        stop_event.is_set.return_value = False
        stop_event.wait.return_value = True
        engine = MagicMock()
        with patch(
            "server.api.scheduler_runtime._catchup_on_startup"
        ), patch(
            "server.api.scheduler_runtime.get_engine", return_value=engine
        ), patch(
            "server.api.scheduler_runtime.get_scheduler_runtime_config",
            return_value={"poll_seconds": 15, "max_concurrent_tasks": 1},
        ), patch(
            "server.api.scheduler_runtime._write_scheduler_heartbeat",
            side_effect=RuntimeError("heartbeat unavailable"),
        ), patch(
            "server.api.scheduler_runtime._cleanup_stale_running_tasks"
        ) as stale_cleanup, patch(
            "server.api.scheduler_runtime._maybe_cleanup_history"
        ) as cleanup_history, patch(
            "server.api.scheduler_runtime._claim_task_run"
        ) as claim, patch(
            "server.api.scheduler_runtime.threading.Thread"
        ) as thread_cls:
            scheduler_runtime._check_and_run_tasks(
                mode="standalone",
                stop_event=stop_event,
            )

        cleanup_history.assert_not_called()
        stale_cleanup.assert_not_called()
        claim.assert_not_called()
        thread_cls.assert_not_called()
        engine.connect.assert_not_called()

    def test_standalone_duplicate_identity_prevents_claim_and_worker(self):
        stop_event = MagicMock()
        stop_event.is_set.return_value = False
        stop_event.wait.return_value = True
        engine = MagicMock()
        with patch(
            "server.api.scheduler_runtime._catchup_on_startup"
        ), patch(
            "server.api.scheduler_runtime.get_engine", return_value=engine
        ), patch(
            "server.api.scheduler_runtime.get_scheduler_runtime_config",
            return_value={"poll_seconds": 15, "max_concurrent_tasks": 1},
        ), patch(
            "server.api.scheduler_runtime._write_scheduler_heartbeat"
        ), patch(
            "server.api.scheduler_runtime._standalone_heartbeat_allows_dispatch",
            return_value=(
                False,
                {"errors": ["fresh_heartbeat_not_unique"]},
            ),
        ), patch(
            "server.api.scheduler_runtime._cleanup_stale_running_tasks"
        ) as stale_cleanup, patch(
            "server.api.scheduler_runtime._maybe_cleanup_history"
        ) as cleanup_history, patch(
            "server.api.scheduler_runtime._claim_task_run"
        ) as claim, patch(
            "server.api.scheduler_runtime.threading.Thread"
        ) as thread_cls:
            scheduler_runtime._check_and_run_tasks(
                mode="standalone",
                stop_event=stop_event,
            )

        cleanup_history.assert_not_called()
        stale_cleanup.assert_not_called()
        claim.assert_not_called()
        thread_cls.assert_not_called()
        engine.connect.assert_not_called()

    def test_standalone_authority_rejects_malformed_future_heartbeat(self):
        engine = MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        build_sha = "a" * 40
        common = {
            "mode": "standalone",
            "host_name": "scheduler-host",
            "build_sha": build_sha,
            "executor_role": "linux_standalone",
            "started_at": datetime(2026, 8, 25, 9, 0),
            "heartbeat_at": datetime(2026, 8, 25, 9, 1),
            "poll_seconds": 30,
            "max_concurrent_tasks": 2,
        }
        connection.execute.return_value.mappings.return_value = [
            {
                **common,
                "instance_id": "scheduler-host-123",
                "pid": 123,
                "heartbeat_age_seconds": 1,
            },
            {
                **common,
                "instance_id": "other-host-456",
                "host_name": "other-host",
                "pid": 456,
                "heartbeat_age_seconds": -60,
                "poll_seconds": None,
            },
        ]
        with patch(
            "server.api.scheduler_runtime.os.name", "posix"
        ), patch(
            "server.api.scheduler_runtime.os.getpid", return_value=123
        ), patch(
            "server.api.scheduler_runtime.gethostname",
            return_value="scheduler-host",
        ), patch(
            "server.api.scheduler_runtime.get_scheduler_runtime_config",
            return_value={"poll_seconds": 30, "max_concurrent_tasks": 2},
        ), patch.dict(
            "server.api.scheduler_runtime.os.environ",
            {"PROBIGA_BUILD_COMMIT_SHA": build_sha},
            clear=False,
        ):
            passed, detail = scheduler_runtime._standalone_heartbeat_allows_dispatch(
                engine,
                "standalone",
            )

        self.assertFalse(passed)
        self.assertIn("future_heartbeat_present", detail["errors"])
        self.assertEqual(detail["future_row_count"], 1)

    def test_standalone_authority_requires_exact_configured_poll(self):
        engine = MagicMock()
        connection = engine.connect.return_value.__enter__.return_value
        build_sha = "a" * 40
        row = {
            "instance_id": "scheduler-host-123",
            "mode": "standalone",
            "host_name": "scheduler-host",
            "pid": 123,
            "build_sha": build_sha,
            "executor_role": "linux_standalone",
            "started_at": datetime(2026, 8, 25, 9, 0),
            "heartbeat_at": datetime(2026, 8, 25, 9, 1),
            "heartbeat_age_seconds": 1,
            "poll_seconds": 30,
            "max_concurrent_tasks": 2,
        }
        connection.execute.return_value.mappings.return_value = [row]
        with patch(
            "server.api.scheduler_runtime.os.name", "posix"
        ), patch(
            "server.api.scheduler_runtime.os.getpid", return_value=123
        ), patch(
            "server.api.scheduler_runtime.gethostname",
            return_value="scheduler-host",
        ), patch(
            "server.api.scheduler_runtime.get_scheduler_runtime_config",
            return_value={"poll_seconds": 30, "max_concurrent_tasks": 2},
        ), patch.dict(
            "server.api.scheduler_runtime.os.environ",
            {"PROBIGA_BUILD_COMMIT_SHA": build_sha},
            clear=False,
        ):
            passed, detail = scheduler_runtime._standalone_heartbeat_allows_dispatch(
                engine,
                "standalone",
            )
            row["poll_seconds"] = 60
            rejected, mismatch = (
                scheduler_runtime._standalone_heartbeat_allows_dispatch(
                    engine,
                    "standalone",
                )
            )

        self.assertTrue(passed, detail)
        self.assertFalse(rejected)
        self.assertIn("poll_seconds_mismatch", mismatch["errors"])

    def test_scheduled_claim_without_audit_row_never_starts_worker(self):
        stop_event = MagicMock()
        stop_event.is_set.return_value = False
        stop_event.wait.return_value = True
        engine = MagicMock()
        result = MagicMock()
        result.keys.return_value = [
            "id",
            "task_name",
            "task_type",
            "script_path",
            "script_args",
            "cron_time",
            "interval_minutes",
            "enabled",
            "date_param",
            "last_run_at",
            "last_triggered_at",
            "last_run_status",
            "last_run_duration",
        ]
        result.fetchall.return_value = [
            (
                901,
                "audited task",
                "analysis_fast",
                "biz/analysis/sync_analysis_fast.py",
                "",
                "00:00",
                1,
                1,
                "",
                None,
                None,
                "",
                0,
            )
        ]
        engine.connect.return_value.__enter__.return_value.execute.return_value = result
        with patch(
            "server.api.scheduler_runtime.get_engine", return_value=engine
        ), patch(
            "server.api.scheduler_runtime.get_scheduler_runtime_config",
            return_value={"poll_seconds": 15, "max_concurrent_tasks": 1},
        ), patch(
            "server.api.scheduler_runtime._write_scheduler_heartbeat"
        ), patch(
            "server.api.scheduler_runtime._standalone_heartbeat_allows_dispatch",
            return_value=(True, {"errors": []}),
        ), patch(
            "server.api.scheduler_runtime._cleanup_stale_running_tasks"
        ), patch(
            "server.api.scheduler_runtime._maybe_cleanup_history"
        ), patch(
            "server.api.scheduler_runtime._should_skip_task_for_host",
            return_value=False,
        ), patch(
            "server.api.scheduler_runtime._strategy_pipeline_dependencies_ready",
            return_value=(True, "ready"),
        ), patch(
            "server.api.scheduler_runtime._release_build_catchup_pending",
            return_value=False,
        ), patch(
            "server.api.scheduler_runtime._should_skip_non_trading_day",
            return_value=False,
        ), patch(
            "server.api.scheduler_runtime._should_skip_outside_intraday_window",
            return_value=False,
        ), patch(
            "server.api.scheduler_runtime._scheduler_lane_has_capacity",
            return_value=True,
        ), patch(
            "server.api.scheduler_runtime._claim_task_run",
            return_value=True,
        ), patch(
            "server.api.scheduler_runtime._task_history_start",
            return_value=None,
        ), patch(
            "server.api.scheduler_runtime.update_scheduler_task"
        ) as update_task, patch(
            "server.api.scheduler_runtime.threading.Thread"
        ) as thread_cls:
            scheduler_runtime._check_and_run_tasks(
                mode="standalone",
                stop_event=stop_event,
            )

        thread_cls.assert_not_called()
        self.assertEqual(
            update_task.call_args.args[2]["last_run_status"],
            "failed",
        )
        self.assertNotIn(901, scheduler_runtime._running_task_ids)

    def test_scheduler_heartbeat_never_labels_unconfigured_windows_as_qmt_edge(self):
        with patch(
            "server.api.scheduler_runtime.os.name", "nt"
        ), patch.dict(
            "server.api.scheduler_runtime.os.environ", {}, clear=True
        ):
            self.assertEqual(
                scheduler_runtime._scheduler_executor_role("standalone"),
                "unclassified_scheduler",
            )
            self.assertEqual(
                scheduler_runtime._scheduler_build_commit_sha(), "0" * 40
            )

    def test_manual_launch_rejects_task_owned_by_other_host_before_claim(self):
        row = {
            "id": 801,
            "task_name": "Windows QMT task",
            "task_type": "qmt_local_history_2024",
            "enabled": 1,
        }
        with patch(
            "server.api.scheduler_runtime._should_skip_task_for_host",
            return_value=True,
        ), patch(
            "server.api.scheduler_runtime._claim_task_run"
        ) as claim, patch(
            "server.api.scheduler_runtime.threading.Thread"
        ) as thread_cls:
            result = scheduler_runtime.launch_scheduler_task(
                row,
                root=Path("E:/fake"),
                engine=MagicMock(),
            )

        self.assertEqual(result["status"], "delegated_to_other_host")
        self.assertFalse(result["accepted"])
        claim.assert_not_called()
        thread_cls.assert_not_called()

    def test_production_manual_launch_requires_release_build_identity(self):
        row = {
            "id": 802,
            "task_name": "Linux task",
            "task_type": "analysis_fast",
            "enabled": 1,
        }
        with patch(
            "server.api.scheduler_runtime._should_skip_task_for_host",
            return_value=False,
        ), patch.dict(
            "server.api.scheduler_runtime.os.environ",
            {"PROBIGA_DEPLOYMENT_MODE": "production"},
            clear=True,
        ), patch(
            "server.api.scheduler_runtime._claim_task_run"
        ) as claim:
            result = scheduler_runtime.launch_scheduler_task(
                row,
                root=Path("E:/fake"),
                engine=MagicMock(),
            )

        self.assertEqual(result["status"], "build_identity_unavailable")
        self.assertFalse(result["accepted"])
        claim.assert_not_called()

    def test_scheduler_run_api_uses_claimed_audited_launcher(self):
        row = {
            "id": 803,
            "task_name": "audited task",
            "task_type": "analysis_fast",
            "enabled": 1,
        }
        expected = {
            "accepted": True,
            "status": "running",
            "task_id": 803,
        }
        engine = MagicMock()
        with patch.object(
            scheduler_router,
            "_read_sql",
            return_value=[row],
        ), patch.object(
            scheduler_router,
            "get_engine",
            return_value=engine,
        ), patch.object(
            scheduler_router,
            "launch_scheduler_task",
            return_value=expected,
        ) as launch:
            result = scheduler_router.run_task_now(803)

        self.assertEqual(result, expected)
        self.assertEqual(launch.call_args.args[0], row)
        self.assertIs(launch.call_args.kwargs["engine"], engine)

    def test_scheduler_stop_api_cannot_mutate_other_host_task(self):
        row = {
            "id": 804,
            "task_name": "Windows QMT task",
            "task_type": "qmt_local_history_2024",
            "script_path": "tools/run_guojin_qmt_full_market_history.py",
            "last_run_status": "running",
        }
        with patch.object(
            scheduler_router,
            "_read_sql",
            return_value=[row],
        ), patch.object(
            scheduler_router,
            "scheduler_task_owned_by_current_host",
            return_value=False,
        ), patch.object(
            scheduler_router,
            "update_scheduler_task",
        ) as update_task:
            result = scheduler_router.stop_task(804)

        self.assertEqual(result["status"], "delegated_to_other_host")
        self.assertFalse(result["process_killed"])
        update_task.assert_not_called()

    def test_scheduler_stop_api_refuses_same_host_foreign_process(self):
        row = {
            "id": 805,
            "task_name": "standalone-owned task",
            "task_type": "analysis_fast",
            "script_path": "tools/run_analysis_fast.py",
            "last_run_status": "running",
        }
        expected = {
            "accepted": False,
            "status": "not_owned_by_api_process",
            "task_id": 805,
            "process_killed": False,
        }
        with patch.object(
            scheduler_router, "_read_sql", return_value=[row]
        ), patch.object(
            scheduler_router,
            "scheduler_task_owned_by_current_host",
            return_value=True,
        ), patch.object(
            scheduler_router,
            "request_stop_owned_scheduler_task",
            return_value=expected,
        ) as request_stop, patch.object(
            scheduler_router, "update_scheduler_task"
        ) as update_task:
            result = scheduler_router.stop_task(805)

        self.assertEqual(result["status"], "not_owned_by_api_process")
        self.assertFalse(result["accepted"])
        request_stop.assert_called_once_with(805)
        update_task.assert_not_called()

    def test_stop_request_requires_exact_live_api_child_and_audit_uid(self):
        scheduler_runtime._running_task_ids.add(806)

        result = scheduler_runtime.request_stop_owned_scheduler_task(806)

        self.assertEqual(result["status"], "not_owned_by_api_process")
        self.assertFalse(result["accepted"])
        self.assertNotIn(806, scheduler_runtime._stop_requested_task_ids)

    def test_stop_request_kill_failure_keeps_owner_state_and_clears_request(self):
        task_id = 807
        proc = MagicMock()
        proc.poll.return_value = None
        scheduler_runtime._running_task_ids.add(task_id)
        scheduler_runtime._running_procs[task_id] = proc
        scheduler_runtime._running_history_uids[task_id] = "run-807"

        with patch(
            "server.api.scheduler_runtime._terminate_process_and_confirm",
            return_value=False,
        ):
            result = scheduler_runtime.request_stop_owned_scheduler_task(task_id)

        self.assertEqual(result["status"], "stop_not_confirmed")
        self.assertFalse(result["accepted"])
        self.assertIs(scheduler_runtime._running_procs[task_id], proc)
        self.assertIn(task_id, scheduler_runtime._running_task_ids)
        self.assertNotIn(task_id, scheduler_runtime._stop_pending_task_ids)
        self.assertNotIn(task_id, scheduler_runtime._stop_requested_task_ids)

    def test_stop_request_confirmed_keeps_worker_as_terminal_state_writer(self):
        task_id = 808
        proc = MagicMock()
        proc.poll.return_value = None
        scheduler_runtime._running_task_ids.add(task_id)
        scheduler_runtime._running_procs[task_id] = proc
        scheduler_runtime._running_history_uids[task_id] = "run-808"

        with patch(
            "server.api.scheduler_runtime._terminate_process_and_confirm",
            return_value=True,
        ) as terminate:
            result = scheduler_runtime.request_stop_owned_scheduler_task(task_id)

        self.assertEqual(result["status"], "stop_requested")
        self.assertTrue(result["accepted"])
        self.assertEqual(result["job_id"], "run-808")
        self.assertTrue(result["process_killed"])
        self.assertIn(task_id, scheduler_runtime._stop_requested_task_ids)
        self.assertIs(scheduler_runtime._running_procs[task_id], proc)
        terminate.assert_called_once_with(proc, timeout_seconds=5.0)

    def test_stop_confirmation_race_does_not_relabel_natural_completion(self):
        task_id = 811
        proc = MagicMock()
        proc.poll.return_value = None
        scheduler_runtime._running_task_ids.add(task_id)
        scheduler_runtime._running_procs[task_id] = proc
        scheduler_runtime._running_history_uids[task_id] = "run-811"

        def finish_naturally(_proc, *, timeout_seconds):
            self.assertIs(_proc, proc)
            self.assertEqual(timeout_seconds, 5.0)
            with scheduler_runtime._running_lock:
                scheduler_runtime._running_procs.pop(task_id, None)
                scheduler_runtime._running_history_uids.pop(task_id, None)
                scheduler_runtime._running_task_ids.discard(task_id)
            return True

        with patch(
            "server.api.scheduler_runtime._terminate_process_and_confirm",
            side_effect=finish_naturally,
        ):
            result = scheduler_runtime.request_stop_owned_scheduler_task(task_id)

        self.assertEqual(result["status"], "stop_raced_with_terminal_state")
        self.assertFalse(result["accepted"])
        self.assertNotIn(task_id, scheduler_runtime._stop_pending_task_ids)
        self.assertNotIn(task_id, scheduler_runtime._stop_requested_task_ids)

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

    def test_intraday_capital_flow_fast_is_latency_sensitive_and_linux_owned(self):
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
        self.assertFalse(
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
            "qmt_catalog_capability_refresh",
            "qmt_intraday_realtime",
            "qmt_membership_snapshot",
            "qmt_announcement_pit",
            "qmt_local_gap_repair_execute",
            "qmt_local_history_2024",
            "qmt_reference_incremental",
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
                {"task_type": "intraday_realtime"},
                platform_name="posix",
            )
        )

    def test_scheduler_host_ownership_prevents_cross_host_execution(self):
        qmt_task = {"task_type": "qmt_membership_snapshot"}
        linux_task = {"task_type": "analysis_fast"}
        unfrozen_task = {"task_type": "stock_kline"}
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
        self.assertTrue(
            scheduler_runtime._should_skip_task_for_host(
                unfrozen_task, platform_name="posix"
            )
        )
        self.assertTrue(
            scheduler_runtime._should_skip_task_for_host(
                unfrozen_task, platform_name="nt"
            )
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
            "script_path": "tools/sync_news_formal.py",
            "script_args": "--pages 2 --json",
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
        self.assertEqual(
            scheduler_runtime._build_task_args(
                row,
                row["script_path"],
                "2026-08-16",
            ),
            ["--limit", "10000", "--max-batches", "10"],
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

    def test_daily_derived_tasks_receive_strict_scheduler_target_date(self):
        today = "2026-07-01"
        snapshot = {"task_type": "stock_snapshot_daily", "script_args": "", "date_param": ""}
        overview = {"task_type": "market_overview_daily", "script_args": "", "date_param": ""}

        self.assertEqual(
            scheduler_runtime._build_task_args(snapshot, "biz/stock_market/sync_stock_snapshot.py", today),
            ["--date", today],
        )
        self.assertEqual(
            scheduler_runtime._build_task_args(overview, "tools/refresh_market_overview_daily.py", today),
            [today],
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

    def test_full_qmt_history_timeout_covers_the_seven_hour_window(self):
        for row in (
            {
                "task_type": "qmt_local_history_2024",
                "script_path": "tools/run_guojin_qmt_full_market_history.py",
                "interval_minutes": 0,
            },
            {
                "task_type": "legacy_name",
                "script_path": (
                    "tools/run_guojin_qmt_full_market_history_2024.py"
                ),
                "interval_minutes": 0,
            },
        ):
            self.assertEqual(
                scheduler_runtime._task_timeout_minutes(row),
                scheduler_runtime.QMT_FULL_HISTORY_TASK_TIMEOUT_MINUTES,
            )

    def test_detached_job_logs_use_external_protected_runtime_root(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            code_root = base / "sealed-release"
            log_root = base / "state" / "jobs"
            code_root.mkdir()
            proc = MagicMock(pid=4321)
            with patch(
                "server.api.scheduler_runtime.subprocess.Popen",
                return_value=proc,
            ) as popen:
                result = scheduler_runtime.start_detached_python_job(
                    cmd=["python", "worker.py"],
                    root=code_root,
                    env={"PROBIGA_JOB_LOG_ROOT": str(log_root)},
                    log_name="recommended/queue",
                )

            self.assertEqual(result["pid"], 4321)
            self.assertEqual(
                Path(result["stdout_log"]).resolve(),
                (log_root / "recommended_queue.out.log").resolve(),
            )
            self.assertEqual(
                Path(result["stderr_log"]).resolve(),
                (log_root / "recommended_queue.err.log").resolve(),
            )
            self.assertFalse((code_root / "data").exists())
            self.assertTrue(Path(result["stdout_log"]).is_file())
            self.assertTrue(Path(result["stderr_log"]).is_file())
            self.assertEqual(popen.call_args.kwargs["cwd"], str(code_root))

    def test_detached_job_log_root_rejects_relative_or_code_tree_paths(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            code_root = Path(temp_dir) / "release"
            code_root.mkdir()
            for configured in ("data/jobs", str(code_root / "data" / "jobs")):
                with self.assertRaises(RuntimeError):
                    scheduler_runtime.start_detached_python_job(
                        cmd=["python", "worker.py"],
                        root=code_root,
                        env={"PROBIGA_JOB_LOG_ROOT": configured},
                        log_name="recommended",
                    )
            self.assertFalse((code_root / "data").exists())

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

    def test_cleanup_after_restart_keeps_unknown_process_claimed(self):
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

        self.assertEqual(cleaned, 0)
        self.assertEqual(updates, [])
        self.assertIn(39, remaining_ids)

    def test_cleanup_timeout_kill_failure_keeps_claim_and_owner_state(self):
        started_at = datetime.now() - timedelta(hours=8)
        task_id = 40
        proc = MagicMock()
        proc.poll.return_value = None
        scheduler_runtime._running_procs[task_id] = proc
        scheduler_runtime._running_task_ids.add(task_id)
        scheduler_runtime._running_history_uids[task_id] = "run-40"

        engine = MagicMock()
        result = MagicMock()
        result.mappings.return_value.all.return_value = [{
            "id": task_id,
            "task_name": "stale local writer",
            "task_type": "analysis_fast",
            "script_path": "biz/analysis/sync_analysis_fast.py",
            "interval_minutes": 0,
            "last_run_at": started_at,
            "last_triggered_at": started_at,
        }]
        engine.connect.return_value.__enter__.return_value.execute.return_value = result

        with patch(
            "server.api.scheduler_runtime._scheduler_started_at",
            started_at - timedelta(minutes=1),
        ), patch(
            "server.api.scheduler_runtime._should_skip_task_for_host",
            return_value=False,
        ), patch(
            "server.api.scheduler_runtime._terminate_process_and_confirm",
            return_value=False,
        ) as terminate, patch(
            "server.api.scheduler_runtime.update_scheduler_task"
        ) as update_task:
            cleaned = scheduler_runtime._cleanup_stale_running_tasks(engine)

        self.assertEqual(cleaned, 0)
        terminate.assert_called_once_with(proc)
        update_task.assert_not_called()
        self.assertIs(scheduler_runtime._running_procs[task_id], proc)
        self.assertIn(task_id, scheduler_runtime._running_task_ids)
        self.assertNotIn(task_id, scheduler_runtime._timeout_pending_task_ids)
        self.assertNotIn(task_id, scheduler_runtime._timeout_requested_task_ids)

    def test_cleanup_confirmed_timeout_leaves_terminal_write_to_exact_owner(self):
        started_at = datetime.now() - timedelta(hours=8)
        task_id = 41
        proc = MagicMock()
        proc.poll.return_value = None
        scheduler_runtime._running_procs[task_id] = proc
        scheduler_runtime._running_task_ids.add(task_id)
        scheduler_runtime._running_history_uids[task_id] = "run-41"

        engine = MagicMock()
        result = MagicMock()
        result.mappings.return_value.all.return_value = [{
            "id": task_id,
            "task_name": "stale owned writer",
            "task_type": "analysis_fast",
            "script_path": "biz/analysis/sync_analysis_fast.py",
            "interval_minutes": 0,
            "last_run_at": started_at,
            "last_triggered_at": started_at,
        }]
        engine.connect.return_value.__enter__.return_value.execute.return_value = result

        with patch(
            "server.api.scheduler_runtime._scheduler_started_at",
            started_at - timedelta(minutes=1),
        ), patch(
            "server.api.scheduler_runtime._should_skip_task_for_host",
            return_value=False,
        ), patch(
            "server.api.scheduler_runtime._terminate_process_and_confirm",
            return_value=True,
        ), patch(
            "server.api.scheduler_runtime.update_scheduler_task"
        ) as update_task:
            cleaned = scheduler_runtime._cleanup_stale_running_tasks(engine)

        self.assertEqual(cleaned, 1)
        update_task.assert_not_called()
        self.assertIn(task_id, scheduler_runtime._timeout_requested_task_ids)
        self.assertIn(task_id, scheduler_runtime._running_task_ids)
        self.assertIs(scheduler_runtime._running_procs[task_id], proc)

    def test_cleanup_timeout_confirmation_race_keeps_natural_terminal_state(self):
        started_at = datetime.now() - timedelta(hours=8)
        task_id = 43
        proc = MagicMock()
        proc.poll.return_value = None
        scheduler_runtime._running_procs[task_id] = proc
        scheduler_runtime._running_task_ids.add(task_id)
        scheduler_runtime._running_history_uids[task_id] = "run-43"
        engine = MagicMock()
        result = MagicMock()
        result.mappings.return_value.all.return_value = [{
            "id": task_id,
            "task_name": "naturally completed writer",
            "task_type": "analysis_fast",
            "script_path": "biz/analysis/sync_analysis_fast.py",
            "interval_minutes": 0,
            "last_run_at": started_at,
            "last_triggered_at": started_at,
        }]
        engine.connect.return_value.__enter__.return_value.execute.return_value = result

        def finish_naturally(_proc):
            self.assertIs(_proc, proc)
            with scheduler_runtime._running_lock:
                scheduler_runtime._running_procs.pop(task_id, None)
                scheduler_runtime._running_history_uids.pop(task_id, None)
                scheduler_runtime._running_task_ids.discard(task_id)
            return True

        with patch(
            "server.api.scheduler_runtime._scheduler_started_at",
            started_at - timedelta(minutes=1),
        ), patch(
            "server.api.scheduler_runtime._should_skip_task_for_host",
            return_value=False,
        ), patch(
            "server.api.scheduler_runtime._terminate_process_and_confirm",
            side_effect=finish_naturally,
        ), patch(
            "server.api.scheduler_runtime.update_scheduler_task"
        ) as update_task:
            cleaned = scheduler_runtime._cleanup_stale_running_tasks(engine)

        self.assertEqual(cleaned, 0)
        update_task.assert_not_called()
        self.assertNotIn(task_id, scheduler_runtime._timeout_pending_task_ids)
        self.assertNotIn(task_id, scheduler_runtime._timeout_requested_task_ids)

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

    def test_postmarket_source_tasks_catch_up_after_busy_worker_slot(self):
        now = datetime(2026, 8, 26, 20, 35, 0)
        for task_type, cron_time in (
            ("sector_heat_east", "17:08"),
            ("alist_daily", "17:40"),
            ("alist_info", "17:45"),
            ("concept_flow", "19:30"),
            ("notice_eastmoney", "20:15"),
            ("stock_snapshot_daily", "18:25"),
            ("quality_check_post", "19:30"),
        ):
            with self.subTest(task_type=task_type):
                self.assertTrue(
                    scheduler_runtime._critical_cron_catchup_allowed(
                        {
                            "task_type": task_type,
                            "last_triggered_at": "2026-08-25 20:15:00",
                            "last_run_status": "success",
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

    def test_cron_due_does_not_retry_terminal_blocked_task(self):
        row = {
            "cron_time": "15:45",
            "last_triggered_at": "2026-07-08 15:45:00",
            "last_run_at": "2026-07-08 15:46:00",
            "last_run_status": "blocked",
        }

        self.assertFalse(
            scheduler_runtime._cron_due(
                row,
                now=datetime(2026, 7, 8, 16, 0),
            )
        )
        self.assertFalse(
            scheduler_runtime._cron_due(
                row,
                now=datetime(2026, 7, 8, 16, 1),
            )
        )

    def test_critical_catchup_does_not_retry_terminal_blocked_qmt_task(self):
        row = {
            "task_type": "qmt_stock_daily_canonical",
            "last_triggered_at": "2026-07-08 15:45:00",
            "last_run_at": "2026-07-08 15:46:00",
            "last_run_status": "blocked",
        }

        self.assertFalse(
            scheduler_runtime._critical_cron_catchup_allowed(
                row,
                now=datetime(2026, 7, 8, 16, 1),
                cron_time="15:45",
            )
        )

    def test_cron_due_does_not_repeat_successful_task_same_day(self):
        row = {
            "cron_time": "08:30",
            "last_triggered_at": "2026-07-08 08:30:00",
            "last_run_status": "success",
        }

        self.assertFalse(scheduler_runtime._cron_due(row, now=datetime(2026, 7, 8, 10, 0)))

    def test_precron_release_success_never_replaces_daily_ordinary_run(self):
        task_crons = {
            "etf_forward_daily": "15:20",
            "alist_daily": "17:40",
            "alist_info": "17:45",
            "sector_heat_east": "17:08",
            "eastmoney_concept_current": "18:05",
            "eastmoney_concept_kline": "18:10",
            "eastmoney_concept_minute": "18:15",
            "eastmoney_concept_flow_snapshot": "19:30",
            "notice_eastmoney": "20:15",
            "stock_finance": "21:00",
            "stock_dividend_baidu": "22:00",
            "news_sync": "00:05",
        }
        self.assertTrue(
            set(task_crons).issubset(
                scheduler_runtime.CRITICAL_CRON_CATCHUP_TASK_TYPES
            )
        )
        for task_type, cron_time in task_crons.items():
            hour, minute = (int(part) for part in cron_time.split(":"))
            row = {
                "task_type": task_type,
                "cron_time": cron_time,
                "last_triggered_at": "2026-08-27 00:01:00",
                "last_run_status": "success",
                "_release_terminal_status": "success",
                "_release_terminal_trigger_source": "release_catchup",
                "_release_terminal_run_at": "2026-08-27 00:01:00",
            }
            with self.subTest(task_type=task_type):
                self.assertFalse(
                    scheduler_runtime._cron_due(
                        row,
                        now=datetime(2026, 8, 27, hour, minute)
                        - timedelta(minutes=1),
                    )
                )
                self.assertTrue(
                    scheduler_runtime._cron_due(
                        row,
                        now=datetime(2026, 8, 27, hour, minute),
                    )
                )

    def test_release_started_at_or_after_cron_can_satisfy_daily_run(self):
        base = {
            "task_type": "notice_eastmoney",
            "cron_time": "20:15",
            "last_run_status": "success",
            "_release_terminal_status": "success",
            "_release_terminal_trigger_source": "release_catchup",
        }
        for run_at in ("2026-08-27 20:15:00", "2026-08-27 21:00:00"):
            row = {
                **base,
                "last_triggered_at": run_at,
                "_release_terminal_run_at": run_at,
            }
            with self.subTest(run_at=run_at):
                self.assertFalse(
                    scheduler_runtime._cron_due(
                        row,
                        now=datetime(2026, 8, 27, 21, 5),
                    )
                )

    def test_completed_scheduled_run_still_suppresses_same_day_duplicate(self):
        row = {
            "task_type": "notice_eastmoney",
            "cron_time": "20:15",
            "last_triggered_at": "2026-08-27 20:15:00",
            "last_run_status": "success",
            "_release_terminal_status": "success",
            "_release_terminal_trigger_source": "scheduled",
            "_release_terminal_run_at": "2026-08-27 20:15:00",
        }

        self.assertFalse(
            scheduler_runtime._cron_due(
                row,
                now=datetime(2026, 8, 27, 20, 30),
            )
        )

    def test_precron_release_success_keeps_critical_overdue_catchup_eligible(self):
        row = {
            "task_type": "notice_eastmoney",
            "cron_time": "20:15",
            "last_triggered_at": "2026-08-27 03:00:00",
            "last_run_status": "success",
            "_release_terminal_status": "success",
            "_release_terminal_trigger_source": "release_catchup",
            "_release_terminal_run_at": "2026-08-27 03:00:00",
        }

        self.assertTrue(
            scheduler_runtime._critical_cron_catchup_allowed(
                row,
                now=datetime(2026, 8, 27, 20, 16),
                cron_time="20:15",
            )
        )

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
            "server.api.scheduler_runtime._task_history_start",
            return_value="run-missing-7",
        ), patch(
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

    def test_run_task_never_executes_without_an_audit_row(self):
        engine = MagicMock()
        row = {
            "id": 69,
            "task_name": "must be audited",
            "task_type": "daily_review",
            "script_path": "biz/review/generate.py",
        }
        with patch(
            "server.api.scheduler_runtime._task_history_start",
            return_value=None,
        ), patch(
            "server.api.scheduler_runtime._run_task_impl"
        ) as implementation, patch(
            "server.api.scheduler_runtime.update_scheduler_task"
        ) as update_task:
            scheduler_runtime._run_task(row, Path("E:/fake"), engine)

        implementation.assert_not_called()
        self.assertEqual(
            update_task.call_args.args[2]["last_run_status"],
            "failed",
        )
        self.assertIn(
            "audit row unavailable",
            update_task.call_args.args[2]["last_run_output"],
        )

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
        self.assertIn("build_sha", params)
        self.assertNotIn("script_args", params)
        self.assertNotIn("should-not-be-stored", str(params))

    def test_runtime_history_schema_check_is_read_only(self):
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        index_result = MagicMock()
        index_result.mappings.return_value.all.return_value = [
            {"Key_name": "run_uid_custom", "Non_unique": 0, "Seq_in_index": 1, "Column_name": "run_uid"},
            {"Key_name": "task_run_custom", "Non_unique": 1, "Seq_in_index": 1, "Column_name": "task_id"},
            {"Key_name": "task_run_custom", "Non_unique": 1, "Seq_in_index": 2, "Column_name": "run_at"},
        ]
        conn.execute.side_effect = [MagicMock(), index_result]

        scheduler_runtime._ensure_task_history_table(engine)

        statements = [str(call.args[0]) for call in conn.execute.call_args_list]
        self.assertTrue(any("LIMIT 0" in sql for sql in statements))
        self.assertFalse(any("CREATE " in sql.upper() for sql in statements))
        self.assertFalse(any("ALTER " in sql.upper() for sql in statements))
        engine.begin.assert_not_called()

    def test_runtime_history_schema_check_rejects_missing_indexes(self):
        engine = MagicMock()
        conn = MagicMock()
        engine.connect.return_value.__enter__.return_value = conn
        index_result = MagicMock()
        index_result.mappings.return_value.all.return_value = [
            {"Key_name": "PRIMARY", "Non_unique": 0, "Seq_in_index": 1, "Column_name": "id"},
        ]
        conn.execute.side_effect = [MagicMock(), index_result]

        with self.assertRaisesRegex(RuntimeError, "unique run_uid"):
            scheduler_runtime._ensure_task_history_table(engine)
        engine.begin.assert_not_called()

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
            return_value="00000000000000000000000000000071",
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
        self.assertEqual(
            history_finish.call_args.args[:2],
            (engine, "00000000000000000000000000000071"),
        )
        self.assertEqual(history_finish.call_args.kwargs["status"], "success")
        self.assertEqual(history_finish.call_args.kwargs["exit_code"], 0)
        self.assertIn("sent", history_finish.call_args.kwargs["output"])

    def test_linux_gap_repair_child_gets_narrow_provider_role_from_standalone_parent(self):
        engine = MagicMock()
        row = {
            "id": 713,
            "task_name": "recent Linux data repair",
            "task_type": "linux_recent_data_gap_repair",
            "script_path": "tools/repair_linux_recent_data_gaps.py",
            "script_args": "--apply --json",
            "date_param": "",
            "interval_minutes": 0,
        }
        fake_proc = MagicMock()
        fake_proc.communicate.return_value = ("{}", "")
        fake_proc.returncode = 0
        validate_result = MagicMock(
            return_value=SchedulerValidationResult(
                checked=True,
                ok=True,
                message="exact repair receipt verified",
            )
        )
        build_sha = "7" * 40

        with patch(
            "server.api.scheduler_runtime._task_history_start",
            return_value="00000000000000000000000000000713",
        ), patch(
            "server.api.scheduler_runtime.os.name",
            "posix",
        ), patch(
            "server.api.scheduler_runtime.os.setsid",
            create=True,
        ), patch(
            "server.api.scheduler_runtime._task_history_finish"
        ), patch(
            "server.api.scheduler_runtime.resolve_scheduler_script",
            return_value=Path("E:/fake/repair_linux_recent_data_gaps.py"),
        ), patch.object(
            Path,
            "exists",
            return_value=True,
        ), patch(
            "server.api.scheduler_runtime.build_child_env",
            return_value={
                "PROBIGA_SCHEDULER_EXECUTOR_ROLE": "linux_standalone",
            },
        ), patch(
            "server.api.scheduler_runtime._build_task_args",
            return_value=["--apply", "--json"],
        ), patch(
            "server.api.scheduler_runtime.subprocess.Popen",
            return_value=fake_proc,
        ) as popen, patch(
            "server.api.scheduler_runtime.validate_scheduler_task_result",
            validate_result,
        ), patch(
            "server.api.scheduler_runtime.scheduler_output_status",
            return_value="success",
        ), patch(
            "server.api.scheduler_runtime._scheduler_build_commit_sha",
            return_value=build_sha,
        ), patch(
            "server.api.scheduler_runtime.update_scheduler_task"
        ):
            scheduler_runtime._run_task(row, Path("E:/fake"), engine)

        child_env = popen.call_args.kwargs["env"]
        self.assertEqual(
            child_env["PROBIGA_SCHEDULER_EXECUTOR_ROLE"],
            "linux_provider",
        )
        self.assertEqual(
            child_env["PROBIGA_SCHEDULER_TASK_TYPE"],
            "linux_recent_data_gap_repair",
        )
        self.assertEqual(
            child_env["PROBIGA_SCHEDULER_HISTORY_RUN_UID"],
            "00000000000000000000000000000713",
        )
        validated_task = validate_result.call_args.args[0]
        self.assertEqual(
            validated_task["_scheduler_history_run_uid"],
            "00000000000000000000000000000713",
        )
        self.assertEqual(
            validated_task["_scheduler_expected_build_sha"],
            build_sha,
        )

    def test_linux_provider_child_role_rejects_task_script_identity_mismatch(self):
        row = {
            "id": 714,
            "task_name": "misbound provider repair",
            "task_type": "linux_recent_data_gap_repair",
            "script_path": "tools/sync_news_formal.py",
            "script_args": "--json",
            "date_param": "",
            "interval_minutes": 0,
        }

        with patch(
            "server.api.scheduler_runtime.resolve_scheduler_script",
            return_value=Path("E:/fake/sync_news_formal.py"),
        ), patch.object(
            Path,
            "exists",
            return_value=True,
        ), patch(
            "server.api.scheduler_runtime.build_child_env",
            return_value={
                "PROBIGA_SCHEDULER_EXECUTOR_ROLE": "linux_standalone",
            },
        ), patch(
            "server.api.scheduler_runtime._build_task_args",
            return_value=["--json"],
        ), patch(
            "server.api.scheduler_runtime.subprocess.Popen",
        ) as popen:
            with self.assertRaisesRegex(RuntimeError, "exact script path"):
                scheduler_runtime._run_task_impl(
                    row,
                    Path("E:/fake"),
                    MagicMock(),
                    history_run_uid="00000000000000000000000000000714",
                )

        popen.assert_not_called()

    def test_linux_provider_child_role_requires_posix_standalone_parent(self):
        row = {
            "id": 715,
            "task_name": "recent Linux data repair",
            "task_type": "linux_recent_data_gap_repair",
            "script_path": "tools/repair_linux_recent_data_gaps.py",
            "script_args": "--apply --json",
            "date_param": "",
            "interval_minutes": 0,
        }
        cases = (
            ("nt", "linux_standalone", "requires a POSIX host"),
            ("posix", "linux_provider", "exact linux_standalone parent"),
            ("posix", "", "exact linux_standalone parent"),
        )
        for host_kind, parent_role, message in cases:
            with self.subTest(host_kind=host_kind, parent_role=parent_role):
                with patch(
                    "server.api.scheduler_runtime.os.name",
                    host_kind,
                ), patch(
                    "server.api.scheduler_runtime.resolve_scheduler_script",
                    return_value=Path(
                        "E:/fake/repair_linux_recent_data_gaps.py"
                    ),
                ), patch.object(
                    Path,
                    "exists",
                    return_value=True,
                ), patch(
                    "server.api.scheduler_runtime.build_child_env",
                    return_value={
                        "PROBIGA_SCHEDULER_EXECUTOR_ROLE": parent_role,
                    },
                ), patch(
                    "server.api.scheduler_runtime._build_task_args",
                    return_value=["--apply", "--json"],
                ), patch(
                    "server.api.scheduler_runtime.subprocess.Popen",
                ) as popen:
                    with self.assertRaisesRegex(RuntimeError, message):
                        scheduler_runtime._run_task_impl(
                            row,
                            Path("E:/fake"),
                            MagicMock(),
                            history_run_uid=(
                                "00000000000000000000000000000715"
                            ),
                        )

                popen.assert_not_called()

    def test_confirmed_user_stop_is_persisted_by_owner_with_same_audit_uid(self):
        engine = MagicMock()
        row = {
            "id": 809,
            "task_name": "manually stopped",
            "task_type": "news_daily",
            "script_path": "biz/early_briefing/generate.py",
            "script_args": "",
            "date_param": "",
            "interval_minutes": 0,
            "_history_run_uid": "00000000000000000000000000000809",
            "_history_started": True,
        }
        fake_proc = MagicMock()
        fake_proc.communicate.return_value = ("partial", "killed")
        fake_proc.returncode = -9
        scheduler_runtime._stop_requested_task_ids.add(809)

        with patch(
            "server.api.scheduler_runtime._task_history_finish"
        ) as history_finish, patch(
            "server.api.scheduler_runtime.resolve_scheduler_script",
            return_value=Path("E:/fake/generate.py"),
        ), patch.object(
            Path, "exists", return_value=True
        ), patch(
            "server.api.scheduler_runtime.build_child_env", return_value={}
        ), patch(
            "server.api.scheduler_runtime._build_task_args", return_value=[]
        ), patch(
            "server.api.scheduler_runtime.subprocess.Popen", return_value=fake_proc
        ), patch(
            "server.api.scheduler_runtime.update_scheduler_task"
        ) as update_task, patch(
            "server.api.scheduler_runtime.validate_scheduler_task_result"
        ) as validate_result:
            scheduler_runtime._run_task(row, Path("E:/fake"), engine)

        final_values = update_task.call_args_list[-1].args[2]
        self.assertEqual(final_values["last_run_status"], "stopped")
        self.assertIn("用户手动停止", final_values["last_run_output"])
        history_finish.assert_called_once()
        self.assertEqual(
            history_finish.call_args.args[:2],
            (engine, "00000000000000000000000000000809"),
        )
        self.assertEqual(history_finish.call_args.kwargs["status"], "stopped")
        validate_result.assert_not_called()

    def test_confirmed_stale_timeout_is_persisted_by_exact_owner(self):
        engine = MagicMock()
        row = {
            "id": 810,
            "task_name": "timed out writer",
            "task_type": "analysis_fast",
            "script_path": "biz/analysis/sync_analysis_fast.py",
            "script_args": "",
            "date_param": "",
            "interval_minutes": 0,
            "_history_run_uid": "00000000000000000000000000000810",
            "_history_started": True,
        }
        fake_proc = MagicMock()
        fake_proc.communicate.return_value = ("partial", "killed")
        fake_proc.returncode = -9
        scheduler_runtime._timeout_requested_task_ids.add(810)

        with patch(
            "server.api.scheduler_runtime._task_history_finish"
        ) as history_finish, patch(
            "server.api.scheduler_runtime.resolve_scheduler_script",
            return_value=Path("E:/fake/sync_analysis_fast.py"),
        ), patch.object(
            Path, "exists", return_value=True
        ), patch(
            "server.api.scheduler_runtime.build_child_env", return_value={}
        ), patch(
            "server.api.scheduler_runtime._build_task_args", return_value=[]
        ), patch(
            "server.api.scheduler_runtime.subprocess.Popen", return_value=fake_proc
        ), patch(
            "server.api.scheduler_runtime.update_scheduler_task"
        ) as update_task, patch(
            "server.api.scheduler_runtime.validate_scheduler_task_result"
        ) as validate_result:
            scheduler_runtime._run_task(row, Path("E:/fake"), engine)

        final_values = update_task.call_args_list[-1].args[2]
        self.assertEqual(final_values["last_run_status"], "timeout")
        self.assertIn("子进程已确认退出", final_values["last_run_output"])
        history_finish.assert_called_once()
        self.assertEqual(
            history_finish.call_args.args[:2],
            (engine, "00000000000000000000000000000810"),
        )
        self.assertEqual(history_finish.call_args.kwargs["status"], "timeout")
        validate_result.assert_not_called()

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
            return_value="00000000000000000000000000000072",
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
            with patch(
                "server.api.scheduler_runtime._task_history_start",
                return_value="00000000000000000000000000000007",
            ), patch("server.api.scheduler_runtime.update_scheduler_task") as update_task, patch(
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
                "server.api.scheduler_runtime._task_history_start",
                return_value="00000000000000000000000000000067",
            ), patch(
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

    def test_strategy_governance_blocked_output_requires_exact_exit_two_contract(self):
        from server.common.scheduler_validation import scheduler_output_status

        payload = _governance_not_ready_payload()
        task = {"task_type": "strategy_governance_daily"}
        output = json.dumps(payload, ensure_ascii=False)
        self.assertEqual(
            scheduler_output_status(task, output, return_code=2),
            "blocked",
        )
        self.assertEqual(
            scheduler_output_status(task, output, return_code=0),
            "failed",
        )
        self.assertEqual(
            scheduler_output_status(
                task, output + "\nunexpected log", return_code=2
            ),
            "failed",
        )

    def test_strategy_governance_blocked_output_rejects_forged_fields(self):
        from server.common.scheduler_validation import scheduler_output_status

        task = {"task_type": "strategy_governance_daily"}
        invalid_payloads = []
        for key, value in (
            ("reason", ""),
            ("target_trade_date", "bad-date"),
            ("automatic_real_order_submission", True),
            ("real_order_authority", True),
            ("retryable", False),
        ):
            forged = _governance_not_ready_payload()
            forged[key] = value
            invalid_payloads.append(forged)
        forged_weight = _governance_not_ready_payload()
        forged_weight["allocations"][0]["simulated_weight_pct"] = 99.0
        invalid_payloads.append(forged_weight)
        for payload in invalid_payloads:
            with self.subTest(payload=payload):
                self.assertEqual(
                    scheduler_output_status(
                        task,
                        json.dumps(payload, ensure_ascii=False),
                        return_code=2,
                    ),
                    "failed",
                )

    def test_strategy_governance_completed_output_is_revalidated(self):
        from server.common.scheduler_validation import scheduler_output_status

        payload = {
            "status": "ok",
            "orchestration_status": "COMPLETED",
            "reason_code": "GOVERNANCE_COMPLETED",
            "run_uid": "a" * 32,
            "trade_date": "2026-08-21",
            "summary": {},
            "build_commit_sha": "b" * 40,
            "allocations": [{
                "target_type": "CASH",
                "simulated_weight_pct": 100.0,
                "real_order_authority": False,
            }],
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        }
        task = {"task_type": "strategy_governance_daily"}
        self.assertEqual(
            scheduler_output_status(
                task, json.dumps(payload), return_code=0
            ),
            "success",
        )
        payload["automatic_real_order_submission"] = True
        self.assertEqual(
            scheduler_output_status(
                task, json.dumps(payload), return_code=0
            ),
            "failed",
        )

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


def _release_terminal_row(
    task_type: str,
    *,
    task_id: int,
    build_sha: str,
    status: str = "success",
    finished_at: datetime | None = None,
    valid_evidence: bool = True,
    target_date: str | None = None,
) -> dict:
    run_uid = f"{task_id:032x}"[-32:]
    replay_output = ""
    core = {
        "schema": "probiga.scheduler-validation-evidence.v1",
        "run_uid": run_uid,
        "task_id": task_id,
        "task_name": task_type,
        "task_type": task_type,
        "build_sha": build_sha,
        "status": "success",
        "exit_code": 0,
        "started_at": (finished_at or datetime(2026, 8, 27, 1, 0)).isoformat(
            sep=" ", timespec="seconds"
        ),
        "validation_checked": True,
        "validation_ok": True,
        "validation_message": "ok",
        "machine_output_sha256": hashlib.sha256(b"").hexdigest(),
        "replay_output": replay_output,
        "replay_output_sha256": hashlib.sha256(
            replay_output.encode("utf-8")
        ).hexdigest(),
    }
    if not valid_evidence:
        core["validation_ok"] = False
    if target_date is not None:
        core["release_target_date"] = target_date
    evidence = {
        **core,
        "evidence_sha256": hashlib.sha256(
            json.dumps(
                core,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest(),
    }
    return {
        "id": task_id,
        "task_type": task_type,
        "_release_history_available": True,
        "_release_catchup_authorized": True,
        "_release_terminal_run_uid": run_uid,
        "_release_terminal_status": status,
        "_release_terminal_build_sha": build_sha,
        "_release_terminal_finished_at": finished_at,
        "_release_terminal_exit_code": 0 if status == "success" else 2,
        "_release_terminal_output": json.dumps(evidence, sort_keys=True),
    }


def test_release_catchup_replays_old_build_and_suppresses_exact_valid_success():
    build_sha = "c" * 40
    now = datetime(2026, 8, 27, 1, 0)
    old = _release_terminal_row(
        "stock_finance",
        task_id=901,
        build_sha="b" * 40,
        finished_at=now,
    )
    exact = _release_terminal_row(
        "stock_finance",
        task_id=901,
        build_sha=build_sha,
        finished_at=now,
    )
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value=build_sha,
    ):
        assert scheduler_runtime._release_build_catchup_allowed(old, now=now)
        assert not scheduler_runtime._release_build_catchup_allowed(exact, now=now)

    old["_release_catchup_authorized"] = False
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value=build_sha,
    ):
        assert not scheduler_runtime._release_build_catchup_allowed(old, now=now)


def test_release_catchup_retries_failed_and_blocked_rows_after_bounded_backoff():
    build_sha = "c" * 40
    now = datetime(2026, 8, 27, 6, 0)
    failed = _release_terminal_row(
        "stock_finance",
        task_id=902,
        build_sha=build_sha,
        status="failed",
        finished_at=now - timedelta(
            minutes=scheduler_runtime.RELEASE_CATCHUP_RETRY_INTERVAL_MINUTES
        ),
        valid_evidence=False,
    )
    blocked = _release_terminal_row(
        "notice_eastmoney",
        task_id=903,
        build_sha=build_sha,
        status="blocked",
        finished_at=now - timedelta(
            minutes=(
                scheduler_runtime.RELEASE_CATCHUP_BLOCKED_RETRY_INTERVAL_MINUTES
                - 1
            )
        ),
        valid_evidence=False,
    )
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value=build_sha,
    ):
        assert scheduler_runtime._release_build_catchup_allowed(failed, now=now)
        assert not scheduler_runtime._release_build_catchup_allowed(
            blocked,
            now=now,
        )
        blocked["_release_terminal_finished_at"] = now - timedelta(
            minutes=scheduler_runtime.RELEASE_CATCHUP_BLOCKED_RETRY_INTERVAL_MINUTES
        )
        assert scheduler_runtime._release_build_catchup_allowed(blocked, now=now)


def test_release_catchup_dependency_graph_is_acyclic_and_never_holds_worker_lane():
    graph = readiness_contract.RELEASE_DATA_CATCHUP_DEPENDENCIES
    nodes = readiness_contract.RELEASE_DATA_CATCHUP_TASK_TYPES
    morning_dependencies = {
        "capital_flow_batch_fast",
        "qmt_announcement_pit",
        "qmt_stock_daily_canonical",
        "stock_finance",
        "notice_eastmoney",
        "linux_recent_data_gap_repair",
    }
    fast_dependencies = morning_dependencies | {
        "target_turnover_snapshot",
        "analysis_upper_evidence_prepare",
        "qmt_membership_snapshot",
    }
    assert set(graph["analysis_fast"]) == fast_dependencies
    assert set(graph["analysis_morning_strict"]) == morning_dependencies
    assert graph["target_turnover_snapshot"] == ("qmt_stock_daily_canonical",)
    assert set(graph["analysis_upper_evidence_prepare"]) == {
        "target_turnover_snapshot",
        "capital_flow_batch_fast",
        "qmt_membership_snapshot",
        "qmt_stock_daily_canonical",
    }
    assert (
        "analysis_morning_strict"
        in readiness_contract.RELEASE_DATA_CATCHUP_SUPPORT_TASK_TYPES
    )
    assert (
        "analysis_morning_strict"
        not in readiness_contract.RELEASE_DATA_READINESS_TASK_TYPES
    )
    assert (
        "qmt_membership_snapshot"
        in readiness_contract.RELEASE_DATA_CATCHUP_SUPPORT_TASK_TYPES
    )
    assert (
        "qmt_membership_snapshot"
        not in readiness_contract.RELEASE_DATA_READINESS_TASK_TYPES
    )
    assert "qmt_membership_snapshot" in graph["analysis_fast"]
    assert "qmt_membership_snapshot" not in graph["analysis_morning_strict"]
    assert {
        "target_turnover_snapshot",
        "analysis_upper_evidence_prepare",
    } <= readiness_contract.RELEASE_DATA_CATCHUP_SUPPORT_TASK_TYPES
    assert set(graph).issubset(nodes)
    assert {
        dependency for dependencies in graph.values() for dependency in dependencies
    }.issubset(nodes)
    assert not (
        nodes & readiness_contract.RELEASE_CATCHUP_FORBIDDEN_EXECUTION_TASK_TYPES
    )

    visited: set[str] = set()
    active: set[str] = set()

    def visit(node: str) -> None:
        assert node not in active, f"release catch-up dependency cycle at {node}"
        if node in visited:
            return
        active.add(node)
        for dependency in graph.get(node, ()):
            visit(dependency)
        active.remove(node)
        visited.add(node)

    for task_type in nodes:
        visit(task_type)
    assert visited == set(nodes)

    loop_source = inspect.getsource(scheduler_runtime._check_and_run_tasks)
    assert "and not membership_ordinary_due" in loop_source
    assert "and not _cron_due(row, now=now)" in loop_source
    assert "elapsed < interval_minutes and not release_catchup_due" in loop_source
    dependency_gate = loop_source.index("_release_catchup_dependencies_ready")
    pending_gate = loop_source.index(
        "release_catchup_pending\n"
    )
    normal_schedule_gate = loop_source.index("if interval_minutes > 0")
    running_lock = loop_source.index("with _running_lock:", dependency_gate)
    claim = loop_source.index("_claim_task_run", running_lock)
    assert pending_gate < normal_schedule_gate
    assert dependency_gate < running_lock < claim


def test_release_catchup_requires_exact_build_evidence_for_dependencies():
    build_sha = "c" * 40
    downstream = {"id": 905, "task_type": "linux_recent_data_gap_repair"}
    dependency = _release_terminal_row(
        "qmt_canonical_history_gap_repair",
        task_id=904,
        build_sha="b" * 40,
        finished_at=datetime(2026, 8, 27, 2, 0),
    )
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value=build_sha,
    ):
        ready, reason = scheduler_runtime._release_catchup_dependencies_ready(
            downstream,
            [downstream, dependency],
        )
        assert not ready
        assert reason.endswith(":exact_build_not_ready")

        dependency = _release_terminal_row(
            "qmt_canonical_history_gap_repair",
            task_id=904,
            build_sha=build_sha,
            finished_at=datetime(2026, 8, 27, 2, 0),
        )
        ready, reason = scheduler_runtime._release_catchup_dependencies_ready(
            downstream,
            [downstream, dependency],
        )
        assert ready
        assert reason == "ready"


def test_release_dependency_rejects_exact_build_receipt_for_old_target_date():
    build_sha = "c" * 40
    downstream = {"id": 905, "task_type": "hot_fused"}
    dependencies = []
    for index, task_type in enumerate(
        readiness_contract.RELEASE_DATA_CATCHUP_DEPENDENCIES["hot_fused"]
    ):
        target_date = "2026-08-26" if index == 0 else "2026-08-27"
        dependency = _release_terminal_row(
            task_type,
            task_id=910 + index,
            build_sha=build_sha,
            target_date=target_date,
        )
        dependency.update(
            {
                "_release_expected_target_required": True,
                "_release_expected_target_available": True,
                "_release_expected_target_date": "2026-08-27",
            }
        )
        dependencies.append(dependency)
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value=build_sha,
    ):
        ready, reason = scheduler_runtime._release_catchup_dependencies_ready(
            downstream,
            [downstream, *dependencies],
        )
    assert not ready
    assert reason == "hot_rank_ths:exact_build_not_ready"


def test_release_analysis_pools_wait_for_every_exact_build_market_input():
    build_sha = "c" * 40
    for downstream_type in ("analysis_fast", "analysis_morning_strict"):
        dependencies = readiness_contract.RELEASE_DATA_CATCHUP_DEPENDENCIES[
            downstream_type
        ]
        downstream = {"id": 950, "task_type": downstream_type}
        exact_rows = [
            _release_terminal_row(
                dependency,
                task_id=951 + index,
                build_sha=build_sha,
                finished_at=datetime(2026, 8, 27, 2, 0),
            )
            for index, dependency in enumerate(dependencies)
        ]
        with patch(
            "server.api.scheduler_runtime._scheduler_build_commit_sha",
            return_value=build_sha,
        ):
            ready, reason = scheduler_runtime._release_catchup_dependencies_ready(
                downstream,
                [downstream, *exact_rows],
            )
            assert ready, reason

            for dependency_row in exact_rows:
                original_build = dependency_row["_release_terminal_build_sha"]
                dependency_row["_release_terminal_build_sha"] = "b" * 40
                ready, reason = (
                    scheduler_runtime._release_catchup_dependencies_ready(
                        downstream,
                        [downstream, *exact_rows],
                    )
                )
                assert not ready
                assert reason == (
                    f"{dependency_row['task_type']}:exact_build_not_ready"
                )
                dependency_row["_release_terminal_build_sha"] = original_build


def test_release_catchup_history_attachment_is_select_only():
    source = inspect.getsource(scheduler_runtime._attach_release_catchup_history).upper()
    assert "SELECT HISTORY.ID" in source
    assert "HISTORY.TRIGGER_SOURCE" in source
    assert "_RELEASE_TERMINAL_TRIGGER_SOURCE" in source
    assert "GROUP BY TASK_ID, TASK_TYPE" in source
    assert "TASK_ID=:RELEASE_TASK_ID_" in source
    assert "TASK_TYPE=:RELEASE_TASK_TYPE_" in source
    for token in ("INSERT ", "UPDATE ", "DELETE ", "ALTER ", "CREATE "):
        assert token not in source


def test_release_catchup_rejects_hash_drift_and_malformed_evidence_fail_closed():
    build_sha = "c" * 40
    row = _release_terminal_row(
        "stock_finance",
        task_id=906,
        build_sha=build_sha,
        finished_at=datetime(2026, 8, 27, 2, 0),
    )
    assert scheduler_runtime._release_history_evidence_valid(row, build_sha)

    evidence = json.loads(row["_release_terminal_output"])
    evidence["validation_message"] = "drifted after hashing"
    row["_release_terminal_output"] = json.dumps(evidence, sort_keys=True)
    assert not scheduler_runtime._release_history_evidence_valid(row, build_sha)

    evidence["task_id"] = {"unexpected": True}
    row["_release_terminal_output"] = json.dumps(evidence, sort_keys=True)
    assert not scheduler_runtime._release_history_evidence_valid(row, build_sha)


def test_release_pending_gate_prevents_ordinary_cron_bypass_without_authority():
    build_sha = "c" * 40
    old = _release_terminal_row(
        "analysis_morning_strict",
        task_id=907,
        build_sha="b" * 40,
        finished_at=datetime(2026, 8, 27, 8, 30),
    )
    old["_release_catchup_authorized"] = False
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value=build_sha,
    ):
        assert scheduler_runtime._release_build_catchup_pending(old)
        assert not scheduler_runtime._release_build_catchup_allowed(
            old,
            now=datetime(2026, 8, 27, 8, 31),
        )

        exact = _release_terminal_row(
            "analysis_morning_strict",
            task_id=907,
            build_sha=build_sha,
            finished_at=datetime(2026, 8, 27, 8, 30),
        )
        assert not scheduler_runtime._release_build_catchup_pending(exact)


class _ClosedDateResult:
    def __init__(self, value):
        self.value = value

    def scalar(self):
        return self.value


class _ClosedDateConnection:
    def __init__(self, value):
        self.value = value
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, statement, params):
        self.calls.append((str(statement), dict(params)))
        return _ClosedDateResult(self.value)


class _ClosedDateEngine:
    def __init__(self, value):
        self.connection = _ClosedDateConnection(value)

    def connect(self):
        return self.connection


def test_release_closed_target_uses_calendar_for_overnight_preclose_and_closed_days():
    shanghai = ZoneInfo("Asia/Shanghai")
    cases = (
        (datetime(2026, 8, 27, 3, 5, tzinfo=shanghai), "2026-08-26", "<"),
        (datetime(2026, 8, 27, 17, 59, tzinfo=shanghai), "2026-08-26", "<"),
        (datetime(2026, 8, 27, 18, 50, tzinfo=shanghai), "2026-08-27", "<="),
        (datetime(2026, 8, 29, 10, 0, tzinfo=shanghai), "2026-08-28", "<"),
        (datetime(2026, 10, 5, 10, 0, tzinfo=shanghai), "2026-09-30", "<"),
    )
    for now, expected, comparator in cases:
        engine = _ClosedDateEngine(expected)
        assert (
            scheduler_runtime._release_catchup_closed_target_date(
                engine,
                now=now,
            )
            == expected
        )
        sql, params = engine.connection.calls[-1]
        assert f"trade_date {comparator} :today" in sql
        assert params == {"today": now.date().isoformat()}


def test_release_expected_target_rollover_waits_for_formal_analysis_cutoff():
    build_sha = "c" * 40
    row = _release_terminal_row(
        "analysis_fast",
        task_id=990,
        build_sha=build_sha,
        finished_at=datetime(2026, 8, 27, 17, 59),
        target_date="2026-08-26",
    )
    row.update(
        {
            "_release_expected_target_required": True,
            "_release_expected_target_available": True,
            "_release_expected_target_date": "2026-08-26",
        }
    )
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value=build_sha,
    ):
        assert not scheduler_runtime._release_build_catchup_allowed(
            row,
            now=datetime(2026, 8, 27, 17, 59),
        )
        assert not scheduler_runtime._release_build_catchup_pending(row)

        row["_release_expected_target_date"] = "2026-08-27"
        assert not scheduler_runtime._release_build_catchup_allowed(
            row,
            now=datetime(2026, 8, 27, 18, 0),
        )
        assert scheduler_runtime._release_build_catchup_pending(row)
        assert scheduler_runtime._release_build_catchup_allowed(
            row,
            now=datetime(2026, 8, 27, 23, 55),
        )


def test_release_daily_analysis_dag_obeys_each_immutable_capture_window():
    active_build = "c" * 40

    def due_row(task_type: str, task_id: int) -> dict:
        row = _release_terminal_row(
            task_type,
            task_id=task_id,
            build_sha="b" * 40,
            target_date="2026-08-27",
        )
        row.update(
            {
                "_release_expected_target_required": True,
                "_release_expected_target_available": True,
                "_release_expected_target_date": "2026-08-27",
            }
        )
        return row

    turnover = due_row("target_turnover_snapshot", 991)
    upper = due_row("analysis_upper_evidence_prepare", 992)
    analysis = due_row("analysis_fast", 993)
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value=active_build,
    ):
        assert scheduler_runtime._release_build_catchup_allowed(
            turnover, now=datetime(2026, 8, 27, 19, 0)
        )
        assert not scheduler_runtime._release_build_catchup_allowed(
            upper, now=datetime(2026, 8, 27, 19, 0)
        )
        assert not scheduler_runtime._release_build_catchup_allowed(
            analysis, now=datetime(2026, 8, 27, 19, 0)
        )
        assert scheduler_runtime._release_build_catchup_allowed(
            upper, now=datetime(2026, 8, 27, 23, 40)
        )
        assert not scheduler_runtime._release_build_catchup_allowed(
            analysis, now=datetime(2026, 8, 27, 23, 40)
        )
        assert scheduler_runtime._release_build_catchup_allowed(
            analysis, now=datetime(2026, 8, 27, 23, 55)
        )


def test_release_daily_analysis_rejects_exact_build_dependency_for_other_date():
    build_sha = "c" * 40
    downstream = {
        "id": 994,
        "task_type": "analysis_fast",
        "_release_expected_target_required": True,
        "_release_expected_target_available": True,
        "_release_expected_target_date": "2026-08-27",
    }
    upper = _release_terminal_row(
        "analysis_upper_evidence_prepare",
        task_id=995,
        build_sha=build_sha,
        target_date="2026-08-26",
    )
    upper.update(
        {
            "_release_expected_target_required": True,
            "_release_expected_target_available": True,
            "_release_expected_target_date": "2026-08-26",
        }
    )
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value=build_sha,
    ):
        ready, reason = scheduler_runtime._release_catchup_dependencies_ready(
            downstream,
            [downstream, upper],
        )
    assert not ready
    assert reason == "analysis_upper_evidence_prepare:target_date_mismatch"


@pytest.mark.parametrize(
    "task_type",
    (
        "qmt_stock_daily_canonical",
        "qmt_stock_minute_canonical",
        "qmt_stock_minute_flow_canonical",
        "qmt_index_kline",
        "qmt_index_minute",
    ),
)
def test_release_qmt_closed_evidence_rolls_over_exactly_at_1800(task_type):
    build_sha = "c" * 40
    row = _release_terminal_row(
        task_type,
        task_id=993,
        build_sha=build_sha,
        finished_at=datetime(2026, 8, 27, 17, 59),
        target_date="2026-08-26",
    )
    shanghai = ZoneInfo("Asia/Shanghai")
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value=build_sha,
    ):
        assert scheduler_runtime._attach_release_catchup_expected_targets(
            _ClosedDateEngine("2026-08-26"),
            [row],
            now=datetime(2026, 8, 27, 17, 59, tzinfo=shanghai),
        )
        assert row["_release_expected_target_date"] == "2026-08-26"
        assert not scheduler_runtime._release_build_catchup_allowed(
            row,
            now=datetime(2026, 8, 27, 17, 59),
        )

        assert scheduler_runtime._attach_release_catchup_expected_targets(
            _ClosedDateEngine("2026-08-27"),
            [row],
            now=datetime(2026, 8, 27, 18, 0, tzinfo=shanghai),
        )
        assert row["_release_expected_target_date"] == "2026-08-27"
        assert scheduler_runtime._release_build_catchup_allowed(
            row,
            now=datetime(2026, 8, 27, 18, 0),
        )


def test_release_membership_evidence_rolls_over_exactly_at_1510():
    build_sha = "c" * 40
    row = _release_terminal_row(
        "qmt_membership_snapshot",
        task_id=994,
        build_sha=build_sha,
        finished_at=datetime(2026, 8, 27, 15, 9),
        target_date="2026-08-26",
    )
    shanghai = ZoneInfo("Asia/Shanghai")
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value=build_sha,
    ):
        assert scheduler_runtime._attach_release_catchup_expected_targets(
            _ClosedDateEngine("2026-08-26"),
            [row],
            now=datetime(2026, 8, 27, 15, 9, tzinfo=shanghai),
        )
        assert not scheduler_runtime._release_build_catchup_pending(row)

        assert scheduler_runtime._attach_release_catchup_expected_targets(
            _ClosedDateEngine("2026-08-27"),
            [row],
            now=datetime(2026, 8, 27, 15, 10, tzinfo=shanghai),
        )
        assert row["_release_expected_target_date"] == "2026-08-27"
        assert scheduler_runtime._release_build_catchup_pending(row)
        assert scheduler_runtime._release_build_catchup_allowed(
            row,
            now=datetime(2026, 8, 27, 15, 10),
        )


def test_release_expected_targets_use_closed_previous_and_current_clocks():
    rows = [
        {"task_type": "analysis_fast"},
        {"task_type": "analysis_morning_strict"},
        {"task_type": "qmt_membership_snapshot"},
        {"task_type": "hot_fused"},
        {"task_type": "sim_trade_signal_prepare"},
        {"task_type": "stock_finance"},
    ]
    shanghai = ZoneInfo("Asia/Shanghai")
    for now, closed_target in (
        (datetime(2026, 8, 27, 17, 59, tzinfo=shanghai), "2026-08-26"),
        (datetime(2026, 8, 27, 18, 0, tzinfo=shanghai), "2026-08-27"),
    ):
        with patch(
            "server.api.scheduler_runtime._release_catchup_closed_target_date",
            return_value=closed_target,
        ), patch(
            "server.api.scheduler_runtime._release_catchup_previous_session_target_date",
            return_value="2026-08-26",
        ):
            assert scheduler_runtime._attach_release_catchup_expected_targets(
                object(),
                rows,
                now=now,
            )
        by_type = {row["task_type"]: row for row in rows}
        assert by_type["analysis_fast"]["_release_expected_target_date"] == closed_target
        assert by_type["analysis_morning_strict"]["_release_expected_target_date"] == "2026-08-26"
        assert (
            by_type["qmt_membership_snapshot"][
                "_release_expected_target_date"
            ]
            == closed_target
        )
        assert by_type["hot_fused"]["_release_expected_target_date"] == "2026-08-27"
        assert by_type["sim_trade_signal_prepare"]["_release_expected_target_date"] == "2026-08-27"
        assert by_type["stock_finance"]["_release_expected_target_required"] is False


def test_release_target_authority_outage_blocks_date_sensitive_catchup():
    build_sha = "c" * 40
    row = _release_terminal_row(
        "analysis_fast",
        task_id=991,
        build_sha="b" * 40,
        finished_at=datetime(2026, 8, 27, 3, 0),
    )
    with patch(
        "server.api.scheduler_runtime._release_catchup_closed_target_date",
        side_effect=RuntimeError("calendar unavailable"),
    ):
        assert not scheduler_runtime._attach_release_catchup_expected_targets(
            object(),
            [row],
            now=datetime(2026, 8, 27, 3, 5),
        )
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value=build_sha,
    ):
        assert not scheduler_runtime._release_build_catchup_allowed(
            row,
            now=datetime(2026, 8, 27, 3, 5),
        )
        assert scheduler_runtime._release_build_catchup_pending(row)


def test_release_closed_target_and_worker_launch_fail_closed_without_calendar():
    now = datetime(2026, 8, 27, 3, 5, tzinfo=ZoneInfo("Asia/Shanghai"))
    with unittest.TestCase().assertRaisesRegex(RuntimeError, "is unavailable"):
        scheduler_runtime._release_catchup_closed_target_date(
            _ClosedDateEngine(None),
            now=now,
        )

    row = {
        "id": 991,
        "task_name": "release analysis",
        "task_type": "analysis_fast",
        "script_path": "tools/run_ai_recommendation_premarket.py",
        "script_args": "--json",
        "date_param": "",
        "_trigger_source": "release_catchup",
    }
    with patch(
        "server.api.scheduler_runtime.resolve_scheduler_script",
        return_value=Path(__file__),
    ), patch(
        "server.api.scheduler_runtime.authoritative_closed_trade_date",
        side_effect=RuntimeError("calendar source down"),
    ), patch("server.api.scheduler_runtime.subprocess.Popen") as popen:
        with unittest.TestCase().assertRaisesRegex(RuntimeError, "is unavailable"):
            scheduler_runtime._run_task_impl(
                row,
                Path.cwd(),
                object(),
                history_run_uid="a" * 32,
            )
    popen.assert_not_called()


def test_release_morning_previous_session_target_uses_calendar_without_fallback():
    shanghai = ZoneInfo("Asia/Shanghai")
    cases = (
        (datetime(2026, 8, 27, 3, 5, tzinfo=shanghai), "2026-08-26"),
        (datetime(2026, 8, 27, 17, 59, tzinfo=shanghai), "2026-08-26"),
        (datetime(2026, 8, 27, 18, 50, tzinfo=shanghai), "2026-08-26"),
        (datetime(2026, 8, 29, 10, 0, tzinfo=shanghai), "2026-08-28"),
        (datetime(2026, 10, 5, 10, 0, tzinfo=shanghai), "2026-09-30"),
    )
    for now, expected in cases:
        engine = _ClosedDateEngine(expected)
        assert scheduler_runtime._release_catchup_previous_session_target_date(
            engine,
            now=now,
        ) == expected
        sql, params = engine.connection.calls[-1]
        assert "trade_date < :today" in sql
        assert params == {"today": now.date().isoformat()}

    with unittest.TestCase().assertRaisesRegex(
        scheduler_runtime.ReleaseCatchupDataBlocked,
        "authority is unavailable",
    ):
        scheduler_runtime._release_catchup_previous_session_target_date(
            _ClosedDateEngine(None),
            now=datetime(2026, 8, 27, 3, 5, tzinfo=shanghai),
        )


def test_release_date_dispatch_preserves_ordinary_and_live_snapshot_semantics():
    shanghai = ZoneInfo("Asia/Shanghai")
    ordinary = {"task_type": "analysis_fast", "_trigger_source": "scheduled"}
    assert scheduler_runtime._task_dispatch_date(
        ordinary,
        object(),
        now=datetime(2026, 8, 27, 18, 50, tzinfo=shanghai),
    ) == "2026-08-27"
    utc_bound = scheduler_runtime._task_argument_row(
        ordinary,
        now=datetime(2026, 8, 27, 10, 50, tzinfo=ZoneInfo("UTC")),
        target_date="2026-08-27",
    )
    assert utc_bound["_scheduler_execution_time"] == "2026-08-27T23:55:00"
    assert utc_bound["_scheduler_pipeline_decision_at"] == (
        "2026-08-27T23:55:00"
    )

    live_snapshot = {
        "task_type": "hot_rank_ths",
        "_trigger_source": "release_catchup",
    }
    engine = _ClosedDateEngine("2026-08-27")
    assert scheduler_runtime._task_dispatch_date(
        live_snapshot,
        engine,
        now=datetime(2026, 8, 27, 17, 12, tzinfo=shanghai),
    ) == "2026-08-27"
    sql, params = engine.connection.calls[-1]
    assert "trade_date <= :today" in sql
    assert params == {"today": "2026-08-27"}

    morning_row = scheduler_runtime._task_argument_row(
        {
            "task_type": "analysis_morning_strict",
            "script_args": "--strict-prev-trade-day --json",
            "date_param": "",
            "_trigger_source": "release_catchup",
        },
        now=datetime(2026, 8, 27, 18, 50, tzinfo=shanghai),
        target_date="2026-08-27",
    )
    morning_row = scheduler_runtime._bind_release_validation_target(
        morning_row,
        _ClosedDateEngine("2026-08-26"),
        dispatch_date="2026-08-27",
        now=datetime(2026, 8, 27, 18, 50, tzinfo=shanghai),
    )
    morning_args = scheduler_runtime._build_task_args(
        morning_row,
        "tools/run_ai_recommendation_premarket.py",
        "2026-08-27",
    )
    assert "--date" not in morning_args
    assert morning_args[-2:] == ["--execution-time", "2026-08-27T18:50:00"]
    assert morning_row["_release_target_date"] == "2026-08-26"


def test_release_current_snapshot_dispatch_blocks_before_publish_and_closed_days():
    shanghai = ZoneInfo("Asia/Shanghai")
    row = {"task_type": "hot_rank_ths", "_trigger_source": "release_catchup"}
    cases = (
        (datetime(2026, 8, 27, 3, 5, tzinfo=shanghai), "2026-08-26"),
        (datetime(2026, 8, 27, 17, 11, tzinfo=shanghai), "2026-08-26"),
        (datetime(2026, 8, 29, 18, 0, tzinfo=shanghai), "2026-08-28"),
        (datetime(2026, 10, 5, 18, 0, tzinfo=shanghai), "2026-09-30"),
    )
    for now, calendar_date in cases:
        with unittest.TestCase().assertRaisesRegex(
            scheduler_runtime.ReleaseCatchupDataBlocked,
            "publication window is not open",
        ):
            scheduler_runtime._task_dispatch_date(
                row,
                _ClosedDateEngine(calendar_date),
                now=now,
            )

    with unittest.TestCase().assertRaisesRegex(
        scheduler_runtime.ReleaseCatchupDataBlocked,
        "authority is unavailable",
    ):
        scheduler_runtime._task_dispatch_date(
            row,
            _ClosedDateEngine(None),
            now=datetime(2026, 8, 27, 17, 12, tzinfo=shanghai),
        )


def test_release_index_current_opens_only_at_1510_for_current_session():
    shanghai = ZoneInfo("Asia/Shanghai")
    row = {
        "task_type": "qmt_index_current",
        "_trigger_source": "release_catchup",
    }
    with unittest.TestCase().assertRaisesRegex(
        scheduler_runtime.ReleaseCatchupDataBlocked,
        "publication window is not open",
    ):
        scheduler_runtime._task_dispatch_date(
            row,
            _ClosedDateEngine("2026-08-26"),
            now=datetime(2026, 8, 27, 15, 9, tzinfo=shanghai),
        )

    assert scheduler_runtime._task_dispatch_date(
        row,
        _ClosedDateEngine("2026-08-27"),
        now=datetime(2026, 8, 27, 15, 10, tzinfo=shanghai),
    ) == "2026-08-27"


def test_release_current_snapshot_prelaunch_block_is_retryable_blocked_history():
    row = {"id": 992, "task_name": "release hot", "task_type": "hot_rank_ths"}
    error = scheduler_runtime.ReleaseCatchupDataBlocked(
        "release catch-up current-snapshot publication window is not open"
    )
    with patch(
        "server.api.scheduler_runtime._task_history_start",
        return_value="b" * 32,
    ), patch(
        "server.api.scheduler_runtime._run_task_impl",
        side_effect=error,
    ), patch(
        "server.api.scheduler_runtime.update_scheduler_task",
    ) as update_task, patch(
        "server.api.scheduler_runtime._task_history_finish",
    ) as history_finish:
        scheduler_runtime._run_task(row, Path.cwd(), object())

    assert update_task.call_args.args[2]["last_run_status"] == "blocked"
    assert update_task.call_args.args[2]["last_run_output"].startswith(
        "DATA_BLOCKED:"
    )
    assert history_finish.call_args.kwargs["status"] == "blocked"


def test_release_date_bound_task_inventory_is_explicit_and_complete():
    from tools.ensure_quality_gate import TASKS, TRADING_V3_TASKS

    definitions = {
        str(row.get("task_type") or ""): dict(row)
        for row in (*TASKS, *TRADING_V3_TASKS)
        if str(row.get("task_type") or "")
        in readiness_contract.RELEASE_DATA_CATCHUP_TASK_TYPES
    }
    date_sensitive = set()
    for task_type, row in definitions.items():
        release_row = {**row, "_trigger_source": "release_catchup"}
        if task_type in (
            scheduler_runtime.ANALYSIS_DAILY_EVIDENCE_TASK_TYPES
            | {"analysis_fast"}
        ):
            release_row.update(
                {
                    "_scheduler_execution_time": "2026-08-26T23:55:00",
                    "_scheduler_pipeline_decision_at": "2026-08-26T23:55:00",
                    "_scheduler_pipeline_target_date": "2026-08-26",
                }
            )
        elif task_type in scheduler_runtime.ANALYSIS_POOL_PUBLISHER_TASK_TYPES:
            release_row["_scheduler_execution_time"] = (
                "2026-08-26T03:05:00"
            )
        if task_type in scheduler_runtime.RELEASE_CATCHUP_PREVIOUS_SESSION_TASK_TYPES:
            release_row["_release_execution_time"] = "2026-08-26T03:05:00"
        args_26 = scheduler_runtime._build_task_args(
            release_row,
            str(row.get("script_path") or ""),
            "2026-08-26",
        )
        if task_type in (
            scheduler_runtime.ANALYSIS_DAILY_EVIDENCE_TASK_TYPES
            | {"analysis_fast"}
        ):
            release_row.update(
                {
                    "_scheduler_execution_time": "2026-08-27T23:55:00",
                    "_scheduler_pipeline_decision_at": "2026-08-27T23:55:00",
                    "_scheduler_pipeline_target_date": "2026-08-27",
                }
            )
        elif task_type in scheduler_runtime.ANALYSIS_POOL_PUBLISHER_TASK_TYPES:
            release_row["_scheduler_execution_time"] = (
                "2026-08-27T03:05:00"
            )
        if task_type in scheduler_runtime.RELEASE_CATCHUP_PREVIOUS_SESSION_TASK_TYPES:
            release_row["_release_execution_time"] = "2026-08-27T03:05:00"
        args_27 = scheduler_runtime._build_task_args(
            release_row,
            str(row.get("script_path") or ""),
            "2026-08-27",
        )
        if args_26 != args_27:
            date_sensitive.add(task_type)
    assert date_sensitive == set(
        readiness_contract.RELEASE_CATCHUP_EXACT_TARGET_TASK_TYPES
    )
    classified = (
        readiness_contract.RELEASE_CATCHUP_CLOSED_TARGET_TASK_TYPES,
        readiness_contract.RELEASE_CATCHUP_CURRENT_TARGET_TASK_TYPES,
        readiness_contract.RELEASE_CATCHUP_PREVIOUS_SESSION_TARGET_TASK_TYPES,
    )
    assert all(
        not left & right
        for index, left in enumerate(classified)
        for right in classified[index + 1 :]
    )
    assert set(scheduler_runtime.RELEASE_CATCHUP_CURRENT_SNAPSHOT_READY_TIMES) == set(
        scheduler_runtime.RELEASE_CATCHUP_RUN_DATE_SNAPSHOT_TASK_TYPES
    )

def test_linux_release_catchup_waits_for_exact_health_and_active_link(monkeypatch):
    build_sha = "c" * 40
    expected_root = f"/opt/ProBigA-releases/{build_sha}"
    health = {
        "status": "ok",
        "automatic_real_order_submission": False,
        "real_order_authority": False,
        "release_revision": {
            "deployment_mode": "production",
            "matches_expected": True,
            "code_worktree_clean": True,
            "expected_git_sha": build_sha,
            "actual_git_sha": build_sha,
        },
        "standalone_scheduler_heartbeat": {
            "ready": True,
            "detail": {"expected_build_sha": build_sha},
        },
    }
    monkeypatch.setattr(
        scheduler_runtime,
        "os",
        SimpleNamespace(
            name="posix",
            environ={"PROBIGA_CODE_ROOT": expected_root},
        ),
    )
    ready, reason = scheduler_runtime._linux_active_release_ready(
        build_sha,
        health_loader=lambda: health,
        active_code_root_loader=lambda: expected_root,
    )
    assert ready
    assert reason == "ready"

    ready, reason = scheduler_runtime._linux_active_release_ready(
        build_sha,
        health_loader=lambda: health,
        active_code_root_loader=lambda: "/opt/ProBigA-releases/" + "b" * 40,
    )
    assert not ready
    assert reason == "active_link_mismatch"

    health["release_revision"]["actual_git_sha"] = "b" * 40
    ready, reason = scheduler_runtime._linux_active_release_ready(
        build_sha,
        health_loader=lambda: health,
        active_code_root_loader=lambda: expected_root,
    )
    assert not ready
    assert reason == "active_health_identity_mismatch"


def test_linux_authorization_never_publishes_before_active_health():
    rows = [{"id": 910, "task_type": "stock_finance"}]
    with patch(
        "server.api.scheduler_runtime._scheduler_build_commit_sha",
        return_value="c" * 40,
    ), patch(
        "server.api.scheduler_runtime._scheduler_executor_role",
        return_value="linux_standalone",
    ), patch(
        "server.api.scheduler_runtime._linux_active_release_ready",
        return_value=(False, "active_link_mismatch"),
    ), patch(
        "server.api.scheduler_runtime._publish_linux_release_activation"
    ) as publish:
        ready, reason = scheduler_runtime._attach_release_catchup_authorization(
            object(),
            rows,
            mode="standalone",
            now=datetime(2026, 8, 27, 7, 0),
        )
    assert not ready
    assert reason == "active_link_mismatch"
    assert rows[0]["_release_catchup_authorized"] is False
    publish.assert_not_called()


def test_windows_catchup_requires_current_linux_activation_and_qmt_bootstrap():
    build_sha = "c" * 40
    instance_id = "linux-prod-4321"
    started_at = "2026-08-27T07:00:00"
    receipt = readiness_contract.build_release_data_activation_receipt(
        build_sha=build_sha,
        scheduler_instance_id=instance_id,
        scheduler_host_name="linux-prod",
        scheduler_pid=4321,
        scheduler_started_at=started_at,
        activated_at="2026-08-27T07:02:00",
    )
    activation_row = {
        "run_uid": readiness_contract.release_data_activation_run_uid(
            build_sha,
            instance_id,
            started_at,
        ),
        "task_type": readiness_contract.RELEASE_DATA_ACTIVATION_TASK_TYPE,
        "status": "success",
        "exit_code": 0,
        "output": json.dumps(receipt, sort_keys=True, separators=(",", ":")),
        "host_name": "linux-prod",
        "scheduler_instance_id": instance_id,
        "build_sha": build_sha,
        "trigger_source": (
            readiness_contract.RELEASE_DATA_ACTIVATION_TRIGGER_SOURCE
        ),
    }
    connection = MagicMock()
    connection.execute.return_value.mappings.return_value = []
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = connection
    linux_detail = {
        "current": {
            "instance_id": instance_id,
            "host_name": "linux-prod",
            "pid": 4321,
            "started_at": started_at,
        }
    }
    with patch(
        "server.api.scheduler_runtime.get_scheduler_runtime_config",
        return_value={"poll_seconds": 60},
    ), patch(
        "server.api.scheduler_runtime.check_linux_standalone_active_release",
        return_value=(True, linux_detail),
    ), patch(
        "server.api.scheduler_runtime.check_qmt_windows_edge_release_receipt",
        return_value=(True, {}),
    ) as qmt_check:
        ready, reason = scheduler_runtime._windows_release_activation_ready(
            engine,
            build_sha=build_sha,
        )
        assert not ready
        assert reason == "linux_activation_receipt_not_unique"
        qmt_check.assert_not_called()

        connection.execute.return_value.mappings.return_value = [activation_row]
        ready, reason = scheduler_runtime._windows_release_activation_ready(
            engine,
            build_sha=build_sha,
        )
        assert ready
        assert reason == "ready"
        qmt_check.assert_called_once()

        qmt_check.reset_mock()
        qmt_check.return_value = (False, {"errors": ["receipt_missing"]})
        ready, reason = scheduler_runtime._windows_release_activation_ready(
            engine,
            build_sha=build_sha,
        )
        assert not ready
        assert reason == "qmt_release_bootstrap_unavailable"


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


def test_scheduler_cron_wall_clock_is_shanghai_when_host_clock_is_utc(
    monkeypatch,
) -> None:
    class UtcHostDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            current = datetime(
                2026, 8, 27, 7, 50, tzinfo=ZoneInfo("UTC")
            )
            if tz is None:
                return current.replace(tzinfo=None)
            return current.astimezone(tz)

    monkeypatch.setattr(scheduler_runtime, "datetime", UtcHostDateTime)

    shanghai_now = scheduler_runtime._now_shanghai_naive()
    assert shanghai_now == datetime(2026, 8, 27, 15, 50)
    assert scheduler_runtime._cron_due(
        {
            "task_type": "target_turnover_snapshot",
            "cron_time": "15:50",
            "last_triggered_at": None,
            "last_run_status": None,
        },
        now=shanghai_now,
    )
    assert not scheduler_runtime._cron_due(
        {
            "task_type": "analysis_upper_evidence_prepare",
            "cron_time": "23:40",
            "last_triggered_at": None,
            "last_run_status": None,
        },
        now=shanghai_now,
    )
    assert "_now_shanghai_naive()" in inspect.getsource(
        scheduler_runtime._cleanup_stale_running_tasks
    )
    assert "startup_time = _now_shanghai_naive()" in inspect.getsource(
        scheduler_runtime._check_and_run_tasks
    )


def test_daily_analysis_evidence_dag_requires_same_day_ordered_success() -> None:
    now = datetime(2026, 8, 27, 23, 40)

    def row(task_type: str, minute: int) -> dict:
        return {
            "task_type": task_type,
            "enabled": 1,
            "last_triggered_at": datetime(2026, 8, 27, 15, minute),
            "last_run_status": "success",
        }

    upper_rows = [
        row("target_turnover_snapshot", 50),
        row("capital_flow_batch_fast", 24),
        row("qmt_membership_snapshot", 12),
        row("qmt_stock_daily_canonical", 45),
        {
            **row("analysis_upper_evidence_prepare", 55),
            "last_triggered_at": None,
            "last_run_status": None,
        },
    ]
    ready, reason = (
        scheduler_runtime.evaluate_daily_analysis_evidence_dependencies(
            "analysis_upper_evidence_prepare", upper_rows, now=now
        )
    )
    assert ready, reason

    missing_flow = [
        item for item in upper_rows
        if item["task_type"] != "capital_flow_batch_fast"
    ]
    ready, reason = (
        scheduler_runtime.evaluate_daily_analysis_evidence_dependencies(
            "analysis_upper_evidence_prepare", missing_flow, now=now
        )
    )
    assert not ready
    assert reason == "capital_flow_batch_fast:missing_or_duplicate"

    analysis_rows = [
        row("analysis_upper_evidence_prepare", 56),
        row("target_turnover_snapshot", 50),
        row("capital_flow_batch_fast", 24),
        row("qmt_membership_snapshot", 12),
        row("qmt_stock_daily_canonical", 45),
        row("qmt_announcement_pit", 30),
        {
            **row("analysis_fast", 57),
            "last_triggered_at": datetime(2026, 8, 27, 15, 40),
        },
    ]
    ready, reason = (
        scheduler_runtime.evaluate_daily_analysis_evidence_dependencies(
            "analysis_fast", analysis_rows, now=now
        )
    )
    assert not ready
    assert reason == "analysis_fast:ran_before_dependency"
