# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch

import sqlalchemy
from sqlalchemy import create_engine, text

from server.common.scheduler_validation import TASK_OUTPUT_REQUIREMENTS
from tools import ensure_quality_gate
from tools import check_and_fix_scheduled_tasks
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

    def test_0908_theme_forecast_task_is_enabled_and_strictly_validated(self):
        task = next(
            item for item in ensure_quality_gate.TASKS
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

    def test_intraday_alert_task_definitions_are_exact_and_consistent(self):
        authoritative = {
            item["task_type"]: item for item in ensure_quality_gate.TASKS
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

    def test_review_pipeline_tasks_are_authoritative_and_ordered(self):
        tasks = {item["task_type"]: item for item in ensure_quality_gate.TASKS}

        self.assertEqual(tasks["news_daily"]["script_path"], "biz/early_briefing/generate.py")
        self.assertEqual(tasks["news_daily"]["cron_time"], "08:30")
        snapshot = tasks["qmt_membership_snapshot"]
        self.assertEqual(snapshot["script_path"], "tools/sync_bigqmt_reference.py")
        self.assertEqual(snapshot["script_args"], "--apply --force-reference-refresh --json")
        self.assertEqual(snapshot["cron_time"], "15:12")
        self.assertEqual(tasks["daily_review"]["script_path"], "biz/review/generate.py")
        self.assertEqual(tasks["daily_review"]["cron_time"], "18:00")
        self.assertEqual(tasks["evening_review"]["script_path"], "biz/evening_review/generate.py")
        self.assertEqual(tasks["evening_review"]["cron_time"], "20:00")
        self.assertTrue(
            all(
                tasks[key]["enabled"] == 1
                for key in (
                    "news_daily",
                    "qmt_membership_snapshot",
                    "daily_review",
                    "evening_review",
                )
            )
        )
        self.assertLess(snapshot["sort_order"], tasks["daily_review"]["sort_order"])
        self.assertLess(snapshot["cron_time"], tasks["daily_review"]["cron_time"])
        ordered = ["news_daily", "qmt_membership_snapshot", "daily_review", "evening_review"]
        self.assertEqual(
            sorted(ordered, key=lambda key: tasks[key]["sort_order"]),
            ordered,
        )
        self.assertEqual(
            sorted(ordered, key=lambda key: tasks[key]["cron_time"]),
            ordered,
        )

    def test_legacy_task_repair_defines_same_review_pipeline(self):
        tasks = {
            item[1]: {
                "script_path": item[2],
                "script_args": item[3],
                "cron_time": item[4],
                "sort_order": item[5],
            }
            for item in check_and_fix_scheduled_tasks.REQUIRED_TASKS
        }

        self.assertEqual(tasks["qmt_membership_snapshot"]["script_path"], "tools/sync_bigqmt_reference.py")
        self.assertEqual(
            tasks["qmt_membership_snapshot"]["script_args"],
            "--apply --force-reference-refresh --json",
        )
        self.assertEqual(tasks["qmt_membership_snapshot"]["cron_time"], "15:12")
        self.assertEqual(tasks["news_daily"]["cron_time"], "08:30")
        self.assertEqual(tasks["daily_review"]["cron_time"], "18:00")
        self.assertEqual(tasks["evening_review"]["cron_time"], "20:00")
        self.assertLess(
            tasks["qmt_membership_snapshot"]["sort_order"],
            tasks["daily_review"]["sort_order"],
        )
        ordered = ["news_daily", "qmt_membership_snapshot", "daily_review", "evening_review"]
        self.assertEqual(sorted(ordered, key=lambda key: tasks[key]["sort_order"]), ordered)
        self.assertEqual(sorted(ordered, key=lambda key: tasks[key]["cron_time"]), ordered)

    def test_main_uses_shared_tls_capable_engine_factory(self):
        sentinel_engine = object()
        with (
            patch.object(ensure_quality_gate, "get_mysql_url", return_value="mysql+pymysql://db") as url,
            patch.object(
                ensure_quality_gate,
                "create_pooled_engine",
                return_value=sentinel_engine,
            ) as factory,
            patch.object(ensure_quality_gate, "run", return_value={}) as run,
            patch.object(
                sqlalchemy,
                "create_engine",
                side_effect=AssertionError("direct SQLAlchemy engine bypassed TLS policy"),
            ),
        ):
            self.assertEqual(ensure_quality_gate.main(["--review-delivery-only"]), 0)

        url.assert_called_once_with(required=True)
        factory.assert_called_once_with("mysql+pymysql://db", pool_pre_ping=True)
        run.assert_called_once_with(
            sentinel_engine,
            task_types=ensure_quality_gate.REVIEW_DELIVERY_TASK_TYPES,
            intraday_alert_mode="shadow",
        )

    def test_validate_cli_is_read_only(self):
        sentinel_engine = object()
        with (
            patch.object(ensure_quality_gate, "get_mysql_url", return_value="mysql+pymysql://db"),
            patch.object(
                ensure_quality_gate,
                "create_pooled_engine",
                return_value=sentinel_engine,
            ),
            patch.object(
                ensure_quality_gate,
                "validate_review_delivery",
                return_value={"news_daily": "validated"},
            ) as validate,
            patch.object(ensure_quality_gate, "run") as run,
        ):
            self.assertEqual(
                ensure_quality_gate.main(["--validate-review-delivery"]),
                0,
            )
        validate.assert_called_once_with(sentinel_engine)
        run.assert_not_called()

    def test_review_delivery_release_scope_is_exact(self):
        self.assertEqual(
            ensure_quality_gate.REVIEW_DELIVERY_TASK_TYPES,
            {
                "qmt_membership_snapshot",
                "news_daily",
                "daily_review",
                "evening_review",
            },
        )

    def test_run_filters_to_requested_task_types(self):
        sentinel_engine = object()
        with (
            patch.object(ensure_quality_gate, "ensure_scheduler_columns") as ensure,
            patch.object(
                ensure_quality_gate,
                "upsert_task",
                side_effect=lambda _engine, task: task["task_type"],
            ) as upsert,
        ):
            result = ensure_quality_gate.run(
                sentinel_engine,
                task_types=ensure_quality_gate.REVIEW_DELIVERY_TASK_TYPES,
            )

        ensure.assert_called_once_with(sentinel_engine)
        self.assertEqual(len(result), 4)
        self.assertEqual(
            {call.args[1]["task_type"] for call in upsert.call_args_list},
            ensure_quality_gate.REVIEW_DELIVERY_TASK_TYPES,
        )

    def test_run_rejects_unknown_scope_before_database_changes(self):
        with patch.object(ensure_quality_gate, "ensure_scheduler_columns") as ensure:
            with self.assertRaisesRegex(ValueError, "unknown scheduled task types: typo"):
                ensure_quality_gate.run(object(), task_types={"typo"})
        ensure.assert_not_called()

    def test_review_delivery_validation_accepts_exact_rows_and_rejects_drift(self):
        engine = create_engine("sqlite+pysqlite:///:memory:")
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    CREATE TABLE st_scheduled_tasks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        task_name TEXT NOT NULL,
                        task_type TEXT NOT NULL UNIQUE,
                        group_name TEXT,
                        script_path TEXT,
                        script_args TEXT,
                        date_param_desc TEXT,
                        cron_time TEXT,
                        interval_minutes INTEGER,
                        enabled INTEGER,
                        description TEXT,
                        sort_order INTEGER,
                        date_param TEXT,
                        updated_at DATETIME,
                        created_at DATETIME,
                        etl_sync_at DATETIME,
                        last_triggered_at DATETIME,
                        last_run_output TEXT,
                        last_run_duration INTEGER,
                        last_run_status TEXT,
                        last_run_at DATETIME
                    )
                    """
                )
            )
            connection.connection.create_function(
                "NOW",
                0,
                lambda: "2026-08-12 22:00:00",
            )
        ensure_quality_gate.run(
            engine,
            task_types=ensure_quality_gate.REVIEW_DELIVERY_TASK_TYPES,
        )
        self.assertEqual(
            ensure_quality_gate.validate_review_delivery(engine),
            {
                task_type: "validated"
                for task_type in sorted(ensure_quality_gate.REVIEW_DELIVERY_TASK_TYPES)
            },
        )
        with engine.begin() as connection:
            connection.execute(
                text(
                    "UPDATE st_scheduled_tasks SET enabled=0 "
                    "WHERE task_type='news_daily'"
                )
            )
        with self.assertRaisesRegex(RuntimeError, "news_daily drifted fields: enabled"):
            ensure_quality_gate.validate_review_delivery(engine)

if __name__ == "__main__":
    unittest.main()
