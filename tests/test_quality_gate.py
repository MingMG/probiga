# -*- coding: utf-8 -*-
import unittest
from unittest.mock import patch

from tools import ensure_quality_gate
from tools.ensure_quality_gate import _task_payload
from server.common.scheduler_validation import (
    TASK_OUTPUT_REQUIREMENTS,
    is_market_closed_skip_output,
    scheduler_output_status,
)


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

    def test_main_uses_batch_engine(self):
        engine = object()
        with patch("tools.ensure_quality_gate.create_batch_engine", return_value=engine) as create_batch_engine, \
             patch("tools.ensure_quality_gate.run", return_value={"quality": "updated"}) as run_mock:
            self.assertEqual(ensure_quality_gate.main(), 0)

        create_batch_engine.assert_called_once_with()
        run_mock.assert_called_once_with(engine)

    def test_price_and_flow_minute_jobs_have_independent_validation(self):
        self.assertEqual(
            [item.table for item in TASK_OUTPUT_REQUIREMENTS["stock_minute"]],
            ["sm_stock_minute"],
        )
        self.assertEqual(
            [item.table for item in TASK_OUTPUT_REQUIREMENTS["stock_minute_flow"]],
            ["sm_stock_capital_flow_min"],
        )

    def test_index_market_data_validates_latest_trade_date_off_hours(self):
        for task_type in ("index_current", "index_kline", "index_minute"):
            requirement = TASK_OUTPUT_REQUIREMENTS[task_type][0]
            self.assertEqual(requirement.target, "latest_trade_date")

        self.assertEqual(
            TASK_OUTPUT_REQUIREMENTS["index_minute"][0].ready_time,
            "15:30",
        )

    def test_full_minute_flow_runs_after_hours_with_atomic_crawler(self):
        task = next(
            item for item in ensure_quality_gate.TASKS
            if item["task_type"] == "stock_minute_flow"
        )
        self.assertEqual(task["enabled"], 1)
        self.assertEqual(task["cron_time"], "22:30")
        self.assertEqual(task["script_path"], "tools/crawl_minute_kline.py")
        self.assertIn("--type flow", task["script_args"])
        self.assertNotIn("--limit", task["script_args"])

    def test_full_minute_price_runs_after_close_without_sampling_limit(self):
        task = next(
            item for item in ensure_quality_gate.TASKS
            if item["task_type"] == "stock_minute"
        )
        self.assertEqual(task["enabled"], 1)
        self.assertEqual(task["cron_time"], "15:30")
        self.assertEqual(task["script_path"], "tools/crawl_minute_kline.py")
        self.assertIn("--type stock", task["script_args"])
        self.assertNotIn("--limit", task["script_args"])

    def test_intraday_minute_flow_covers_full_market_without_overlapping(self):
        task = next(
            item for item in ensure_quality_gate.TASKS
            if item["task_type"] == "intraday_minute_flow"
        )
        self.assertEqual(task["enabled"], 1)
        self.assertEqual(task["cron_time"], "09:40")
        self.assertEqual(task["interval_minutes"], 30)
        self.assertIn("--type flow", task["script_args"])
        self.assertIn("--min-coverage 0.98", task["script_args"])
        self.assertIn("--fetch-attempts 2", task["script_args"])
        self.assertIn("--skip-closed", task["script_args"])
        self.assertNotIn("--limit", task["script_args"])
        self.assertGreaterEqual(
            TASK_OUTPUT_REQUIREMENTS["intraday_minute_flow"][0].min_distinct,
            5000,
        )

    def test_intraday_minute_price_uses_coverage_not_full_day_row_count(self):
        requirement = TASK_OUTPUT_REQUIREMENTS["intraday_minute_kline"][0]

        self.assertEqual(requirement.min_rows, 5000)
        self.assertGreaterEqual(requirement.min_distinct, 5000)

    def test_analysis_validation_accepts_a_completed_zero_pick_run(self):
        for task_type in (
            "analysis_fast",
            "analysis_morning_strict",
            "analysis_premarket_external",
        ):
            requirements = TASK_OUTPUT_REQUIREMENTS[task_type]

            self.assertEqual(len(requirements), 1)
            self.assertEqual(requirements[0].table, "stock_analysis_result")
            self.assertIn("recommend_status IS NOT NULL", requirements[0].where_sql)
            self.assertNotIn(
                "st_recommended_stocks",
                [requirement.table for requirement in requirements],
            )

    def test_market_closed_minute_skip_is_not_post_validated(self):
        self.assertTrue(
            is_market_closed_skip_output("Minute sync skipped: market closed")
        )
        self.assertTrue(
            is_market_closed_skip_output(
                '{"status": "skipped", "reason": "market_closed"}'
            )
        )

    def test_level1_capability_status_is_not_hidden_by_zero_exit_code(self):
        task = {"task_type": "trading_v2_level1_validation"}

        self.assertEqual(
            scheduler_output_status(task, '{"status": "BLOCK"}'),
            "blocked",
        )
        self.assertEqual(
            scheduler_output_status(task, '{"status": "PASS"}'),
            "success",
        )
        self.assertIsNone(
            scheduler_output_status(
                {"task_type": "stock_kline"},
                '{"status": "BLOCK"}',
            )
        )


if __name__ == "__main__":
    unittest.main()
