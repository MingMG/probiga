# -*- coding: utf-8 -*-
import unittest
from unittest.mock import MagicMock, patch

import sqlalchemy
from sqlalchemy import create_engine, text

from server.common.scheduler_validation import TASK_OUTPUT_REQUIREMENTS
from tools import ensure_quality_gate
from tools import check_and_fix_scheduled_tasks
from tools.ensure_quality_gate import _task_payload


class QualityGateTaskTest(unittest.TestCase):
    def test_capital_flow_writer_uses_eastmoney_contract(self):
        task = {
            item["task_type"]: item for item in ensure_quality_gate.TASKS
        }["capital_flow_batch_fast"]

        self.assertEqual(
            task["script_path"],
            "tools/crawl_realtime_batch.py",
        )
        self.assertEqual(
            task["script_args"],
            "--only flow --min-coverage 0.70 --json",
        )
        self.assertEqual(task["cron_time"], "15:20")
        self.assertEqual(task["enabled"], 1)

    def test_existing_direct_capital_flow_mode_is_replaced_by_eastmoney(self):
        engine = create_engine("sqlite+pysqlite:///:memory:")
        with engine.begin() as connection:
            connection.execute(text("""
                CREATE TABLE st_scheduled_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_name TEXT NOT NULL,
                    task_type TEXT NOT NULL UNIQUE,
                    group_name TEXT,
                    script_path TEXT,
                    script_args TEXT,
                    cron_time TEXT,
                    interval_minutes INTEGER,
                    enabled INTEGER,
                    description TEXT,
                    sort_order INTEGER,
                    date_param TEXT
                )
            """))
            connection.execute(text("""
                INSERT INTO st_scheduled_tasks (
                    task_name, task_type, group_name, script_path, script_args,
                    cron_time, interval_minutes, enabled, description,
                    sort_order, date_param
                ) VALUES (
                    '国金 QMT 日资金流验收', 'capital_flow_batch_fast', '系统管理',
                    'tools/verify_direct_capital_flow_daily.py', '--wrong',
                    '00:00', 5, 0, 'drifted', 1, 'stale'
                )
            """))

        with self.assertRaisesRegex(
            RuntimeError,
            "scheduler task capital_flow_batch_fast drifted fields",
        ):
            ensure_quality_gate.validate_managed_task_contracts(
                engine,
                task_types={"capital_flow_batch_fast"},
            )
        with self.assertRaisesRegex(
            RuntimeError,
            "release scheduler identity is owned by another row",
        ):
            ensure_quality_gate._load_release_task_rows(
                engine,
                {
                    "capital_flow_batch_fast":
                    ensure_quality_gate.LEGACY_CAPITAL_FLOW_BATCH_TASK,
                },
            )

        with patch.object(ensure_quality_gate, "ensure_scheduler_columns"):
            ensure_quality_gate.run(
                engine,
                task_types={"capital_flow_batch_fast"},
            )

        with engine.connect() as connection:
            row = dict(connection.execute(text("""
                SELECT task_name, task_type, group_name, script_path,
                       script_args, cron_time, interval_minutes, enabled,
                       description, sort_order, date_param
                  FROM st_scheduled_tasks
                 WHERE task_type='capital_flow_batch_fast'
            """)).mappings().one())
        self.assertEqual(row, ensure_quality_gate.LEGACY_CAPITAL_FLOW_BATCH_TASK)
        self.assertEqual(
            ensure_quality_gate.validate_managed_task_contracts(
                engine,
                task_types={"capital_flow_batch_fast"},
            ),
            {"capital_flow_batch_fast": "validated"},
        )
        self.assertEqual(
            ensure_quality_gate._load_release_task_rows(
                engine,
                {
                    "capital_flow_batch_fast":
                    ensure_quality_gate.LEGACY_CAPITAL_FLOW_BATCH_TASK,
                },
            )["capital_flow_batch_fast"]["script_path"],
            ensure_quality_gate.LEGACY_CAPITAL_FLOW_BATCH_TASK["script_path"],
        )

    def test_mysql_task_mode_selection_locks_existing_row(self):
        connection = MagicMock()
        connection.dialect.name = "mysql"
        selected = MagicMock()
        selected.mappings.return_value = []
        connection.execute.side_effect = [selected, MagicMock()]
        transaction = MagicMock()
        transaction.__enter__.return_value = connection
        engine = MagicMock()
        engine.begin.return_value = transaction

        with patch.object(
            ensure_quality_gate,
            "_table_columns",
            return_value=set(ensure_quality_gate.TASK_PAYLOAD_COLUMNS),
        ):
            ensure_quality_gate.upsert_task(
                engine,
                ensure_quality_gate.LEGACY_CAPITAL_FLOW_BATCH_TASK,
            )

        select_sql = str(connection.execute.call_args_list[0].args[0])
        self.assertIn("ORDER BY id FOR UPDATE", select_sql)

    def test_acquisition_monitor_is_separate_periodic_read_only_task(self):
        tasks = {item["task_type"]: item for item in ensure_quality_gate.TASKS}
        task = tasks["acquisition_quality_check"]
        self.assertEqual(task["script_path"], "tools/data_quality_check.py")
        self.assertEqual(task["script_args"], "--acquisition --json --fail-on-warn")
        self.assertEqual(task["interval_minutes"], 15)
        self.assertEqual(task["enabled"], 1)
        self.assertEqual(task["date_param"], "")
        self.assertEqual(sum(item["task_type"] == "acquisition_quality_check" for item in ensure_quality_gate.TASKS), 1)
        for existing in ("quality_check_pre", "quality_check_post", "intraday_quality_check"):
            self.assertNotIn("--acquisition", tasks[existing]["script_args"])

    def test_portfolio_quote_refresh_is_registered_as_independent_lane(self):
        task = {
            item["task_type"]: item for item in ensure_quality_gate.TASKS
        }["portfolio_quote_refresh"]
        self.assertEqual(
            task["script_path"],
            "tools/run_portfolio_quote_refresh.py",
        )
        self.assertEqual(task["interval_minutes"], 1)
        self.assertEqual(task["group_name"], "盘中交易")

    def test_public_quote_failover_is_registered_as_qmt_outage_fallback(self):
        task = {
            item["task_type"]: item for item in ensure_quality_gate.TASKS
        }["public_quote_failover"]

        self.assertEqual(task["script_path"], "tools/run_public_quote_failover.py")
        self.assertEqual(task["cron_time"], "09:25")
        self.assertEqual(task["interval_minutes"], 1)
        self.assertEqual(task["enabled"], 1)
        self.assertIn("新浪和腾讯", task["description"])

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

    def test_legacy_0908_combined_publisher_is_disabled_and_validated(self):
        task = next(
            item for item in ensure_quality_gate.TASKS
            if item.get("task_type") == "analysis_premarket_external"
        )
        self.assertEqual(task["cron_time"], "09:07")
        self.assertEqual(task["enabled"], 0)
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
                ensure_quality_gate,
                "validate_managed_task_contracts",
                return_value={},
            ) as validate_managed,
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
        validate_managed.assert_called_once_with(
            sentinel_engine,
            task_types=ensure_quality_gate.REVIEW_DELIVERY_TASK_TYPES,
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

    def test_release_quarantines_legacy_generic_stock_writers(self):
        engine = create_engine("sqlite+pysqlite:///:memory:")
        with engine.begin() as connection:
            connection.execute(text("""
                CREATE TABLE st_scheduled_tasks (
                    id INTEGER PRIMARY KEY,
                    task_type TEXT NOT NULL,
                    enabled INTEGER NOT NULL,
                    updated_at DATETIME
                )
            """))
            connection.connection.create_function(
                "NOW", 0, lambda: "2026-08-26 20:00:00"
            )
            connection.execute(text("""
                INSERT INTO st_scheduled_tasks VALUES
                    (1,'stock_kline',1,NULL),
                    (2,'stock_minute',1,NULL),
                    (3,'intraday_minute_flow',1,NULL),
                    (4,'capital_flow_batch_fast',1,NULL),
                    (5,'capital_flow',1,NULL)
            """))

        assert (
            ensure_quality_gate.quarantine_legacy_canonical_market_writers(
                engine
            )
            == 2
        )
        with engine.connect() as connection:
            rows = dict(connection.execute(text(
                "SELECT task_type,enabled FROM st_scheduled_tasks"
            )).all())
        self.assertEqual(rows["stock_kline"], 0)
        self.assertEqual(rows["stock_minute"], 0)
        self.assertEqual(rows["capital_flow_batch_fast"], 1)
        self.assertEqual(rows["capital_flow"], 1)
        self.assertEqual(rows["intraday_minute_flow"], 1)

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
