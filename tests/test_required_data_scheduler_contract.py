from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from server.api import scheduler_runtime
from server.common import scheduler_validation
from tools import ensure_quality_gate


ROOT = Path(__file__).resolve().parents[1]


def _tasks() -> dict[str, dict]:
    return {task["task_type"]: task for task in ensure_quality_gate.TASKS}


def _scheduler_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE st_scheduled_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name TEXT NOT NULL,
                task_type TEXT NOT NULL,
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
        """))
        connection.connection.create_function(
            "NOW", 0, lambda: "2026-08-26 20:00:00"
        )
    return engine


def test_required_finance_notice_and_dividend_tasks_are_exact() -> None:
    tasks = _tasks()
    assert ensure_quality_gate.REQUIRED_DATA_COMPLETION_TASK_TYPES == {
        "stock_finance",
        "notice_eastmoney",
        "notice_eastmoney_historical_repair",
        "stock_dividend_baidu",
    }
    assert tasks["stock_finance"] == {
        "task_name": "全市场股票财务PIT同步",
        "task_type": "stock_finance",
        "group_name": "资讯公告",
        "script_path": "biz/stock_finance/sync_finance.py",
        "script_args": "--limit 0 --sleep 0.3 --min-code-coverage 1.0",
        "cron_time": "21:00",
        "interval_minutes": 0,
        "enabled": 1,
        "sort_order": 35,
        "date_param": "",
        "description": (
            "全市场逐股同步非空财务报告并追加PIT覆盖凭证；任一股票失败、"
            "空响应或最新报告期过旧时整批失败。"
        ),
    }
    assert tasks["notice_eastmoney"]["task_name"] == "东财个股公告同步"
    assert tasks["notice_eastmoney"]["script_args"] == (
        "--mode incremental --from-si-all-code --limit 0 "
        "--lookback-days 45 --forward-days 1 --page-size 100 "
        "--max-pages 1000 --sleep 0.15 --min-coverage 1.00 "
        "--min-row-coverage 0.00"
    )
    assert tasks["notice_eastmoney"]["cron_time"] == "20:15"
    assert tasks["notice_eastmoney"]["enabled"] == 1
    assert tasks["notice_eastmoney_historical_repair"] == {
        "task_name": "东财公告全历史错配修复",
        "task_type": "notice_eastmoney_historical_repair",
        "group_name": "资讯公告",
        "script_path": "biz/notice/sync_notice_em.py",
        "script_args": (
            "--mode historical-repair --from-si-all-code --limit 0 "
            "--history-state-file "
            "/var/lib/probiga/jobs/notice-eastmoney-history-repair-v1.json "
            "--history-shard-size 250 --page-size 100 --max-pages 1000 "
            "--sleep 0.15"
        ),
        "cron_time": "00:05",
        "interval_minutes": 5,
        "enabled": 1,
        "sort_order": 33,
        "date_param": "",
            "description": (
                "每5分钟恢复下一段当前目录与历史公告代码并集（单批最多250只）；"
                "PROGRESS失败重试，"
                "仅整池hash账本COMPLETE成功；代码集增加时保留旧COMPLETE代次，"
                "新建不可变代次并只抓新增代码。"
            ),
    }
    assert tasks["stock_dividend_baidu"]["script_path"] == (
        "biz/stock_market/sync_dividend_baidu.py"
    )
    assert tasks["stock_dividend_baidu"]["script_args"] == (
        "--execute --workers 4 --sleep 0.1 --min-nonempty-code-ratio 0.2"
    )


def test_required_data_task_install_is_idempotent_and_validated() -> None:
    engine = _scheduler_engine()
    expected_types = ensure_quality_gate.REQUIRED_DATA_COMPLETION_TASK_TYPES

    ensure_quality_gate.run(engine, task_types=expected_types)
    ensure_quality_gate.run(engine, task_types=expected_types)

    with engine.connect() as connection:
        rows = connection.execute(
            text("SELECT task_type, COUNT(*) FROM st_scheduled_tasks GROUP BY task_type")
        ).all()
    assert dict(rows) == {
        "notice_eastmoney": 1,
        "notice_eastmoney_historical_repair": 1,
        "stock_dividend_baidu": 1,
        "stock_finance": 1,
    }
    assert ensure_quality_gate.validate_required_data_completion(engine) == {
        "notice_eastmoney": "validated",
        "notice_eastmoney_historical_repair": "validated",
        "stock_dividend_baidu": "validated",
        "stock_finance": "validated",
    }


def test_required_data_validation_rejects_duplicate_task_identity() -> None:
    engine = _scheduler_engine()
    task = _tasks()["stock_finance"]
    with engine.begin() as connection:
        for suffix in ("", " duplicate"):
            connection.execute(
                text("""
                    INSERT INTO st_scheduled_tasks
                    (task_name, task_type, group_name, script_path, script_args,
                     cron_time, interval_minutes, enabled, description,
                     sort_order, date_param)
                    VALUES
                    (:task_name, :task_type, :group_name, :script_path,
                     :script_args, :cron_time, :interval_minutes, :enabled,
                     :description, :sort_order, :date_param)
                """),
                {**task, "task_name": task["task_name"] + suffix},
            )

    with pytest.raises(RuntimeError, match="duplicate scheduler task type"):
        ensure_quality_gate.validate_required_data_completion(engine)


def test_finance_machine_result_requires_nonempty_full_coverage() -> None:
    passing = {
        "schema": "probiga.finance-sync-result.v1",
        "status": "PASS",
        "minimum_report_date": "2026-03-31",
        "minimum_report_disclosure_deadline": "2026-04-30",
        "oldest_latest_report_date": "2026-03-31",
        "oldest_latest_applicable_report_date": "2026-03-31",
        "report_period_applicable_code_count": 5200,
        "new_listing_period_exempt_code_count": 0,
        "requested_code_count": 5200,
        "nonempty_code_count": 5200,
        "nonempty_code_coverage": 1.0,
        "written_report_count": 5200,
        "failure_count": 0,
    }
    assert scheduler_validation.scheduler_output_status(
        {"task_type": "stock_finance"},
        json.dumps(passing),
        return_code=0,
    ) == "success"

    empty = {**passing, "nonempty_code_count": 0, "nonempty_code_coverage": 0.0,
             "written_report_count": 0}
    assert scheduler_validation.scheduler_output_status(
        {"task_type": "stock_finance"},
        json.dumps(empty),
        return_code=0,
    ) == "failed"


def test_finance_db_validator_requires_fresh_nonempty_receipt_for_every_code(
    monkeypatch,
) -> None:
    def fake_read_all(engine, sql, params=None):
        normalized = " ".join(sql.split())
        if "FROM si_all_code" in normalized and "LEFT JOIN" not in normalized:
            return [
                {"stock_code": "000001", "list_date": "1991-01-01"},
                {"stock_code": "000002", "list_date": "1991-01-01"},
            ]
        if "FROM st_pit_source_coverage" in normalized:
            return [
                {
                    "stock_code": "000001",
                    "latest_known_at": "2026-08-26 21:01:00",
                    "max_result_count": 4,
                }
            ]
        if "LEFT JOIN si_stock_finance" in normalized:
            return [
                {"stock_code": "000001", "latest_report_date": "2026-06-30"},
                {"stock_code": "000002", "latest_report_date": "2026-06-30"},
            ]
        raise AssertionError(normalized)

    monkeypatch.setattr(scheduler_validation, "_read_all", fake_read_all)
    ok, message = scheduler_validation._validate_finance_scheduler_coverage(
        object(),
        started_at=datetime(2026, 8, 26, 21, 0),
        now=datetime(2026, 8, 26, 21, 30),
    )

    assert ok is False
    assert "expected=2 actual=1" in message
    assert "000002" in message


def test_finance_db_period_gate_respects_post_deadline_listing(monkeypatch) -> None:
    def fake_read_all(engine, sql, params=None):
        normalized = " ".join(sql.split())
        if "FROM si_all_code" in normalized and "LEFT JOIN" not in normalized:
            return [
                {"stock_code": "000001", "list_date": "1991-01-01"},
                {"stock_code": "000002", "list_date": "2026-05-08"},
            ]
        if "FROM st_pit_source_coverage" in normalized:
            return [
                {"stock_code": "000001", "max_result_count": 4},
                {"stock_code": "000002", "max_result_count": 1},
            ]
        if "LEFT JOIN si_stock_finance" in normalized:
            return [
                {
                    "stock_code": "000001",
                    "list_date": "1991-01-01",
                    "latest_report_date": "2026-03-31",
                },
                {
                    "stock_code": "000002",
                    "list_date": "2026-05-08",
                    "latest_report_date": "2025-12-31",
                },
            ]
        raise AssertionError(normalized)

    monkeypatch.setattr(scheduler_validation, "_read_all", fake_read_all)
    ok, message = scheduler_validation._validate_finance_scheduler_coverage(
        object(),
        started_at=datetime(2026, 8, 26, 21, 0),
        now=datetime(2026, 8, 26, 21, 30),
    )

    assert ok is True
    assert "new_listing_period_exempt=1" in message


def test_finance_has_bounded_same_day_catchup() -> None:
    row = {
        "task_type": "stock_finance",
        "last_triggered_at": "2026-08-25 21:00:00",
        "last_run_status": "success",
    }
    assert scheduler_runtime.CRITICAL_CRON_CATCHUP_WINDOWS_SECONDS["stock_finance"] == 4 * 60 * 60
    assert scheduler_runtime._critical_cron_catchup_allowed(
        row,
        now=datetime(2026, 8, 26, 23, 59),
        cron_time="21:00",
    )
    assert not scheduler_runtime._critical_cron_catchup_allowed(
        row,
        now=datetime(2026, 8, 27, 1, 1),
        cron_time="21:00",
    )


def test_production_deploy_installs_and_validates_required_data_tasks() -> None:
    script = (ROOT / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )
    assert script.count("tools/ensure_quality_gate.py\"") >= 2
    assert "--task-type stock_finance" not in script
    assert "--task-type stock_dividend_baidu" not in script
    assert "--task-type etf_forward_daily" not in script
    assert script.count("--validate-required-data-completion") >= 1
