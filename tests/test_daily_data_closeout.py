from __future__ import annotations

from datetime import datetime

from sqlalchemy import create_engine, text

from tools import daily_data_closeout as closeout


TARGET = "2026-10-29"
NOW = datetime(2026, 10, 29, 22, 50)


def _engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE st_scheduled_tasks (
                    id INTEGER PRIMARY KEY,
                    task_name TEXT,
                    task_type TEXT,
                    enabled INTEGER,
                    last_run_status TEXT,
                    last_run_at DATETIME
                )
                """
            )
        )
        connection.execute(
            text(
                "INSERT INTO st_scheduled_tasks "
                "(id,task_name,task_type,enabled,last_run_status,last_run_at) "
                "VALUES (:id,:name,:task_type,:enabled,:status,:run_at)"
            ),
            [
                {
                    "id": index,
                    "name": task_type,
                    "task_type": task_type,
                    "enabled": 1,
                    "status": "success",
                    "run_at": NOW,
                }
                for index, task_type in enumerate(
                    sorted(closeout.TASK_LABELS),
                    start=1,
                )
            ],
        )
    return engine


class _Observer:
    def inspect_day(self, day, current, *, missing_limit):
        assert day == TARGET
        assert current == NOW
        assert missing_limit == 5
        cells = {}
        for key, spec in closeout.BY_KEY.items():
            cells[key] = {
                "dataset": key,
                "status": "full",
                "reason": "完整源回执与实际入库内容一致",
                "expected_count": 1,
                "actual_count": 1,
                "coverage_ratio": 1.0,
                "task_type": spec.task_type,
            }
        cells["daily"].update(
            status="missing",
            reason="缺少当日完整源回执",
            expected_count=5578,
            actual_count=0,
            coverage_ratio=0.0,
        )
        return cells


def test_collect_closeout_reports_exact_missing_dataset():
    report = closeout.collect_closeout(
        _engine(),
        target_date=TARGET,
        now=NOW,
        observer=_Observer(),
    )

    assert report["status"] == "WARN"
    assert report["failed_count"] == 1
    assert report["waiting_count"] == 0
    assert report["problems"] == [
        {
            "name": "股票日线",
            "status": "missing",
            "detail": "缺少当日完整源回执；覆盖 0/5578；0.00%",
            "task_type": "qmt_stock_daily_canonical",
        }
    ]


def test_launch_repairs_uses_scheduler_guard_and_never_relaunches_waiting(
    monkeypatch,
):
    calls = []

    def launch(row, *, root, engine):
        calls.append((row["task_type"], root, engine))
        return {"accepted": True, "status": "accepted"}

    monkeypatch.setattr(
        "server.api.scheduler_runtime.launch_scheduler_task",
        launch,
    )
    report = {
        "problems": [
            {"status": "missing", "task_type": "news_sync"},
            {"status": "waiting", "task_type": "stock_finance"},
        ]
    }
    attempts = closeout.launch_repairs(_engine(), report, max_launches=4)

    assert [item[0] for item in calls] == ["news_sync"]
    assert attempts == [
        {
            "task_type": "news_sync",
            "task_name": "news_sync",
            "accepted": True,
            "status": "accepted",
        }
    ]


def test_render_message_is_actionable_for_incomplete_closeout():
    message = closeout.render_message(
        {
            "target_date": TARGET,
            "observed_at": NOW.isoformat(timespec="seconds"),
            "failed_count": 1,
            "waiting_count": 1,
            "problems": [
                {
                    "name": "股票日线",
                    "status": "missing",
                    "detail": "覆盖 0/5578",
                },
                {
                    "name": "股票财务 PIT",
                    "status": "waiting",
                    "detail": "2026-10-29 运行中",
                },
            ],
        },
        mode="repair",
        attempts=[
            {
                "task_type": "qmt_stock_daily_canonical",
                "task_name": "股票日线补采",
                "accepted": True,
                "status": "accepted",
            }
        ],
    )

    assert "数据闭环仍有缺口" in message
    assert "股票日线：覆盖 0/5578" in message
    assert "股票财务 PIT：2026-10-29 运行中" in message
    assert "已自动触发重试" in message
    assert "23:20 将自动复验" in message
