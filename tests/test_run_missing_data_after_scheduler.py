from unittest.mock import patch
from types import SimpleNamespace

from tools import run_missing_data_after_scheduler
from tools.run_missing_data_after_scheduler import backfill_mode_for_coverage


def test_backfill_mode_uses_minute_when_daily_is_complete():
    mode = backfill_mode_for_coverage(
        {"daily_stocks": 5559, "minute_rows": 48200},
        min_daily_stocks=4441,
        min_minute_rows=1_070_425,
    )

    assert mode == "minute"


def test_backfill_mode_uses_all_when_daily_and_minute_are_missing():
    mode = backfill_mode_for_coverage(
        {"daily_stocks": 200, "minute_rows": 48200},
        min_daily_stocks=4441,
        min_minute_rows=1_070_425,
    )

    assert mode == "all"


def test_main_uses_batch_engine_when_no_gaps():
    engine = object()

    with patch.object(run_missing_data_after_scheduler.sys, "argv", ["run_missing_data_after_scheduler.py"]), \
         patch("tools.run_missing_data_after_scheduler.load_project_env"), \
         patch("tools.run_missing_data_after_scheduler.create_batch_engine", return_value=engine) as create_batch_engine, \
         patch("tools.run_missing_data_after_scheduler.wait_for_scheduler_idle") as wait_for_scheduler_idle, \
         patch("tools.run_missing_data_after_scheduler.find_gaps", return_value=[]) as find_gaps:
        assert run_missing_data_after_scheduler.main() == 0

    create_batch_engine.assert_called_once_with(future=True)
    wait_for_scheduler_idle.assert_called_once()
    assert wait_for_scheduler_idle.call_args.args[0] is engine
    find_gaps.assert_called_once()
    assert find_gaps.call_args.args[0] is engine


def test_run_cmd_uses_timeout_and_returns_timeout_code():
    calls = []

    def fake_run(*args, **kwargs):
        calls.append((args, kwargs))
        raise run_missing_data_after_scheduler.subprocess.TimeoutExpired(args[0], timeout=kwargs["timeout"])

    with patch("tools.run_missing_data_after_scheduler.subprocess.run", side_effect=fake_run), \
         patch("tools.run_missing_data_after_scheduler.build_child_env", return_value={"PYTHONPATH": "repo"}):
        assert run_missing_data_after_scheduler.run_cmd(["python", "job.py"], timeout_seconds=7) == 124

    assert calls[0][1]["timeout"] == 7


def test_run_cmd_returns_subprocess_code():
    with patch(
        "tools.run_missing_data_after_scheduler.subprocess.run",
        return_value=SimpleNamespace(returncode=3),
    ) as run, patch(
        "tools.run_missing_data_after_scheduler.build_child_env",
        return_value={"PYTHONPATH": "repo"},
    ):
        assert run_missing_data_after_scheduler.run_cmd(["python", "job.py"], timeout_seconds=9) == 3

    assert run.call_args.kwargs["timeout"] == 9
