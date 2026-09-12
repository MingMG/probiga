"""The table runner delegates to final publishers and preserves their authority."""
from unittest.mock import MagicMock

import pytest

from tools import run_single_table as runner


@pytest.mark.parametrize("table, path, date_args", [
    ("sm_index_minute", "tools/sync_qmt_index_edge.py", ["--start-date", "2026-09-11", "--end-date", "2026-09-11"]),
    ("sm_concept_east_minute", "tools/sync_eastmoney_concept_market.py", ["--trade-date", "2026-09-11"]),
])
@pytest.mark.parametrize("return_code", [0, 3])
def test_dedicated_minute_route_preserves_date_identity_and_failure(monkeypatch, table, path, date_args, return_code):
    env = {"PROBIGA_SCHEDULER_EXECUTOR_ROLE": "linux_provider", "PROBIGA_SCHEDULER_BUILD_SHA": "a" * 40,
           "PROBIGA_SCHEDULER_TASK_TYPE": "legacy_table_task", "DATA_SOURCE_INDEX_MINUTE": "other",
           "DATA_SOURCE_CONCEPT_MINUTE": "qmt"}
    original = dict(env)
    run = MagicMock(return_value=return_code)
    monkeypatch.setattr(runner, "_child_env", lambda: env)
    monkeypatch.setattr(runner, "_run_subprocess", run)
    assert runner._run_one_table(table, "2026-09-11") == return_code
    run.assert_called_once()
    command, passed_env = run.call_args.args
    assert command[1] == path
    assert command[2:4] == ["--dataset", "minute"]
    assert command[-len(date_args):] == date_args
    assert "--apply" in command if table == "sm_index_minute" else "--apply" not in command
    assert passed_env == original
    assert env == original


def test_index_default_uses_publishers_authoritative_latest_session(monkeypatch):
    run = MagicMock(return_value=0)
    monkeypatch.setattr(runner, "_child_env", lambda: {})
    monkeypatch.setattr(runner, "_run_subprocess", run)
    monkeypatch.setattr(runner, "_latest_trade_date", MagicMock(side_effect=AssertionError("do not guess index date")))
    assert runner._run_one_table("sm_index_minute") == 0
    assert run.call_args.args[0][-1] == "--latest-session"


@pytest.mark.parametrize("table, kind", [("sm_stock_minute", "stock"), ("sm_stock_capital_flow_min", "flow")])
def test_public_minute_receives_explicit_date_not_only_legacy_environment(monkeypatch, table, kind):
    run = MagicMock(return_value=0)
    monkeypatch.setattr(runner, "_child_env", lambda: {})
    monkeypatch.setattr(runner, "_run_subprocess", run)
    assert runner._run_one_table(table, "2026-09-11") == 0
    assert run.call_args.args[0] == [runner.sys.executable, "tools/crawl_minute_kline.py", "--type", kind,
                                    "--trade-date", "2026-09-11"]


@pytest.mark.parametrize("kind", ["index", "concept", "all"])
def test_removed_public_minute_mode_fails_before_subprocess(monkeypatch, kind):
    run = MagicMock()
    monkeypatch.setattr(runner, "_run_subprocess", run)
    with pytest.raises(ValueError, match="dedicated"):
        runner._sub_run_minute(kind)
    run.assert_not_called()
