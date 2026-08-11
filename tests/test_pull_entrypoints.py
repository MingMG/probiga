# -*- coding: utf-8 -*-
from types import SimpleNamespace
from unittest.mock import patch

from tools import pull_all, pull_local, run_all_changes


def test_pull_all_run_merges_base_and_step_env_without_global_mutation():
    pull_all.FAILURES.clear()
    pull_all.BASE_ENV.clear()
    pull_all.BASE_ENV.update({"MYSQL_URL": "mysql://example", "SM_MAX_STOCKS": "200"})
    completed = SimpleNamespace(returncode=2, stdout=b"out", stderr=b"err")

    with patch("tools.pull_all.build_child_env", return_value={"PYTHONPATH": "repo"}), patch(
        "tools.pull_all.subprocess.run",
        return_value=completed,
    ) as run:
        rc = pull_all.run(["python", "job.py"], capture=True, extra_env={"SE_A_LIST_DATE": "2026-07-01"})

    assert rc == 2
    env = run.call_args.kwargs["env"]
    assert env["PYTHONPATH"] == "repo"
    assert env["MYSQL_URL"] == "mysql://example"
    assert env["SM_MAX_STOCKS"] == "200"
    assert env["SE_A_LIST_DATE"] == "2026-07-01"
    assert run.call_args.kwargs["timeout"] == 30 * 60
    assert pull_all.FAILURES == [(["python", "job.py"], 2, "outerr")]


def test_pull_all_run_records_timeout():
    pull_all.FAILURES.clear()
    pull_all.BASE_ENV.clear()

    with patch("tools.pull_all.build_child_env", return_value={"PYTHONPATH": "repo"}), patch(
        "tools.pull_all.subprocess.run",
        side_effect=pull_all.subprocess.TimeoutExpired(["python", "job.py"], timeout=1),
    ):
        rc = pull_all.run(["python", "job.py"], capture=True)

    assert rc == 124
    assert pull_all.FAILURES == [(["python", "job.py"], 124, "")]


def test_run_all_changes_passes_base_env_to_subprocess():
    run_all_changes.BASE_ENV.clear()
    run_all_changes.BASE_ENV["MYSQL_URL"] = "mysql://example"
    completed = SimpleNamespace(returncode=0)

    with patch("tools.run_all_changes.build_child_env", return_value={"PYTHONPATH": "repo"}), patch(
        "tools.run_all_changes.subprocess.run",
        return_value=completed,
    ) as run:
        assert run_all_changes.run("tools/job.py", ["2026-07-01"]) == 0

    env = run.call_args.kwargs["env"]
    assert env == {"PYTHONPATH": "repo", "MYSQL_URL": "mysql://example"}
    assert run.call_args.kwargs["timeout"] == 45 * 60


def test_pull_local_returns_failure_when_any_step_fails():
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return SimpleNamespace(returncode=1 if len(calls) == 2 else 0)

    with patch("tools.pull_local.subprocess.run", side_effect=fake_run), patch(
        "tools.pull_local.build_child_env",
        return_value={"PYTHONPATH": "repo"},
    ):
        rc = pull_local.main()

    assert rc == 1
    assert len(calls) == len(pull_local.STEPS)
    assert calls[0][1]["env"]["SM_MAX_STOCKS"] == "200"
    assert calls[0][1]["timeout"] == 30 * 60
