from datetime import datetime, timedelta
from unittest.mock import MagicMock

import pytest
from server.api import scheduler_runtime as runtime


def test_ths_failed_cron_retries_same_day_without_replaying_success():
    now = datetime(2026, 9, 13, 8, 0)
    row = {"task_type": "sync_concept_ths", "cron_time": "06:00", "last_run_status": "failed",
           "last_run_at": datetime(2026, 9, 13, 6, 0), "last_triggered_at": datetime(2026, 9, 13, 6, 0),
           "last_run_duration": 300}
    assert runtime._overdue_cron_allowed(row, now=now, cron_time="06:00", startup_time=now)
    row["last_run_status"] = "success"
    assert not runtime._cron_due(row, now=now)


@pytest.mark.parametrize("older_owner_gone", [True, False])
def test_all_duplicate_claim_owners_must_be_proven_dead_before_any_write(monkeypatch, older_owner_gone):
    started = datetime(2026, 9, 7, 9, 16, 51)
    history = [dict(run_uid="a" * 32, run_at=started, status="running", host_name="prod-host",
                    scheduler_instance_id="prod-host-1857867", build_sha="a" * 40, trigger_source="scheduled"),
               dict(run_uid="b" * 32, run_at=started-timedelta(days=3), status="running", host_name="prod-host",
                    scheduler_instance_id="prod-host-626392", build_sha="b" * 40, trigger_source="scheduled")]
    selected = MagicMock()
    selected.mappings.return_value.all.return_value = history
    connection = MagicMock()
    connection.execute.side_effect = [selected, MagicMock(rowcount=1), MagicMock(rowcount=1), MagicMock(rowcount=1)]
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value = connection
    monkeypatch.setattr(runtime, "gethostname", lambda: "prod-host")
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: "c" * 40)
    owners = []
    def absent(instance, *, host_name):
        owners.append(instance)
        return instance.endswith("1857867") or older_owner_gone
    monkeypatch.setattr(runtime, "_owner_pid_is_absent", absent)
    result = runtime._recover_interrupted_manual_claim(engine, {"id": 41}, started)
    assert result is older_owner_gone
    assert owners == ["prod-host-1857867", "prod-host-626392"]
    if older_owner_gone:
        assert connection.execute.call_count == 4
        assert [call.args[1]["run_uid"] for call in connection.execute.call_args_list[1:3]] == ["a" * 32, "b" * 32]
        assert "1857867" in connection.execute.call_args_list[-1].args[1]["output"]
    else:
        connection.execute.assert_called_once()


@pytest.mark.parametrize("terminal_matches,owner_absent,expected", [
    (True, True, True), (False, True, False), (True, False, False),
])
def test_old_review_orphan_does_not_strand_a_later_terminal_claim(
    monkeypatch, terminal_matches, owner_absent, expected,
):
    started = datetime(2026, 9, 23, 21, 36, 23)
    history = dict(run_uid="a" * 32, run_at=datetime(2026, 9, 4, 21, 24, 39),
                   status="running", host_name="prod-host", scheduler_instance_id="prod-host-820881",
                   build_sha="a" * 40, trigger_source="scheduled")
    terminal = dict(task_type="daily_review", run_at=started,
                    finished_at=started + timedelta(seconds=66), status="failed")
    if not terminal_matches:
        terminal["run_at"] -= timedelta(days=1)
    selected, ended, connection, engine = MagicMock(), MagicMock(), MagicMock(), MagicMock()
    selected.mappings.return_value.all.return_value = [history]
    ended.mappings.return_value.first.return_value = terminal
    connection.execute.side_effect = [selected, ended, MagicMock(rowcount=1), MagicMock(rowcount=1)]
    engine.begin.return_value.__enter__.return_value = connection
    monkeypatch.setattr(runtime, "gethostname", lambda: "prod-host")
    monkeypatch.setattr(runtime, "_scheduler_build_commit_sha", lambda: "c" * 40)
    monkeypatch.setattr(runtime, "_owner_pid_is_absent", lambda *a, **kw: owner_absent)
    assert runtime._recover_interrupted_manual_claim(
        engine, {"id": 32, "task_type": "daily_review"}, started,
    ) is expected
    assert connection.execute.call_count == (4 if expected else 2)


@pytest.mark.parametrize("task_type,cron", [("daily_pool", "18:30"), ("daily_review", "18:00")])
def test_closed_session_products_recover_then_still_run_tonight(task_type, cron):
    row = {"task_type": task_type, "cron_time": cron, "last_run_status": "success",
           "last_triggered_at": datetime(2026, 11, 3, 18, 30)}
    morning = datetime(2026, 11, 5, 8, 30)
    assert runtime._cron_due(row, now=morning)
    assert runtime._overdue_cron_allowed(row, now=morning, cron_time=cron,
                                       startup_time=morning-timedelta(hours=5))
    row["last_triggered_at"] = morning
    assert not runtime._cron_due(row, now=morning+timedelta(minutes=1))
    tonight = datetime(2026, 11, 5, 19, 0)
    assert runtime._cron_due(row, now=tonight)
    assert runtime._overdue_cron_allowed(row, now=tonight, cron_time=cron,
                                       startup_time=morning)
    row["last_triggered_at"] = tonight
    assert not runtime._cron_due(row, now=tonight+timedelta(minutes=1))
