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
