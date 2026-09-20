"""A slow or failing history job must not starve the close snapshot."""

from tools import run_big_qmt_bridge as bridge


def test_long_etf_attempt_cannot_reclaim_worker_before_waiting_membership(monkeypatch):
    state = {"busy_until": 0.0}
    last_checks = {}
    clock = [1000.0]
    started = []

    def launch(_state, *, name, runner):
        if clock[0] < _state["busy_until"]:
            return False
        started.append(name)
        # ETF fails after two minutes, longer than its one-minute interval.
        _state["busy_until"] = clock[0] + (120 if name == "etf" else 5)
        return True

    monkeypatch.setattr(bridge, "_launch_maintenance_job", launch)
    jobs = [("etf", 60.0, lambda: None), ("membership", 300.0, lambda: None)]
    for now in [1000.0, 1060.0, 1120.0, 1125.0, 1245.0, 1365.0, 1425.0, 1485.0]:
        clock[0] = now
        bridge._launch_due_maintenance_jobs(
            state, last_checks=last_checks, jobs=jobs, now=now,
        )
    assert started[:3] == ["etf", "membership", "etf"]
    assert started.count("membership") == 2


def test_busy_slot_does_not_consume_waiting_jobs_retry_interval(monkeypatch):
    last_checks = {"etf": 1000.0, "membership": 700.0}
    offers = []
    monkeypatch.setattr(
        bridge, "_launch_maintenance_job",
        lambda _state, *, name, runner: offers.append(name) or False,
    )
    bridge._launch_due_maintenance_jobs(
        {}, last_checks=last_checks,
        jobs=[("etf", 60.0, None), ("membership", 300.0, None)], now=1120.0,
    )
    assert offers == ["membership", "etf"]
    assert last_checks == {"etf": 1000.0, "membership": 700.0}


def test_not_due_job_does_not_delay_due_job(monkeypatch):
    last_checks = {"etf": 1000.0, "membership": 900.0}
    offers = []
    monkeypatch.setattr(
        bridge, "_launch_maintenance_job",
        lambda _state, *, name, runner: offers.append(name) or True,
    )
    bridge._launch_due_maintenance_jobs(
        {}, last_checks=last_checks,
        jobs=[("etf", 60.0, None), ("membership", 300.0, None)], now=1060.0,
    )
    assert offers == ["etf"]
    assert last_checks["etf"] == 1060.0
