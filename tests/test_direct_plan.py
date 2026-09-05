"""Small planning tests; no provider, clock wait or real database."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from acquisition.datasets import get_spec
from acquisition.plan import (sessions, latest_closed, plan_units, flow_dependency,
                              refresh_cutoff, summarize)

NOW = datetime(2026, 9, 4, 18, 10, tzinfo=ZoneInfo("Asia/Shanghai"))
CATALOG = {"000001.SZ": {"list_date": "1991-04-03"}, "600000.SH": {"list_date": "1999-11-10"}}


def completed(code, target="2026-09-04", success="2026-09-04 15:40:00"):
    return dict(target_date=target, partition_key=code + ":1d:none", status="complete",
                last_success_at=success, next_retry_at=None, detail_json='{"traded":true}')


def test_calendar_requires_actual_closed_day_and_does_not_guess_weekend():
    saturday = NOW + timedelta(days=1)
    assert latest_closed({"2026-09-04": 1, "2026-09-05": 0}, saturday) == "2026-09-04"
    with pytest.raises(ValueError, match="CALENDAR_MISSING"):
        latest_closed({"2026-09-04": 1}, saturday)
    with pytest.raises(ValueError, match="CALENDAR_MISSING"):
        sessions({"2026-09-04": 1, "2026-09-06": 0}, "2026-09-04", "2026-09-06")


def test_newest_success_does_not_hide_old_gap():
    spec = get_spec("stock_daily")
    states = [completed(c) for c in CATALOG]
    assert plan_units(spec, "2026-09-04", CATALOG, states, now=NOW) == []
    assert len(plan_units(spec, "2026-09-03", CATALOG, states, now=NOW)) == 2


def test_overlap_refresh_is_once_per_slot_not_every_task_trigger():
    spec = get_spec("notices")
    state = [completed(c, success="2026-09-04 18:01:00") for c in CATALOG]
    assert plan_units(spec, "2026-09-04", CATALOG, state, now=NOW, refresh=True,
                      refresh_after=refresh_cutoff(spec, NOW)) == []
    later = NOW.replace(hour=22)
    assert len(plan_units(spec, "2026-09-04", CATALOG, state, now=later, refresh=True,
                          refresh_after=refresh_cutoff(spec, later))) == 2


def test_running_and_not_due_errors_are_not_replaced():
    states = [dict(completed("000001.SZ"), status="running"),
              dict(completed("600000.SH"), status="error", next_retry_at="2026-09-04 18:20:00")]
    assert plan_units(get_spec("stock_daily"), "2026-09-04", CATALOG, states, now=NOW, refresh=True) == []


def test_flow_dependency_does_not_hide_missing_daily_or_include_future_listing():
    catalog = dict(CATALOG, **{"000002.SZ": {"list_date": "2026-09-05"},
                              "430047.BJ": {"list_date": "2020-01-01"}})
    dep = flow_dependency(catalog, "2026-09-04", [completed("000001.SZ")])
    assert dep == {"traded": ["000001.SZ"], "no_trade": [], "missing_daily": ["600000.SH"]}
    report = summarize(get_spec("stock_daily"), "2026-09-04", CATALOG, [completed("000001.SZ")])
    assert report["expected"] == 2 and report["missing"] == 1 and report["status"] == "partial"
