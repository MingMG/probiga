"""Small planning tests; no provider, clock wait or real database."""
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from acquisition.datasets import get_spec
from acquisition.models import WorkUnit, key_fingerprint
from acquisition.plan import (daily_candidate_days, sessions, latest_closed,
                              plan_units, flow_dependency, refresh_cutoff,
                              summarize)

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


def test_staged_daily_flow_is_not_refetched_before_atomic_publication():
    spec = get_spec("capital_flow_daily")
    state = [dict(completed("000001.SZ"), status="staged",
                  partition_key="000001.SZ:transactioncount1d:none")]
    planned = plan_units(spec, "2026-09-04", CATALOG, state, now=NOW)
    assert [unit.code for unit in planned] == ["600000.SH"]


def test_old_source_progress_cannot_complete_new_source_plan_or_summary():
    spec = get_spec("capital_flow_daily")
    old = [dict(completed("000001.SZ"), source="eastmoney",
                partition_key="000001.SZ:transactioncount1d:none"),
           dict(completed("600000.SH"), source="eastmoney",
                partition_key="600000.SH:transactioncount1d:none")]
    assert {unit.code for unit in plan_units(spec, "2026-09-04", CATALOG, old, now=NOW)} == set(CATALOG)
    report = summarize(spec, "2026-09-04", CATALOG, old)
    assert report["complete"] == 0 and report["missing"] == 2


def test_daily_skips_complete_history_but_keeps_latest_and_progress_gaps():
    spec = get_spec("stock_daily")
    days = ["2026-09-02", "2026-09-03", "2026-09-04"]
    states = [
        {"source": spec.source, "target_date": "2026-09-02",
         "status": "complete", "unit_count": 2},
        {"source": spec.source, "target_date": "2026-09-03",
         "status": "complete", "unit_count": 1},
    ]
    exact = {
        "2026-09-02": key_fingerprint(
            WorkUnit(spec.name, spec.source, "2026-09-02", code, spec.period, "none").partition_key
            for code in CATALOG
        ),
    }
    assert daily_candidate_days(
        spec, days, CATALOG, states, terminal_fingerprints=exact,
    ) == [
        "2026-09-03", "2026-09-04",
    ]


def test_daily_same_count_replacement_and_stale_extra_key_remain_candidates():
    spec = get_spec("stock_daily")
    counts = [{
        "source": spec.source, "target_date": "2026-09-03",
        "status": "complete", "unit_count": 3,
    }]
    stale = key_fingerprint({
        "000001.SZ:1d:none", "300001.SZ:1d:none", "900001.SH:1d:none",
    })
    assert daily_candidate_days(
        spec, ["2026-09-03", "2026-09-04"], CATALOG, counts,
        terminal_fingerprints={"2026-09-03": stale},
    ) == ["2026-09-03", "2026-09-04"]

    same_count = [{**counts[0], "unit_count": 2}]
    replaced = key_fingerprint({"000001.SZ:1d:none", "300001.SZ:1d:none"})
    assert daily_candidate_days(
        spec, ["2026-09-03", "2026-09-04"], CATALOG, same_count,
        terminal_fingerprints={"2026-09-03": replaced},
    ) == ["2026-09-03", "2026-09-04"]


def test_daily_keeps_staged_history_and_ignores_retired_source_completion():
    spec = get_spec("capital_flow_daily")
    days = ["2026-09-03", "2026-09-04"]
    states = [
        {"source": spec.source, "target_date": "2026-09-03",
         "status": "staged", "unit_count": 1},
        {"source": "eastmoney", "target_date": "2026-09-03",
         "status": "complete", "unit_count": 2},
    ]
    assert daily_candidate_days(spec, days, CATALOG, states) == days


def test_daily_flow_uses_atomic_complete_state_not_full_catalog_count():
    spec = get_spec("capital_flow_daily")
    counts = [{
        "source": spec.source, "target_date": "2026-09-03",
        "status": "complete", "unit_count": 1,
    }]
    assert daily_candidate_days(
        spec, ["2026-09-03", "2026-09-04"], CATALOG, counts,
        flow_health={"2026-09-03": True},
    ) == ["2026-09-04"]
    assert daily_candidate_days(
        spec, ["2026-09-03", "2026-09-04"], CATALOG, counts,
        flow_health={"2026-09-03": False},
    ) == ["2026-09-03", "2026-09-04"]
