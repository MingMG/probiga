"""Read-only operational checks must not confuse liveness with data readiness."""
from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from tools import data_quality_check as quality


def test_flow_uses_exact_target_keys_not_aggregate_count():
    with patch.object(quality, "_rows", side_effect=[
        [{"stock_code": "000001"}, {"stock_code": "600000"}],
        [{"stock_code": "000001", "fields_present": 1, "data_source": "push2hist"},
         {"stock_code": "600001", "fields_present": 1, "data_source": "push2hist"}],
    ]) as rows:
        result = quality.check_flow_coverage(object(), "2026-09-04")
    assert result.status == "FAIL"
    assert result.details["missing_codes"] == ["600000"]
    assert all(call.args[2] == {"d": "2026-09-04"} for call in rows.call_args_list)
    assert "adjust_type = 0" in rows.call_args_list[0].args[1]
    assert "^(00|30|60|68)" in rows.call_args_list[0].args[1]


def test_empty_daily_prerequisite_never_passes():
    with patch.object(quality, "_rows", side_effect=[[], []]):
        result = quality.check_flow_coverage(object(), "2026-09-03")
    assert result.status == "FAIL"
    assert result.details["prerequisite_missing"]


@pytest.mark.parametrize("fields,source,status", [
    (1, "push2hist", "PASS"), (0, "push2hist", "FAIL"), (1, None, "WARN"),
])
def test_flow_requires_fields_and_reports_unknown_source(fields, source, status):
    with patch.object(quality, "_rows", side_effect=[
        [{"stock_code": "000001"}],
        [{"stock_code": "000001", "fields_present": fields, "data_source": source}],
    ]):
        result = quality.check_flow_coverage(object(), "2026-09-04")
    assert result.status == status


def test_mixed_sources_are_visible_not_equivalence_claim():
    with patch.object(quality, "_rows", side_effect=[
        [{"stock_code": "000001"}, {"stock_code": "600000"}],
        [{"stock_code": "000001", "fields_present": 1, "data_source": "push2hist"},
         {"stock_code": "600000", "fields_present": 1, "data_source": "baidu"}],
    ]):
        result = quality.check_flow_coverage(object(), "2026-09-04")
    assert result.status == "WARN"
    assert result.details["source_counts"] == {"push2hist": 1, "baidu": 1}


def test_calendar_requires_closed_dates_too():
    now = datetime(2026, 9, 5, 12)
    rows = [{"trade_date": now.date() + timedelta(days=i), "trade_status": 0}
            for i in range(8)]
    with patch.object(quality, "_rows", return_value=rows):
        assert quality.check_acquisition_calendar(object(), now=now).status == "PASS"
    with patch.object(quality, "_rows", return_value=rows[1:]):
        result = quality.check_acquisition_calendar(object(), now=now)
    assert result.status == "FAIL"
    assert result.details["missing_dates"] == ["2026-09-05"]


@pytest.mark.parametrize("windows_ages,status", [
    ([30], "PASS"), ([121], "FAIL"), ([], "FAIL"), ([-1], "FAIL"), ([20, 30], "FAIL"),
])
def test_executor_monitor_detects_stopped_future_and_duplicate_owners(windows_ages, status):
    rows = [{"executor_role": "linux_standalone", "age_seconds": 30}]
    rows.extend({"executor_role": "qmt_windows_edge", "age_seconds": age}
                for age in windows_ages)
    with patch.object(quality, "_rows", return_value=rows):
        result = quality.check_acquisition_executors(object())
    assert result.status == status
    assert result.details["observation_only"] is True


def test_acquisition_report_aggregates_failure_without_strategy_queries():
    ok = quality.CheckResult("check", "PASS", "ok")
    with patch.object(quality, "expected_completed_trade_date", side_effect=RuntimeError("private connection string")), \
         patch.object(quality, "check_acquisition_calendar", side_effect=RuntimeError("private connection string")), \
         patch.object(quality, "check_acquisition_executors", return_value=ok) as heartbeat, \
         patch.object(quality, "check_analysis_outputs") as analysis:
        result = quality.run_acquisition_checks(object())
    assert result["status"] == "FAIL"
    assert result["trade_date"] == ""
    assert "private connection string" not in str(result)
    heartbeat.assert_called_once()
    analysis.assert_not_called()


def test_acquisition_report_checks_history_not_only_latest_date():
    ok = quality.CheckResult("check", "PASS", "ok")
    gap = quality.CheckResult("recent_kline_calendar_completeness", "FAIL", "missing September 3")
    with patch.object(quality, "check_acquisition_calendar", return_value=ok), \
         patch.object(quality, "check_acquisition_executors", return_value=ok), \
         patch.object(quality, "latest_trade_date", return_value="2026-09-04"), \
         patch.object(quality, "check_recent_kline_calendar_completeness", return_value=gap) as history, \
         patch.object(quality, "check_recent_flow_calendar_completeness", return_value=ok), \
         patch.object(quality, "check_flow_coverage", return_value=ok):
        report = quality.run_acquisition_checks(object(), "2026-09-04")
    assert report["status"] == "FAIL"
    assert history.call_args.kwargs["lookback"] == 21


def test_repaired_daily_bars_do_not_hide_missing_prior_day_flow():
    with patch.object(quality, "_rows", side_effect=[
        [{"trade_date": "2026-09-04"}, {"trade_date": "2026-09-03"}],
        [{"trade_date": "2026-09-04", "stock_count": 5207}],
    ]):
        result = quality.check_recent_flow_calendar_completeness(object(), "2026-09-04")
    assert result.status == "FAIL"
    assert result.details["missing_dates"] == ["2026-09-03"]
    assert result.details["coverage_basis"] == "calendar_partition_presence_only"


def test_acquisition_cannot_skip_weekend_backlog():
    with patch.object(quality.sys, "argv", ["quality", "--acquisition", "--skip-closed"]), \
         patch.object(quality, "create_batch_engine") as engine:
        with pytest.raises(SystemExit) as exc:
            quality.main()
    assert exc.value.code == 2
    engine.assert_not_called()
