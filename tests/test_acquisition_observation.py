"""Read-only operational checks must not confuse liveness with data readiness."""
from datetime import datetime, timedelta
import json
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
    assert "REGEXP" not in rows.call_args_list[0].args[1]
    assert "amount > 0" in rows.call_args_list[0].args[1]


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
        [{"trade_date": "2026-09-04", "stock_codes": '["600000"]'},
         {"trade_date": "2026-09-03", "stock_codes": '["600000"]'}],
        [{"trade_date": "2026-09-04", "stock_codes": '["600000"]'}],
    ]):
        result = quality.check_recent_flow_calendar_completeness(object(), "2026-09-04")
    assert result.status == "FAIL"
    assert result.details["missing_dates"] == ["2026-09-03"]
    assert result.details["coverage_basis"] == "target_date_traded_daily_keys_all_markets"


def test_history_does_not_hide_beijing_gap_behind_nonempty_daily_partitions():
    with patch.object(quality, "_rows", side_effect=[
        [{"trade_date": "2026-09-04"}],
        [{"trade_date": "2026-09-04", "stock_codes": '["600000", "920001"]'}],
        [{"trade_date": "2026-09-04", "stock_codes": '["600000"]'}],
    ]):
        result = quality.check_recent_flow_calendar_completeness(object(), "2026-09-04")
    assert result.status == "FAIL"
    assert result.details["missing_dates"] == []
    assert result.details["incomplete_dates"] == [{"trade_date": "2026-09-04", "missing_count": 1,
                                                 "missing_sample": ["920001"]}]


def test_acquisition_cannot_skip_weekend_backlog():
    with patch.object(quality.sys, "argv", ["quality", "--acquisition", "--skip-closed"]), \
         patch.object(quality, "create_batch_engine") as engine:
        with pytest.raises(SystemExit) as exc:
            quality.main()
    assert exc.value.code == 2
    engine.assert_not_called()


def test_one_fresh_ths_partition_cannot_hide_stale_members_or_missing_concept():
    with patch.object(quality, "_row", return_value={}), patch.object(quality, "_rows", side_effect=[
        [{"index_code": "885001", "concept_code": "300001"},
         {"index_code": "885002", "concept_code": "300002"},
         {"index_code": None, "concept_code": "300003"}],
        [{"query_type": "index_code", "query_key": "885001", "member_count": 55000, "oldest_sync": "2026-09-11"},
         {"query_type": "index_code", "query_key": "885002", "member_count": 50, "oldest_sync": "2026-08-11"}],
    ]):
        result = quality.check_ths_membership_freshness(object(), "2026-09-11")
    assert result.status == "FAIL"
    assert result.details["stale_or_missing_count"] == 2


def test_snapshot_large_row_count_does_not_hide_missing_identity():
    with patch.object(quality, "_table_exists", return_value=True), \
         patch.object(quality, "_latest_day_count", return_value={"latest_date": "2026-09-11", "entity_count": 5500}), \
         patch.object(quality, "_rows", side_effect=[
             [{"stock_code": "600000"}, {"stock_code": "920001"}],
             [{"stock_code": "600000"}, {"stock_code": "600001"}],
         ]):
        result = quality.check_stock_snapshot_freshness(object(), "2026-09-11")
    assert result.status == "FAIL"
    assert result.details["missing_sample"] == ["920001"]


@pytest.mark.parametrize("row", [{}, {"observed_at": "2026-08-11", "requested": 5562,
                                    "responded": 5562, "event_count": 56973, "failures": 0}])
def test_dividend_requires_recent_completed_source_snapshot(row):
    with patch.object(quality, "_row", return_value=row):
        assert quality.check_dividend_acquisition(object(), "2026-09-11").status == "FAIL"


def test_notice_incremental_success_cannot_clear_historical_backlog():
    with patch.object(quality, "_row", return_value={"unverified_rows": 1, "affected_stocks": 1}):
        assert quality.check_notice_history_backlog(object()).status == "FAIL"


@pytest.mark.parametrize("valid_hash", [True, False])
def test_empty_ths_partition_requires_matching_fresh_publication_receipt(valid_hash):
    from biz.stock_info.ths_members import RESULT_SCHEMA, result_hash
    receipt = {"schema": RESULT_SCHEMA, "provider": "ths_native_members", "status": "COMPLETE",
               "source_trade_date": "2026-09-11", "observed_at": "2026-09-13T08:30:00",
               "publication_scope": "exact_native_index_partition", "completed_indices": ["885001"],
               "empty_indices": ["885001"]}
    receipt["result_sha256"] = result_hash(receipt) if valid_hash else "wrong"
    with patch.object(quality, "_rows", side_effect=[[{"index_code": "885001", "concept_code": "300001"}], []]), \
         patch.object(quality, "_row", return_value={"last_run_at": "2026-09-13", "last_run_output": json.dumps(receipt)}):
        assert quality.check_ths_membership_freshness(object(), "2026-09-11").status == ("PASS" if valid_hash else "FAIL")
