from __future__ import annotations

from datetime import date
import inspect

import pytest

from integrations.qmt import reconciliation
from integrations.qmt.reconciliation import _coverage_status, _price_consistency_status
from tools.ensure_quality_gate import TASKS


def test_coverage_status_thresholds():
    assert _coverage_status(0.96, warn_threshold=0.80, pass_threshold=0.95) == "PASS"
    assert _coverage_status(0.85, warn_threshold=0.80, pass_threshold=0.95) == "WARN"
    assert _coverage_status(0.50, warn_threshold=0.80, pass_threshold=0.95) == "FAIL"


def test_price_consistency_status_thresholds():
    assert _price_consistency_status(checked_rows=100, failed_rows=0) == "PASS"
    assert _price_consistency_status(checked_rows=100, failed_rows=1) == "FAIL"
    assert _price_consistency_status(checked_rows=0, failed_rows=0) == "WARN"


def test_qmt_nightly_reconciliation_task_is_registered():
    task = next(item for item in TASKS if item["task_type"] == "qmt_nightly_reconciliation")

    assert task["cron_time"] == "01:30"
    assert task["script_path"] == "tools/nightly_guojin_qmt_reconciliation.py"
    assert "--scan-days 20" in task["script_args"]
    assert task["enabled"] == 1


def test_qmt_intraday_realtime_task_is_independent_channel():
    task = next(item for item in TASKS if item["task_type"] == "qmt_intraday_realtime")

    assert task["group_name"] == "国金QMT"
    assert task["script_path"] == "tools/sync_qmt_realtime.py"
    assert "--no-archive-snapshot" in task["script_args"]


def test_qmt_gap_repair_plan_task_is_registered_as_dry_run():
    task = next(item for item in TASKS if item["task_type"] == "qmt_gap_repair_plan")

    assert task["cron_time"] == "02:00"
    assert task["script_path"] == "tools/repair_guojin_qmt_gaps.py"
    assert "--apply" not in task["script_args"]


def test_qmt_catalog_refresh_task_is_registered():
    task = next(item for item in TASKS if item["task_type"] == "qmt_catalog_capability_refresh")

    assert task["cron_time"] == "01:10"
    assert task["script_path"] == "tools/setup_guojin_qmt_catalog.py"
    assert task["enabled"] == 1


def test_qmt_reference_sync_captures_immutable_calendar_receipt():
    task = next(item for item in TASKS if item["task_type"] == "qmt_reference_incremental")

    assert "--include-calendar" in task["script_args"]


def test_daily_reconciliation_is_exact_and_uses_independent_roots():
    source = inspect.getsource(reconciliation.build_coverage_results)
    previous_dates_source = inspect.getsource(
        reconciliation._previous_trade_dates
    )
    expected_source = inspect.getsource(reconciliation._expected_stock_sets)

    assert 'status = "PASS" if not missing and not unexpected else "FAIL"' in source
    assert '"exact_set_required": dataset == "sm_stock_kline.1d"' in source
    assert "load_stock_catalog" in expected_source
    assert "load_trade_calendar_receipt" in expected_source
    assert "si_all_code" not in expected_source
    assert "sm_stock_kline" not in previous_dates_source


def test_nightly_reconciliation_fails_closed_before_business_dml_on_schema_drift(
    monkeypatch,
):
    class _Engine:
        def begin(self):
            raise AssertionError("business DML must not start before schema validation")

    def reject_schema(_engine):
        raise RuntimeError("audit schema drift")

    monkeypatch.setattr(reconciliation, "validate_audit_schema", reject_schema)
    with pytest.raises(RuntimeError, match="audit schema drift"):
        reconciliation.run_nightly_reconciliation(_Engine())


def test_qmt_local_gap_repair_execute_task_is_registered():
    task = next(item for item in TASKS if item["task_type"] == "qmt_local_gap_repair_execute")

    assert task["cron_time"] == "07:05"
    assert task["script_path"] == "tools/backfill_guojin_qmt_local_history.py"
    assert "from-gaps" in task["script_args"]
    assert "--apply" in task["script_args"]


def test_date_import_keeps_pytest_collection_stable():
    assert date(2026, 6, 27).isoformat() == "2026-06-27"
