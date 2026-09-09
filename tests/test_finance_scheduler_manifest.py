from copy import deepcopy
from datetime import date, datetime
import json
from types import SimpleNamespace

import pytest

from server.common import scheduler_validation as validation
from server.common.pit_facts import build_finance_data_exclusion


def _receipt(excluded=True):
    exclusions = [build_finance_data_exclusion(
        stock_code="000002", target_date="2026-09-07",
        observed_at="2026-09-07 06:34:00", source="eastmoney.finance.mainfinadata.direct",
        reason_code="FINANCE_FETCH_FAILED", error_detail="Timeout: source unavailable",
    )] if excluded else []
    count = len(exclusions)
    summary = {
        "data_excluded_count": count,
        "available_code_count": 2 - count,
        "data_excluded_codes": [item["stock_code"] for item in exclusions],
        "data_excluded_reasons": {item["stock_code"]: item["reason_code"] for item in exclusions},
        "data_exclusions": exclusions,
    }
    atomic = {
        "schema": "probiga.pit-finance-atomic-batch.v2",
        "as_of_date": "2026-09-07", "completed_known_at": "2026-09-07 06:35:00",
        "seal_coverage_id": "a" * 64, "batch_root_sha256": "b" * 64,
        "eligible_code_count": 2, **summary,
    }
    return {
        "schema": "probiga.finance-sync-result.v2",
        "status": "DEGRADED" if excluded else "PASS",
        "as_of": "2026-09-07", "minimum_report_date": "2026-06-30",
        "minimum_report_disclosure_deadline": "2026-08-31",
        "requested_code_count": 2, "provider_fetch_code_count": 2,
        "reused_immutable_code_count": 0, "checkpoint_resumed_code_count": 0,
        "nonempty_code_count": 2 - count, "nonempty_code_coverage": (2 - count) / 2,
        "expected_unavailable_code_count": 0, "expected_unavailable_code_sample": {},
        "legal_empty_new_listing_code_count": 0, "legal_empty_new_listing_code_sample": [],
        "resolved_code_count": 2 - count, "resolution_coverage": (2 - count) / 2,
        "written_report_count": 2 - count, "failure_count": count,
        "failure_sample": [{"stock_code": "000002", "error": "Timeout"}] if count else [],
        "disposition_code_count": 2, "disposition_coverage": 1.0, "shared_failure_count": 0,
        "candidate_input_root_sha256": "c" * 64,
        "execution_mode": "EXACT_REUSE_AND_TARGETED_REFRESH",
        "atomic_batch": atomic, **summary,
    }


def _install(monkeypatch, receipt):
    observed = []
    actual = {
        **deepcopy(receipt["atomic_batch"]),
        "members": {"000001": {"coverage_status": "COMPLETE"},
                    "000002": {"coverage_status": "DATA_EXCLUDED"}},
    }
    monkeypatch.setattr(validation, "load_target_stock_catalog", lambda *a, **k: (
        SimpleNamespace(members=()), ["000001", "000002"],
    ))
    def load(*args, **kwargs):
        observed.append(kwargs)
        return actual
    monkeypatch.setattr(validation, "load_finance_atomic_batch_seal", load)
    return actual, observed


@pytest.mark.parametrize("excluded", [False, True])
def test_scheduler_accepts_complete_dispositions_with_truthful_source_status(monkeypatch, excluded):
    receipt = _receipt(excluded)
    actual, observed = _install(monkeypatch, receipt)
    assert validation.scheduler_output_status(
        {"task_type": "stock_finance"}, json.dumps(receipt), return_code=0,
    ) == "success"
    ok, message = validation._validate_finance_scheduler_coverage(
        object(), started_at=datetime(2026, 9, 7, 6, 30), now=datetime(2026, 9, 7, 6, 40),
        output=json.dumps(receipt),
    )
    assert ok, message
    assert observed[0]["seal_coverage_id"] == receipt["atomic_batch"]["seal_coverage_id"]
    assert f"data_excluded={int(excluded)}" in message


@pytest.mark.parametrize("field,value", [
    ("status", "PASS"), ("failure_count", 0), ("disposition_coverage", .5),
    ("resolution_coverage", 1.0), ("data_excluded_codes", []),
    ("shared_failure_count", 1), ("data_exclusions", []),
])
def test_scheduler_rejects_exclusions_disguised_as_complete_data(field, value):
    receipt = _receipt()
    receipt[field] = value
    assert validation.scheduler_output_status(
        {"task_type": "stock_finance"}, json.dumps(receipt), return_code=0,
    ) == "failed"


@pytest.mark.parametrize("difference", ["seal_id", "root", "members", "exclusions", "future"])
def test_scheduler_rejects_a_different_or_unbound_actual_seal(monkeypatch, difference):
    receipt = _receipt()
    actual, _observed = _install(monkeypatch, receipt)
    if difference == "seal_id":
        actual["seal_coverage_id"] = "d" * 64
    elif difference == "root":
        actual["batch_root_sha256"] = "e" * 64
    elif difference == "members":
        actual["members"].pop("000002")
    elif difference == "exclusions":
        actual["data_exclusions"] = []
    else:
        actual["completed_known_at"] = "2026-09-07 06:41:00"
    ok, message = validation._validate_finance_scheduler_coverage(
        object(), started_at=datetime(2026, 9, 7, 6, 30), now=datetime(2026, 9, 7, 6, 40),
        output=json.dumps(receipt),
    )
    assert not ok
    assert "differ" in message


def test_scheduler_never_replaces_a_missing_run_receipt_with_raw_fresh_data(monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("missing run receipt must not trigger a raw-data fallback")
    monkeypatch.setattr(validation, "_read_all", forbid)
    monkeypatch.setattr(validation, "load_finance_atomic_batch_seal", forbid)
    ok, message = validation._validate_finance_scheduler_coverage(
        object(), started_at=datetime(2026, 9, 7, 6, 30), now=datetime(2026, 9, 7, 6, 40),
    )
    assert not ok
    assert "current run" in message


def test_finance_exact_run_binding_keeps_prior_target_after_midnight(monkeypatch):
    receipt = _receipt()
    receipt["atomic_batch"]["completed_known_at"] = "2026-09-08 00:01:00"
    _actual, observed = _install(monkeypatch, receipt)
    ok, message = validation._validate_finance_scheduler_coverage(
        object(), started_at=datetime(2026, 9, 8), now=datetime(2026, 9, 8, 0, 2),
        target_date=date(2026, 9, 7), output=json.dumps(receipt),
    )
    assert ok, message
    assert observed[0]["as_of_date"] == date(2026, 9, 7)
