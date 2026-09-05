"""Finance producer v2 and exact-target readback regressions (no provider calls)."""
from datetime import date, datetime, timedelta
import json

import pytest

from server.common import scheduler_validation as validation


TARGET = date(2026, 9, 4)
NOW = datetime(2026, 9, 5, 20, 0)
START = NOW - timedelta(minutes=2)
TASK = {"task_type": "stock_finance", "_scheduler_target_trade_date": TARGET.isoformat(),
        "_scheduler_target_available": True, "_scheduler_historical_recovery": True}


def _receipt(*, requested=3, fetched=1, reused=1, resumed=1):
    return {
        "schema": "probiga.finance-sync-result.v2", "status": "PASS", "as_of": TARGET.isoformat(),
        "minimum_report_date": "2026-06-30", "minimum_report_disclosure_deadline": "2026-08-31",
        "oldest_latest_applicable_report_date": "2026-06-30" if fetched else None,
        "requested_code_count": requested, "provider_fetch_code_count": fetched,
        "reused_immutable_code_count": reused, "checkpoint_resumed_code_count": resumed,
        "nonempty_code_count": requested, "expected_unavailable_code_count": 0,
        "legal_empty_new_listing_code_count": 0, "resolved_code_count": requested,
        "written_report_count": fetched, "failure_count": 0,
        "report_period_applicable_code_count": fetched, "new_listing_period_exempt_code_count": 0,
        "nonempty_code_coverage": 1.0, "resolution_coverage": 1.0,
        "expected_unavailable_code_sample": {}, "candidate_input_root_sha256": "e" * 64,
        "atomic_batch": {
            "schema": "probiga.pit-finance-atomic-batch.v2", "as_of_date": TARGET.isoformat(),
            "minimum_report_date": "2026-06-30", "eligible_code_count": requested,
            "seal_coverage_id": "a" * 64, "batch_root_sha256": "b" * 64,
            "coverage_root_sha256": "c" * 64, "eligible_code_set_hash": "d" * 64,
        },
    }


def _status(receipt, code=0):
    return validation.scheduler_output_status(TASK, json.dumps(receipt), return_code=code)


def _mock_database(monkeypatch, receipt, *, actual_seal=None):
    calls = []

    def read_all(_engine, sql, params=None):
        if "FROM si_all_code" in sql:
            return [{"stock_code": f"{index:06}", "list_date": "1991-01-01"}
                    for index in range(1, receipt["requested_code_count"] + 1)]
        if "FROM st_pit_source_coverage" in sql:
            return []  # The unchanged issuers have no newly captured facts.
        raise AssertionError(sql)

    def load_seal(_engine, **kwargs):
        calls.append(kwargs)
        return ({**receipt["atomic_batch"], "completed_known_at": NOW.isoformat(sep=" ")}
                if actual_seal is None else actual_seal)

    monkeypatch.setattr(validation, "_read_all", read_all)
    monkeypatch.setattr(validation, "load_finance_atomic_batch_seal", load_seal)
    monkeypatch.setattr(validation, "_validate_requirement", lambda *a, **kw:
                        pytest.fail("v2 must use the complete seal, not require new facts for reused issuers"))
    return calls


def test_v2_checkpoint_fetch_and_reuse_counters_match_producer():
    # Producer removes resumed codes from fetch_codes before reporting counts.
    assert _status(_receipt()) == "success"
    assert _status(_receipt(requested=5558, fetched=0, reused=5558, resumed=0)) == "success"


@pytest.mark.parametrize("kind", ["unavailable", "legal_empty"])
def test_v2_fetched_resolutions_are_not_invented_nonempty_writes(kind):
    receipt = _receipt()
    receipt.update(nonempty_code_count=2, nonempty_code_coverage=2 / 3,
                   written_report_count=0, report_period_applicable_code_count=0,
                   oldest_latest_applicable_report_date=None)
    if kind == "unavailable":
        receipt.update(expected_unavailable_code_count=1,
                       expected_unavailable_code_sample={"002731": {"disposition_id": "proof"}})
    else:
        receipt.update(legal_empty_new_listing_code_count=1, new_listing_period_exempt_code_count=1)
    assert _status(receipt) == "success"  # Still requires real persisted seal in the next stage.


def test_actual_5557_of_5558_missing_002731_stays_failed_without_seal():
    receipt = _receipt(requested=5558, fetched=1, reused=5557, resumed=0)
    receipt.update(status="DATA_BLOCKED", nonempty_code_count=5557,
                   resolved_code_count=5557, nonempty_code_coverage=5557 / 5558,
                   resolution_coverage=5557 / 5558, failure_count=1, written_report_count=0,
                   failure_sample=[{"stock_code": "002731"}], atomic_batch={})
    assert _status(receipt, 1) == "failed"
    assert _status(receipt, 0) == "failed"


@pytest.mark.parametrize("field,value", [
    ("provider_fetch_code_count", 2), ("checkpoint_resumed_code_count", 2),
    ("written_report_count", 0), ("failure_count", 1), ("requested_code_count", True),
    ("resolution_coverage", 0.99), ("nonempty_code_coverage", float("nan")),
    ("atomic_batch", {}), ("candidate_input_root_sha256", "bad"),
])
def test_v2_malformed_or_incomplete_receipts_fail(field, value):
    receipt = _receipt()
    receipt[field] = value
    assert _status(receipt) == "failed"


def test_duplicate_or_mixed_finance_receipts_fail():
    receipt = _receipt()
    output = json.dumps(receipt) + "\n" + json.dumps(receipt)
    assert validation.scheduler_output_status(TASK, output, return_code=0) == "failed"
    output = json.dumps(receipt) + "\n" + json.dumps({"schema": "probiga.finance-sync-result.v1"})
    assert validation.scheduler_output_status(TASK, output, return_code=0) == "failed"


def test_weekend_incremental_reuse_checks_friday_seal_at_actual_observation_time(monkeypatch):
    receipt = _receipt(requested=3, fetched=0, reused=3, resumed=0)
    calls = _mock_database(monkeypatch, receipt)
    result = validation.validate_scheduler_task_result(
        TASK, engine=object(), started_at=START, now=NOW, output=json.dumps(receipt),
    )
    assert result.checked and result.ok, result.message
    assert calls[0]["as_of_date"] == TARGET
    assert calls[0]["decision_at"] == NOW  # Never backdate PIT knowledge to Friday.


@pytest.mark.parametrize("mutation", [
    {"batch_root_sha256": "f" * 64}, {"seal_coverage_id": "f" * 64},
    {"as_of_date": "2026-09-05"}, {"eligible_code_count": 2},
    {"completed_known_at": "2026-09-04 21:00:00"},
    {"completed_known_at": "2026-09-05 20:01:00"},
    {"schema": "probiga.pit-finance-atomic-batch.v1"},
])
def test_v2_readback_drift_stale_future_or_incomplete_seal_fails(monkeypatch, mutation):
    receipt = _receipt()
    actual = {**receipt["atomic_batch"], "completed_known_at": NOW.isoformat(sep=" "), **mutation}
    _mock_database(monkeypatch, receipt, actual_seal=actual)
    result = validation.validate_scheduler_task_result(
        TASK, engine=object(), started_at=START, now=NOW, output=json.dumps(receipt),
    )
    assert result.checked and not result.ok


def test_v2_missing_real_seal_cannot_fall_back_to_provider_row_counts(monkeypatch):
    receipt = _receipt()
    _mock_database(monkeypatch, receipt, actual_seal={})
    result = validation.validate_scheduler_task_result(
        TASK, engine=object(), started_at=START, now=NOW, output=json.dumps(receipt),
    )
    assert not result.ok and "readback differs" in result.message


@pytest.mark.parametrize("mutation", [
    {"_scheduler_target_trade_date": "bad"}, {"_scheduler_target_trade_date": "20260904"},
    {"_scheduler_target_trade_date": "2026-09-06"}, {"_scheduler_target_available": False},
    {"_scheduler_target_trade_date": ""},
    {"_trigger_source": "release_catchup", "_release_target_date": "2026-09-03"},
])
def test_finance_target_malformed_missing_conflicting_or_future_fails(mutation):
    with pytest.raises(ValueError):
        validation._finance_scheduler_target_date({**TASK, **mutation}, now=NOW)


def test_finance_release_target_is_used_without_backdating_observation():
    task = {"task_type": "stock_finance", "_trigger_source": "release_catchup",
            "_release_target_date": "2026-08-31"}
    assert validation._finance_scheduler_target_date(task, now=NOW) == date(2026, 8, 31)


def test_legacy_fallback_disclosure_gate_uses_bound_target_not_new_month(monkeypatch):
    observed = []
    monkeypatch.setattr(validation, "load_finance_atomic_batch_seal", lambda *a, **kw: {})
    monkeypatch.setattr(validation, "_read_all", lambda *a, **kw:
                        [{"stock_code": "000001", "list_date": "1991-01-01"}])
    def capture(target):
        observed.append(target)
        raise RuntimeError("stop after gate observation")
    monkeypatch.setattr(validation, "finance_disclosure_gate", capture)
    with pytest.raises(RuntimeError, match="gate observation"):
        validation._validate_finance_scheduler_coverage(
            object(), started_at=START, now=NOW, target_date=date(2026, 8, 31),
        )
    assert observed == [date(2026, 8, 31)]
