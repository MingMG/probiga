from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime
import json
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.exc import OperationalError

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
    ) == ("degraded" if excluded else "success")
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


def _sqlite_finance_metadata(engine):
    @event.listens_for(engine, "before_cursor_execute", retval=True)
    def translate_metadata(_conn, _cursor, statement, parameters, _context, _many):
        if "FROM information_schema.COLUMNS" in statement:
            # Adapt metadata introspection only. Coverage and seal queries use
            # the real public validator and execute unchanged against SQLite.
            statement = "SELECT name AS COLUMN_NAME FROM pragma_table_info(?)"
        return statement, parameters


@pytest.fixture(scope="module")
def finance_manifest_history():
    from server.common.finance_coverage import finance_disclosure_gate
    from server.common.pit_facts import append_finance_atomic_batch_seal
    from tests.test_pit_finance_event_facts import (
        _engine, _finance_data_exclusion, _install_finance_test_catalog,
        _install_prior_finance_coverage,
    )

    engine = _engine()
    _install_finance_test_catalog(engine)
    _install_prior_finance_coverage(engine)
    target = date(2026, 8, 30)
    seal = append_finance_atomic_batch_seal(
        engine, as_of_date=target, completed_known_at="2026-08-30 01:10:00",
        data_exclusions=[_finance_data_exclusion(observed="2026-08-30 01:08:00")],
    )
    gate = finance_disclosure_gate(target)
    receipt = {
        **_receipt(),
        "as_of": target.isoformat(),
        "minimum_report_date": gate.minimum_report_date.isoformat(),
        "minimum_report_disclosure_deadline": gate.disclosure_deadline.isoformat(),
        "atomic_batch": seal,
        "failure_sample": [{"stock_code": "002731", "error": "Timeout"}],
        **{key: seal[key] for key in (
            "data_excluded_count", "available_code_count", "data_excluded_codes",
            "data_excluded_reasons", "data_exclusions",
        )},
    }
    with engine.begin() as connection:
        historical = dict(connection.execute(text(
            "SELECT * FROM st_pit_source_coverage WHERE source='adata.finance.core_index'"
        )).mappings().one())
        columns = tuple(historical)
        connection.execute(text(
            "CREATE INDEX idx_pit_coverage_decision ON st_pit_source_coverage "
            "(fact_kind,stock_code,known_at,window_start,window_end)"
        ))
        # Earlier matching rows outside the two-member target catalog grow the
        # generic aggregate. The exact persisted seal still covers two members.
        connection.execute(text(
            "INSERT INTO st_pit_source_coverage (" + ",".join(columns) + ") VALUES ("
            + ",".join(":" + column for column in columns) + ")"
        ), [
            {
                **historical,
                "coverage_id": f"{number:064x}",
                "scope_hash": f"{number + 6000:064x}",
                "stock_code": f"{number + 100000:06d}",
                "batch_id": f"older-finance-{number}",
            }
            for number in range(1, 6001)
        ])
    _sqlite_finance_metadata(engine)
    yield engine, receipt
    engine.dispose()


@contextmanager
def _sqlite_query_work_budget(engine, *, maximum_steps=20_000):
    raw = engine.raw_connection()
    connection = raw.driver_connection
    raw.close()
    completed_steps = 0

    def progress():
        nonlocal completed_steps
        completed_steps += 100
        return int(completed_steps > maximum_steps)

    connection.set_progress_handler(progress, 100)
    try:
        yield
    finally:
        connection.set_progress_handler(None, 0)


def _validate_finance_history(engine, receipt):
    return validation.validate_scheduler_task_result(
        {"task_type": "stock_finance"}, engine=engine,
        started_at=datetime(2026, 8, 30, 1, 6), now=datetime(2026, 8, 30, 1, 11),
        output=json.dumps(receipt),
    )


def test_finance_scheduler_checks_presence_then_reads_the_exact_seal_within_budget(finance_manifest_history):
    engine, receipt = finance_manifest_history
    where = validation.TASK_OUTPUT_REQUIREMENTS["stock_finance"][0].where_sql
    with _sqlite_query_work_budget(engine):
        with pytest.raises(OperationalError, match="interrupted"):
            with engine.connect() as connection:
                connection.execute(text(
                    "SELECT COUNT(*) AS row_count, MAX(known_at) AS max_freshness "
                    f"FROM st_pit_source_coverage WHERE ({where})"
                )).one()

    with _sqlite_query_work_budget(engine):
        result = _validate_finance_history(engine, receipt)
    assert result.checked and result.ok, result.message
    assert "codes=2 available=1 data_excluded=1" in result.message
    assert f"seal={receipt['atomic_batch']['seal_coverage_id']}" in result.message


@pytest.mark.parametrize("field,reason", [
    ("seal_coverage_id", "output atomic seal validation failed"),
    ("batch_root_sha256", "output seal identity, full catalog or exclusions differ"),
])
def test_finance_scheduler_requires_the_submitted_seal_after_presence_check(
    finance_manifest_history, field, reason,
):
    engine, receipt = finance_manifest_history
    wrong = deepcopy(receipt)
    wrong["atomic_batch"][field] = "f" * 64
    with _sqlite_query_work_budget(engine):
        result = _validate_finance_history(engine, wrong)
    assert result.checked and not result.ok
    assert reason in result.message


def test_finance_scheduler_presence_requires_an_actual_qualifying_source_row():
    engine = create_engine("sqlite://")
    try:
        with engine.begin() as connection:
            connection.execute(text(
                "CREATE TABLE st_pit_source_coverage (fact_kind TEXT, source TEXT, "
                "coverage_status TEXT, result_count INTEGER, known_at TEXT)"
            ))
            connection.execute(text(
                "INSERT INTO st_pit_source_coverage VALUES "
                "('event','adata.finance.core_index','COMPLETE',1,'2026-08-29'),"
                "('finance','some.other.provider','COMPLETE',1,'2026-08-29'),"
                "('finance','adata.finance.core_index','BLOCKED',1,'2026-08-29'),"
                "('finance','eastmoney.finance.mainfinadata.direct','COMPLETE',0,'2026-08-29')"
            ))
        _sqlite_finance_metadata(engine)
        result = _validate_finance_history(engine, _receipt())
        assert result.checked and not result.ok
        assert "st_pit_source_coverage: only 0 rows" in result.message
    finally:
        engine.dispose()


def test_full_market_nested_discovery_receipt_fits_actual_scheduler_history_budget():
    from biz.stock_finance import sync_finance
    from server.api import scheduler_runtime

    report = _receipt()
    report.update({
        "requested_code_count": 5578, "provider_fetch_code_count": 1,
        "reused_immutable_code_count": 5577, "nonempty_code_count": 5577,
        "nonempty_code_coverage": 5577 / 5578, "resolved_code_count": 5577,
        "resolution_coverage": 5577 / 5578, "disposition_code_count": 5578,
    })
    report["atomic_batch"].update({"eligible_code_count": 5578, "available_code_count": 5577})
    codes = [f"{i:06d}" for i in range(5337)]
    binding = {"coverage_id": "d" * 64, "changed_code_set_sha256": "e" * 64,
               "changed_codes": codes}
    report["atomic_batch"]["incremental_discovery_binding"] = binding
    with pytest.raises(RuntimeError, match="machine receipt exceeds"):
        scheduler_runtime._history_validation_replay_output(json.dumps(report))

    report["atomic_batch"] = sync_finance._finance_atomic_batch_receipt(report["atomic_batch"])
    replay = scheduler_runtime._history_validation_replay_output(json.dumps(report))
    restored = json.loads(replay)
    assert validation.scheduler_output_status({"task_type": "stock_finance"}, replay, return_code=0) == "degraded"
    assert restored["atomic_batch"]["incremental_discovery_binding"] == {
        "coverage_id": "d" * 64, "changed_code_set_sha256": "e" * 64,
        "changed_code_count": 5337,
    }
    assert restored["atomic_batch"]["batch_root_sha256"] == report["atomic_batch"]["batch_root_sha256"]
    assert restored["data_exclusions"] == report["data_exclusions"]
    assert binding["changed_codes"] == codes
