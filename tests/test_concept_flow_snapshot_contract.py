from __future__ import annotations

from datetime import datetime
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from server.common.qmt_attestation_contract import canonical_digest
from server.common.scheduler_validation import (
    scheduler_output_status,
    validate_scheduler_task_result,
)
from tools import fetch_concept_flow_datacenter as concept_flow


def test_latest_snapshot_keeps_the_actual_provider_date(monkeypatch):
    calls: list[str] = []

    def fake_fetch(source_date: str) -> pd.DataFrame:
        calls.append(source_date)
        if source_date == "2026-08-21":
            return pd.DataFrame([{"index_code": "BK001"}])
        return pd.DataFrame()

    monkeypatch.setattr(concept_flow, "_fetch_all_for_date", fake_fetch)

    frame, source_date = concept_flow._fetch_latest_available_snapshot(
        now=datetime.fromisoformat("2026-08-24T19:30:00+08:00"),
        lookback_days=7,
    )

    assert not frame.empty
    assert source_date == "2026-08-21"
    assert calls == ["2026-08-24", "2026-08-23", "2026-08-22", "2026-08-21"]


def test_latest_snapshot_fails_when_the_provider_window_is_empty(monkeypatch):
    monkeypatch.setattr(
        concept_flow,
        "_fetch_all_for_date",
        lambda source_date: pd.DataFrame(),
    )

    with pytest.raises(
        RuntimeError,
        match="DATA_BLOCKED: no Eastmoney concept-flow rows",
    ):
        concept_flow._fetch_latest_available_snapshot(
            now=datetime.fromisoformat("2026-08-26T19:30:00+08:00"),
            lookback_days=3,
        )


def test_publisher_stamps_source_date_not_capture_date(monkeypatch):
    frame = pd.DataFrame(
        [
            {
                "index_code": f"BK{index:03d}",
                "index_name": f"concept-{index}",
                "stock_name": "",
            }
            for index in range(100)
        ]
    )
    writes: list[pd.DataFrame] = []
    monkeypatch.setattr(concept_flow, "create_batch_engine", lambda: object())
    monkeypatch.setattr(
        concept_flow,
        "_fetch_latest_available_snapshot",
        lambda: (frame.copy(), "2026-08-21"),
    )
    monkeypatch.setattr(concept_flow, "_lookup_stock_codes", lambda engine, names: {})
    monkeypatch.setattr(
        concept_flow,
        "replace_table_rows",
        lambda output, table, engine, **kwargs: writes.append(output.copy()),
    )

    concept_flow.fetch_concept_flow()

    assert len(writes) == 1
    assert {
        value.strftime("%Y-%m-%d") for value in writes[0]["snapshot_at"]
    } == {"2026-08-21"}


def test_partial_snapshot_is_blocked_before_atomic_replacement(monkeypatch):
    frame = pd.DataFrame(
        [
            {
                "index_code": f"BK{index:03d}",
                "index_name": f"concept-{index}",
                "stock_name": "",
            }
            for index in range(99)
        ]
    )
    writes: list[pd.DataFrame] = []
    monkeypatch.setattr(concept_flow, "create_batch_engine", lambda: object())
    monkeypatch.setattr(
        concept_flow,
        "_fetch_latest_available_snapshot",
        lambda: (frame, "2026-08-26"),
    )
    monkeypatch.setattr(
        concept_flow,
        "replace_table_rows",
        lambda output, table, engine, **kwargs: writes.append(output.copy()),
    )

    with pytest.raises(RuntimeError, match="DATA_BLOCKED:.*incomplete"):
        concept_flow.fetch_concept_flow()

    assert writes == []


def test_strict_provider_capture_rejects_missing_pagination_page(monkeypatch):
    first_page = {
        "data": [{"BOARD_CODE": "BK001", "BOARD_NAME": "one"}],
        "pages": 2,
        "count": 2,
    }
    monkeypatch.setattr(
        concept_flow,
        "_fetch_page",
        lambda source_date, page, page_size=500: first_page if page == 1 else None,
    )
    monkeypatch.setattr(concept_flow.time, "sleep", lambda seconds: None)

    with pytest.raises(RuntimeError, match="pagination is incomplete"):
        concept_flow._fetch_all_for_date_capture("2026-08-26")


def test_strict_concept_result_is_calendar_and_manifest_bound():
    manifest = {
        "schema": concept_flow.MANIFEST_SCHEMA,
        "provider": "eastmoney.datacenter-web",
        "report_name": concept_flow.REPORT_NAME,
        "source_date": "2026-08-26",
        "row_count": 100,
        "verified_row_count": 100,
        "code_count": 100,
        "code_set_hash": "1" * 64,
        "provider_code_set_hash": "4" * 64,
        "strict_authority": True,
        "captured_at": "2026-08-26 19:30:00",
        "provider_page_count": 1,
        "provider_reported_row_count": 100,
        "provider_pagination_complete": True,
        "calendar_batch_id": "qmt-batch-1",
        "calendar_manifest_hash": "2" * 64,
        "calendar_session_set_hash": "3" * 64,
    }
    payload = {
        "schema": concept_flow.RESULT_SCHEMA,
        "status": "COMPLETE",
        "source_date": "2026-08-26",
        "provider": "eastmoney.datacenter-web",
        "strict_authority": True,
        "written_rows": 100,
        "db_verified_rows": 100,
        "manifest": manifest,
        "manifest_hash": canonical_digest(manifest),
    }

    assert concept_flow.validate_task_result(payload, 0) == "complete"
    with pytest.raises(ValueError, match="expected session"):
        concept_flow.validate_task_result(
            payload,
            0,
            expected_session="2026-08-25",
        )
    assert scheduler_output_status(
        {"task_type": "eastmoney_concept_flow_snapshot"},
        json.dumps(payload),
        return_code=0,
    ) == "success"
    mismatch = validate_scheduler_task_result(
        {
            "task_type": "eastmoney_concept_flow_snapshot",
            "_trigger_source": "release_catchup",
            "_release_target_date": "2026-08-25",
        },
        engine=object(),
        output=json.dumps(payload),
        started_at=datetime(2026, 8, 26, 19, 29),
        now=datetime(2026, 8, 26, 19, 31),
    )
    assert mismatch.checked is True
    assert mismatch.ok is False
    assert "release target" in mismatch.message
    payload["manifest"]["provider_reported_row_count"] = 99
    with pytest.raises(ValueError, match="proof differs"):
        concept_flow.validate_task_result(payload, 0)


def test_strict_snapshot_requires_the_authoritative_session(monkeypatch):
    calendar = SimpleNamespace()
    monkeypatch.setattr(
        concept_flow,
        "_authoritative_closed_session",
        lambda engine, now: ("2026-08-26", calendar),
    )
    monkeypatch.setattr(
        concept_flow,
        "_fetch_all_for_date_capture",
        lambda source_date: (
            pd.DataFrame(),
            {
                "source_date": source_date,
                "pagination_complete": False,
                "provider_row_count": 0,
                "observed_row_count": 0,
            },
        ),
    )

    with pytest.raises(RuntimeError, match="no rows for authoritative session"):
        concept_flow._strict_provider_snapshot(
            object(),
            now=datetime.fromisoformat("2026-08-26T19:30:00+08:00"),
        )


def test_strict_snapshot_accepts_only_an_open_date_not_after_authority(monkeypatch):
    calendar = SimpleNamespace(
        sessions_between=lambda start_date, end_date: (start_date,)
    )
    observed = {}
    engine = MagicMock()
    monkeypatch.setattr(
        concept_flow,
        "authoritative_closed_trade_date",
        lambda _engine, *, now, close_ready_time: (
            observed.update(
                {"now": now, "close_ready_time": close_ready_time}
            )
            or "2026-08-27"
        ),
    )
    monkeypatch.setattr(
        concept_flow,
        "validate_trade_calendar_runtime_schema",
        lambda _engine: None,
    )
    monkeypatch.setattr(
        concept_flow,
        "load_trade_calendar_receipt",
        lambda *_args, **_kwargs: calendar,
    )

    target, receipt = concept_flow._authoritative_closed_session(
        engine,
        now=datetime.fromisoformat("2026-08-27T17:59:00+08:00"),
        requested_trade_date="2026-08-26",
    )
    assert target == "2026-08-26"
    assert receipt is calendar
    assert observed["close_ready_time"] == concept_flow.CONCEPT_FLOW_CLOSE_READY_TIME

    with pytest.raises(RuntimeError, match="not yet authoritative"):
        concept_flow._authoritative_closed_session(
            engine,
            now=datetime.fromisoformat("2026-08-27T17:59:00+08:00"),
            requested_trade_date="2026-08-28",
        )


def test_concept_flow_cli_forwards_release_trade_date(monkeypatch, capsys):
    observed = {}

    def fake_fetch(**kwargs):
        observed.update(kwargs)
        return {"schema": concept_flow.RESULT_SCHEMA, "status": "COMPLETE"}

    monkeypatch.setattr(concept_flow, "fetch_concept_flow", fake_fetch)

    assert concept_flow.main(
        ["--strict-authority", "--trade-date", "2026-08-26", "--json"]
    ) == 0
    assert observed == {
        "strict_authority": True,
        "trade_date": "2026-08-26",
    }
    assert json.loads(capsys.readouterr().out)["status"] == "COMPLETE"
