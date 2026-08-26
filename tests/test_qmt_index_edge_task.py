from __future__ import annotations

from datetime import datetime, timedelta
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from integrations.bigqmt import bridge
from server.common.qmt_history_coverage import minute_time_grid
from server.common.scheduler_validation import (
    scheduler_output_status,
    validate_scheduler_task_result,
)
from tools import sync_qmt_index_edge as publisher


def _catalog():
    return [
        publisher.IndexCatalogMember(
            index_code="000001",
            qmt_code="000001.SH",
            name="上证指数",
            list_date="1990-12-19",
            expire_date=None,
            batch_id="batch-1",
        ),
        publisher.IndexCatalogMember(
            index_code="399001",
            qmt_code="399001.SZ",
            name="深证成指",
            list_date="1991-04-03",
            expire_date=None,
            batch_id="batch-1",
        ),
    ]


def _minute_rows(*, codes=("000001", "399001"), minutes=None):
    symbols = {"000001": "000001.SH", "399001": "399001.SZ"}
    return pd.DataFrame([
        {
            "stock_code": code,
            "qmt_code": symbols[code],
            "trade_time": f"2026-08-26 {minute}",
            "trade_date": "2026-08-26",
            "price": 100.0,
            "avg_price": 99.5,
            "change": 1.0,
            "change_pct": 1.0,
            "volume": 100.0,
            "amount": 10000.0,
        }
        for code in codes
        for minute in (minutes or minute_time_grid())
    ])


def test_index_minute_requires_every_code_on_exact_native_241_grid():
    validated = publisher.validate_minute_frame(
        _minute_rows(),
        catalog=_catalog(),
        expected_by_session={"2026-08-26": ("000001", "399001")},
        captured_at=datetime(2026, 8, 26, 15, 35),
    )

    assert len(validated) == 2 * 241
    assert validated.groupby("index_code").size().to_dict() == {
        "000001": 241,
        "399001": 241,
    }


@pytest.mark.parametrize(
    "partial",
    [
        _minute_rows(codes=("000001",)),
        _minute_rows(minutes=minute_time_grid()[200:]),
    ],
    ids=("missing-code", "late-window-only"),
)
def test_index_minute_rejects_partial_code_or_local_window(partial):
    with pytest.raises(publisher.IndexDataBlocked, match="DATA_BLOCKED"):
        publisher.validate_minute_frame(
            partial,
            catalog=_catalog(),
            expected_by_session={"2026-08-26": ("000001", "399001")},
            captured_at=datetime(2026, 8, 26, 15, 35),
        )


def test_index_kline_requires_cartesian_code_session_inventory():
    frame = pd.DataFrame([
        {
            "stock_code": "000001",
            "qmt_code": "000001.SH",
            "trade_date": "2026-08-26",
            "trade_time": "2026-08-26 15:00:00",
            "open": 100,
            "close": 101,
            "high": 102,
            "low": 99,
            "volume": 10,
            "amount": 1000,
        }
    ])

    with pytest.raises(publisher.IndexDataBlocked, match="grid is incomplete"):
        publisher.validate_kline_frame(
            frame,
            catalog=_catalog(),
            expected_by_session={"2026-08-26": ("000001", "399001")},
            captured_at=datetime(2026, 8, 26, 15, 25),
        )


def test_index_result_receipt_is_manifest_bound_and_tamper_evident(monkeypatch):
    release = {
        "strategy_git_blob": "1" * 40,
        "strategy_source_sha256": "2" * 64,
        "strategy_artifact_sha256": "3" * 64,
        "strategy_loaded_identity_sha256": "4" * 64,
    }
    calendar = SimpleNamespace(
        batch_id="batch-1",
        manifest_hash="5" * 64,
        session_set_hash="6" * 64,
    )
    manifest = publisher._manifest(
        dataset="minute",
        build_sha="7" * 40,
        release=release,
        calendar=calendar,
        expected_by_session={"2026-08-26": ("000001", "399001")},
        row_count=482,
        source_frame_hash="8" * 64,
        capture_receipts=[{"request_id": "capture-1"}],
        captured_at=datetime(2026, 8, 26, 15, 35),
        applied=True,
    )
    result = publisher.build_complete_result(
        dataset="minute",
        manifest=manifest,
        written_rows=482,
        verified_rows=482,
    )
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", "7" * 40)

    assert publisher.validate_task_result(result, 0) == "complete"
    assert scheduler_output_status(
        {"task_type": "qmt_index_minute"},
        __import__("json").dumps(result),
        return_code=0,
    ) == "success"
    result["manifest"]["minute_grid_count"] = 240
    with pytest.raises(ValueError, match="proof differs"):
        publisher.validate_task_result(result, 0)


def test_scheduler_binds_index_receipt_to_outer_task_and_reads_database(
    monkeypatch,
):
    release = {
        "strategy_git_blob": "1" * 40,
        "strategy_source_sha256": "2" * 64,
        "strategy_artifact_sha256": "3" * 64,
        "strategy_loaded_identity_sha256": "4" * 64,
    }
    calendar = SimpleNamespace(
        batch_id="batch-1",
        manifest_hash="5" * 64,
        session_set_hash="6" * 64,
    )
    captured_at = datetime(2026, 8, 26, 15, 35)
    manifest = publisher._manifest(
        dataset="minute",
        build_sha="7" * 40,
        release=release,
        calendar=calendar,
        expected_by_session={"2026-08-26": ("000001", "399001")},
        row_count=482,
        source_frame_hash="8" * 64,
        capture_receipts=[{"request_id": "capture-1"}],
        captured_at=captured_at,
        applied=True,
    )
    result = publisher.build_complete_result(
        dataset="minute",
        manifest=manifest,
        written_rows=482,
        verified_rows=482,
    )
    rendered = json.dumps(result)
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", "7" * 40)

    assert scheduler_output_status(
        {"task_type": "qmt_index_current"},
        rendered,
        return_code=0,
    ) == "failed"

    calls = []
    monkeypatch.setattr(
        publisher,
        "validate_persisted_result",
        lambda *args, **kwargs: calls.append((args, kwargs)) or {
            "sessions": ["2026-08-26"],
            "row_count": 482,
        },
    )
    validation = validate_scheduler_task_result(
        {
            "task_type": "qmt_index_minute",
            "_release_target_date": "2026-08-26",
            "_trigger_source": "release_catchup",
        },
        engine=object(),
        output=rendered,
        started_at=captured_at - timedelta(minutes=1),
        now=captured_at + timedelta(minutes=1),
    )
    assert validation.checked and validation.ok
    assert len(calls) == 1
    assert calls[0][1]["expected_session"] == "2026-08-26"

    mismatch = validate_scheduler_task_result(
        {
            "task_type": "qmt_index_minute",
            "_release_target_date": "2026-08-27",
            "_trigger_source": "release_catchup",
        },
        engine=object(),
        output=rendered,
        started_at=captured_at - timedelta(minutes=1),
        now=captured_at + timedelta(minutes=1),
    )
    assert mismatch.checked and not mismatch.ok
    assert "receipt session differs" in mismatch.message
    assert len(calls) == 1


def test_index_persisted_gate_rebuilds_authoritative_partition(monkeypatch):
    captured_at = datetime(2026, 8, 26, 15, 35)
    raw = _minute_rows()
    verified = publisher.validate_minute_frame(
        raw,
        catalog=_catalog(),
        expected_by_session={"2026-08-26": ("000001", "399001")},
        captured_at=captured_at,
    )
    calendar = SimpleNamespace(
        batch_id="batch-1",
        manifest_hash="5" * 64,
        session_set_hash="6" * 64,
    )
    manifest = publisher._manifest(
        dataset="minute",
        build_sha="7" * 40,
        release={
            "strategy_git_blob": "1" * 40,
            "strategy_source_sha256": "2" * 64,
            "strategy_artifact_sha256": "3" * 64,
            "strategy_loaded_identity_sha256": "4" * 64,
        },
        calendar=calendar,
        expected_by_session={"2026-08-26": ("000001", "399001")},
        row_count=len(verified),
        source_frame_hash=publisher._digest(
            verified.astype(object).where(pd.notna(verified), None).to_dict("records")
        ),
        capture_receipts=[{"request_id": "capture-1"}],
        captured_at=captured_at,
        applied=True,
    )
    result = publisher.build_complete_result(
        dataset="minute",
        manifest=manifest,
        written_rows=len(verified),
        verified_rows=len(verified),
    )
    monkeypatch.setattr(publisher, "_expected_build_sha", lambda value="": "7" * 40)
    session_calls = []

    def resolve_sessions(*_args, **kwargs):
        session_calls.append(kwargs)
        session = (
            kwargs["start_date"]
            if not kwargs["latest_session"]
            else "2026-08-26"
        )
        return calendar, [session]

    monkeypatch.setattr(publisher, "_resolve_sessions", resolve_sessions)
    monkeypatch.setattr(publisher, "_load_index_catalog", lambda *_args, **_kwargs: _catalog())
    monkeypatch.setattr(publisher, "get_kline_engine", lambda: object())
    monkeypatch.setattr(publisher, "_read_published", lambda **_kwargs: raw)

    proof = publisher.validate_persisted_result(
        object(),
        result,
        now=datetime(2026, 8, 27, 17, 59),
        expected_session="2026-08-26",
    )
    assert proof["row_count"] == 482
    assert session_calls[-1]["latest_session"] is False
    assert session_calls[-1]["start_date"] == "2026-08-26"

    with pytest.raises(publisher.IndexDataBlocked, match="stale index session"):
        publisher.validate_persisted_result(
            object(),
            result,
            now=datetime(2026, 8, 27, 18, 0),
            expected_session="2026-08-27",
        )

    drifted = raw.copy()
    drifted.loc[0, "price"] = 101.0
    monkeypatch.setattr(publisher, "_read_published", lambda **_kwargs: drifted)
    with pytest.raises(publisher.IndexDataBlocked, match="partition differs"):
        publisher.validate_persisted_result(
            object(),
            result,
            now=datetime(2026, 8, 27, 17, 59),
            expected_session="2026-08-26",
        )


def test_historical_current_is_explicitly_not_reconstructable(monkeypatch):
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge")
    monkeypatch.setattr(publisher, "_expected_build_sha", lambda value="": "a" * 40)
    monkeypatch.setattr(publisher, "_validate_release", lambda value: {})
    monkeypatch.setattr(publisher, "create_batch_engine", lambda: object())
    monkeypatch.setattr(
        publisher,
        "_resolve_sessions",
        lambda *args, **kwargs: (SimpleNamespace(), ["2026-08-25"]),
    )

    with pytest.raises(publisher.IndexDataBlocked, match="cannot reconstruct"):
        publisher.run(
            dataset="current",
            latest_session=True,
            start_date="",
            end_date="",
            apply=False,
            now=datetime(2026, 8, 26, 10, 0),
        )


def test_current_and_minute_capture_preserve_per_response_identity(monkeypatch):
    calls = []

    def fake_call(action, **kwargs):
        calls.append((action, kwargs))
        return {
            "request_id": f"request-{len(calls)}",
            "action": action,
            "status": "ok",
            "source": publisher.PROVIDER,
            "bridge_version": "bigqmt_inner_v2",
            "strategy_loaded_identity_sha256": "a" * 64,
            "rows": [
                {"stock_code": code.split(".", 1)[0]}
                for code in kwargs["stock_codes"]
            ],
        }

    monkeypatch.setattr(bridge, "_call", fake_call)
    current = bridge.current_capture(["000001.SH"])
    minute = bridge.minute_capture(
        [f"{index:06d}.SH" for index in range(51)],
        trade_date="2026-08-26",
        batch_size=100,
    )

    assert current["request_id"] == "request-1"
    assert len(minute["batch_receipts"]) == 2
    assert [item["row_count"] for item in minute["batch_receipts"]] == [50, 1]
    assert all(
        item["strategy_loaded_identity_sha256"] == "a" * 64
        for item in minute["batch_receipts"]
    )


class _CalendarReceipt:
    batch_id = "batch-1"
    manifest_hash = "a" * 64
    session_set_hash = "b" * 64

    def __init__(self, sessions):
        self.sessions = tuple(sessions)

    def sessions_between(self, start_date, end_date):
        return tuple(
            session
            for session in self.sessions
            if start_date <= session <= end_date
        )


@pytest.mark.parametrize("dataset", ("kline", "minute"))
@pytest.mark.parametrize(
    ("now", "expected"),
    (
        (datetime(2026, 8, 27, 0, 0), "2026-08-26"),
        (datetime(2026, 8, 27, 8, 0), "2026-08-26"),
        (datetime(2026, 8, 27, 15, 9), "2026-08-26"),
        (datetime(2026, 8, 27, 15, 10), "2026-08-27"),
        (datetime(2026, 8, 29, 8, 0), "2026-08-28"),
    ),
)
def test_latest_index_history_uses_close_cutoff_and_calendar(
    monkeypatch,
    dataset,
    now,
    expected,
):
    receipt = _CalendarReceipt(
        ("2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28")
    )
    monkeypatch.setattr(
        publisher,
        "_load_calendar_receipt",
        lambda *_args, **_kwargs: receipt,
    )

    _calendar, sessions = publisher._resolve_sessions(
        object(),
        dataset=dataset,
        latest_session=True,
        start_date="",
        end_date="",
        now=now,
    )

    assert sessions == [expected]


@pytest.mark.parametrize(
    ("now", "expected"),
    (
        (datetime(2026, 8, 27, 0, 0), "2026-08-27"),
        (datetime(2026, 8, 27, 8, 0), "2026-08-27"),
        (datetime(2026, 8, 27, 15, 9), "2026-08-27"),
        (datetime(2026, 8, 29, 8, 0), "2026-08-28"),
    ),
)
def test_latest_index_current_keeps_live_date_semantics(
    monkeypatch,
    now,
    expected,
):
    receipt = _CalendarReceipt(
        ("2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28")
    )
    monkeypatch.setattr(
        publisher,
        "_load_calendar_receipt",
        lambda *_args, **_kwargs: receipt,
    )

    _calendar, sessions = publisher._resolve_sessions(
        object(),
        dataset="current",
        latest_session=True,
        start_date="",
        end_date="",
        now=now,
    )

    assert sessions == [expected]
