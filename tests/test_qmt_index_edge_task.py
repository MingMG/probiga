from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
import json
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from integrations.bigqmt import bridge
from server.common.qmt_history_coverage import minute_time_grid
from server.common.scheduler_validation import (
    scheduler_output_status,
    validate_scheduler_task_result,
)
from tools import sync_qmt_index_edge as publisher


def test_release_transport_failure_is_distinct_from_invalid_frozen_identity(monkeypatch):
    def unavailable(**_kw):
        raise TimeoutError("bridge unavailable")

    monkeypatch.setattr(publisher.bridge, "capabilities", unavailable)
    with pytest.raises(publisher._IndexTransportUnavailable, match="QMT_BRIDGE_RELEASE_UNAVAILABLE"):
        publisher._validate_release("a" * 40)
    monkeypatch.setattr(publisher.bridge, "capabilities", lambda **_kw: {})

    def invalid(*_a, **_kw):
        raise ValueError("frozen identity differs")

    monkeypatch.setattr(publisher, "validate_bigqmt_strategy_release", invalid)
    with pytest.raises(publisher.IndexDataBlocked) as captured:
        publisher._validate_release("a" * 40)
    assert type(captured.value) is publisher.IndexDataBlocked


def _capture_session(monkeypatch, *, releases, recovered=True):
    outcomes = iter(releases)
    events = []

    def read(_build):
        events.append("release")
        outcome = next(outcomes)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def recover():
        events.append("recover")
        if isinstance(recovered, Exception):
            raise recovered
        return recovered

    monkeypatch.setattr(publisher, "_validate_release", read)
    monkeypatch.setattr(publisher, "_recover_qmt_session_after_failure", recover)
    return lambda: publisher._IndexCaptureSession("a" * 40), events


def test_initial_release_offline_is_recovered_once_before_capture(monkeypatch):
    factory, events = _capture_session(monkeypatch, releases=[
        publisher._IndexTransportUnavailable("offline"), {"identity": "fixed"},
    ])
    session = factory()
    assert session.release == {"identity": "fixed"}
    assert events == ["release", "recover", "release"]


@pytest.mark.parametrize("error", [publisher.IndexDataBlocked("invalid identity"), ValueError("malformed")])
def test_invalid_release_never_calls_login_recovery(monkeypatch, error):
    factory, events = _capture_session(monkeypatch, releases=[error])
    with pytest.raises(type(error)):
        factory()
    assert events == ["release"]


@pytest.mark.parametrize("recovered", [False, RuntimeError("QMT_LOGIN_UNKNOWN")])
def test_unconfirmed_recovery_never_requests_a_new_capture(monkeypatch, recovered):
    factory, events = _capture_session(monkeypatch, releases=[{"identity": "fixed"}], recovered=recovered)
    session = factory()
    calls = []

    def read():
        calls.append(True)
        raise TimeoutError("offline")

    with pytest.raises((TimeoutError, RuntimeError)):
        session.capture(read)
    assert len(calls) == 1
    assert events == ["release", "recover"]


def test_recovery_keeps_successful_minute_batches_and_original_scope(monkeypatch):
    release = {"identity": "fixed"}
    factory, events = _capture_session(monkeypatch, releases=[release, release, release])
    session = factory()
    catalog = [publisher.IndexCatalogMember(f"{i:06d}", f"{i:06d}.SH", "index", None, None, "catalog") for i in range(41)]
    calls = []

    def capture(symbols, **kwargs):
        calls.append((tuple(symbols), kwargs))
        if len(calls) == 2:
            raise TimeoutError("offline")
        return {"rows": [{"stock_code": code[:6]} for code in symbols], "batch_receipts": [
            {"requested_codes": list(symbols), "row_count": len(symbols)}
        ]}

    monkeypatch.setattr(publisher.bridge, "minute_capture", capture)
    frame, receipts = publisher._fetch_frames(
        dataset="minute", catalog=catalog,
        expected_by_session={"2026-09-11": tuple(member.index_code for member in catalog)},
        read_capture=session.capture,
    )
    session.verify_final()
    assert len(frame) == 41 and len(receipts) == 2
    assert [len(call[0]) for call in calls] == [40, 1, 1]
    assert calls[1][0] == calls[2][0]
    assert all(call[1]["trade_date"] == call[1]["start_date"] == call[1]["end_date"] == "2026-09-11" for call in calls)
    assert events == ["release", "recover", "release", "release"]


def test_final_release_recovery_preserves_already_captured_rows(monkeypatch):
    release = {"identity": "fixed"}
    factory, events = _capture_session(monkeypatch, releases=[
        release, publisher._IndexTransportUnavailable("offline"), release,
    ])
    session = factory()
    calls = []
    rows = session.capture(lambda: calls.append(True) or {"rows": [1]})
    session.verify_final()
    assert rows == {"rows": [1]} and calls == [True]
    assert events == ["release", "release", "recover", "release"]


def test_recovered_release_drift_blocks_retry_and_budget_is_shared(monkeypatch):
    factory, events = _capture_session(monkeypatch, releases=[{"identity": "old"}, {"identity": "new"}])
    session = factory()
    calls = []

    def offline():
        calls.append(True)
        raise TimeoutError("offline")

    with pytest.raises(publisher.IndexDataBlocked, match="changed during recovery"):
        session.capture(offline)
    assert len(calls) == 1
    with pytest.raises(TimeoutError):
        session.capture(offline)
    assert events.count("recover") == 1


def test_source_value_failure_never_triggers_login(monkeypatch):
    factory, events = _capture_session(monkeypatch, releases=[{"identity": "fixed"}])
    session = factory()

    def invalid():
        raise RuntimeError("invalid source response")

    with pytest.raises(RuntimeError):
        session.capture(invalid)
    assert events == ["release"]


def test_capture_keeps_verified_strategy_identity_across_app_releases():
    app_build = "a" * 40
    release = {
        "strategy_build_sha": "b" * 40,
        "compatible_app_build_sha": app_build,
        "strategy_release_protocol": "release-v2",
        "strategy_identity_protocol": "identity-v1",
        "strategy_git_blob": "c" * 40,
        "strategy_source_sha256": "d" * 64,
        "strategy_artifact_sha256": "e" * 64,
        "strategy_loaded_identity_sha256": "f" * 64,
    }
    receipt = {
        **release, "request_id": "capture-1", "action": "kline", "status": "ok",
        "source": publisher.PROVIDER, "bridge_version": "bigqmt_inner_v2",
        "strategy_identity_frozen": True, "strategy_identity_status": "BOUND",
        "requested_code_count": 1, "row_count": 1,
    }
    publisher._validate_capture_receipts([receipt], dataset="kline", build_sha=app_build, release=release)
    for field in ("strategy_build_sha", "strategy_git_blob", "strategy_source_sha256", "strategy_artifact_sha256", "strategy_loaded_identity_sha256"):
        with pytest.raises(publisher.IndexDataBlocked):
            publisher._validate_capture_receipts([{**receipt, field: "wrong"}], dataset="kline", build_sha=app_build, release=release)
    with pytest.raises(publisher.IndexDataBlocked):
        publisher._validate_capture_receipts([receipt], dataset="kline", build_sha="0" * 40, release=release)


@pytest.mark.parametrize("source", ["qmt", publisher.PROVIDER])
@pytest.mark.parametrize("detail_source", ["gj_qmt", publisher.PROVIDER])
def test_index_catalog_accepts_full_qmt_source_with_bound_details(monkeypatch, source, detail_source):
    engine = create_engine("sqlite:///:memory:")
    monkeypatch.setattr(publisher, "MIN_FORMAL_INDEX_COUNT", 1)
    reference_reads = []
    monkeypatch.setattr(publisher, "load_stock_catalog", lambda connection, **kw: reference_reads.append(kw["batch_id"]))
    with engine.begin() as c:
        c.execute(text("CREATE TABLE si_all_index_code (index_code TEXT,name TEXT,source TEXT)"))
        c.execute(text("CREATE TABLE qmt_instrument_detail (qmt_code TEXT,stock_code TEXT,short_name TEXT,list_date TEXT,expire_date TEXT,batch_id TEXT,data_source TEXT,permission_status TEXT)"))
        c.execute(text("INSERT INTO si_all_index_code VALUES ('000001','index',:source)"), {"source": source})
        c.execute(text("INSERT INTO si_all_index_code VALUES ('395001','volume statistics',:source)"), {"source": source})
        c.execute(text("INSERT INTO qmt_instrument_detail VALUES ('000001.SH','000001','index','1990-12-19',NULL,'batch-1',:source,'SUPPORTED')"), {"source": detail_source})
    try:
        assert len(publisher._load_index_catalog(engine, expected_batch_id="batch-1")) == 1
        assert reference_reads == ["batch-1"]
        with engine.begin() as c:
            c.execute(text("UPDATE qmt_instrument_detail SET list_date=NULL"))
        member = publisher._load_index_catalog(engine)[0]
        assert member.list_date is None and member.eligible("2026-09-04")
        with pytest.raises(publisher.IndexDataBlocked, match="batch"):
            publisher._load_index_catalog(engine, expected_batch_id="wrong-batch")
        with engine.begin() as c:
            c.execute(text("UPDATE si_all_index_code SET source='unrelated-provider'"))
        with pytest.raises(publisher.IndexDataBlocked, match="identity"):
            publisher._load_index_catalog(engine, expected_batch_id="batch-1")
    finally:
        engine.dispose()


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


def _native_index_frame(symbol, minutes):
    frame = _minute_rows(codes=("000001",), minutes=minutes)
    frame["stock_code"] = symbol[:6]
    frame["qmt_code"] = symbol
    catalog = [publisher.IndexCatalogMember(symbol[:6], symbol, "native index", None, None, "batch-1")]
    return frame, catalog


def test_bond_index_requires_and_preserves_complete_1530_session():
    from server.common.qmt_index_minute_grid import BOND_GRID
    frame, catalog = _native_index_frame("000012.SH", BOND_GRID)
    kwargs = dict(catalog=catalog, expected_by_session={"2026-08-26": ("000012",)}, captured_at=datetime(2026, 8, 27))
    assert len(publisher.validate_minute_frame(frame, **kwargs)) == 271
    with pytest.raises(publisher.IndexDataBlocked, match="required grid is incomplete"):
        publisher.validate_minute_frame(frame.iloc[:-1], **kwargs)


@pytest.mark.parametrize("extension", [("11:31:00", "16:01:00", "16:10:00"), ("11:59:00", "16:07:00", "16:09:00")])
def test_cross_market_preserves_date_specific_native_observations_without_filling(extension):
    frame, catalog = _native_index_frame("980001.SZ", (*minute_time_grid(), *extension))
    kwargs = dict(catalog=catalog, expected_by_session={"2026-08-26": ("980001",)}, captured_at=datetime(2026, 8, 27))
    result = publisher.validate_minute_frame(frame, **kwargs)
    assert len(result) == 244
    assert set(result.trade_time.dt.strftime("%H:%M:%S")) == set(minute_time_grid()) | set(extension)
    with pytest.raises(publisher.IndexDataBlocked, match="required grid is incomplete"):
        publisher.validate_minute_frame(frame.iloc[1:], **kwargs)
    with pytest.raises(publisher.IndexDataBlocked, match="key inventory differs"):
        publisher.validate_minute_frame(pd.concat([frame, frame.iloc[-1:]]), **kwargs)


@pytest.mark.parametrize("symbol,extra", [("000001.SH", "15:01:00"), ("000012.SH", "15:31:00"), ("980001.SZ", "16:11:00"), ("980001.SZ", "12:01:00")])
def test_index_extensions_outside_instrument_session_are_rejected(symbol, extra):
    from server.common.qmt_index_minute_grid import index_minute_grids
    required, _ = index_minute_grids(symbol)
    frame, catalog = _native_index_frame(symbol, (*required, extra))
    with pytest.raises(publisher.IndexDataBlocked, match="key inventory differs"):
        publisher.validate_minute_frame(frame, catalog=catalog,
            expected_by_session={"2026-08-26": (symbol[:6],)}, captured_at=datetime(2026, 8, 27))


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


def test_index_storage_precision_hash_survives_mysql_decimal_readback():
    source = pd.DataFrame([{
        "index_code": "000001",
        "open": 100.12345678,
        "close": 101.87654321,
        "change": None,
    }])
    persisted = pd.DataFrame([{
        "index_code": "000001",
        "open": Decimal("100.123457"),
        "close": Decimal("101.876543"),
        "change": None,
    }])

    source_normalized = publisher._normalize_storage_precision(source)
    persisted_normalized = publisher._normalize_storage_precision(persisted)
    source_hash = publisher._digest(
        source_normalized.astype(object)
        .where(pd.notna(source_normalized), None).to_dict("records")
    )
    persisted_hash = publisher._digest(
        persisted_normalized.astype(object)
        .where(pd.notna(persisted_normalized), None).to_dict("records")
    )

    assert source_hash == persisted_hash


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
        catalog=_catalog(),
        expected_by_session={"2026-08-26": ("000001", "399001")},
        row_count=482,
        source_frame_hash="8" * 64,
        capture_receipts=[{"request_id": "capture-1"}],
        captured_at=datetime(2026, 8, 26, 15, 35),
        applied=True,
    )
    # Exchange calendar and QMT catalog have independent real source batches.
    manifest["calendar_batch_id"] = "exchange-calendar-2026"
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
        catalog=_catalog(),
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
        catalog=_catalog(),
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

    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE si_trade_calendar (trade_date TEXT, trade_status INTEGER)"))
        connection.execute(text("INSERT INTO si_trade_calendar VALUES (:day, 1)"), [{"day": day} for day in receipt.sessions])
    _calendar, sessions = publisher._resolve_sessions(
        engine,
        dataset=dataset,
        latest_session=True,
        start_date="",
        end_date="",
        now=now,
    )

    expected_minute = "2026-08-28" if now.date().isoformat() == "2026-08-29" else "2026-08-26"
    assert sessions == [expected_minute if dataset == "minute" else expected]


def test_explicit_index_minute_same_day_is_rejected_without_rewriting_target():
    with pytest.raises(publisher.IndexDataBlocked, match="elapsed calendar date"):
        publisher._resolve_sessions(object(), dataset="minute", latest_session=False,
                                    start_date="2026-09-11", end_date="2026-09-11",
                                    now=datetime(2026, 9, 11, 23, 59))


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
