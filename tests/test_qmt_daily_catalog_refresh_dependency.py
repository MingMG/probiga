from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from server.common import qmt_stock_catalog as catalogs
from server.common.qmt_attestation_contract import expected_stock_set_contract
from server.common.qmt_daily_no_row import NATIVE_QMT_NO_TRADE_CONTRACT_SCHEMA
from tools import repair_qmt_canonical_history_gaps as repair
from tools import sync_qmt_stock_edge as publisher


DAY = "2026-09-04"
NOW = datetime(2026, 9, 21, 1)


def _catalog(batch, members):
    return catalogs.StockCatalogBatch(
        batch_id=batch, captured_at="2026-09-20 16:00:00",
        history_complete_from="1970-01-01", member_count=len(members),
        member_set_hash="a" * 64, manifest_hash="b" * 64,
        native_sectors=(), members=tuple(
            {"stock_code": code, "list_date": listed, "expire_date": expired}
            for code, listed, expired in members
        ),
    )


@pytest.fixture
def state(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("""
            CREATE TABLE qmt_kline_attestation_run (
                run_id TEXT, provider TEXT, status TEXT, start_date TEXT,
                end_date TEXT, tolerance_json TEXT, finished_at TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO qmt_kline_attestation_run VALUES
            ('daily-run',:provider,'COMPLETED',:day,:day,'{}','2026-09-20 17:00:00')
        """), {"provider": publisher.PROVIDER, "day": DAY})
        connection.execute(text("""
            CREATE TABLE sm_stock_kline (
                stock_code TEXT, trade_date TEXT, k_type INTEGER, adjust_type INTEGER,
                open REAL, close REAL, high REAL, low REAL, volume REAL, amount REAL,
                pre_close REAL, data_source TEXT, batch_id TEXT, data_version TEXT,
                quality_status TEXT, permission_status TEXT
            )
        """))
        connection.execute(text("""
            INSERT INTO sm_stock_kline VALUES
            ('000001',:day,1,0,10,10,11,9,100,1000,10,
             :provider,'batch','version','QMT_ATTESTED','SUPPORTED')
        """), {"provider": publisher.PROVIDER, "day": DAY})
    initial = _catalog("old", [("000001", "1991-01-01", None)])
    latest = _catalog("latest", [
        ("000001", "1991-01-01", None), ("301686", "1970-01-01", None),
    ])
    current = SimpleNamespace(
        engine=engine, source=initial, latest=latest, native_dates={},
        read_catalogs=[], traded_codes=["000001"],
    )

    def truth(*_args, **_kwargs):
        return SimpleNamespace(
            run_id="daily-run", requested_sessions=(DAY,),
            attested_row_count=len(current.traded_codes),
            catalog_batch_id=current.source.batch_id,
            catalog_manifest_hash="b" * 64, calendar_manifest_hash="c" * 64,
            calendar_batch_id="calendar", calendar_session_set_hash="d" * 64,
        )

    def manifest(*_args, **_kwargs):
        return {DAY: {
            **expected_stock_set_contract(DAY, current.traded_codes),
            "catalog_manifest_hash": "b" * 64, "calendar_manifest_hash": "c" * 64,
        }}

    def no_row(*_args, **_kwargs):
        if not current.native_dates:
            return None
        return {
            "schema": NATIVE_QMT_NO_TRADE_CONTRACT_SCHEMA,
            "entities": [{"stock_code": code, "affected_trade_dates": dates}
                         for code, dates in current.native_dates.items()],
        }

    def load_catalog(_connection, *, decision_known_at, batch_id=None):
        assert decision_known_at == NOW
        current.read_catalogs.append(batch_id)
        if batch_id is None:
            return current.latest
        assert batch_id == current.source.batch_id
        return current.source

    monkeypatch.setattr(publisher, "load_qmt_daily_market_truth", truth)
    monkeypatch.setattr(publisher, "validated_universe_manifest", manifest)
    monkeypatch.setattr(publisher, "validated_no_row_exception_contract", no_row)
    monkeypatch.setattr(catalogs, "load_stock_catalog", load_catalog)
    return current


def _reuse(state):
    return publisher._reusable_daily_partition(
        state.engine, trade_date=DAY, decision_known_at=NOW,
    )


def test_valid_old_daily_truth_cannot_hide_new_catalog_security(state):
    assert _reuse(state) is None
    assert state.read_catalogs == [None, "old"]


def test_new_catalog_identity_without_universe_expansion_does_not_recapture(state):
    state.latest = _catalog("new-version", [("000001", "1991-01-01", None)])
    proof = _reuse(state)
    assert proof["row_count"] == 1
    assert proof["attestation_run_id"] == "daily-run"
    assert proof["catalog_manifest_hash"] == state.source.manifest_hash


@pytest.mark.parametrize("listed,expired", [
    ("2026-09-11", None), ("1991-01-01", "2026-09-03"),
])
def test_actual_lifecycle_excludes_only_dates_outside_eligibility(state, listed, expired):
    state.latest = _catalog("latest", [
        ("000001", "1991-01-01", None), ("301686", listed, expired),
    ])
    assert _reuse(state)["row_count"] == 1


def test_refresh_with_independent_exact_day_native_no_trade_closes_dependency(state):
    state.source = state.latest
    state.native_dates = {"301686": [DAY]}
    proof = _reuse(state)
    assert proof["native_no_trade_codes"] == ["301686"]
    assert proof["native_no_trade_rows"] == 1
    assert state.read_catalogs == [None]


def test_traded_new_security_also_closes_dependency(state):
    state.source = state.latest
    state.traded_codes.append("301686")
    with state.engine.begin() as connection:
        connection.execute(text("""
            INSERT INTO sm_stock_kline SELECT '301686',trade_date,k_type,adjust_type,
                open,close,high,low,volume,amount,pre_close,data_source,batch_id,
                data_version,quality_status,permission_status FROM sm_stock_kline
        """))
    assert _reuse(state)["row_count"] == 2


@pytest.mark.parametrize("native_dates", [{}, {"301686": ["2026-09-07"]}])
def test_absence_or_another_day_is_not_native_no_trade(state, native_dates):
    state.source = state.latest
    state.native_dates = native_dates
    with pytest.raises(publisher.StockDataBlocked, match="catalog coverage is invalid"):
        _reuse(state)


def test_latest_catalog_corruption_is_terminal(state, monkeypatch):
    monkeypatch.setattr(catalogs, "load_stock_catalog", lambda *_a, **_k: (
        _ for _ in ()
    ).throw(ValueError("manifest hash differs")))
    with pytest.raises(publisher.StockDataBlocked, match="catalog coverage is invalid"):
        _reuse(state)


def test_planner_and_daily_publisher_share_refresh_then_reuse_decision(state, monkeypatch):
    window = repair.CalendarWindow(
        sessions=(DAY,), batch_id="calendar", manifest_hash="c" * 64,
        source_session_set_hash="d" * 64,
    )
    inspector = repair.CanonicalPartitionInspector(
        state.engine, state.engine, state.engine, window=window, decision_time=NOW,
    )
    partition = repair.PartitionRef("stock_daily", DAY)
    with pytest.raises(repair.CanonicalGapRepairBlocked, match="attestation unavailable"):
        inspector(partition)
    monkeypatch.setattr(publisher, "_validate_executor", lambda *_a: None)
    monkeypatch.setattr(publisher, "_build_sha", lambda *_a: "1" * 40)
    monkeypatch.setattr(publisher, "create_batch_engine", lambda **_k: state.engine)
    monkeypatch.setattr(publisher, "get_kline_engine", lambda: state.engine)
    calendar = SimpleNamespace(batch_id="calendar", manifest_hash="c" * 64,
                               session_set_hash="d" * 64)
    monkeypatch.setattr(publisher, "_sessions", lambda *_a, **_k: (calendar, [DAY]))
    monkeypatch.setattr(publisher, "_release", lambda *_a: {})
    monkeypatch.setattr(publisher, "_release_identity", lambda value: value)
    calls = []

    def capture(dataset, **kwargs):
        calls.append((dataset, kwargs))
        state.source = state.latest
        state.native_dates = {"301686": [DAY]}
        return {"status": "success", "source_policy": "bigqmt_primary", "attestation": {
            "status": "COMPLETED", "apply": True, "provider": publisher.PROVIDER,
            "start_date": DAY, "end_date": DAY, "target_rows": 1,
            "qmt_rows": 1, "matched_rows": 1, "run_id": "daily-run",
            "daily_universe": {DAY: expected_stock_set_contract(DAY, ["000001"])},
            "native_no_trade_rows": 1, "native_no_trade_by_date": {DAY: ["301686"]},
            "catalog_manifest_hash": "b" * 64, "calendar_manifest_hash": "c" * 64,
        }}

    monkeypatch.setattr(publisher, "run_dataset", capture)
    kwargs = dict(dataset="daily", latest_session=False, start_date=DAY, end_date=DAY,
                  expected_build_sha="1" * 40, apply=True, now=NOW)
    assert publisher.run(**kwargs)["execution"]["captured_sessions"] == [DAY]
    assert inspector(partition)["row_count"] == 1
    assert publisher.run(**kwargs)["execution"]["reused_sessions"] == [DAY]
    assert calls == [("daily_kline", {"date_str": DAY, "require_bigqmt": True})]
