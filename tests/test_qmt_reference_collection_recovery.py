from __future__ import annotations

import errno
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine

from integrations.bigqmt import reference
from tools import sync_bigqmt_reference as membership
from tools import sync_guojin_qmt_reference_data as catalog


BUILD = "a" * 40


@pytest.fixture
def validate_release(monkeypatch):
    calls = []
    def validate(payload, *, expected_build_sha, root, source_path):
        assert expected_build_sha == BUILD
        assert source_path.is_relative_to(root)
        calls.append(dict(payload))
        if payload.get("invalid"):
            raise RuntimeError("QMT_FROZEN_RELEASE_INVALID")
        return {"strategy_source_sha256": "b" * 64, "compatible_app_build_sha": BUILD}
    monkeypatch.setattr(reference, "validate_strategy_release_payload", validate)
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD)
    return calls


class Source:
    def __init__(self):
        self.model = "old-model"
        self.capability_calls = 0
        self.reads = []
        self.fail_capability = set()
        self.fail_read = set()
        self.invalid = False

    def capabilities(self, **kwargs):
        self.capability_calls += 1
        if self.capability_calls in self.fail_capability:
            raise TimeoutError("native transport did not respond")
        return {"model_instance_id": self.model, "invalid": self.invalid}

    def sector_members(self, name, **kwargs):
        self.reads.append((self.model, name))
        if len(self.reads) in self.fail_read:
            raise ConnectionResetError("connection reset")
        return [self.model, name]


@pytest.mark.parametrize("stage", ["before", "during", "after"])
def test_one_recovery_restarts_whole_reference_batch_with_new_model(validate_release, stage):
    source = Source()
    if stage == "before": source.fail_capability = {1}
    if stage == "during": source.fail_read = {2}
    if stage == "after": source.fail_capability = {2}
    attempts = []
    recovery = []
    def collect(session):
        attempts.append(session.identity["model_instance_id"])
        return [session.sector_members("first"), session.sector_members("last")]
    def recover():
        recovery.append(True)
        source.model = "new-model"
        return True
    result = reference.run_reference_capture(collect, source_bridge=source, recover_session=recover)
    assert result == [["new-model", "first"], ["new-model", "last"]]
    assert recovery == [True]
    assert attempts == (["new-model"] if stage == "before" else ["old-model", "new-model"])
    assert all(row[0] == "new-model" for row in result)
    assert validate_release[-1]["model_instance_id"] == "new-model"


def test_second_disconnect_does_not_relogin_again(validate_release):
    source = Source()
    source.fail_read = {1, 2}
    recoveries = []
    with pytest.raises(reference.ReferenceTransportUnavailable):
        reference.run_reference_capture(lambda session: session.sector_members("one"),
            source_bridge=source, recover_session=lambda: recoveries.append(True) or True)
    assert len(source.reads) == 2 and recoveries == [True]


@pytest.mark.parametrize("recovered", [False, None, 1, "true"])
def test_only_confirmed_recovery_permits_retry(validate_release, recovered):
    source = Source()
    source.fail_read = {1}
    with pytest.raises(reference.ReferenceTransportUnavailable):
        reference.run_reference_capture(lambda session: session.sector_members("one"),
            source_bridge=source, recover_session=lambda: recovered)
    assert len(source.reads) == 1


@pytest.mark.parametrize("failure", [ValueError("invalid field"), RuntimeError("native permission denied"),
    PermissionError("directory denied"), FileNotFoundError("missing configuration"),
    OSError(errno.ENOSPC, "disk full"), OSError("unclassified failure")])
def test_integrity_permission_and_local_storage_errors_never_relogin(validate_release, failure):
    source = Source()
    def fail(*_a, **_k): raise failure
    source.sector_members = fail
    with pytest.raises(type(failure)):
        reference.run_reference_capture(lambda session: session.sector_members("one"), source_bridge=source,
            recover_session=lambda: pytest.fail("non-transport error attempted login"))


def test_model_drift_without_disconnect_is_rejected(validate_release):
    source = Source()
    def collect(session):
        result = session.sector_members("one")
        source.model = "unexpected-model"
        return result
    with pytest.raises(RuntimeError, match="MODEL_CHANGED"):
        reference.run_reference_capture(collect, source_bridge=source,
            recover_session=lambda: pytest.fail("identity drift attempted login"))


def test_invalid_recovered_release_is_never_accepted(validate_release):
    source = Source()
    source.fail_read = {1}
    def recover(): source.invalid = True; return True
    with pytest.raises(RuntimeError, match="FROZEN_RELEASE_INVALID"):
        reference.run_reference_capture(lambda session: session.sector_members("one"),
            source_bridge=source, recover_session=recover)
    assert len(source.reads) == 1


def test_bound_reference_session_never_reads_or_writes_unbound_sector_cache(validate_release, monkeypatch):
    source = Source()
    source.sector_list = lambda **_k: pd.DataFrame()
    session = reference.ReferenceReadSession(BUILD, source_bridge=source)
    monkeypatch.setattr(reference, "_read_sector_cache", lambda: pytest.fail("stale cache reused"))
    monkeypatch.setattr(reference, "_write_sector_cache", lambda *_a: pytest.fail("unverified whole capture cached"))
    assert all(frame.empty for frame in reference.fetch_sector_datasets(source_bridge=session).values())


@pytest.mark.parametrize("failure", [TimeoutError("transport timeout"), RuntimeError("native denied")])
def test_index_discovery_does_not_silently_drop_failed_sector(failure):
    def fail(*_a, **_k): raise failure
    with pytest.raises(type(failure)):
        reference.fetch_all_index_codes(source_bridge=SimpleNamespace(sector_members=fail))


def test_membership_fetch_uses_whole_capture_boundary(validate_release, monkeypatch):
    source = Source()
    source.fail_read = {2}
    attempts = []
    def collect(_engine, *, force_reference_refresh, source_bridge):
        attempts.append(source_bridge.identity["model_instance_id"])
        first = source_bridge.sector_members("stock")
        last = source_bridge.sector_members("membership")
        return {"first": first, "last": last}, {"count": 2}
    monkeypatch.setattr(membership, "_fetch_and_validate", collect)
    def recover(): source.model = "new-model"; return True
    monkeypatch.setattr(membership, "run_reference_capture", lambda callback: reference.run_reference_capture(
        callback, source_bridge=source, recover_session=recover))
    frames, counts = membership.fetch_and_validate(object())
    assert frames == {"first": ["new-model", "stock"], "last": ["new-model", "membership"]}
    assert counts == {"count": 2} and attempts == ["old-model", "new-model"]


def test_calendar_transport_failure_preserves_recovery_marker(validate_release, monkeypatch):
    source = Source()
    def fail(*_a, **_k): raise TimeoutError("calendar disconnected")
    source.trading_calendar_capture = fail
    session = reference.ReferenceReadSession(BUILD, source_bridge=source)
    monkeypatch.setattr(catalog, "validate_strategy_release_payload", lambda *_a, **_k: {})
    with pytest.raises(reference.ReferenceTransportUnavailable):
        catalog._fetch_trading_calendar(2026, 2026, expected_build_sha=BUILD,
            as_of_date=date(2026, 9, 11), source_bridge=session)


@pytest.mark.parametrize("database_failure", [False, True])
def test_actual_catalog_capture_discards_pre_disconnect_stocks_and_never_retries_dml(
    validate_release, monkeypatch, database_failure,
):
    source = Source()
    captures = []
    writes = []
    recoveries = []
    engine = create_engine("sqlite://")
    monkeypatch.setattr(catalog, "create_batch_engine", lambda *_a, **_k: engine)
    monkeypatch.setattr(catalog, "get_mysql_url", lambda **_k: "sqlite://")
    monkeypatch.setattr(catalog, "validate_reference_tables", lambda _e: None)
    monkeypatch.setattr(catalog, "load_stock_catalog", lambda *_a, **_k: None)
    monkeypatch.setattr(catalog, "_read_index_qmt_codes", lambda _e: [])

    def code(): return "600000" if source.model == "old-model" else "920071"
    def symbol(): return code() + (".SH" if code() == "600000" else ".BJ")
    def native_members(_names, **_kwargs):
        captures.append((source.model, "members"))
        return pd.DataFrame([{"qmt_code": symbol(), "stock_code": code(), "sector_name": sector}
                             for sector in catalog.DEFAULT_STOCK_SECTORS.values()])
    source.sector_members_many = native_members
    source.instrument_details = lambda _codes, **_kwargs: pd.DataFrame([{
        "qmt_code": symbol(), "stock_code": code(), "short_name": source.model,
        "exchange": symbol().split(".")[1], "product_type": "STOCK",
        "list_date": "2026-09-03", "expire_date": None,
    }])
    def native_calendar(*_args, **_kwargs):
        captures.append((source.model, "calendar"))
        assert not writes
        if source.model == "old-model":
            raise TimeoutError("calendar lost its model after stock capture")
        return {"rows": [{"calendar_year": 2026, "trade_date": "2026-09-11", "trade_status": 1, "day_week": 5}]}
    source.trading_calendar_capture = native_calendar
    def calendar_capture(*_args, source_bridge, **_kwargs):
        response = source_bridge.trading_calendar_capture("SH")
        return pd.DataFrame(response["rows"]), {"model": source_bridge.identity["model_instance_id"]}
    monkeypatch.setattr(catalog, "_fetch_trading_calendar", calendar_capture)
    monkeypatch.setattr(catalog, "_build_proven_calendar_manifest", lambda **kwargs: ({
        "known_at": kwargs["captured_at"], "start_date": "2026-09-11", "end_date": "2026-09-11",
    }, "native-calendar-source"))
    def recover():
        assert not writes
        recoveries.append(True)
        source.model = "new-model"
        return True
    monkeypatch.setattr(catalog, "run_reference_capture", lambda callback, **kwargs: reference.run_reference_capture(
        callback, source_bridge=source, recover_session=recover, **kwargs))
    def publish(_engine, *, table_name, frame, **_kwargs):
        if not frame.empty:
            writes.append((table_name, frame.copy()))
            if database_failure:
                raise TimeoutError("database timed out after native capture completed")
        return {}
    monkeypatch.setattr(catalog, "_safe_upsert_frame", publish)
    monkeypatch.setattr(catalog, "insert_catalog_batch", lambda *_a, **kwargs: {"batch_id": kwargs["batch_id"]})
    monkeypatch.setattr(catalog, "insert_trade_calendar_receipt", lambda *_a, **_k: {})
    kwargs = dict(start_year=2026, end_year=2026, iscomplete=True, refresh_timeout=1,
                  skip_refresh=True, skip_calendar=False, catalog_only=True, release_build_sha=BUILD)
    try:
        if database_failure:
            with pytest.raises(TimeoutError, match="database timed out"):
                catalog.sync_reference_data(**kwargs)
        else:
            result = catalog.sync_reference_data(**kwargs)
            assert result["status"] == "success"
        assert recoveries == [True]
        assert captures == [("old-model", "members"), ("old-model", "calendar"),
                            ("new-model", "members"), ("new-model", "calendar")]
        for _table, frame in writes:
            if "stock_code" in frame:
                assert set(frame["stock_code"]) == {"920071"}
    finally:
        engine.dispose()
