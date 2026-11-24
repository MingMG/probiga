"""Synthetic fixed-action fixtures reproducing the Nov23 missing-bar boundary."""
from copy import deepcopy
from datetime import datetime
from hashlib import sha256
import json
from types import SimpleNamespace

import pytest

from integrations.myquant.bridge import UPPER_LIMIT_HISTORY_ACTION, UPPER_LIMIT_HISTORY_FIELDS
from server.common.historical_suspension_evidence import build_suspension_evidence
from server.common import qmt_datadir_daily_coverage as coverage

DAY = "2026-11-23"
CUTOFF = datetime.fromisoformat("2026-11-25T02:49:03+08:00")


def synthetic_response():
    payload = {"ok": True, "action": UPPER_LIMIT_HISTORY_ACTION,
        "fields": UPPER_LIMIT_HISTORY_FIELDS, "columns": UPPER_LIMIT_HISTORY_FIELDS.split(","),
        "requested_symbols": ["SZSE.002717"], "start_date": DAY, "end_date": DAY,
        "request_started_at": "2026-11-25T02:49:01+08:00",
        "captured_at": "2026-11-25T02:49:02.453573+08:00", "timezone": "Asia/Shanghai",
        "sdk_version": "synthetic-test", "python_version": "synthetic-test",
        "entitlement_status": "SUPPORTED", "errors": {}, "rows": [{
            "symbol": "SZSE.002717", "trade_date": DAY + "T00:00:00+08:00",
            "pre_close": 10.0, "upper_limit": 11.0, "lower_limit": 9.0, "is_suspended": 1}]}
    raw = json.dumps(payload)
    request = json.dumps({"action": UPPER_LIMIT_HISTORY_ACTION, "symbols": ["SZSE.002717"],
                          "start_date": DAY, "end_date": DAY})
    return {**payload, "raw_stdout": raw, "raw_stdout_sha256": sha256(raw.encode()).hexdigest(),
        "canonical_request_json": request, "canonical_request_sha256": sha256(request.encode()).hexdigest(),
        "worker_sha256": "a" * 64}


def authorities():
    return (SimpleNamespace(batch_id="catalog", manifest_hash="a" * 64, member_set_hash="b" * 64,
                            eligible_codes=lambda _: ["002717", "600000"]),
            SimpleNamespace(batch_id="calendar", manifest_hash="c" * 64,
                            sessions_between=lambda *_: [DAY]))


def traded_bar():
    return {"stock_code": "600000", "trade_date": DAY, "open": 10, "close": 10,
            "high": 11, "low": 9, "volume": 100, "amount": 1000, "pre_close": 10}


def test_native_archive_accounts_for_dated_suspension_without_a_fake_bar():
    catalog, calendar = authorities()
    claim = dict(catalog=catalog, calendar=calendar, day=DAY, rows=[traded_bar()],
                 run_id="native-history", captured_at=CUTOFF, historical_documents={},
                 native_content_sha256="d" * 64)
    with pytest.raises(ValueError, match="lifecycle inventory"):
        coverage.build_manifest(**claim)
    evidence = build_suspension_evidence(synthetic_response(), codes=["002717"], day=DAY, cutoff=CUTOFF)
    manifest = coverage.build_manifest(**claim, historical_suspensions=evidence)
    assert coverage.validate_manifest(manifest, catalog=catalog, calendar=calendar) == manifest
    assert coverage.native_evidence_fingerprint(manifest)["row_count"] == 1
    assert [(row["stock_code"], row["classification"]) for row in manifest["entities"]] == [
        ("002717", "NO_TRADE"), ("600000", "TRADED")]
    altered = deepcopy(manifest)
    altered["entities"][0]["evidence"]["response"]["rows"][0]["is_suspended"] = 0
    with pytest.raises(ValueError):
        coverage.validate_manifest(altered, catalog=catalog, calendar=calendar)


@pytest.mark.parametrize("change", ["raw_hash", "decoded_row", "request", "worker", "omission"])
def test_historical_suspension_rejects_transport_and_security_drift(change):
    response = synthetic_response()
    if change == "raw_hash": response["raw_stdout_sha256"] = "0" * 64
    elif change == "decoded_row": response["rows"][0]["is_suspended"] = 0
    elif change == "request": response["canonical_request_json"] = "{}"
    elif change == "worker": response["worker_sha256"] = "unknown"
    elif change == "omission": response["rows"] = []
    with pytest.raises(ValueError):
        build_suspension_evidence(response, codes=["002717"], day=DAY, cutoff=CUTOFF)


def test_historical_suspension_rejects_normal_trading_wrong_date_and_future_capture():
    response = synthetic_response()
    response["rows"][0]["is_suspended"] = 0
    response["raw_stdout"] = json.dumps({k: v for k, v in response.items() if k not in {
        "raw_stdout", "raw_stdout_sha256", "canonical_request_json", "canonical_request_sha256", "worker_sha256"}})
    response["raw_stdout_sha256"] = sha256(response["raw_stdout"].encode()).hexdigest()
    with pytest.raises(ValueError, match="not confirmed"):
        build_suspension_evidence(response, codes=["002717"], day=DAY, cutoff=CUTOFF)
    with pytest.raises(ValueError):
        build_suspension_evidence(synthetic_response(), codes=["002717"], day="2026-11-24", cutoff=CUTOFF)
    with pytest.raises(ValueError, match="knowledge cutoff"):
        build_suspension_evidence(synthetic_response(), codes=["002717"], day=DAY,
                                  cutoff=datetime.fromisoformat("2026-11-25T02:49:02+08:00"))


def test_native_archive_producer_reuses_durable_suspension_capture(monkeypatch):
    import datetime as dt
    from sqlalchemy import create_engine, text
    from integrations.myquant import bridge
    from server.db.formal_analysis_schema import privileged_migrate_formal_analysis_schema
    from tools import import_qmt_datadir_daily as publisher

    engine = create_engine("sqlite+pysqlite:///:memory:")
    privileged_migrate_formal_analysis_schema(engine)
    catalog, calendar = authorities()
    monkeypatch.setattr(publisher, "load_stock_catalog", lambda *a, **kw: catalog)
    monkeypatch.setattr(publisher, "load_trade_calendar_receipt", lambda *a, **kw: calendar)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            value = CUTOFF + dt.timedelta(seconds=20)
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)

    monkeypatch.setattr(publisher, "dt", SimpleNamespace(datetime=Clock, date=dt.date, timedelta=dt.timedelta))
    calls = []

    def capture(symbols, **kwargs):
        calls.append(symbols)
        return synthetic_response()

    monkeypatch.setattr(bridge, "upper_limit_history_evidence", capture)
    row = traded_bar()
    columns = ("stock_code", "trade_time", "trade_date", "open", "close", "high", "low",
               "volume", "amount", "pre_close", "change", "change_pct")
    harvested = {"rows": [tuple(row.get(k) for k in columns)], "codes": {"600000"}, "digest": "d" * 64}
    for uid in ("first", "resumed"):
        manifest = publisher._coverage_manifest(engine, session=dt.date.fromisoformat(DAY),
            harvested=harvested, run_id=uid, archive_native_evidence=True)
        assert coverage.validate_manifest(manifest, catalog=catalog, calendar=calendar) == manifest
        assert coverage.native_evidence_fingerprint(manifest)["row_count"] == 1
    assert calls == [["SZSE.002717"]]
    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM st_formal_analysis_checkpoint")).scalar() == 1
    engine.dispose()
