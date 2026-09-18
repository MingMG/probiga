"""Publication must reject old native acquisition envelopes before any writes."""

from datetime import datetime

import pytest

from integrations.bigqmt.bridge import SNAPSHOT_ACQUISITION_MODE, SNAPSHOT_ACQUISITION_PROTOCOL
from integrations.bigqmt.spool import PROVIDER_ID
from tools import run_big_qmt_bridge as consumer


def _payload():
    now = datetime.now()
    return {
        "source": PROVIDER_ID,
        "quote_acquisition_protocol": SNAPSHOT_ACQUISITION_PROTOCOL,
        "quote_acquisition_mode": SNAPSHOT_ACQUISITION_MODE,
        "generated_ts": now.timestamp(),
        "quotes": {"000001.SZ": {
            "lastPrice": 10.5, "lastClose": 10, "volume": 100, "amount": 105000,
            "time": int(now.timestamp() * 1000),
            "_probiga_observed_at": now.isoformat(),
            "_probiga_acquisition_method": "ContextInfo.get_full_tick",
        }},
    }


@pytest.mark.parametrize("invalid_kind", ["full", "tracked"])
@pytest.mark.parametrize("fault", ["old_callback", "wrong_source", "future_publication"])
def test_ingest_validates_both_envelopes_before_any_write(monkeypatch, tmp_path, invalid_kind, fault):
    payloads = {"full": _payload(), "tracked": _payload()}
    bad = payloads[invalid_kind]
    if fault == "old_callback":
        bad["quote_acquisition_mode"] = "whole_quote_cache"
    elif fault == "wrong_source":
        bad["source"] = "external_vendor"
    else:
        bad["generated_ts"] += 3600
    monkeypatch.setattr(consumer, "_snapshot_freshness_required", lambda _engine: True)
    monkeypatch.setattr(consumer, "read_json", lambda _path: {})
    monkeypatch.setattr(consumer, "_read_snapshot_if_changed", lambda kind, **kw: (payloads[kind], kind))
    for writer in ("_replace_full_snapshot", "_replace_tracked_subset", "_record_realtime_sync_receipt", "persist_quote_events"):
        monkeypatch.setattr(consumer, writer, lambda *args, **kw: pytest.fail("invalid envelope reached a writer"))

    with pytest.raises(RuntimeError, match="snapshot"):
        consumer.ingest_once(object(), qmt_home=tmp_path, universe=["000001"], tracked=["000001"], short_name_map={})


def _coverage_payload():
    payload = _payload()
    template = payload["quotes"]["000001.SZ"]
    payload["quotes"] = {f"{index:06d}.SZ": dict(template) for index in range(1, 11)}
    return payload


def _prepare_coverage_ingest(monkeypatch, payload):
    monkeypatch.setattr(consumer, "_snapshot_freshness_required", lambda _engine: True)
    monkeypatch.setattr(consumer, "read_json", lambda _path: {})
    monkeypatch.setattr(consumer, "_read_snapshot_if_changed", lambda kind, **kw:
                        (payload, "full-file") if kind == "full" else ({}, ""))
    monkeypatch.setattr(consumer, "_write_status", lambda *args, **kw: None)
    monkeypatch.setenv("BIG_QMT_MIN_FULL_COVERAGE", "0.95")
    monkeypatch.setenv("BIG_QMT_MAX_UNPRICED_RATIO", "0.10")


@pytest.mark.parametrize("fault", [
    "missing_observation", "wrong_method", "missing_native_time", "negative_volume",
    "missing_amount", "malformed_price", "negative_price", "nonfinite_price",
    "boolean_price", "unproven_zero_price", "stale_native_event",
])
def test_rejected_native_rows_remain_eligible_in_coverage(monkeypatch, tmp_path, fault):
    payload = _coverage_payload()
    bad = payload["quotes"]["000010.SZ"]
    if fault == "missing_observation":
        del bad["_probiga_observed_at"]
    elif fault == "wrong_method":
        bad["_probiga_acquisition_method"] = "callback"
    elif fault == "missing_native_time":
        del bad["time"]
    elif fault == "stale_native_event":
        bad["time"] -= 121_000
    elif fault == "negative_volume":
        bad["volume"] = -1
    elif fault == "missing_amount":
        del bad["amount"]
    elif fault == "unproven_zero_price":
        bad["lastPrice"] = 0
        del bad["_probiga_observed_at"]
    else:
        bad["lastPrice"] = {
            "malformed_price": "invalid", "negative_price": -1,
            "nonfinite_price": float("nan"), "boolean_price": False,
        }[fault]
    _prepare_coverage_ingest(monkeypatch, payload)
    for writer in ("_replace_full_snapshot", "_replace_tracked_subset", "_record_realtime_sync_receipt"):
        monkeypatch.setattr(consumer, writer, lambda *args, **kw: pytest.fail("invalid coverage reached a writer"))

    with pytest.raises(consumer.BigQmtDataQualityError) as caught:
        consumer.ingest_once(object(), qmt_home=tmp_path,
                            universe=[f"{index:06d}" for index in range(1, 11)],
                            tracked=[], short_name_map={})
    details = caught.value.details
    assert details["full_expected_eligible"] == 10
    assert details["full_unpriced_count"] == 0
    assert details["full_invalid_count"] == 1
    assert details["full_invalid_sample"] == ["000010"]
    assert details["full_coverage"] == 0.9


def test_observed_native_zero_price_retains_unpriced_allowance(monkeypatch, tmp_path):
    payload = _coverage_payload()
    payload["quotes"]["000010.SZ"]["lastPrice"] = 0
    _prepare_coverage_ingest(monkeypatch, payload)
    monkeypatch.setattr(consumer, "_replace_full_snapshot", lambda _engine, frame: len(frame))
    receipts = []
    monkeypatch.setattr(consumer, "_record_realtime_sync_receipt",
                        lambda _engine, **kw: receipts.append(kw) or {"quality_status": "PASS"})

    result = consumer.ingest_once(object(), qmt_home=tmp_path,
                                 universe=[f"{index:06d}" for index in range(1, 11)],
                                 tracked=[], short_name_map={})
    assert result["full_expected_eligible"] == 9
    assert result["full_unpriced_count"] == 1
    assert result["full_invalid_count"] == 0
    assert result["full_coverage"] == 1.0
    assert result["full_rows"] == 9
    assert receipts[0]["expected_count"] == receipts[0]["observed_count"] == 9


def test_fresh_tracked_envelope_does_not_republish_stale_native_event(monkeypatch, tmp_path):
    payload = _payload()
    payload["quotes"]["000001.SZ"]["time"] -= 121_000
    monkeypatch.setattr(consumer, "_snapshot_freshness_required", lambda _engine: True)
    monkeypatch.setattr(consumer, "read_json", lambda _path: {})
    monkeypatch.setattr(consumer, "_read_snapshot_if_changed", lambda kind, **kw:
                        (payload, "tracked-file") if kind == "tracked" else ({}, ""))
    monkeypatch.setattr(consumer, "_table_exists", lambda *args: False)
    monkeypatch.setattr(consumer, "_write_status", lambda *args, **kw: None)
    def publish(_engine, frame):
        assert frame.empty
        return 0
    monkeypatch.setattr(consumer, "_replace_tracked_subset", publish)
    result = consumer.ingest_once(object(), qmt_home=tmp_path, universe=["000001"],
                                 tracked=["000001"], short_name_map={})
    assert result["tracked_rows"] == 0
