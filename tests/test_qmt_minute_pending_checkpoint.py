from __future__ import annotations

from copy import deepcopy
from datetime import timedelta
import gzip
import json

import pytest

from server.common import qmt_minute_checkpoint as checkpoint_module
from server.common.qmt_history_coverage import (
    QmtHistoryCoverageError, assess_minute_coverage, validate_coverage_bundle,
)
from server.common.qmt_minute_checkpoint import MinuteCheckpointInvalid, frame_from_payload
from test_qmt_minute_checkpoint import (
    DAY, NOW, PROVIDER, ROOTS, coverage, daily_frame, make_checkpoint, minute_frame,
)


def incomplete_response(request_id="native-request-1"):
    codes = ["000001"]
    minute = minute_frame(codes).iloc[:-1].copy()
    daily = daily_frame(codes)
    receipt = {"request_id": request_id, "generated_at": "2026-09-18 18:01:02",
               "requested_codes": codes, "source": PROVIDER}
    minute.attrs["bigqmt_capture"] = {"batch_receipts": [{**receipt, "action": "minute"}]}
    daily.attrs["bigqmt_capture"] = {**receipt, "action": "kline"}
    result = assess_minute_coverage(
        expected_codes=codes,
        minute_rows=minute.assign(batch_id="minute-run").to_dict("records"),
        daily_rows=daily.assign(batch_id="daily-run").to_dict("records"),
        trade_date=DAY, provider=PROVIDER, daily_provider=PROVIDER,
        run_id="minute-run", source_batch_id="minute-run", daily_source_batch_id="daily-run",
        captured_at=NOW, catalog_batch_id="catalog", catalog_manifest_hash="a" * 64,
        calendar_batch_id="calendar", calendar_manifest_hash="b" * 64,
    )
    assert validate_coverage_bundle(result)["status"] == "INCOMPLETE"
    return {"minute": minute, "daily": daily, "coverage": result,
            "source_receipts": [receipt]}


def test_pending_keeps_original_data_without_becoming_exact_or_reusable(tmp_path):
    cp = make_checkpoint(tmp_path)
    response = incomplete_response()
    index = cp.save_pending_batch(["000001"], **response)
    assert cp.load_batch(["000001"]) is None
    assert cp.evidence()["batch_hashes"] == []
    assert index["validation_status"] == "INCOMPLETE"
    path = next(cp.root.glob("pending-*.json.gz"))
    saved = json.loads(gzip.decompress(path.read_bytes()))["payload"]
    original = frame_from_payload(saved["minute"])
    assert len(original) == 240
    assert original["received_at"].tolist() == response["minute"]["received_at"].tolist()
    assert original["source_time"].tolist() == response["minute"]["source_time"].tolist()
    assert original.attrs == response["minute"].attrs
    assert saved["source_receipts"] == response["source_receipts"]
    cp.close()
    resumed = make_checkpoint(tmp_path, at=NOW + timedelta(minutes=15), run="unused-new-run")
    pending = resumed.pending_evidence()
    assert pending["validation_status"] == "NOT_PUBLICATION_AUTHORITY"
    assert pending["response_count"] == pending["batch_count"] == 1
    assert pending["records"][0]["sha256"] == index["sha256"]
    assert resumed.frozen["minute_run_id"] == "minute-run"
    assert resumed.load_batch(["000001"]) is None
    resumed.close()


def test_repeated_response_is_idempotent_and_new_receipt_is_retained(tmp_path):
    cp = make_checkpoint(tmp_path)
    response = incomplete_response()
    first = cp.save_pending_batch(["000001"], **response)
    assert cp.save_pending_batch(["000001"], **response) == first
    second = cp.save_pending_batch(["000001"], **incomplete_response("native-request-2"))
    assert second["sha256"] != first["sha256"]
    assert len(list(cp.root.glob("pending-*.json.gz"))) == 2
    assert cp.pending_evidence()["batch_count"] == 1
    cp.close()


def test_pending_rejects_exact_invalid_coverage_and_wrong_capture_binding(tmp_path):
    cp = make_checkpoint(tmp_path)
    response = incomplete_response()
    with pytest.raises(MinuteCheckpointInvalid, match="coverage identity"):
        cp.save_pending_batch(["000001"], **{**response, "coverage": coverage(["000001"])})
    with pytest.raises(MinuteCheckpointInvalid, match="coverage identity"):
        cp.save_pending_batch(["000002"], **response)
    malformed = deepcopy(response["coverage"])
    malformed["manifest"]["bar_count"] += 1
    with pytest.raises(QmtHistoryCoverageError):
        cp.save_pending_batch(["000001"], **{**response, "coverage": malformed})
    assert not list(cp.root.glob("pending-*.json.gz"))
    cp.close()


def test_exact_cache_remains_reusable_and_pass_retains_pending_receipts(tmp_path):
    cp = make_checkpoint(tmp_path)
    pending = cp.save_pending_batch(["000001"], **incomplete_response())
    cp.save_batch(["000001"], minute=minute_frame(["000001"]), daily=daily_frame(["000001"]),
                  coverage=coverage(["000001"]), source_receipts=[{"request_id": "exact-native"}])
    cp.close()
    resumed = make_checkpoint(tmp_path)
    cached = resumed.load_batch(["000001"])
    assert cached["coverage"]["manifest"]["status"] == "EXACT"
    resumed.verify_replayed_batch(cached, coverage=coverage(["000001"]),
                                   source_receipts=[{"request_id": "exact-native"}])
    with pytest.raises(MinuteCheckpointInvalid):
        resumed.complete({"publication_state": "PUBLISHING"})
    assert list(resumed.root.glob("pending-*.json.gz"))
    resumed.complete({"publication_state": "PASS"})
    completed = json.loads(gzip.decompress((resumed.root / "completed.json.gz").read_bytes()))["payload"]
    item = completed["pending_evidence"]["records"][0]
    assert item["sha256"] == pending["sha256"]
    assert item["source_receipts"][0]["request_id"] == "native-request-1"
    assert item["native_capture_receipts"]["minute"]["batch_receipts"][0]["action"] == "minute"
    assert item["coverage_manifest"]["status"] == "INCOMPLETE"
    assert not list(resumed.root.glob("pending-*.json.gz"))
    assert not list(resumed.root.glob("batch-*.json.gz"))
    resumed.close()


def test_pending_budget_spans_capture_scopes_and_dedup_needs_no_more_space(tmp_path, monkeypatch):
    cp = make_checkpoint(tmp_path)
    response = incomplete_response()
    cp.save_pending_batch(["000001"], **response)
    stored_bytes = next(cp.root.glob("pending-*.json.gz")).stat().st_size
    monkeypatch.setattr(checkpoint_module, "MAX_PENDING_BYTES", stored_bytes)
    cp.save_pending_batch(["000001"], **response)
    with pytest.raises(MinuteCheckpointInvalid, match="disk budget"):
        cp.save_pending_batch(["000001"], **incomplete_response("another-request"))
    other = make_checkpoint(tmp_path, roots={**ROOTS, "release_build_sha": "b" * 40})
    with pytest.raises(MinuteCheckpointInvalid, match="disk budget"):
        other.save_pending_batch(["000001"], **response)
    assert len(list(tmp_path.glob("*/pending-*.json.gz"))) == 1
    other.close()
    cp.close()


def test_corrupt_pending_fails_closed_on_resume(tmp_path):
    cp = make_checkpoint(tmp_path)
    cp.save_pending_batch(["000001"], **incomplete_response())
    path = next(cp.root.glob("pending-*.json.gz"))
    cp.close()
    record = json.loads(gzip.decompress(path.read_bytes()))
    record["payload"]["minute"]["rows"][0]["price"] = 10000
    path.write_bytes(gzip.compress(json.dumps(record).encode()))
    with pytest.raises(MinuteCheckpointInvalid, match="invalid durable"):
        make_checkpoint(tmp_path)


def test_pending_disk_error_preserves_exact_data_and_is_typed(tmp_path, monkeypatch):
    cp = make_checkpoint(tmp_path)
    cp.save_batch(["000001"], minute=minute_frame(["000001"]), daily=daily_frame(["000001"]),
                  coverage=coverage(["000001"]), source_receipts=[])
    with monkeypatch.context() as patch:
        def disk_full(*args):
            raise OSError("disk full")
        patch.setattr(checkpoint_module.os, "replace", disk_full)
        with pytest.raises(MinuteCheckpointInvalid, match="I/O failed"):
            cp.save_pending_batch(["000001"], **incomplete_response())
    assert cp.load_batch(["000001"]) is not None
    assert not list(cp.root.glob("pending-*.json.gz"))
    assert not list(cp.root.glob(".writing-*"))
    cp.close()
