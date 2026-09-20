from __future__ import annotations

from datetime import timedelta
import gzip
import json
import os

import pytest

from server.common.qmt_history_coverage import QmtHistoryCoverageError, require_exact_coverage
from server.common.qmt_minute_checkpoint import MinuteCheckpointInvalid, digest, frame_from_payload
from test_qmt_minute_checkpoint import NOW, coverage, daily_frame, make_checkpoint, minute_frame
from test_qmt_minute_pending_checkpoint import incomplete_response


def test_pending_replay_retains_native_identity_and_stays_incomplete(tmp_path):
    checkpoint = make_checkpoint(tmp_path)
    response = incomplete_response()
    checkpoint.save_pending_batch(["000001"], **response)
    checkpoint.close()

    resumed = make_checkpoint(tmp_path, at=NOW + timedelta(minutes=10), run="unused-new-run")
    pending = resumed.load_pending_batch(["000001"])
    assert pending["validation_status"] == "INCOMPLETE"
    assert frame_from_payload(pending["minute"]).attrs == response["minute"].attrs
    assert frame_from_payload(pending["daily"]).attrs == response["daily"].attrs
    assert pending["source_receipts"] == response["source_receipts"]
    assert pending["coverage"] == response["coverage"]
    assert resumed.load_batch(["000001"]) is None
    assert resumed.evidence()["batch_hashes"] == []
    with pytest.raises(QmtHistoryCoverageError):
        require_exact_coverage(pending["coverage"])
    with pytest.raises(MinuteCheckpointInvalid):
        resumed.complete({"publication_state": "PASS"})
    resumed.close()


def test_latest_pending_response_is_returned_without_combining_receipts(tmp_path):
    checkpoint = make_checkpoint(tmp_path)
    first = checkpoint.save_pending_batch(["000001"], **incomplete_response("first"))
    second = checkpoint.save_pending_batch(["000001"], **incomplete_response("second"))
    os.utime(checkpoint._path(first["key"]), ns=(100_000_000, 100_000_000))
    os.utime(checkpoint._path(second["key"]), ns=(200_000_000, 200_000_000))
    result = checkpoint.load_pending_batch(["000001"])
    assert result["source_receipts"][0]["request_id"] == "second"
    assert len(result["source_receipts"]) == 1
    assert checkpoint.pending_evidence()["response_count"] == 2
    assert checkpoint.load_pending_batch(["000002"]) is None
    checkpoint.close()


@pytest.mark.parametrize("change", ["hash", "schema", "frozen", "native_identity"])
def test_any_corrupt_pending_candidate_is_rejected_even_when_newer_exists(tmp_path, change):
    checkpoint = make_checkpoint(tmp_path)
    first = checkpoint.save_pending_batch(["000001"], **incomplete_response("first"))
    checkpoint.save_pending_batch(["000001"], **incomplete_response("second"))
    path = checkpoint._path(first["key"])
    record = json.loads(gzip.decompress(path.read_bytes()))
    if change == "hash":
        record["payload"]["minute"]["rows"][0]["price"] = 99999
    else:
        if change == "schema":
            record["schema"] = "unknown"
        elif change == "frozen":
            record["payload"]["manifest_hash"] = "0" * 64
        else:
            record["payload"]["minute"]["attrs"]["bigqmt_capture"]["batch_receipts"][0]["request_id"] = "forged"
        record["sha256"] = digest({key: value for key, value in record.items() if key != "sha256"})
    path.write_bytes(gzip.compress(json.dumps(record).encode()))
    with pytest.raises(MinuteCheckpointInvalid):
        checkpoint.load_pending_batch(["000001"])
    checkpoint.close()


def test_acquisition_completion_is_separate_from_exact_publication(tmp_path):
    checkpoint = make_checkpoint(tmp_path)
    assert checkpoint.acquisition_complete([]) is False
    assert checkpoint.acquisition_complete([["000001"], ["000002"]]) is False
    checkpoint.save_pending_batch(["000001"], **incomplete_response())
    assert checkpoint.acquisition_complete([["000001"], ["000002"]]) is False
    checkpoint.save_batch(["000002"], minute=minute_frame(["000002"]),
                          daily=daily_frame(["000002"]), coverage=coverage(["000002"]),
                          source_receipts=[])
    assert checkpoint.acquisition_complete(iter([["000001"], ["000002"]])) is True
    assert checkpoint.load_batch(["000001"]) is None
    assert checkpoint.load_pending_batch(["000001"])["validation_status"] == "INCOMPLETE"
    checkpoint.close()


def test_acquisition_check_does_not_hide_corrupt_batch_after_missing_one(tmp_path):
    checkpoint = make_checkpoint(tmp_path)
    saved = checkpoint.save_pending_batch(["000001"], **incomplete_response())
    checkpoint._path(saved["key"]).write_bytes(b"truncated")
    with pytest.raises(MinuteCheckpointInvalid):
        checkpoint.acquisition_complete([["000002"], ["000001"]])
    checkpoint.close()
