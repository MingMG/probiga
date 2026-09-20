from __future__ import annotations

from datetime import datetime, timedelta
import gzip
import json

import pytest

from integrations.bigqmt.spool import BigQmtResourceBlocked
from server.common.qmt_history_coverage import QmtHistoryCoverageError
from server.common.qmt_minute_checkpoint import MinuteCheckpoint, MinuteCheckpointInvalid
from test_qmt_minute_checkpoint import publisher  # noqa: F401


def test_restart_finishes_unvisited_batches_then_retries_only_pending(publisher, monkeypatch):
    p = publisher
    original_fetch = p.backend.fetch_minute
    partial = True

    def fetch(codes, *args, **kwargs):
        frame = original_fetch(codes, *args, **kwargs)
        if partial and codes == p.codes[:5]:
            return frame.iloc[:-1].copy()
        return frame

    monkeypatch.setattr(p.backend, "fetch_minute", fetch)
    with pytest.raises(BigQmtResourceBlocked):
        p.run()
    paths = list(p.root.glob("qmt-minute-checkpoints/*/pending-*.json.gz"))
    assert len(paths) == 1
    pending = json.loads(gzip.decompress(paths[0].read_bytes()))["payload"]
    frozen_run = pending["coverage"]["manifest"]["run_id"]
    assert not p.receipts and not p.publications

    # The second run has untouched work, so it must retain the first raw
    # partial batch and get to the second batch despite the persistent gap.
    p.backend.calls.clear()
    p.backend.fail = False
    p.clock.current += timedelta(minutes=5)
    with pytest.raises(QmtHistoryCoverageError):
        p.run()
    assert p.backend.calls == [("minute", p.codes[5:]), ("daily", p.codes[5:])]
    assert len(list(p.root.glob("qmt-minute-checkpoints/*/batch-*.json.gz"))) == 1
    assert len(list(p.root.glob("qmt-minute-checkpoints/*/pending-*.json.gz"))) == 1
    assert not p.receipts and not p.publications

    # After the first sweep has visited everything, retry the partial batch.
    # The completed second batch must not be fetched again.
    partial = False
    p.backend.calls.clear()
    p.clock.current += timedelta(minutes=5)
    p.run()
    assert p.backend.calls == [("minute", p.codes[:5]), ("daily", p.codes[:5])]
    assert [item["quality_status"] for item in p.receipts] == ["PUBLISHING", "PASS"]
    assert len(p.publications) == 1
    assert len(p.publications[0]) == 241 * len(p.codes)
    assert set(p.publications[0]["batch_id"]) == {frozen_run}
    assert p.receipts[-1]["forward_eligible"] is False
    completed_path = next(p.root.glob("qmt-minute-checkpoints/*/completed.json.gz"))
    completed = json.loads(gzip.decompress(completed_path.read_bytes()))["payload"]
    assert len(completed["native_capture_receipts"]) == 2
    assert completed["pending_evidence"]["response_count"] == 1
    assert completed["pending_evidence"]["records"][0]["validation_status"] == "INCOMPLETE"


def test_acquisition_progress_failure_closes_owned_checkpoint(publisher, monkeypatch):
    owned = []

    def corrupt_progress(self, _batches):
        owned.append(self)
        raise MinuteCheckpointInvalid("damaged acquisition progress")

    monkeypatch.setattr(MinuteCheckpoint, "acquisition_complete", corrupt_progress)
    with pytest.raises(MinuteCheckpointInvalid, match="damaged acquisition"):
        publisher.run()
    assert len(owned) == 1
    assert owned[0]._lock_file.closed
    assert not publisher.backend.calls
    assert not publisher.receipts


def test_postclose_pending_uses_fresh_historical_capture_next_day(publisher):
    p = publisher
    p.backend.fail = False
    p.clock.current = datetime(2026, 9, 1, 20)
    with pytest.raises(QmtHistoryCoverageError):
        p.run()
    assert len(list(p.root.glob("qmt-minute-checkpoints/*/pending-*.json.gz"))) == 2
    assert not p.publications

    p.backend.calls.clear()
    p.clock.current = datetime(2026, 9, 2, 1)
    p.run()
    assert p.receipts[-1]["quality_status"] == "PASS"
    assert p.receipts[-1]["evidence"]["minute_coverage_manifest"]["captured_at"] == p.clock.current.isoformat()
    # Prior same-day raw evidence keeps its original time and remains pending;
    # it must never be retimed to impersonate a later completed source capture.
    pending_paths = list(p.root.glob("qmt-minute-checkpoints/*/pending-*.json.gz"))
    assert len(pending_paths) == 2
    for path in pending_paths:
        payload = json.loads(gzip.decompress(path.read_bytes()))["payload"]
        assert payload["coverage"]["manifest"]["captured_at"] == "2026-09-01T20:00:00"
