from __future__ import annotations

from datetime import timedelta
import gzip
import json
from pathlib import Path

import pytest

from server.common.qmt_history_coverage import QmtHistoryCoverageError
from server.common.qmt_minute_checkpoint import (
    MinuteCheckpointInvalid,
    frame_from_payload,
)
from test_qmt_minute_checkpoint import DAY, NOW, PROVIDER, publisher


def _three_batches(p):
    p.codes.extend(f"{code:06}" for code in range(11, 16))
    p.backend.fail = False
    return [p.codes[index:index + 5] for index in range(0, 15, 5)]


def _native_gaps(p, monkeypatch, *, codes, missing_kind):
    """Return real fixture receipts with rows absent, never a forged suspension."""
    state = {"missing": set(codes)}
    original_minute = p.backend.fetch_minute
    original_daily = p.backend.fetch_kline

    def minute(batch, *args, **kwargs):
        frame = original_minute(batch, *args, **kwargs)
        if missing_kind in {"minute", "both"}:
            frame = frame.loc[~frame["stock_code"].isin(state["missing"])].copy()
        return frame

    def daily(batch, *args, **kwargs):
        frame = original_daily(batch, *args, **kwargs)
        if missing_kind in {"daily", "both"}:
            frame = frame.loc[~frame["stock_code"].isin(state["missing"])].copy()
        return frame

    monkeypatch.setattr(p.backend, "fetch_minute", minute)
    monkeypatch.setattr(p.backend, "fetch_kline", daily)
    return state


def _records(p, prefix):
    result = []
    for path in (p.root / "qmt-minute-checkpoints").glob("*/*.json.gz"):
        record = json.loads(gzip.decompress(path.read_bytes()))
        if record["key"].startswith(prefix):
            result.append(record["payload"])
    return result


def _all_native_calls(batches):
    return [(kind, batch) for batch in batches for kind in ("minute", "daily")]


@pytest.mark.parametrize("bad_batch", [0, 1], ids=["first-batch", "middle-batch"])
@pytest.mark.parametrize("missing_kind", ["both", "daily", "minute"])
def test_missing_native_rows_do_not_block_later_batches_and_only_gaps_are_retried(
    publisher, monkeypatch, bad_batch, missing_kind,
):
    p = publisher
    _three_batches(p)
    # Reproduce the production missing symbol without assuming it is suspended.
    p.codes[bad_batch * 5 + 2] = "301686"
    batches = [p.codes[index:index + 5] for index in range(0, 15, 5)]
    state = _native_gaps(p, monkeypatch, codes={"301686"}, missing_kind=missing_kind)
    coverage_writes = []

    def insert_coverage(*args):
        coverage_writes.append(args)
        return {"inserted": True}

    monkeypatch.setattr(p.sync, "insert_coverage_bundle", insert_coverage)

    with pytest.raises(QmtHistoryCoverageError, match="not exact"):
        p.run()

    assert p.backend.calls == _all_native_calls(batches)
    assert p.pauses == [2.0, 2.0]
    assert not coverage_writes and not p.publications and not p.receipts
    exact = _records(p, "batch-")
    assert {tuple(item["codes"]) for item in exact} == {
        tuple(batch) for number, batch in enumerate(batches) if number != bad_batch
    }
    assert any(item["codes"] == batches[-1] for item in exact)
    pending = _records(p, "pending-")
    assert len(pending) == 1
    saved = pending[0]
    assert saved["codes"] == batches[bad_batch]
    assert saved["coverage"]["manifest"]["status"] == "INCOMPLETE"
    assert saved["coverage"]["manifest"]["strategy_eligible"] is False
    assert saved["coverage"]["manifest"]["captured_at"] == NOW.isoformat()
    raw_minute = frame_from_payload(saved["minute"])
    raw_daily = frame_from_payload(saved["daily"])
    assert len(raw_minute) == (4 if missing_kind in {"minute", "both"} else 5) * 241
    assert len(raw_daily) == (4 if missing_kind in {"daily", "both"} else 5)
    assert set(raw_minute["data_source"]) == {PROVIDER}
    assert set(raw_minute["received_at"]) == {"2026-09-18 18:01:02"}
    assert all(value.startswith(DAY + " ") for value in raw_minute["source_time"])
    native_receipt = raw_minute.attrs["bigqmt_capture"]["batch_receipts"][0]
    assert native_receipt["request_id"] == f"original-minute-{batches[bad_batch][0]}"
    assert native_receipt["requested_codes"] == batches[bad_batch]
    assert native_receipt["generated_at"] == "2026-09-18 18:01:02"
    assert saved["source_receipts"]

    first_run_id = p.staged[0]["batch_id"].iloc[0]
    state["missing"].clear()
    p.backend.calls.clear()
    p.clock.current += timedelta(minutes=15)
    p.run()

    assert p.backend.calls == _all_native_calls([batches[bad_batch]])
    assert len(coverage_writes) == len(p.publications) == 1
    assert len(p.publications[0]) == 15 * 241
    assert set(p.publications[0]["batch_id"]) == {first_run_id}
    assert [item["quality_status"] for item in p.receipts] == ["PUBLISHING", "PASS"]
    assert p.receipts[-1]["forward_eligible"] is False
    assert p.receipts[-1]["capture_mode"] == "AFTER_CLOSE_BACKFILL"
    assert len(p.receipts[-1]["evidence"]["acquisition_checkpoint"]["batch_hashes"]) == 3
    assert not _records(p, "batch-")
    assert not _records(p, "pending-")
    completed = _records(p, "completed")[0]
    pending_summary = completed["pending_evidence"]
    assert pending_summary["validation_status"] == "NOT_PUBLICATION_AUTHORITY"
    assert pending_summary["batch_count"] == pending_summary["response_count"] == 1
    retained = pending_summary["records"][0]["native_capture_receipts"]["minute"]
    assert retained["batch_receipts"][0] == native_receipt


def test_consecutive_incomplete_batches_still_pause_and_reach_final_batch(publisher, monkeypatch):
    p = publisher
    batches = _three_batches(p)
    _native_gaps(p, monkeypatch, codes={batches[0][2], batches[1][2]}, missing_kind="both")

    with pytest.raises(QmtHistoryCoverageError, match="not exact"):
        p.run()

    assert p.backend.calls == _all_native_calls(batches)
    assert p.pauses == [2.0, 2.0]
    assert {tuple(item["codes"]) for item in _records(p, "pending-")} == {
        tuple(batches[0]), tuple(batches[1]),
    }
    assert [item["codes"] for item in _records(p, "batch-")] == [batches[-1]]
    assert not p.publications and not p.receipts


def test_incomplete_response_persistence_failure_stops_before_next_native_batch(
    publisher, monkeypatch,
):
    from server.common import qmt_minute_checkpoint as checkpoint_module

    p = publisher
    batches = _three_batches(p)
    _native_gaps(p, monkeypatch, codes={batches[0][2]}, missing_kind="both")
    replace = checkpoint_module.os.replace

    def disk_full(source, target):
        if Path(target).name.startswith("pending-"):
            raise OSError("pending response disk is full")
        return replace(source, target)

    monkeypatch.setattr(checkpoint_module.os, "replace", disk_full)
    with pytest.raises(MinuteCheckpointInvalid, match="I/O failed"):
        p.run()

    assert p.backend.calls == _all_native_calls([batches[0]])
    assert not p.publications and not p.receipts
    assert not _records(p, "pending-") and not _records(p, "batch-")


@pytest.mark.parametrize("invalid", ["source", "coverage_hash"])
def test_invalid_provenance_or_manifest_is_not_treated_as_an_ordinary_gap(
    publisher, monkeypatch, invalid,
):
    p = publisher
    batches = _three_batches(p)
    _native_gaps(p, monkeypatch, codes={batches[0][2]}, missing_kind="both")
    if invalid == "source":
        fetch = p.backend.fetch_minute

        def wrong_source(*args, **kwargs):
            frame = fetch(*args, **kwargs)
            frame.attrs["bigqmt_capture"]["batch_receipts"][0]["source"] = "untrusted"
            return frame

        monkeypatch.setattr(p.backend, "fetch_minute", wrong_source)
    else:
        assess = p.sync.assess_minute_coverage

        def wrong_hash(*args, **kwargs):
            bundle = assess(*args, **kwargs)
            bundle["manifest"]["manifest_hash"] = "0" * 64
            return bundle

        monkeypatch.setattr(p.sync, "assess_minute_coverage", wrong_hash)

    with pytest.raises(QmtHistoryCoverageError):
        p.run()

    expected = [("minute", batches[0])] if invalid == "source" else _all_native_calls([batches[0]])
    assert p.backend.calls == expected
    assert not p.publications and not p.receipts
    assert not _records(p, "pending-") and not _records(p, "batch-")
