from __future__ import annotations

from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime, timedelta
import gzip
import json
from types import SimpleNamespace

import pandas as pd
import pytest

from integrations.bigqmt.spool import BigQmtResourceBlocked
from server.common.qmt_history_coverage import (
    assess_minute_coverage, minute_time_grid, require_exact_coverage,
)
from server.common.qmt_minute_checkpoint import (
    MAX_AGE, MinuteCheckpoint, MinuteCheckpointInvalid, digest,
    frame_from_payload, stable_no_trade_identity,
)


DAY = "2026-09-01"
NOW = datetime(2026, 9, 18, 20, 0, 0)
PROVIDER = "gj_big_qmt_inner"
ROOTS = {"catalog_batch_id": "catalog", "catalog_manifest_hash": "a" * 64,
         "calendar_batch_id": "calendar", "calendar_manifest_hash": "b" * 64,
         "release_build_sha": "a" * 40, "trade_date": DAY}


def frozen(at=NOW, *, run="minute-run", roots=None):
    return {"minute_run_id": run, "daily_run_id": "daily-run",
            "coverage_captured_at": at.isoformat(), "reference_roots": roots or ROOTS,
            "native_no_trade_evidence": None}


def minute_frame(codes):
    return pd.DataFrame([
        {"stock_code": code, "trade_time": pd.Timestamp(f"{DAY} {value}"),
         "trade_date": DAY, "period": "1m", "price": 10.5 if value == "15:00:00" else 10.0, "avg_price": None,
         "volume": 100, "amount": 1000, "data_source": PROVIDER,
         "source_time": f"{DAY} {value}", "received_at": "2026-09-18 18:01:02"}
        for code in codes for value in minute_time_grid()
    ])


def daily_frame(codes):
    return pd.DataFrame([
        {"stock_code": code, "trade_date": DAY, "period": "1d", "k_type": 1,
         "adjust_type": 0, "open": 10, "high": 11, "low": 9, "close": 10.5,
         "volume": 24100, "amount": 241000, "pre_close": 10,
         "pre_close_origin": "NATIVE_QMT", "data_source": PROVIDER}
        for code in codes
    ])


def coverage(codes):
    minute, daily = minute_frame(codes), daily_frame(codes)
    minute["batch_id"], daily["batch_id"] = "minute-run", "daily-run"
    result = assess_minute_coverage(
        expected_codes=codes, daily_rows=daily.to_dict("records"),
        minute_rows=minute.to_dict("records"), trade_date=DAY, provider=PROVIDER,
        daily_provider=PROVIDER, run_id="minute-run", source_batch_id="minute-run",
        daily_source_batch_id="daily-run", captured_at=NOW,
        catalog_batch_id="catalog", catalog_manifest_hash="a" * 64,
        calendar_batch_id="calendar", calendar_manifest_hash="b" * 64,
    )
    require_exact_coverage(result)
    return result


def make_checkpoint(tmp_path, at=NOW, roots=None, run="minute-run"):
    return MinuteCheckpoint({"reference_roots": roots or ROOTS, "codes": ["000001"]},
                            frozen(at, roots=roots, run=run), root=tmp_path, now=at)


def test_checkpoint_freezes_identity_and_preserves_native_values(tmp_path):
    cp = make_checkpoint(tmp_path)
    minute = minute_frame(["000001"])
    minute.attrs = {"bigqmt_capture": {"generated_at": "2026-09-18 18:01:02",
                                       "request_id": "original-request"}}
    cp.save_batch(["000001"], minute=minute, daily=daily_frame(["000001"]),
                  coverage=coverage(["000001"]), source_receipts=[{"request_id": "original"}])
    cp.close()
    resumed = make_checkpoint(tmp_path, NOW + timedelta(minutes=5), run="new-run")
    assert resumed.frozen == frozen()
    saved = resumed.load_batch(["000001"])
    restored = frame_from_payload(saved["minute"])
    assert restored.attrs == minute.attrs
    assert restored["source_time"].tolist() == minute["source_time"].tolist()
    assert restored["received_at"].tolist() == minute["received_at"].tolist()
    assert restored["trade_time"].tolist() == minute["trade_time"].tolist()
    assert restored["avg_price"].isna().all()
    resumed.verify_replayed_batch(saved, coverage=coverage(["000001"]),
                                   source_receipts=[{"request_id": "original"}])
    assert len(resumed.evidence()["batch_hashes"]) == 1
    resumed.close()


@pytest.mark.parametrize("field,value", [
    ("release_build_sha", "b" * 40), ("catalog_manifest_hash", "c" * 64),
    ("calendar_manifest_hash", "d" * 64),
])
def test_changed_reference_identity_has_isolated_checkpoint(tmp_path, field, value):
    old = make_checkpoint(tmp_path)
    old_path = old.root
    old.close()
    roots = dict(ROOTS, **{field: value})
    fresh = make_checkpoint(tmp_path, roots=roots, run="different-run")
    assert fresh.root != old_path
    assert fresh.load_batch(["000001"]) is None
    assert fresh.frozen["minute_run_id"] == "different-run"
    fresh.close()


def test_expired_capture_is_preserved_and_isolated(tmp_path):
    cp = make_checkpoint(tmp_path)
    old_root = cp.root
    cp.close()
    later = NOW + MAX_AGE
    resumed = make_checkpoint(tmp_path, later, run="new-run")
    assert resumed.frozen == frozen(later, run="new-run")
    assert list(tmp_path.glob(old_root.name + ".expired-*/manifest.json.gz"))
    resumed.close()


def test_concurrent_owner_fails_closed_and_close_releases_lock(tmp_path):
    first = make_checkpoint(tmp_path)
    with pytest.raises(MinuteCheckpointInvalid, match="active owner"):
        make_checkpoint(tmp_path)
    first.close()
    second = make_checkpoint(tmp_path)
    second.close()


@pytest.mark.parametrize("change", ["hash", "schema", "scope", "frozen"])
def test_tampered_manifest_is_never_silently_replaced(tmp_path, change):
    cp = make_checkpoint(tmp_path)
    path = cp.root / "manifest.json.gz"
    cp.close()
    record = json.loads(gzip.decompress(path.read_bytes()))
    if change == "hash":
        record["payload"]["frozen"]["minute_run_id"] = "changed"
    else:
        if change == "schema":
            record["schema"] = "unknown"
        elif change == "scope":
            record["scope"]["reference_roots"]["release_build_sha"] = "bad"
        else:
            record["payload"]["frozen"]["reference_roots"] = {}
        record["sha256"] = digest({k: v for k, v in record.items() if k != "sha256"})
    path.write_bytes(gzip.compress(json.dumps(record).encode()))
    with pytest.raises(MinuteCheckpointInvalid):
        make_checkpoint(tmp_path)


def test_no_trade_identity_retains_source_roots_but_freezes_read_cutoff():
    proof = {"daily_truth": {"source_batch_id": "source", "decision_known_at": "first",
                             "truth_hash": "hash", "row_hash": "immutable"},
             "no_row_contract": {"proof_sha256": "proof"}}
    later = deepcopy(proof)
    later["daily_truth"].update(decision_known_at="later", truth_hash="other")
    assert stable_no_trade_identity(proof) == stable_no_trade_identity(later)
    later["daily_truth"]["row_hash"] = "new"
    assert stable_no_trade_identity(proof) != stable_no_trade_identity(later)


@pytest.mark.parametrize("content", [b"truncated", gzip.compress(b"null"),
                                     gzip.compress(b"[]"), gzip.compress(b"42")])
def test_corrupt_compressed_record_has_typed_data_block(tmp_path, content):
    cp = make_checkpoint(tmp_path)
    path = cp.root / "manifest.json.gz"
    cp.close()
    if content == b"truncated":
        content = path.read_bytes()[:-5]
    path.write_bytes(content)
    with pytest.raises(MinuteCheckpointInvalid):
        make_checkpoint(tmp_path)


def test_disk_full_is_local_data_block_and_never_a_login_error(tmp_path, monkeypatch):
    from server.common import qmt_minute_checkpoint as checkpoint_module
    cp = make_checkpoint(tmp_path)

    def disk_full(*args):
        raise OSError("disk full")

    with monkeypatch.context() as patch:
        patch.setattr(checkpoint_module.os, "replace", disk_full)
        with pytest.raises(MinuteCheckpointInvalid, match="I/O failed") as error:
            cp.save_batch(["000001"], minute=minute_frame(["000001"]),
                          daily=daily_frame(["000001"]), coverage=coverage(["000001"]),
                          source_receipts=[])
        assert isinstance(error.value.__cause__, OSError)
    assert not list(cp.root.glob("batch-*.json.gz"))
    cp.close()


@pytest.fixture
def publisher(tmp_path, monkeypatch):
    from biz.stock_market import sync_stock_market as sync
    from server.common import qmt_stock_catalog, qmt_trade_calendar

    class Clock(datetime):
        current = NOW

        @classmethod
        def now(cls, tz=None):
            return cls.current

    codes = [f"{code:06}" for code in range(1, 11)]
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(tmp_path))
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge")
    monkeypatch.setenv("PROBIGA_SCHEDULER_BUILD_SHA", "a" * 40)
    monkeypatch.setenv("QMT_PRODUCTION_MINUTE_BATCH_SIZE", "5")
    monkeypatch.setenv("QMT_PRODUCTION_MINUTE_PAUSE_SECONDS", "0")
    monkeypatch.setenv("QMT_MINUTE_COUNT", "0")
    monkeypatch.setattr(sync, "datetime", Clock)
    monkeypatch.setattr(sync, "_default_myquant_minute_date", lambda _: DAY)
    proof = {field: "identity-" + field for field in sync._BIGQMT_IDENTITY_FIELDS}
    proof["strategy_identity_frozen"] = True
    monkeypatch.setattr(sync, "_formal_bigqmt_release_proof", lambda: proof)
    catalog = SimpleNamespace(batch_id="catalog", manifest_hash="a" * 64,
                              member_set_hash="c" * 64)
    calendar = SimpleNamespace(batch_id="calendar", manifest_hash="b" * 64,
                               source_batch_id="calendar-source", sessions_between=lambda *_: [DAY])
    monkeypatch.setattr(qmt_stock_catalog, "load_target_stock_catalog", lambda *a, **k: (catalog, codes))
    monkeypatch.setattr(qmt_trade_calendar, "load_trade_calendar_receipt", lambda *a, **k: calendar)
    monkeypatch.setattr(sync, "load_minute_native_no_trade_evidence", lambda *a, **k: None)
    monkeypatch.setattr(sync, "load_minute_daily_finality_evidence", lambda *a, **k: None)
    engine = SimpleNamespace(connect=lambda: nullcontext(None), begin=lambda: nullcontext(None))
    monkeypatch.setattr(sync, "get_kline_engine", lambda: engine)
    staged, publications, receipts, pauses = [], [], [], []

    def create(*_):
        staged.clear()
        return engine

    def append(_connection, _table, frame):
        staged.append(frame.copy())
        return len(frame)

    def publish(*args, **kwargs):
        publications.append(pd.concat(staged))
        return len(publications[-1])

    monkeypatch.setattr(sync, "_create_qmt_minute_stage", create)
    monkeypatch.setattr(sync, "_append_qmt_minute_stage", append)
    monkeypatch.setattr(sync, "_drop_qmt_minute_stage", lambda *a: None)
    monkeypatch.setattr(sync, "_commit_qmt_minute_stage", publish)
    monkeypatch.setattr(sync, "insert_coverage_bundle", lambda *a: {"inserted": True})
    monkeypatch.setattr(sync, "mysql_named_lock", lambda *a, **k: nullcontext(None))
    monkeypatch.setattr(sync, "_record_qmt_minute_receipt", lambda *a, **k: receipts.append(k))
    monkeypatch.setattr(sync.time, "sleep", pauses.append)

    class Backend:
        name = "bigqmt"
        fail = True
        calls = []

        def capture(self, frame, codes, action):
            receipt = {**proof, "status": "ok", "source": PROVIDER,
                       "bridge_version": "bigqmt_inner_v2", "action": action,
                       "request_id": f"original-{action}-{codes[0]}",
                       "requested_codes": codes, "generated_at": "2026-09-18 18:01:02"}
            frame.attrs["bigqmt_capture"] = {"batch_receipts": [receipt]} if action == "minute" else receipt
            return frame

        def fetch_minute(self, codes, *args, **kwargs):
            self.calls.append(("minute", list(codes)))
            if codes[0] == "000006" and self.fail:
                raise BigQmtResourceBlocked("native capacity exhausted")
            return self.capture(minute_frame(codes), codes, "minute")

        def fetch_kline(self, codes, *args, **kwargs):
            self.calls.append(("daily", list(codes)))
            return self.capture(daily_frame(codes), codes, "kline")

    backend = Backend()
    return SimpleNamespace(sync=sync, backend=backend, engine=engine, codes=codes,
                           root=tmp_path, clock=Clock, staged=staged, publications=publications,
                           receipts=receipts, pauses=pauses,
                           run=lambda: sync._step_stock_minute_qmt(engine, backend, codes))


def test_restart_skips_native_fetch_and_publishes_only_complete_day(publisher):
    p = publisher
    with pytest.raises(BigQmtResourceBlocked):
        p.run()
    assert not p.publications and not p.receipts
    assert p.pauses == [0.25]
    assert len(list(p.root.glob("qmt-minute-checkpoints/*/batch-*.json.gz"))) == 1
    first_run = p.staged[0]["batch_id"].iloc[0]
    p.backend.calls.clear()
    p.backend.fail = False
    p.clock.current += timedelta(minutes=5)
    p.run()
    assert p.backend.calls == [("minute", p.codes[5:]), ("daily", p.codes[5:])]
    assert len(p.publications) == 1
    assert len(p.publications[0]) == 241 * 10
    assert set(p.publications[0]["batch_id"]) == {first_run}
    assert set(p.publications[0]["received_at"]) == {"2026-09-18 18:01:02"}
    assert [item["quality_status"] for item in p.receipts] == ["PUBLISHING", "PASS"]
    final = p.receipts[-1]
    assert final["forward_eligible"] is False
    assert final["capture_mode"] == "AFTER_CLOSE_BACKFILL"
    evidence = final["evidence"]
    assert len(evidence["acquisition_checkpoint"]["batch_hashes"]) == 2
    assert evidence["minute_coverage_manifest"]["captured_at"] == NOW.isoformat()
    assert not list(p.root.glob("qmt-minute-checkpoints/*/batch-*.json.gz"))
    completed_path = next(p.root.glob("qmt-minute-checkpoints/*/completed.json.gz"))
    completed = json.loads(gzip.decompress(completed_path.read_bytes()))["payload"]
    assert len(completed["native_capture_receipts"]) == 2
    first_capture = completed["native_capture_receipts"][0]["minute"]["batch_receipts"][0]
    assert first_capture["request_id"] == "original-minute-000001"
    assert first_capture["generated_at"] == "2026-09-18 18:01:02"

    # An explicit later refresh starts a new frozen run; compact completed
    # evidence remains, while prior successful native rows need no disk copy.
    p.clock.current += timedelta(minutes=5)
    p.backend.calls.clear()
    p.run()
    assert p.backend.calls == [("minute", p.codes[:5]), ("daily", p.codes[:5]),
                               ("minute", p.codes[5:]), ("daily", p.codes[5:])]
    assert set(p.publications[-1]["batch_id"]) != {first_run}
    assert len(list(p.root.glob("qmt-minute-checkpoints/*.completed-*/completed.json.gz"))) == 1


def test_corrupt_checkpoint_blocks_fetch_and_publication(publisher):
    p = publisher
    with pytest.raises(BigQmtResourceBlocked):
        p.run()
    path = next(p.root.glob("qmt-minute-checkpoints/*/batch-*.json.gz"))
    record = json.loads(gzip.decompress(path.read_bytes()))
    record["payload"]["minute"]["rows"][0]["price"] = 10000
    path.write_bytes(gzip.compress(json.dumps(record).encode()))
    p.backend.calls.clear()
    p.backend.fail = False
    with pytest.raises(MinuteCheckpointInvalid):
        p.run()
    assert p.backend.calls == []
    assert not p.publications and not p.receipts


def test_cached_native_identity_is_revalidated_even_with_valid_hash(publisher):
    p = publisher
    with pytest.raises(BigQmtResourceBlocked):
        p.run()
    path = next(p.root.glob("qmt-minute-checkpoints/*/batch-*.json.gz"))
    record = json.loads(gzip.decompress(path.read_bytes()))
    capture = record["payload"]["minute"]["attrs"]["bigqmt_capture"]
    capture["batch_receipts"][0]["strategy_build_sha"] = "wrong-source"
    record["sha256"] = digest({k: v for k, v in record.items() if k != "sha256"})
    path.write_bytes(gzip.compress(json.dumps(record).encode()))
    p.backend.calls.clear()
    with pytest.raises(RuntimeError, match="response release identity differs"):
        p.run()
    assert not p.backend.calls and not p.publications


def test_final_receipt_failure_keeps_all_batches_for_recovery(publisher, monkeypatch):
    p = publisher
    p.backend.fail = False
    record = p.sync._record_qmt_minute_receipt

    def fail_pass(*args, **kwargs):
        if kwargs["quality_status"] == "PASS":
            raise OSError("receipt storage unavailable")
        return record(*args, **kwargs)

    monkeypatch.setattr(p.sync, "_record_qmt_minute_receipt", fail_pass)
    with pytest.raises(OSError, match="receipt storage"):
        p.run()
    assert len(list(p.root.glob("qmt-minute-checkpoints/*/batch-*.json.gz"))) == 2
    assert not list(p.root.glob("qmt-minute-checkpoints/*/completed.json.gz"))
    p.backend.calls.clear()
    monkeypatch.setattr(p.sync, "_record_qmt_minute_receipt", record)
    p.run()
    assert not p.backend.calls
    assert p.receipts[-1]["quality_status"] == "PASS"
    assert not list(p.root.glob("qmt-minute-checkpoints/*/batch-*.json.gz"))


def test_direct_invocation_cannot_exceed_safe_native_batch_limit(publisher, monkeypatch):
    p = publisher
    p.codes.extend(f"{code:06}" for code in range(11, 46))
    p.backend.fail = False
    monkeypatch.setenv("QMT_PRODUCTION_MINUTE_BATCH_SIZE", "200")
    p.run()
    assert [len(codes) for kind, codes in p.backend.calls if kind == "minute"] == [5] * 9
    assert p.pauses == [0.25] * 8


def test_completed_captures_in_same_second_get_distinct_run_ids(publisher):
    p = publisher
    p.backend.fail = False
    p.run()
    first = p.publications[-1]["batch_id"].iloc[0]
    p.run()
    assert p.publications[-1]["batch_id"].iloc[0] != first


@pytest.mark.parametrize("count,captured_at", [
    (1, NOW), (0, datetime(2026, 9, 1, 14, 59)),
    (0, datetime(2026, 9, 1, 15, 4, 59)),
])
def test_bounded_or_unsettled_current_day_capture_never_reuses_checkpoint(publisher, monkeypatch, count, captured_at):
    p = publisher
    monkeypatch.setenv("QMT_MINUTE_COUNT", str(count))
    p.clock.current = captured_at
    if count == 0:
        def first_native_block(*args, **kwargs):
            raise BigQmtResourceBlocked("native capacity exhausted")
        monkeypatch.setattr(p.backend, "fetch_minute", first_native_block)
    with pytest.raises(BigQmtResourceBlocked):
        p.run()
    assert not list(p.root.glob("qmt-minute-checkpoints/*"))
    assert not p.publications and not p.receipts
    assert p.pauses == ([0.25] if count == 1 else [])


@pytest.mark.parametrize("captured_at", [datetime(2026, 9, 1, 15, 5), datetime(2026, 9, 1, 20)])
def test_same_day_postclose_rotation_resumes_completed_batches(publisher, captured_at):
    p = publisher
    p.clock.current = captured_at
    with pytest.raises(BigQmtResourceBlocked):
        p.run()
    assert len(list(p.root.glob("qmt-minute-checkpoints/*/pending-*.json.gz"))) == 1
    assert not p.publications
    p.backend.calls.clear()
    p.backend.fail = False
    p.clock.current += timedelta(minutes=5)
    from server.common.qmt_history_coverage import QmtHistoryCoverageError

    with pytest.raises(QmtHistoryCoverageError):
        p.run()
    assert p.backend.calls == [("minute", p.codes[5:]), ("daily", p.codes[5:])]
    assert len(list(p.root.glob("qmt-minute-checkpoints/*/pending-*.json.gz"))) == 2
    assert not p.publications
    assert not any(item["quality_status"] == "PASS" for item in p.receipts)


def test_malformed_checkpoint_cli_is_data_integrity_block(monkeypatch, capsys):
    from biz.stock_market import sync_stock_market as sync

    def invalid():
        raise MinuteCheckpointInvalid("tampered native batch")

    monkeypatch.setattr(sync, "main", invalid)
    assert sync._cli() == 3
    output = json.loads(capsys.readouterr().out)
    assert output["error_type"] == "MinuteCheckpointInvalid"
    assert output["status"] == "DATA_BLOCKED"
