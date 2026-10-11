from datetime import datetime
import gzip
import json

import pandas as pd
import pytest
from sqlalchemy import text

from integrations.bigqmt import reference, reference_capture as capture
from tools import sync_guojin_qmt_reference_data as publisher
from tests.test_qmt_reference_bulk_publication import _sqlite_engine, _catalog_arguments, _calendar_arguments

BUILD = "a" * 40


@pytest.fixture
def proof(monkeypatch):
    def validate(payload, *, expected_build_sha, **_kwargs):
        return {"strategy_source_sha256": "b" * 64, "compatible_app_build_sha": expected_build_sha,
                "strategy_compatibility_status": "CONTENT_COMPATIBLE"}
    monkeypatch.setattr(reference, "validate_strategy_release_payload", validate)
    monkeypatch.setattr(capture, "validate_strategy_release_payload", validate)


class Source:
    model = "model-one"

    def __init__(self):
        self.reads = []

    def capabilities(self, **_kwargs):
        return {"model_instance_id": self.model}

    def instrument_details(self, codes, **_kwargs):
        self.reads.append(list(codes))
        return pd.DataFrame({"qmt_code": codes, "list_date": [None] * len(codes)})


def _session(source, build=BUILD):
    return reference.ReferenceReadSession(build, source_bridge=source)


def _evidence(session):
    return {"collector_build_sha": session.build_sha, "started_at": session.started_at.isoformat(),
            "captured_at": datetime.now().isoformat(), "identity": session.identity,
            "capabilities_before": session.initial_capabilities, "capabilities_after": session.last_capabilities}


def _request(codes):
    return {"method": "instrument_details", "args": [codes], "kwargs": {"timeout": 20}}


def test_process_failure_resumes_verified_partitions_and_app_sha_does_not_recapture(tmp_path, proof):
    source = Source()
    first = capture.ReferenceCaptureStore({"contract": "one"}, expected_build_sha=BUILD, root=tmp_path)
    with first.locked():
        session = _session(source)
        frame = first.read_partition(_request(["600000.SH"]), session)
        assert frame.to_dict("records") == [{"qmt_code": "600000.SH", "list_date": None}]
    # Process interruption before final seal. The native response is durable.
    resumed = capture.ReferenceCaptureStore({"contract": "one"}, expected_build_sha="c" * 40, root=tmp_path)
    with resumed.locked():
        session = _session(source, "c" * 40)
        first_frame = resumed.read_partition(_request(["600000.SH"]), session)
        second_frame = resumed.read_partition(_request(["000001.SZ"]), session)
        resumed.seal({"frame": pd.concat([first_frame, second_frame]), "batch_id": "original"}, _evidence(session))
    assert source.reads == [["600000.SH"], ["000001.SZ"]]
    # A publisher retry after DB failure consumes the sealed artifact, not QMT.
    retry = capture.ReferenceCaptureStore({"contract": "one"}, expected_build_sha="d" * 40, root=tmp_path)
    with retry.locked():
        sealed = retry.load_sealed()
    assert sealed["batch_id"] == "original"
    assert sealed["frame"]["qmt_code"].tolist() == ["600000.SH", "000001.SZ"]
    assert len(source.reads) == 2


def test_pending_capture_model_change_is_blocked_without_recapture(tmp_path, proof):
    source = Source()
    store = capture.ReferenceCaptureStore({}, expected_build_sha=BUILD, root=tmp_path)
    store.read_partition(_request(["600000.SH"]), _session(source))
    source.model = "model-two"
    with pytest.raises(RuntimeError, match="model/request differs"):
        store.read_partition(_request(["600000.SH"]), _session(source))
    assert len(source.reads) == 1


@pytest.mark.parametrize("damage", ["missing", "changed"])
def test_seal_cannot_survive_missing_or_changed_native_partition(tmp_path, proof, damage):
    source = Source()
    store = capture.ReferenceCaptureStore({}, expected_build_sha=BUILD, root=tmp_path)
    session = _session(source)
    value = store.read_partition(_request(["600000.SH"]), session)
    store.seal({"frame": value}, _evidence(session))
    path = next(store.root.glob("partition-*.json.gz"))
    if damage == "missing":
        path.unlink()
    else:
        payload = json.loads(gzip.decompress(path.read_bytes()))
        payload["value"]["rows"][0]["qmt_code"] = "fake"
        path.write_bytes(gzip.compress(json.dumps(payload).encode()))
    with pytest.raises(RuntimeError, match="DATA_BLOCKED"):
        store.load_sealed()


def test_capture_ownership_lock_prevents_duplicate_collector(tmp_path, proof):
    first = capture.ReferenceCaptureStore({}, expected_build_sha=BUILD, root=tmp_path)
    second = capture.ReferenceCaptureStore({}, expected_build_sha=BUILD, root=tmp_path)
    with first.locked():
        with pytest.raises(OSError):
            with second.locked():
                pytest.fail("duplicate owner entered")


def _snapshot():
    arguments = _catalog_arguments(2)
    arguments["captured_at"] = datetime.now().replace(microsecond=0).isoformat(sep=" ")
    catalog, members = publisher.build_catalog_manifest(**arguments)
    calendar_args = _calendar_arguments(2)
    calendar_args["batch_id"] = arguments["batch_id"]
    calendar_args["known_at"] = arguments["captured_at"]
    calendar, _ = publisher.build_calendar_manifest(**calendar_args)
    return {"batch_id": arguments["batch_id"], "catalog_manifest": catalog,
            "catalog_members": pd.DataFrame(members), "calendar_manifest": calendar,
            "calendar_source_id": calendar_args["source_batch_id"],
            "calendar": pd.DataFrame({"trade_date": calendar_args["sessions"]})}


def test_exact_sealed_pair_publication_can_be_replayed_after_commit_unknown(monkeypatch):
    engine = _sqlite_engine()
    snapshot = _snapshot()
    # Use empty ordinary tables: this test exercises the exact production
    # immutable publishing/readback path against a real transactional DB.
    for name in ("sector_df", "sector_members", "stock_details", "index_details", "all_details", "index_weight"):
        snapshot[name] = pd.DataFrame()
    snapshot.update(refresh_result={}, sector_datasets={}, index_symbols=[],
                    index_weight_coverage={}, calendar_error="", calendar_capture_evidence={})
    monkeypatch.setattr(publisher, "_safe_upsert_frame", lambda *_a, **_k: {})
    store = type("Budget", (), {"remaining": lambda *_a: 100, "progress": lambda *_a: None})()
    try:
        first = publisher._publish_reference_snapshot(engine, snapshot, BUILD,
                                    dry_run=False, catalog_only=True, store=store)
        monkeypatch.setattr(publisher, "insert_catalog_batch", lambda *_a, **_k: pytest.fail("reinserted"))
        second = publisher._publish_reference_snapshot(engine, snapshot, BUILD,
                                    dry_run=False, catalog_only=True, store=store)
        assert first["tables"] == second["tables"]
        with engine.connect() as connection:
            assert connection.execute(text("SELECT COUNT(*) FROM qmt_stock_catalog_member")).scalar_one() == 2
    finally:
        engine.dispose()


def test_one_sided_commit_is_fail_closed_not_repaired(monkeypatch):
    engine = _sqlite_engine()
    snapshot = _snapshot()
    with engine.begin() as connection:
        publisher.insert_catalog_batch(connection, batch_id=snapshot["batch_id"],
                captured_at=snapshot["catalog_manifest"]["captured_at"],
                history_complete_from=snapshot["catalog_manifest"]["history_complete_from"],
                members=snapshot["catalog_members"].to_dict("records"),
                discovery=snapshot["catalog_manifest"]["discovery"])
    for name in ("sector_df", "sector_members", "stock_details", "index_details", "all_details", "index_weight"):
        snapshot[name] = pd.DataFrame()
    snapshot.update(refresh_result={}, sector_datasets={}, index_symbols=[],
                    index_weight_coverage={}, calendar_error="", calendar_capture_evidence={})
    monkeypatch.setattr(publisher, "_safe_upsert_frame", lambda *_a, **_k: {})
    try:
        with pytest.raises(RuntimeError, match="one-sided"):
            publisher._publish_reference_snapshot(engine, snapshot, BUILD, dry_run=False,
                    catalog_only=True, store=type("Budget", (), {"remaining": lambda *_a: 100, "progress": lambda *_a: None})())
    finally:
        engine.dispose()


def test_initial_and_final_identity_probes_are_bounded_by_remaining_capture_deadline(proof):
    source = Source()
    timeouts = []
    source.capabilities = lambda **kwargs: timeouts.append(kwargs["timeout"]) or {"model_instance_id": source.model}
    session = reference.ReferenceReadSession(BUILD, source_bridge=source, remaining=lambda _stage: 0.05)
    session.verify_complete()
    assert timeouts == [0.05, 0.05]


def test_zero_remaining_budget_does_not_send_native_identity_request(proof):
    source = Source()
    source.capabilities = lambda **_k: pytest.fail("request after deadline")
    def expired(_stage):
        raise TimeoutError("native_identity deadline")
    with pytest.raises(TimeoutError, match="native_identity"):
        reference.ReferenceReadSession(BUILD, source_bridge=source, remaining=expired)
