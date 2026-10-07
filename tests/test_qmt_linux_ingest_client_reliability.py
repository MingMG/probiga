from __future__ import annotations

import pytest

from server.common.qmt_linux_ingest_protocol import canonical_sha256, signed_response
from tools import run_qmt_linux_ingest as ingest


IDENTITY = {"edge_build_sha": "a" * 40, "model_sha256": "b" * 64}
DAY = "2026-09-30"
CODE = "000001.SZ"
SECRET = "test-credential-that-is-long-enough"


def coverage(complete=0):
    return [{"dataset": "stock_daily", "source": "guojin_qmt", "target_date": DAY,
             "status": "complete" if complete else "partial", "expected": 1,
             "complete": complete, "no_data": 0, "missing": 1 - complete,
             "next_retry_at": None, "errors": []}]


def plan(*, complete=0, batches=False):
    result = {"status": "ready", **IDENTITY, "dataset": "stock_daily",
              "start_date": DAY, "end_date": DAY, "session_count": 1,
              "source_cooldown": False, "source_retry_at": None,
              "batch_count": int(batches), "coverage": coverage(complete),
              "batches": [{"dataset": "stock_daily", "source": "guojin_qmt",
                           "target_date": DAY, "period": "1d", "adjustment": "none",
                           "codes": [CODE]}] if batches else []}
    result["plan_sha256"] = canonical_sha256(result)
    return result


class Transport:
    def __init__(self):
        self.active = None
        self.prepared = {}
        self.archived = []

    def recover(self):
        return {"active": self.active, "prepared": [], "ready": []}

    def prepare(self, request):
        self.prepared[request["request_id"]] = request

    def activate(self, request_id):
        self.active = self.prepared[request_id]

    def read_result(self, request_id):
        return {"request": self.prepared[request_id],
                "outcomes": {CODE: {"status": "data", "rows": [{"close": 1}]}}}

    def wait_result(self, request_id, **_kwargs):
        return self.read_result(request_id)

    def archive(self, request_id):
        self.archived.append(request_id)
        self.active = None


class Client:
    closed = False

    def close(self):
        self.closed = True


def configure(monkeypatch, tmp_path, plans):
    transport = Transport()
    client = Client()
    values = iter(plans)
    monkeypatch.setattr(ingest, "_edge_identity", lambda: (IDENTITY, tmp_path))
    monkeypatch.setattr(ingest, "get_ai_bridge_config", lambda: {"token": "test-secret"})
    monkeypatch.setattr(ingest, "Client", lambda *_args: client)
    monkeypatch.setattr(ingest, "QmtTransport", lambda _root: transport)
    monkeypatch.setattr(ingest, "history_allowed", lambda _now: True)
    monkeypatch.setattr(ingest, "_plan", lambda *_args, **_kwargs: next(values))

    def commit(_client, _identity, raw, **_kwargs):
        return {"request_id": raw["request"]["request_id"],
                "counts": {"complete": 1, "no_data": 0, "error": 0, "replayed": 0}}

    monkeypatch.setattr(ingest, "_commit", commit)
    return transport, client


def run(*, apply=True):
    return ingest.run(server_url="http://linux.test", datasets=["stock_daily"],
                      start_date=DAY, end_date=DAY, apply=apply, budget_seconds=60)


def test_zero_batch_cooldown_is_partial_not_complete(monkeypatch, tmp_path):
    transport, client = configure(monkeypatch, tmp_path, [plan(), plan()])
    result = run()
    assert result["status"] == "partial"
    assert result["pending_units"] == 1
    assert result["coverage_verified"] is True
    assert not transport.archived
    assert client.closed


def test_source_cooldown_never_starts_a_new_capture(monkeypatch, tmp_path):
    paused = plan(batches=False)
    paused["source_cooldown"] = True
    paused["source_retry_at"] = "2026-10-07 22:15:00"
    transport, _client = configure(monkeypatch, tmp_path, [paused])
    result = run()
    assert result["status"] == "source_cooldown"
    assert result["source_retry_at"] == paused["source_retry_at"]
    assert not transport.prepared


def test_native_source_failure_stops_remaining_precomputed_batches(monkeypatch, tmp_path):
    queued = plan(batches=True)
    queued["batches"] *= 2
    queued["batch_count"] = 2
    transport, _client = configure(monkeypatch, tmp_path, [queued])
    monkeypatch.setattr(ingest, "_commit", lambda *_args, **_kwargs:
                        {"counts": {"complete": 0, "no_data": 0, "error": 1,
                                    "replayed": 0, "error_codes": ["NATIVE_CALL_FAILED"]}})
    result = run()
    assert result["status"] == "source_cooldown"
    assert result["source_error_codes"] == ["NATIVE_CALL_FAILED"]
    assert len(transport.archived) == len(transport.prepared) == 1


def test_dry_run_is_a_plan_not_acquisition_completion(monkeypatch, tmp_path):
    transport, _client = configure(monkeypatch, tmp_path, [plan(batches=True)])
    result = run(apply=False)
    assert result["status"] == "planned"
    assert result["pending_units"] == 1
    assert result["committed_units"] == 0
    assert transport.active is None and not transport.prepared


def test_completion_requires_fresh_authoritative_coverage(monkeypatch, tmp_path):
    transport, _client = configure(monkeypatch, tmp_path,
                                   [plan(batches=True), plan(complete=1)])
    result = run()
    assert result["status"] == "complete"
    assert result["pending_units"] == 0
    assert result["newly_committed_units"] == 1
    assert result["replayed_units"] == 0
    assert len(transport.archived) == 1


def test_archive_failure_preserves_acknowledged_progress_and_request(monkeypatch, tmp_path):
    transport, client = configure(monkeypatch, tmp_path, [plan(batches=True)])

    def blocked(_request_id):
        error = PermissionError("sensitive exception text must not be printed")
        error.winerror = 32
        raise error

    monkeypatch.setattr(transport, "archive", blocked)
    with pytest.raises(PermissionError) as failure:
        run()
    result = failure.value.ingestion_progress
    assert result["committed_units"] == 1
    assert result["newly_committed_units"] == 1
    assert result["retained_request_id"] == transport.active["request_id"]
    assert result["coverage_verified"] is False
    assert result["pending_units"] is None
    assert result["winerror"] == 32
    assert client.closed


def test_commit_budget_end_keeps_raw_request_without_claiming_completion(monkeypatch, tmp_path):
    transport, _client = configure(monkeypatch, tmp_path, [plan(batches=True)])

    def expired(*_args, **_kwargs):
        raise ingest.QmtIngestBudgetExpired()

    monkeypatch.setattr(ingest, "_commit", expired)
    result = run()
    assert result["status"] == "partial"
    assert result["pending_units"] is None
    assert result["committed_units"] == 0
    assert transport.active is not None
    assert result["retained_request_id"] == transport.active["request_id"]
    assert not transport.archived


def test_recovery_replays_are_not_reported_as_new_writes(monkeypatch, tmp_path):
    transport, _client = configure(monkeypatch, tmp_path,
                                   [plan(complete=1), plan(complete=1)])
    retained = {"request_id": "retained_batch", "codes": [CODE]}
    transport.prepare(retained)
    transport.activate("retained_batch")
    monkeypatch.setattr(ingest, "_commit", lambda *_args, **_kwargs:
                        {"counts": {"complete": 1, "no_data": 0, "error": 0, "replayed": 1}})
    result = run()
    assert result["status"] == "complete"
    assert result["committed_units"] == 1
    assert result["replayed_units"] == 1
    assert result["newly_committed_units"] == 0
    assert transport.archived == ["retained_batch"]


def test_prepared_without_result_does_not_activate_in_live_window(monkeypatch):
    transport = Transport()
    transport.prepared["batch_1"] = {"request_id": "batch_1"}
    monkeypatch.setattr(transport, "recover", lambda:
                        {"active": None, "prepared": ["batch_1"]})
    monkeypatch.setattr(transport, "read_result", lambda _request: None)
    monkeypatch.setattr(ingest, "history_allowed", lambda _now: False)
    assert ingest._recover(transport, Client(), IDENTITY, deadline=ingest.time.monotonic()+60) == []
    assert transport.active is None


class Response:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self.payload = payload

    def json(self):
        return self.payload


def test_http_retries_cannot_sleep_past_budget(monkeypatch):
    client = ingest.Client("http://linux.test", SECRET)
    sleeps = []
    calls = []
    monkeypatch.setattr(ingest.time, "monotonic", lambda: 10)
    monkeypatch.setattr(ingest.time, "sleep", sleeps.append)
    monkeypatch.setattr(client.session, "post", lambda *_args, **_kwargs:
                        calls.append(True) or Response(504))
    try:
        with pytest.raises(ingest.QmtIngestBudgetExpired):
            client.post("/api/qmt-ingest/commit", {}, retry_delays=(15,), deadline=15)
    finally:
        client.close()
    assert calls == [True]
    assert sleeps == []


def test_http_timeout_is_capped_to_remaining_budget(monkeypatch):
    client = ingest.Client("http://linux.test", SECRET)
    timeouts = []
    monkeypatch.setattr(ingest.time, "monotonic", lambda: 10)

    def post(*_args, **kwargs):
        timeouts.append(kwargs["timeout"])
        return Response(200, signed_response(SECRET, {"status": "ready"}))

    monkeypatch.setattr(client.session, "post", post)
    try:
        assert client.post("/api/qmt-ingest/plan", {}, deadline=15)["status"] == "ready"
    finally:
        client.close()
    assert timeouts == [5]


def test_plan_rejects_missing_or_inconsistent_coverage():
    class PlannedClient:
        def post(self, *_args, **_kwargs):
            return self.result

    client = PlannedClient()
    for broken in (None, [{**coverage()[0], "missing": 0}],
                   [{**coverage()[0], "expected": True}]):
        client.result = plan()
        client.result["coverage"] = broken
        client.result.pop("plan_sha256")
        client.result["plan_sha256"] = canonical_sha256(client.result)
        with pytest.raises(ingest.QmtLinuxIngestClientError, match="coverage"):
            ingest._plan(client, IDENTITY, dataset="stock_daily", start_date=DAY, end_date=DAY)


@pytest.mark.parametrize("field,value", [("dataset", "index_daily"),
                                        ("start_date", "2026-09-01"),
                                        ("end_date", "2026-09-29")])
def test_plan_binds_exact_requested_product_and_range(field, value):
    payload = plan(complete=1)
    payload[field] = value
    payload.pop("plan_sha256")
    payload["plan_sha256"] = canonical_sha256(payload)

    class PlannedClient:
        def post(self, *_args, **_kwargs):
            return payload

    with pytest.raises(ingest.QmtLinuxIngestClientError, match="plan proof"):
        ingest._plan(PlannedClient(), IDENTITY, dataset="stock_daily", start_date=DAY, end_date=DAY)


def test_late_http_success_cannot_claim_in_budget_completion(monkeypatch):
    client = ingest.Client("http://linux.test", SECRET)
    clock = {"now": 10}
    monkeypatch.setattr(ingest.time, "monotonic", lambda: clock["now"])

    def post(*_args, **_kwargs):
        clock["now"] = 16
        return Response(200, signed_response(SECRET, {"status": "ready"}))

    monkeypatch.setattr(client.session, "post", post)
    try:
        with pytest.raises(ingest.QmtIngestBudgetExpired):
            client.post("/api/qmt-ingest/plan", {}, deadline=15)
    finally:
        client.close()


def test_wait_budget_timeout_is_not_a_source_failure(monkeypatch):
    clock = {"now": 10}
    monkeypatch.setattr(ingest.time, "monotonic", lambda: clock["now"])

    class WaitingTransport:
        def wait_result(self, *_args, **_kwargs):
            clock["now"] = 15
            raise TimeoutError()

    with pytest.raises(ingest.QmtIngestBudgetExpired):
        ingest._wait_result(WaitingTransport(), "batch_1", 15)


def test_commit_rejects_false_replay_success():
    raw = {"request": {"request_id": "batch_1"}, "outcomes": {CODE: {}}}

    class CommittedClient:
        def post(self, *_args, **_kwargs):
            return {"status": "committed", "request_id": "batch_1",
                    "result_sha256": canonical_sha256(raw),
                    "counts": {"complete": 0, "no_data": 0, "error": 0, "replayed": 1}}

    with pytest.raises(ingest.QmtLinuxIngestClientError, match="outcome proof"):
        ingest._commit(CommittedClient(), IDENTITY, raw)


def test_main_error_receipt_retains_progress_without_raw_exception(monkeypatch, capsys):
    failure = PermissionError("do-not-print-secret")
    failure.ingestion_progress = {"status": "error", "error": "PermissionError",
                                  "committed_units": 40, "retained_request_id": "batch_1"}
    monkeypatch.setattr(ingest, "load_project_env", lambda *_args: None)
    monkeypatch.setattr(ingest, "run", lambda **_kwargs: (_ for _ in ()).throw(failure))
    assert ingest.main(["--server-url", "http://linux.test", "--dataset", "stock_daily",
                        "--start-date", DAY, "--end-date", DAY, "--apply"]) == 1
    output = capsys.readouterr().out
    assert '"committed_units": 40' in output
    assert '"retained_request_id": "batch_1"' in output
    assert "do-not-print-secret" not in output
