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

    def read_request(self, request_id):
        return self.prepared.get(request_id)

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


def retained_request(request_id="pending_batch", *, dataset="stock_daily", day=DAY):
    return {"request_id": request_id, "dataset": dataset, "source": "guojin_qmt",
            "codes": [CODE], "start_date": day, "end_date": day,
            "period": "1m" if dataset.endswith("minute") else "1d", "adjustment": "none",
            "requested_at": "2026-10-07T22:00:00+08:00",
            "deadline_at": "2026-10-07T22:20:00+08:00"}


class RecoveryTransport(Transport):
    def __init__(self, requests, *, ready=(), active=None):
        super().__init__()
        self.prepared = {request["request_id"]: request for request in requests}
        self.results = {request_id: self.result(request_id) for request_id in ready}
        self.active = self.prepared[active] if active else None
        self.activated = []
        self.waited = []

    def result(self, request_id):
        return {"request": self.prepared[request_id],
                "outcomes": {CODE: {"status": "data", "rows": [{"close": 1}]}}}

    def recover(self):
        return {"active": self.active, "prepared": list(self.prepared),
                "ready": list(self.results)}

    def read_result(self, request_id):
        return self.results.get(request_id)

    def activate(self, request_id):
        self.activated.append(request_id)
        super().activate(request_id)

    def wait_result(self, request_id, **_kwargs):
        self.waited.append(request_id)
        return self.result(request_id)

    def archive(self, request_id):
        super().archive(request_id)
        self.prepared.pop(request_id)
        self.results.pop(request_id, None)


def setup_recovery(monkeypatch, tmp_path, requests, *, ready=(), active=None):
    _transport, client = configure(monkeypatch, tmp_path, [])
    transport = RecoveryTransport(requests, ready=ready, active=active)
    monkeypatch.setattr(ingest, "QmtTransport", lambda _root: transport)
    return transport, client


@pytest.mark.parametrize("dataset", ["stock_daily", "stock_minute", "index_daily", "index_minute"])
def test_pending_recovery_cooldown_checks_original_product_and_date(monkeypatch, tmp_path, dataset):
    request = retained_request(dataset=dataset, day="2026-09-28")
    transport, client = setup_recovery(monkeypatch, tmp_path, [request])
    planned = []
    def paused_plan(_client, _identity, **kwargs):
        planned.append(kwargs)
        return {"source_cooldown": True, "source_retry_at": "2026-10-07 22:14:31"}
    monkeypatch.setattr(ingest, "_plan", paused_plan)
    result = run()
    assert result["status"] == "source_cooldown"
    assert result["source_retry_at"] == "2026-10-07 22:14:31"
    assert result["retained_request_id"] == request["request_id"]
    assert result["coverage_verified"] is False and result["pending_units"] is None
    assert result["committed_units"] == 0
    assert len(planned) == 1
    assert planned[0]["dataset"] == dataset
    assert planned[0]["start_date"] == planned[0]["end_date"] == "2026-09-28"
    assert transport.prepared == {request["request_id"]: request}
    assert transport.active is None
    assert transport.activated == transport.waited == transport.archived == []
    assert client.closed


def test_recovery_finishes_retained_results_before_cooldown_blocks_pending_capture(monkeypatch, tmp_path):
    pending, finished = retained_request("a_pending"), retained_request("z_finished")
    transport, _client = setup_recovery(monkeypatch, tmp_path, [pending, finished], ready=["z_finished"])
    def paused_plan(*_args, **_kwargs):
        assert transport.archived == ["z_finished"]
        return {"source_cooldown": True, "source_retry_at": "2026-10-07 22:14:31"}
    monkeypatch.setattr(ingest, "_plan", paused_plan)
    result = run()
    assert result["status"] == "source_cooldown"
    assert result["committed_units"] == 1
    assert result["retained_request_id"] == "a_pending"
    assert transport.prepared == {"a_pending": pending}
    assert transport.activated == transport.archived == ["z_finished"]
    assert transport.waited == []


def test_pending_recovery_activates_only_after_successful_source_plan(monkeypatch, tmp_path):
    request = retained_request()
    transport, _client = setup_recovery(monkeypatch, tmp_path, [request])
    events = []
    def allowed_plan(*_args, **_kwargs):
        events.append("source_plan")
        assert transport.active is None and not transport.activated
        return plan(batches=True)
    monkeypatch.setattr(ingest, "_plan", allowed_plan)
    receipts = ingest._recover(transport, Client(), IDENTITY, deadline=ingest.time.monotonic()+60)
    assert events == ["source_plan"]
    assert len(receipts) == 1
    assert transport.activated == transport.waited == transport.archived == ["pending_batch"]
    assert transport.prepared == {} and transport.active is None


def test_pending_recovery_plan_failure_does_not_dispatch_or_claim_complete(monkeypatch, tmp_path):
    request = retained_request()
    transport, client = setup_recovery(monkeypatch, tmp_path, [request])
    def rejected_plan(*_args, **_kwargs):
        raise ingest.QmtLinuxIngestClientError("plan proof differs")
    monkeypatch.setattr(ingest, "_plan", rejected_plan)
    with pytest.raises(ingest.QmtLinuxIngestClientError) as failure:
        run()
    result = failure.value.ingestion_progress
    assert result["status"] == "error" and result["committed_units"] == 0
    assert result["coverage_verified"] is False and result["pending_units"] is None
    assert transport.prepared == {"pending_batch": request}
    assert transport.active is None
    assert transport.activated == transport.waited == transport.archived == []
    assert client.closed


@pytest.mark.parametrize("stop", ["window", "budget"])
def test_source_plan_cannot_authorize_activation_after_window_or_budget_ends(monkeypatch, tmp_path, stop):
    request = retained_request()
    transport, _client = setup_recovery(monkeypatch, tmp_path, [request])
    def delayed_plan(*_args, **_kwargs):
        if stop == "window":
            monkeypatch.setattr(ingest, "history_allowed", lambda _now: False)
        else:
            raise ingest.QmtIngestBudgetExpired()
        return plan(batches=True)
    monkeypatch.setattr(ingest, "_plan", delayed_plan)
    result = run()
    assert result["status"] == ("waiting_history_window" if stop == "window" else "partial")
    assert result["committed_units"] == 0 and result["pending_units"] is None
    assert transport.prepared == {"pending_batch": request}
    assert transport.active is None
    assert transport.activated == transport.waited == transport.archived == []


def test_existing_active_without_result_waits_without_reauthorizing_or_cancelling(monkeypatch, tmp_path):
    request = retained_request("already_dispatched")
    transport, _client = setup_recovery(monkeypatch, tmp_path, [request], active="already_dispatched")
    monkeypatch.setattr(ingest, "history_allowed", lambda _now: False)
    monkeypatch.setattr(ingest, "_plan", lambda *_args, **_kwargs: pytest.fail("active request is already dispatched"))
    receipts = ingest._recover(transport, Client(), IDENTITY, deadline=ingest.time.monotonic()+60)
    assert len(receipts) == 1
    assert transport.waited == transport.archived == ["already_dispatched"]
    assert transport.activated == []


def test_live_window_still_receives_ready_results_behind_undispatched_plan(monkeypatch, tmp_path):
    pending, finished = retained_request("a_pending"), retained_request("z_finished")
    transport, _client = setup_recovery(monkeypatch, tmp_path, [pending, finished], ready=["z_finished"])
    monkeypatch.setattr(ingest, "history_allowed", lambda _now: False)
    monkeypatch.setattr(ingest, "_plan", lambda *_args, **_kwargs: pytest.fail("new capture window is closed"))
    result = run()
    assert result["status"] == "waiting_history_window"
    assert result["committed_units"] == 1
    assert transport.archived == transport.activated == ["z_finished"]
    assert transport.prepared == {"a_pending": pending}
    assert transport.waited == []


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


@pytest.mark.parametrize("broken", ["signature", "plan_hash"])
def test_pending_recovery_requires_real_signed_plan_proof_before_dispatch(monkeypatch, tmp_path, broken):
    real_plan, real_client = ingest._plan, ingest.Client("http://linux.test", SECRET)
    request = retained_request()
    transport, _client = setup_recovery(monkeypatch, tmp_path, [request])
    monkeypatch.setattr(ingest, "Client", lambda *_args: real_client)
    monkeypatch.setattr(ingest, "_plan", real_plan)
    payload = plan(batches=True)
    if broken == "plan_hash":
        payload["plan_sha256"] = "0" * 64
    response = signed_response(SECRET, payload)
    if broken == "signature":
        response["proof"] = "0" * 64
    calls = []
    def post(url, **_kwargs):
        calls.append(url)
        return Response(200, response)
    monkeypatch.setattr(real_client.session, "post", post)
    try:
        with pytest.raises(ingest.QmtLinuxIngestClientError) as failure:
            run()
    finally:
        real_client.close()
    assert failure.value.ingestion_progress["status"] == "error"
    assert failure.value.ingestion_progress["committed_units"] == 0
    assert calls == ["http://linux.test/api/qmt-ingest/plan"]
    assert transport.prepared == {"pending_batch": request}
    assert transport.active is None
    assert transport.activated == transport.waited == transport.archived == []


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
