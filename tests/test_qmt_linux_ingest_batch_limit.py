"""Real local spool/HMAC handoff models; no SDK, HTTP network, or production IO."""
from datetime import datetime
import json
from types import SimpleNamespace

import pytest

from acquisition import qmt_model
from acquisition.qmt_transport import QmtTransport
from acquisition.runner import make_request
from acquisition.models import WorkUnit
from server.common.qmt_linux_ingest_protocol import (
    canonical_sha256, signed_response, verify_request_headers,
)
from tools import run_qmt_linux_ingest as ingest


DAY = "2026-09-30"
IDENTITY = {"edge_build_sha": "a" * 40, "model_sha256": "b" * 64}
SECRET = "MODEL-only-long-enough-machine-key"


@pytest.fixture
def handoff(tmp_path, monkeypatch):
    state = SimpleNamespace(root=tmp_path / "spool", events=[], completed=[],
                            error_code=None, bad_commit_proof=False, commit_bodies=[],
                            after_commit_response=lambda: None)
    state.transport = QmtTransport(str(state.root))
    client = ingest.Client("http://model.invalid", SECRET)
    monkeypatch.setattr(ingest, "_edge_identity", lambda: (IDENTITY, state.root))
    monkeypatch.setattr(ingest, "get_ai_bridge_config", lambda: {"token": SECRET})
    monkeypatch.setattr(ingest, "Client", lambda *_args: client)
    monkeypatch.setattr(ingest, "QmtTransport", lambda _root: state.transport)
    monkeypatch.setattr(ingest, "history_allowed", lambda _now: True)

    def produce(request_id, **kwargs):
        state.events.append(("wait", request_id))
        request = state.transport.read_request(request_id)
        raw = {"request": request, "received_at": datetime.now().astimezone().isoformat(),
               "source_method": "MODEL.local-original",
               "outcomes": {code: {"status": "data", "rows": [{"close": 1}]}
                            for code in request["codes"]}}
        qmt_model.publish_json(str(state.root / (request_id + ".ready.json")),
                               raw, qmt_model.MAX_RESULT_BYTES, immutable=True)
        return state.transport.read_result(request_id)

    monkeypatch.setattr(state.transport, "wait_result", produce)
    state.produce = produce

    class Response:
        status_code = 200
        def __init__(self, payload):
            self.payload = payload
            self.content = json.dumps(payload, ensure_ascii=False, indent=1).encode("utf-8")
        def json(self):
            return self.payload

    def post(url, *, data, headers, **kwargs):
        payload = json.loads(data)
        verify_request_headers(SECRET, payload,
                               timestamp=headers["X-ProBigA-QMT-Ingest-Time"],
                               nonce=headers["X-ProBigA-QMT-Ingest-Nonce"],
                               signature=headers["X-ProBigA-QMT-Ingest-Signature"])
        if url.endswith("/plan"):
            state.events.append(("plan", payload["dataset"]))
            count = len(state.completed)
            core = {"status": "ready", **IDENTITY, "dataset": payload["dataset"],
                    "start_date": payload["start_date"], "end_date": payload["end_date"],
                    "session_count": 1, "source_cooldown": False, "source_retry_at": None,
                    "coverage": [{"dataset": payload["dataset"], "target_date": DAY,
                                  "expected": 3, "complete": min(count, 3), "no_data": 0,
                                  "missing": max(3-count, 0),
                                  "status": "complete" if count >= 3 else "partial"}],
                    "batches": [{"dataset": payload["dataset"], "source": "guojin_qmt",
                                 "target_date": DAY, "period": "1d", "adjustment": "none",
                                 "codes": [f"{number:06}.SZ"]} for number in range(1, 4)]}
            core["batch_count"] = len(core["batches"])
            core["plan_sha256"] = canonical_sha256(core)
            return Response(signed_response(SECRET, core))
        assert url.endswith("/commit")
        raw = payload["result"]
        request_id = raw["request"]["request_id"]
        state.events.append(("commit", request_id))
        state.completed.append(request_id)
        count = len(raw["outcomes"])
        counts = {"complete": 0 if state.error_code else count, "no_data": 0,
                  "error": count if state.error_code else 0, "replayed": 0,
                  "error_codes": [state.error_code] if state.error_code else []}
        envelope = signed_response(SECRET, {"status": "committed", "request_id": request_id,
                                            "result_sha256": canonical_sha256(raw), "counts": counts})
        if state.bad_commit_proof:
            envelope["proof"] = "0" * 64
        response = Response(envelope)
        state.commit_bodies.append(response.content)
        state.after_commit_response()
        return response

    monkeypatch.setattr(client.session, "post", post)
    original_archive = state.transport.archive
    def archive(request_id):
        assert state.completed[-1] == request_id
        original_archive(request_id)
        state.events.append(("archive", request_id))
    monkeypatch.setattr(state.transport, "archive", archive)

    def retain(*, day=DAY, dataset="stock_daily", active=False, ready=False):
        period = "1m" if dataset.endswith("minute") else "1d"
        request = make_request([WorkUnit(dataset, "guojin_qmt", day, "000001.SZ", period, "none")],
                               datetime.now().astimezone(), timeout=1200)
        state.transport.prepare(request)
        if active:
            state.transport.activate(request["request_id"])
        if ready:
            produce(request["request_id"])
        state.events.clear()
        return request
    state.retain = retain
    return state


def run(**kwargs):
    return ingest.run(**dict(dict(server_url="http://model.invalid", datasets=["stock_daily"],
                                 start_date=DAY, end_date=DAY, apply=True,
                                 budget_seconds=7200, max_batches=1), **kwargs))


@pytest.mark.parametrize("limit", [1, 2])
def test_limit_is_after_real_hmac_commit_and_original_archive(handoff, limit):
    result = run(max_batches=limit)
    assert result["status"] == "partial" and result["stop_reason"] == "batch_limit"
    assert result["committed_batches"] == limit and result["error_units"] == 0
    assert not result["coverage_verified"] and result["pending_units"] is None
    assert len(list((handoff.root / "processed").iterdir())) == limit
    assert not (handoff.root / "active.json").exists()
    assert [kind for kind, _ in handoff.events] == ["plan"] + ["wait", "commit", "archive"] * limit
    import base64
    retained_bodies = []
    for request_id in handoff.completed:
        record = json.loads((handoff.root / "processed" / request_id /
                             (request_id + ".commit-response.json")).read_bytes())
        retained_bodies.append(base64.b64decode(record["http_body_base64"], validate=True))
    assert retained_bodies == handoff.commit_bodies  # Actual pretty HTTP bytes, not regenerated JSON.


def test_unlimited_still_requires_fresh_coverage(handoff):
    result = run(max_batches=None)
    assert result["status"] == "complete" and result["pending_units"] == 0
    assert result["coverage_verified"] and result["committed_batches"] == 3
    assert handoff.events[-1] == ("plan", "stock_daily")


@pytest.mark.parametrize("ready,active", [(True, False), (False, True), (False, False)])
def test_original_recovery_consumes_same_global_batch_limit(handoff, ready, active):
    request = handoff.retain(ready=ready, active=active)
    request_id = request["request_id"]
    original = (handoff.root / (request_id + ".prepared.json")).read_bytes()
    result = run()
    assert result["stop_reason"] == "batch_limit" and result["committed_batches"] == 1
    assert handoff.completed == [request_id]
    assert (handoff.root / "processed" / request_id / (request_id + ".prepared.json")).read_bytes() == original
    # Only an undispatched original needs its source permission plan; none
    # gets another fresh plan/native request after completing the first batch.
    assert [item for item in handoff.events if item[0] == "plan"] == (
        [] if ready or active else [("plan", "stock_daily")])


@pytest.mark.parametrize("error", ["EMPTY_NATIVE_RESULT", "MINUTE_GRID_INVALID", "NATIVE_CALL_FAILED"])
@pytest.mark.parametrize("recovered", [False, True])
def test_every_committed_error_archives_original_then_stops(handoff, error, recovered):
    if recovered:
        handoff.retain(ready=True)
    handoff.error_code = error
    result = run(max_batches=None)
    assert result["stop_reason"] == "error_units"
    assert result["status"] == ("source_cooldown" if error == "NATIVE_CALL_FAILED" else "partial")
    assert result["error_units"] == 1 and result["committed_units"] == 0
    assert result["committed_batches"] == 1 and not result["coverage_verified"]
    assert handoff.events[-1][0] == "archive"
    assert len(handoff.completed) == 1 and len(list((handoff.root / "processed").iterdir())) == 1


@pytest.mark.parametrize("day,dataset", [("2026-08-31", "stock_daily"),
                                          ("2026-09-29", "stock_daily"),
                                          (DAY, "index_daily")])
@pytest.mark.parametrize("ready,active", [(True, False), (False, True), (False, False)])
def test_out_of_scope_original_never_dispatches_or_commits(handoff, day, dataset, ready, active):
    request = handoff.retain(day=day, dataset=dataset, ready=ready, active=active)
    before = {path.name: path.read_bytes() for path in handoff.root.iterdir() if path.is_file()}
    result = run()
    assert result["status"] == "partial" and result["stop_reason"] == "retained_scope"
    assert result["retained_request_id"] == request["request_id"]
    assert not result["coverage_verified"] and result["committed_batches"] == 0
    assert handoff.events == []
    assert {path.name: path.read_bytes() for path in handoff.root.iterdir() if path.is_file()} == before


@pytest.mark.parametrize("name", ["unknown.tmp", "orphan.ready.json"])
def test_unknown_retained_namespace_stops_new_capture(handoff, name):
    path = handoff.root / name
    path.write_bytes(b"MODEL-unknown-original")  # Isolated retained fault, not production data.
    result = run()
    assert result["stop_reason"] == "retained_scope" and handoff.events == []
    assert path.read_bytes() == b"MODEL-unknown-original"


def test_archive_failure_is_not_a_successful_limit_boundary(handoff, monkeypatch):
    monkeypatch.setattr(handoff.transport, "archive", lambda _uid: (_ for _ in ()).throw(PermissionError("MODEL")))
    with pytest.raises(PermissionError) as error:
        run()
    assert error.value.ingestion_progress["committed_batches"] == 1
    assert "stop_reason" not in error.value.ingestion_progress
    assert (handoff.root / "active.json").exists()
    assert len(list(handoff.root.glob("*.ready.json"))) == 1
    request_id=handoff.completed[0]
    directory=handoff.root / "processed" / request_id
    assert [path.name for path in directory.iterdir()]==[request_id+".commit-response.json"]
    original=(directory/(request_id+".commit-response.json")).read_bytes()
    # A receipt-only directory is not an archived batch. Recovery must use
    # the original active/raw and same signed receipt, without another HTTP commit.
    monkeypatch.setattr(handoff.transport,"archive",lambda uid:QmtTransport.archive(handoff.transport,uid))
    recovered=run()
    assert recovered["stop_reason"]=="batch_limit" and recovered["committed_batches"]==1
    assert handoff.completed==[request_id]
    assert (directory/(request_id+".commit-response.json")).read_bytes()==original
    assert (directory/(request_id+".prepared.json")).exists()
    assert (directory/(request_id+".ready.json")).exists()
    assert not (handoff.root/"active.json").exists()


def test_invalid_commit_hmac_cannot_archive_or_count_limit(handoff):
    handoff.bad_commit_proof = True
    with pytest.raises(ingest.QmtLinuxIngestClientError) as error:
        run()
    assert error.value.ingestion_progress["committed_batches"] == 0
    assert (handoff.root / "active.json").exists()
    assert not any(kind == "archive" for kind, _ in handoff.events)


def test_commit_response_write_failure_retains_active_raw_without_archive(handoff,monkeypatch):
    monkeypatch.setattr(ingest,"publish_json",lambda *a,**k:(_ for _ in ()).throw(OSError("MODEL receipt fsync")))
    with pytest.raises(OSError) as error:
        run()
    assert error.value.ingestion_progress["committed_batches"]==0
    assert len(handoff.completed)==1
    assert (handoff.root/"active.json").exists() and len(list(handoff.root.glob("*.ready.json")))==1
    assert not any(kind=="archive" for kind,_ in handoff.events)


def test_commit_response_readback_failure_is_not_a_batch_boundary(handoff,monkeypatch):
    original_read=ingest.read_spool_json
    def unreadable(path,limit):
        if str(path).endswith(".commit-response.json") and __import__("os").path.exists(path):
            raise OSError("MODEL original readback")
        return original_read(path,limit)
    monkeypatch.setattr(ingest,"read_spool_json",unreadable)
    with pytest.raises(OSError) as error:
        run()
    assert error.value.ingestion_progress["committed_batches"]==0
    assert len(list((handoff.root/"processed").glob("*/*.commit-response.json")))==1
    assert (handoff.root/"active.json").exists()
    assert not any(kind=="archive" for kind,_ in handoff.events)


def test_late_success_retains_original_before_budget_and_reuses_without_http(handoff,monkeypatch):
    clock={"now":0.0}
    monkeypatch.setattr(ingest.time,"monotonic",lambda:clock["now"])
    handoff.after_commit_response=lambda:clock.update(now=7201.0)
    result=run()
    assert result["status"]=="partial" and result["committed_batches"]==0
    assert (handoff.root/"active.json").exists()
    path=next((handoff.root/"processed").glob("*/*.commit-response.json"))
    original=path.read_bytes()
    recovered=run()
    assert recovered["stop_reason"]=="batch_limit" and recovered["committed_batches"]==1
    assert len(handoff.completed)==1 and path.read_bytes()==original
    assert not (handoff.root/"active.json").exists()


@pytest.mark.parametrize("fault",["body","raw_sha","signature"])
def test_changed_retained_commit_original_never_reissues_or_archives(handoff,monkeypatch,fault):
    monkeypatch.setattr(handoff.transport,"archive",lambda uid:(_ for _ in ()).throw(OSError("MODEL interrupted archive")))
    with pytest.raises(OSError):
        run()
    path=next((handoff.root/"processed").glob("*/*.commit-response.json"))
    record=json.loads(path.read_bytes())
    if fault=="raw_sha":
        record["result_sha256"]="0"*64
    elif fault=="body":
        record["http_body_sha256"]="0"*64
    else:
        import base64,hashlib
        envelope=json.loads(base64.b64decode(record["http_body_base64"]))
        envelope["proof"]="0"*64
        body=json.dumps(envelope).encode()
        record["http_body_base64"]=base64.b64encode(body).decode()
        record["http_body_sha256"]=hashlib.sha256(body).hexdigest()
    path.write_bytes(json.dumps(record).encode())  # Isolated fault injection, never production.
    with pytest.raises(ingest.QmtLinuxIngestClientError):
        run()
    assert len(handoff.completed)==1 and (handoff.root/"active.json").exists()


@pytest.mark.parametrize("value", [True, False, 0, -1, 1.0, "1", [], {}, float("inf")])
def test_invalid_limit_rejected_before_identity_or_filesystem(monkeypatch, value):
    monkeypatch.setattr(ingest, "_edge_identity", lambda: pytest.fail("identity/FS before limit validation"))
    with pytest.raises(ValueError, match="positive integer"):
        run(max_batches=value)


def test_cli_passes_positive_limit_without_shortening_budget(monkeypatch, capsys):
    monkeypatch.setattr(ingest, "load_project_env", lambda *_args: None)
    calls = []
    def capture(**kwargs):
        calls.append(kwargs)
        return {"status": "partial", "stop_reason": "batch_limit"}
    monkeypatch.setattr(ingest, "run", capture)
    assert ingest.main(["--server-url", "http://model.invalid", "--dataset", "stock_daily",
                        "--start-date", DAY, "--end-date", DAY, "--max-batches", "1", "--apply"]) == 2
    assert calls[0]["max_batches"] == 1 and calls[0]["budget_seconds"] == 7200
    assert json.loads(capsys.readouterr().out)["stop_reason"] == "batch_limit"


@pytest.mark.parametrize("value", ["0", "-1", "1.5", "false"])
def test_cli_rejects_nonpositive_or_noninteger_limit(value):
    with pytest.raises(SystemExit):
        ingest._parser().parse_args(["--dataset", "stock_daily", "--start-date", DAY,
                                    "--end-date", DAY, "--max-batches", value])
