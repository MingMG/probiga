import json

import pytest
import requests

from server.common.qmt_linux_ingest_protocol import signed_response
from tools import run_qmt_linux_ingest as ingest


SECRET = "do-not-log-hmac-secret-32-bytes"


class Response:
    def __init__(self, status, payload=None):
        self.status_code = status
        self.payload = payload
        self.content = json.dumps(payload).encode("utf-8")

    def json(self):
        return self.payload


@pytest.mark.parametrize("first,category,status", [
    (requests.ReadTimeout("private-url-and-response"), "timeout", None),
    (requests.ConnectionError("private-url-and-response"), "connection", None),
    (requests.RequestException("private-url-and-response"), "request", None),
    (Response(504), None, 504),
])
def test_actual_attempt_categories_without_payload_or_credentials(monkeypatch, capsys,
                                                                 first, category, status):
    client = ingest.Client("http://private-user:private-password@private-host", SECRET)
    replies = [first, Response(200, signed_response(SECRET, {"status": "committed"}))]
    calls, waits = [], []

    def post(*args, **kwargs):
        calls.append((args, kwargs))
        response = replies.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    monkeypatch.setattr(client.session, "post", post)
    monkeypatch.setattr(ingest, "_retry_wait", lambda delay, deadline: waits.append(delay))
    try:
        assert client.post("/api/qmt-ingest/commit", {"private-body": "private-value"},
                           retry_delays=(15,))["status"] == "committed"
    finally:
        client.close()
    output = capsys.readouterr()
    assert output.out == ""
    for secret in (SECRET, "private-url", "private-host", "private-user",
                   "private-password", "private-body", "private-value"):
        assert secret not in output.err
    records = [json.loads(line) for line in output.err.splitlines()]
    assert len(records) == len(calls) == 2 and waits == [15]
    assert [r["attempt"] for r in records] == [1, 2]
    assert records[0]["failure"] == category and records[0]["http_status"] == status
    assert records[0]["configured_retry_delay_seconds"] == 15
    assert records[1]["http_status"] == 200 and records[1]["failure"] is None
    assert records[1]["configured_retry_delay_seconds"] is None
    assert all(r["event"] == "QMT_COMMIT_HTTP_ATTEMPT" and
               r["business_completion_inferred"] is False and
               type(r["elapsed_ms"]) is int and r["elapsed_ms"] >= 0 for r in records)
    assert all(len(line) < 512 for line in output.err.splitlines())


@pytest.mark.parametrize("error", [OSError("stderr failed"), ValueError("stderr closed"),
                                 RuntimeError("reentrant stderr")])
def test_diagnostic_failure_never_reposts_verified_http(monkeypatch, error):
    class BrokenStderr:
        def write(self, _text):
            raise error

        def flush(self):
            raise error

    client = ingest.Client("http://linux.test", SECRET)
    calls, retained = [], []
    response = Response(200, signed_response(SECRET, {"status": "committed"}))
    monkeypatch.setattr(ingest.sys, "stderr", BrokenStderr())
    monkeypatch.setattr(client.session, "post", lambda *_a, **_kw:
                        calls.append(True) or response)
    try:
        assert client.post("/api/qmt-ingest/commit", {}, retry_delays=(15,),
                           retain_response=retained.append)["status"] == "committed"
    finally:
        client.close()
    assert calls == [True]
    assert retained == [response.content]


def test_transport_diagnostic_does_not_authorize_invalid_signature(monkeypatch, capsys):
    client = ingest.Client("http://linux.test", SECRET)
    monkeypatch.setattr(client.session, "post", lambda *_a, **_kw:
                        Response(200, signed_response("wrong-secret-32-bytes-minimum", {"status": "committed"})))
    try:
        with pytest.raises(ingest.QmtLinuxIngestClientError):
            client.post("/api/qmt-ingest/commit", {})
    finally:
        client.close()
    record = json.loads(capsys.readouterr().err)
    assert record["http_status"] == 200 and record["business_completion_inferred"] is False


def test_missing_stderr_never_redirects_diagnostic_into_json_stdout(monkeypatch, capsys):
    client = ingest.Client("http://linux.test", SECRET)
    response = Response(200, signed_response(SECRET, {"status": "committed"}))
    calls, retained = [], []
    monkeypatch.setattr(ingest.sys, "stderr", None)
    monkeypatch.setattr(client.session, "post", lambda *_a, **_kw:
                        calls.append(True) or response)
    try:
        assert client.post("/api/qmt-ingest/commit", {},
                           retain_response=retained.append)["status"] == "committed"
    finally:
        client.close()
    assert calls == [True] and retained == [response.content]
    assert capsys.readouterr().out == ""


def test_plan_does_not_emit_commit_diagnostics(monkeypatch, capsys):
    client = ingest.Client("http://linux.test", SECRET)
    monkeypatch.setattr(client.session, "post", lambda *_a, **_kw:
                        Response(200, signed_response(SECRET, {"status": "ready"})))
    try:
        assert client.post("/api/qmt-ingest/plan", {})["status"] == "ready"
    finally:
        client.close()
    assert capsys.readouterr().err == ""
