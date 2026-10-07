from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from server.api import qmt_linux_ingest as service
from server.api.admin_auth import is_admin_protected_path
from server.api.routers import qmt_ingest as router_module
from server.common.qmt_linux_ingest_protocol import (
    COMMIT_SCHEMA,
    PLAN_SCHEMA,
    QmtLinuxIngestProtocolError,
    new_request_headers,
    signed_response,
    verify_request_headers,
    verify_signed_response,
)
from tools import run_qmt_linux_ingest as ingest_client


SECRET = "edge-worker-secret-that-is-long-enough"
IDENTITY = {"edge_build_sha": "a" * 40, "model_sha256": "b" * 64}


def test_protocol_uses_domain_signature_and_signed_response():
    payload = {"schema": PLAN_SCHEMA, "dataset": "stock_daily"}
    headers = new_request_headers(SECRET, payload, now=1_800_000_000, nonce="c" * 32)
    verify_request_headers(
        SECRET,
        payload,
        timestamp=headers["X-ProBigA-QMT-Ingest-Time"],
        nonce=headers["X-ProBigA-QMT-Ingest-Nonce"],
        signature=headers["X-ProBigA-QMT-Ingest-Signature"],
        now=1_800_000_001,
    )
    with pytest.raises(QmtLinuxIngestProtocolError, match="authentication"):
        verify_request_headers(
            SECRET,
            {**payload, "dataset": "stock_minute"},
            timestamp=headers["X-ProBigA-QMT-Ingest-Time"],
            nonce=headers["X-ProBigA-QMT-Ingest-Nonce"],
            signature=headers["X-ProBigA-QMT-Ingest-Signature"],
            now=1_800_000_001,
        )
    with pytest.raises(QmtLinuxIngestProtocolError, match="expired"):
        verify_request_headers(
            SECRET,
            payload,
            timestamp=headers["X-ProBigA-QMT-Ingest-Time"],
            nonce=headers["X-ProBigA-QMT-Ingest-Nonce"],
            signature=headers["X-ProBigA-QMT-Ingest-Signature"],
            now=1_800_000_301,
        )
    response = signed_response(SECRET, {"status": "committed", "request_id": "request_1"})
    assert verify_signed_response(SECRET, response)["request_id"] == "request_1"
    with pytest.raises(QmtLinuxIngestProtocolError, match="proof"):
        verify_signed_response(SECRET, {**response, "status": "rejected"})


def test_qmt_ingest_route_is_public_only_for_its_own_hmac(monkeypatch):
    app = FastAPI()
    app.include_router(router_module.router, prefix="/api")
    monkeypatch.setattr(
        router_module,
        "get_ai_bridge_config",
        lambda: {"token": SECRET, "lease_seconds": 900},
    )
    monkeypatch.setattr(
        router_module,
        "build_plan",
        lambda payload: {"status": "ready", "dataset": payload["dataset"]},
    )
    payload = {
        "schema": PLAN_SCHEMA,
        **IDENTITY,
        "dataset": "stock_daily",
        "start_date": "2026-09-28",
        "end_date": "2026-09-29",
    }
    assert is_admin_protected_path("/api/qmt-ingest/plan", "POST") is False
    assert TestClient(app).post("/api/qmt-ingest/plan", json=payload).status_code == 401
    response = TestClient(app).post(
        "/api/qmt-ingest/plan",
        content=json.dumps(payload, separators=(",", ":")),
        headers={"Content-Type": "application/json", **new_request_headers(SECRET, payload)},
    )
    assert response.status_code == 200
    assert verify_signed_response(SECRET, response.json())["status"] == "ready"


class _Config:
    def require_writes(self):
        return None


class _Store:
    def __init__(self):
        self.begun = []
        self.failed = []

    def calendar(self, start, end):
        return {"2026-09-28": 1, "2026-09-29": 1}

    def states(self, _dataset, _target):
        return []

    def retrying_sources(self, _now):
        return []

    def validate_spec(self, _spec):
        return None

    def begin_request(self, units, request_id, _now):
        self.begun.append((tuple(units), request_id))

    def fail_request(self, units, request_id, code, _now):
        self.failed.append((tuple(units), request_id, code))


class _Runner:
    instances = []

    def __init__(self, _config):
        self.config = _Config()
        self.primary = _Store()
        self.history = _Store()
        self.consumed = []
        self.closed = False
        self.instances.append(self)

    def store(self, database):
        return self.primary if database == "primary" else self.history

    def catalog(self, _spec):
        return {
            "000001.SZ": {"list_date": "1991-01-01"},
            "600000.SH": {"list_date": "1999-01-01"},
        }

    def _target(self, _spec, requested):
        return requested

    def clock(self):
        return datetime(2026, 10, 7, 1, 0, tzinfo=timezone(timedelta(hours=8)))

    def _consume(self, raw):
        self.consumed.append(raw)
        return {"complete": 1, "no_data": 0, "error": 0, "replayed": 0}

    def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def _service_boundary(monkeypatch):
    _Runner.instances.clear()
    monkeypatch.setattr(service, "Runner", _Runner)
    monkeypatch.setattr(service, "expected_edge_identity", lambda: dict(IDENTITY))
    monkeypatch.setattr(service, "_configuration", lambda: _Config())


def test_linux_plan_uses_authoritative_calendar_catalog_and_bounded_batches():
    result = service.build_plan({
        "schema": PLAN_SCHEMA,
        **IDENTITY,
        "dataset": "stock_daily",
        "start_date": "2026-09-28",
        "end_date": "2026-09-29",
    })
    assert result["status"] == "ready"
    assert result["session_count"] == 2
    assert result["batch_count"] == 2
    assert [item["target_date"] for item in result["batches"]] == [
        "2026-09-29",
        "2026-09-28",
    ]
    assert all(item["codes"] == ["000001.SZ", "600000.SH"] for item in result["batches"])
    assert [item["target_date"] for item in result["coverage"]] == [
        "2026-09-28", "2026-09-29",
    ]
    assert all(item["expected"] == item["missing"] == 2 for item in result["coverage"])
    assert _Runner.instances[-1].closed is True


def test_linux_plan_preserves_running_and_cooldown_gaps_when_no_batch_is_due(monkeypatch):
    def states(_self, dataset, target):
        return [
            {"dataset": dataset, "source": "guojin_qmt", "target_date": target,
             "partition_key": "000001.SZ:1d:none", "status": "running"},
            {"dataset": dataset, "source": "guojin_qmt", "target_date": target,
             "partition_key": "600000.SH:1d:none", "status": "error",
             "next_retry_at": "2026-10-07 01:15:00", "last_error_code": "SOURCE_UNAVAILABLE"},
        ]

    monkeypatch.setattr(_Store, "states", states)
    result = service.build_plan({
        "schema": PLAN_SCHEMA, **IDENTITY, "dataset": "stock_daily",
        "start_date": "2026-09-28", "end_date": "2026-09-29",
    })
    assert result["batch_count"] == 0 and result["batches"] == []
    assert [item["target_date"] for item in result["coverage"]] == ["2026-09-28", "2026-09-29"]
    for item in result["coverage"]:
        assert item["status"] == "partial"
        assert item["expected"] == item["missing"] == 2
        assert item["complete"] == item["no_data"] == 0
        assert item["errors"] == ["SOURCE_UNAVAILABLE"]
        assert item["next_retry_at"] == "2026-10-07 01:15:00"


@pytest.mark.parametrize("dataset", ["stock_daily", "index_daily"])
def test_source_cooldown_blocks_new_queue_but_preserves_visible_gaps(monkeypatch, dataset):
    retry_at = datetime(2026, 10, 7, 1, 15)
    monkeypatch.setattr(_Store, "retrying_sources", lambda _self, _now:
                        [{"dataset": "stock_daily", "source": "guojin_qmt",
                          "next_retry_at": retry_at}])
    result = service.build_plan({"schema": PLAN_SCHEMA, **IDENTITY, "dataset": dataset,
                                 "start_date": "2026-09-28", "end_date": "2026-09-29"})
    assert result["source_cooldown"] is True
    assert result["source_retry_at"] == str(retry_at)
    assert result["batch_count"] == 0 and result["batches"] == []
    assert all(item["missing"] == 2 for item in result["coverage"])


def _raw_result(code="000001.SZ"):
    now = datetime.now(timezone(timedelta(hours=8))).replace(microsecond=0)
    request = {
        "request_id": "request_1",
        "dataset": "stock_daily",
        "source": "guojin_qmt",
        "codes": [code],
        "start_date": "2026-09-28",
        "end_date": "2026-09-28",
        "period": "1d",
        "adjustment": "none",
        "requested_at": (now - timedelta(seconds=5)).isoformat(),
        "deadline_at": (now + timedelta(minutes=5)).isoformat(),
    }
    return {
        "request": request,
        "received_at": now.isoformat(),
        "source_method": "ContextInfo.get_market_data_ex_ori",
        "outcomes": {
            code: {
                "status": "data",
                "rows": [{"qmt_code": code}],
            },
        },
    }


def test_linux_commit_begins_and_consumes_on_linux_owned_store():
    raw = _raw_result()
    result = service.commit_result({
        "schema": COMMIT_SCHEMA,
        **IDENTITY,
        "result": raw,
    })
    runner = _Runner.instances[-1]
    assert result["status"] == "committed"
    assert result["counts"] == {"complete": 1, "no_data": 0, "error": 0, "replayed": 0}
    assert runner.history.begun[0][1] == "request_1"
    assert runner.consumed == [raw]
    assert runner.closed is True


def test_linux_commit_rejects_code_outside_authoritative_catalog_before_write():
    with pytest.raises(service.QmtLinuxIngestError, match="security scope"):
        service.commit_result({
            "schema": COMMIT_SCHEMA,
            **IDENTITY,
            "result": _raw_result("300001.SZ"),
        })
    runner = _Runner.instances[-1]
    assert runner.history.begun == []
    assert runner.consumed == []


def test_linux_commit_infrastructure_failure_preserves_recoverable_request(monkeypatch):
    raw = _raw_result()
    original = _Runner._consume
    calls = []

    def consume(self, result):
        calls.append(result)
        if len(calls) == 1:
            raise ConnectionError("isolated infrastructure failure")
        return original(self, result)

    monkeypatch.setattr(_Runner, "_consume", consume)
    payload = {"schema": COMMIT_SCHEMA, **IDENTITY, "result": raw}
    with pytest.raises(ConnectionError):
        service.commit_result(payload)
    failed_runner = _Runner.instances[-1]
    assert failed_runner.history.begun[0][1] == "request_1"
    assert failed_runner.history.failed == []
    assert failed_runner.closed is True

    receipt = service.commit_result(payload)
    assert receipt["status"] == "committed"
    assert receipt["request_id"] == "request_1"
    assert receipt["counts"] == {"complete": 1, "no_data": 0, "error": 0, "replayed": 0}
    assert calls == [raw, raw]


def test_linux_commit_keeps_committed_native_error_outcomes_in_receipt(monkeypatch):
    raw = _raw_result()
    raw["outcomes"]["000001.SZ"] = {
        "status": "error", "rows": [], "error_code": "EMPTY_NATIVE_RESULT",
        "reason": "native result has no legal empty evidence",
    }
    monkeypatch.setattr(_Runner, "_consume", lambda _self, _raw: {
        "complete": 0, "no_data": 0, "error": 1, "replayed": 0,
        "error_codes": ["EMPTY_NATIVE_RESULT"],
    })
    receipt = service.commit_result({"schema": COMMIT_SCHEMA, **IDENTITY, "result": raw})
    assert receipt["status"] == "committed"
    assert receipt["counts"]["error"] == 1
    assert receipt["counts"]["error_codes"] == ["EMPTY_NATIVE_RESULT"]
    assert _Runner.instances[-1].history.failed == []


class _HttpResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


def test_commit_client_retries_gateway_timeout_with_fresh_authentication(monkeypatch):
    payload = {"schema": COMMIT_SCHEMA, **IDENTITY, "result": _raw_result()}
    committed = signed_response(
        SECRET,
        {
            "status": "committed",
            "request_id": "request_1",
            "result_sha256": "c" * 64,
        },
    )
    responses = [_HttpResponse(504), _HttpResponse(200, committed)]
    headers = []
    sleeps = []
    client = ingest_client.Client("http://linux.test", SECRET)

    def post(_url, **kwargs):
        headers.append(dict(kwargs["headers"]))
        return responses.pop(0)

    monkeypatch.setattr(client.session, "post", post)
    monkeypatch.setattr(ingest_client.time, "sleep", sleeps.append)
    try:
        result = client.post("/api/qmt-ingest/commit", payload, retry_delays=(15,))
    finally:
        client.close()

    assert result["status"] == "committed"
    assert sleeps == [15]
    assert len(headers) == 2
    assert (
        headers[0]["X-ProBigA-QMT-Ingest-Nonce"]
        != headers[1]["X-ProBigA-QMT-Ingest-Nonce"]
    )


def test_commit_client_does_not_retry_contract_rejection(monkeypatch):
    payload = {"schema": COMMIT_SCHEMA, **IDENTITY, "result": _raw_result()}
    calls = []
    client = ingest_client.Client("http://linux.test", SECRET)

    def post(_url, **_kwargs):
        calls.append(True)
        return _HttpResponse(422)

    monkeypatch.setattr(client.session, "post", post)
    try:
        with pytest.raises(
            ingest_client.QmtLinuxIngestClientError,
            match="HTTP 422",
        ):
            client.post("/api/qmt-ingest/commit", payload, retry_delays=(0, 0))
    finally:
        client.close()

    assert calls == [True]
