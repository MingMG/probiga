"""Durable fact-job polling must survive transport failure without duplicate work."""
from copy import deepcopy
import json

import pytest

from server.common.qmt_linux_ingest_protocol import canonical_sha256
from tools import run_qmt_strategy_daily as worker
from tools.run_qmt_linux_ingest import QmtLinuxIngestClientError


BUILD = "a" * 40
TARGET = "2026-09-30"


def response(payload, status="ISSUED", **changes):
    snapshot = {"trade_date": TARGET, "mode": "DAILY"}
    return {"request_id": payload["request_id"], "edge_build_sha": BUILD,
            "status": status, "trade_date": TARGET, "run_mode": "DAILY",
            "snapshot_id": "b" * 32, "snapshot": snapshot,
            "snapshot_sha256": canonical_sha256(snapshot),
            "simulation_only": True, "real_order_allowed": False,
            "automatic_real_order_submission": False, "real_order_authority": False,
            **changes}


class Client:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def post(self, endpoint, payload, **kwargs):
        self.calls.append((endpoint, deepcopy(payload), kwargs))
        value = next(self.responses)
        if isinstance(value, Exception):
            raise value
        if isinstance(value, str):
            return response(payload, value)
        return response(payload, **value)


@pytest.fixture
def time_clock(monkeypatch):
    clock = [0.0]
    monkeypatch.setattr(worker.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(worker.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    monkeypatch.setattr(worker, "PREPARATION_BUDGET_SECONDS", 20)
    return clock


def test_queued_preparing_and_issued_share_one_retained_request(tmp_path, time_clock):
    client = Client(["QUEUED", "PREPARING", "ISSUED"])
    path, issued = worker.prepare_issued_input(client, tmp_path, BUILD, "")
    assert issued["status"] == "ISSUED" and time_clock[0] == 10
    requests = [call[1] for call in client.calls]
    assert requests[0] == requests[1] == requests[2]
    assert requests[0]["trade_date"] == ""
    assert json.loads(path.read_text()) == requests[0]
    assert path.exists()  # A fact job is not a completed simulation.
    worker.complete_input_request(path)
    assert not path.exists()
    assert len(list(tmp_path.glob("*.input-completed.json"))) == 1


@pytest.mark.parametrize("failure", [
    QmtLinuxIngestClientError("Linux ingestion API is unavailable"),
    QmtLinuxIngestClientError("Linux ingestion API rejected the request: HTTP 502"),
    QmtLinuxIngestClientError("Linux ingestion API rejected the request: HTTP 503"),
    QmtLinuxIngestClientError("Linux ingestion API rejected the request: HTTP 504"),
])
def test_transient_http_failure_retries_same_durable_job(tmp_path, time_clock, failure):
    client = Client([failure, "ISSUED"])
    path, _issued = worker.prepare_issued_input(client, tmp_path, BUILD, TARGET)
    assert client.calls[0][1] == client.calls[1][1]
    assert path.exists() and time_clock[0] == 5


@pytest.mark.parametrize("code", [400, 401, 403, 404, 409, 422])
def test_authentication_and_contract_errors_do_not_spin_or_change_identity(tmp_path, time_clock, code):
    client = Client([QmtLinuxIngestClientError(f"Linux ingestion API rejected the request: HTTP {code}")])
    with pytest.raises(QmtLinuxIngestClientError):
        worker.prepare_issued_input(client, tmp_path, BUILD, TARGET)
    assert len(client.calls) == 1 and time_clock[0] == 0
    assert len(list(tmp_path.glob("*.input-request.json"))) == 1


def test_timeout_keeps_request_and_next_invocation_resumes_it(tmp_path, time_clock):
    first = Client(["PREPARING"] * 4)
    with pytest.raises(worker.SimulationRuntimeError, match="BUDGET_EXPIRED_REQUEST_RETAINED"):
        worker.prepare_issued_input(first, tmp_path, BUILD, TARGET)
    path = next(tmp_path.glob("*.input-request.json"))
    encoded = path.read_bytes()
    second = Client(["ISSUED"])
    resumed, _issued = worker.prepare_issued_input(second, tmp_path, BUILD, TARGET)
    assert resumed == path and resumed.read_bytes() == encoded
    assert first.calls[0][1] == second.calls[0][1]


def test_fixed_job_date_is_bound_across_process_restarts(tmp_path, time_clock):
    first = Client(["PREPARING"] * 4)
    with pytest.raises(worker.SimulationRuntimeError, match="BUDGET_EXPIRED"):
        worker.prepare_issued_input(first, tmp_path, BUILD, "")
    second = Client([{"trade_date": "2026-10-08"}])
    with pytest.raises(worker.SimulationRuntimeError, match="JOB_DATE_OR_MODE_CHANGED"):
        worker.prepare_issued_input(second, tmp_path, BUILD, "")
    assert first.calls[0][1] == second.calls[0][1]


@pytest.mark.parametrize("change", [
    {"request_id": "c" * 32}, {"edge_build_sha": "d" * 40},
    {"simulation_only": False}, {"real_order_allowed": True},
    {"automatic_real_order_submission": True}, {"real_order_authority": True},
])
def test_unsafe_or_wrong_signed_job_identity_is_not_evaluated(tmp_path, time_clock, change):
    with pytest.raises(worker.SimulationRuntimeError, match="SIGNED_INPUT_JOB_IDENTITY_DIFFERS"):
        worker.prepare_issued_input(Client([change]), tmp_path, BUILD, TARGET)
    assert len(list(tmp_path.glob("*.input-request.json"))) == 1
    assert not list(tmp_path.glob("*.input-completed.json"))


def test_failed_fact_job_retains_evidence_and_new_attempt_gets_new_identity(tmp_path, time_clock):
    first = Client([{"status": "FAILED", "error_code": "CLOSED_SESSION_ROLLOVER"}])
    with pytest.raises(worker.SimulationRuntimeError, match="CLOSED_SESSION_ROLLOVER"):
        worker.prepare_issued_input(first, tmp_path, BUILD, "")
    assert not list(tmp_path.glob("*.input-request.json"))
    assert len(list(tmp_path.glob("*.input-failed.json"))) == 1
    assert len(list(tmp_path.glob("*.input-failure.json"))) == 1
    second = Client(["ISSUED"])
    worker.prepare_issued_input(second, tmp_path, BUILD, "")
    assert first.calls[0][1]["request_id"] != second.calls[0][1]["request_id"]


def test_failed_job_does_not_leak_arbitrary_remote_error_into_console_reason(tmp_path, time_clock):
    with pytest.raises(worker.SimulationRuntimeError, match="^INPUT_PREPARATION_FAILED$"):
        worker.prepare_issued_input(Client([{"status": "FAILED", "error_code": "password=secret\nSELECT..."}]), tmp_path, BUILD, TARGET)


def test_prior_release_request_stays_unmodified(tmp_path, time_clock):
    path, payload = worker.retained_input_request(tmp_path, "d" * 40, TARGET)
    original = path.read_bytes()
    current, _issued = worker.prepare_issued_input(Client(["ISSUED"]), tmp_path, BUILD, TARGET)
    assert current != path and path.read_bytes() == original
    assert payload["edge_build_sha"] == "d" * 40


def test_two_unresolved_jobs_for_one_scope_do_not_create_a_third(tmp_path):
    _path, payload = worker.retained_input_request(tmp_path, BUILD, TARGET)
    other = {**payload, "request_id": "c" * 32}
    worker.atomic_json(tmp_path / (other["request_id"] + ".input-request.json"), other)
    with pytest.raises(worker.SimulationRuntimeError, match="MULTIPLE_RETAINED_INPUT_REQUESTS"):
        worker.retained_input_request(tmp_path, BUILD, TARGET)
    assert len(list(tmp_path.glob("*.input-request.json"))) == 2


def test_corrupt_request_filename_identity_is_rejected(tmp_path):
    path, payload = worker.retained_input_request(tmp_path, BUILD, TARGET)
    worker.atomic_json(path, {**payload, "request_id": "c" * 32})
    with pytest.raises(worker.SimulationRuntimeError, match="RETAINED_INPUT_REQUEST_IDENTITY_DIFFERS"):
        worker.retained_input_request(tmp_path, BUILD, TARGET)
