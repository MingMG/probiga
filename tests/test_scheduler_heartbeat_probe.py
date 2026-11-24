import json
import sys
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from tools import check_scheduler_runtime_heartbeat as probe
from tools.linux_release_activation import Services


SHA = "a" * 40
PID = 1234


def healthy():
    return {
        "status": "ok",
        "release_revision": {"actual_git_sha": SHA, "matches_expected": True},
        "standalone_scheduler": {"verified": True, "active": True, "pid": PID},
        "standalone_scheduler_heartbeat": {
            "ready": True,
            "detail": {"fresh_row_count": 1, "future_row_count": 0, "errors": [],
                       "current": {"pid": PID, "build_sha": SHA,
                                   "executor_role": "linux_standalone"}},
        },
    }


@contextmanager
def local_api(payload):
    state = {"payload": payload, "http": 200, "requests": 0}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state["requests"] += 1
            self.send_response(state["http"])
            self.end_headers()
            self.wfile.write(json.dumps(state["payload"]).encode())

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    class LocalServices(Services):
        def open_http(self, request, *, timeout):
            assert request == "http://127.0.0.1/api/health"
            return super().open_http(f"http://127.0.0.1:{server.server_port}/api/health", timeout=timeout)

    try:
        yield state, LocalServices()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_probe_uses_live_application_without_controller_database_credentials(monkeypatch, capsys):
    # Immutable release checkouts have no .env. The application already owns
    # its verified DB configuration; the controller must use that live view.
    monkeypatch.delenv("MYSQL_URL", raising=False)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    monkeypatch.setattr(sys, "argv", ["probe", "--expected-build-sha", SHA,
                                      "--expected-scheduler-pid", str(PID)])
    with local_api(healthy()) as (state, services):
        monkeypatch.setattr(probe, "Services", lambda: services)
        assert probe.main() == 0
        assert state["requests"] == 1
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "PASS"
    assert result["detail"]["current"]["pid"] == PID


@pytest.mark.parametrize("fault", ["old_api", "wrong_process", "ambiguous_lease", "wrong_build"])
def test_live_heartbeat_cannot_override_wrong_owner_or_release(fault):
    payload = healthy()
    if fault == "old_api":
        payload["release_revision"]["actual_git_sha"] = "b" * 40
    elif fault == "wrong_process":
        payload["standalone_scheduler"]["pid"] += 1
    elif fault == "ambiguous_lease":
        payload["standalone_scheduler_heartbeat"]["detail"]["fresh_row_count"] = 2
    else:
        payload["standalone_scheduler_heartbeat"]["detail"]["current"]["build_sha"] = "b" * 40
    with local_api(payload) as (_, services):
        result = probe.probe_scheduler_heartbeat(SHA, PID, services=services)
    assert result["status"] == "FAIL"
    assert result["detail"]["errors"]


def test_starting_api_is_not_success_and_next_live_probe_can_pass(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["probe", "--expected-build-sha", SHA,
                                      "--expected-scheduler-pid", str(PID)])
    with local_api(healthy()) as (state, services):
        monkeypatch.setattr(probe, "Services", lambda: services)
        state["http"] = 503
        assert probe.main() == 2
        error = json.loads(capsys.readouterr().out)
        assert error["error_type"] == "HTTPError"
        state["http"] = 200
        assert probe.main() == 0
        assert state["requests"] == 2
