from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from tools import check_production_security


class _SecurityProbeHandler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path == "/api/health":
            return self._json(200, {"status": "ok"})
        if self.path == "/api/health/security":
            return self._json(200, {"status": "ok", "admin_auth": {"enabled": True, "token_configured": True}})
        if self.path == "/":
            return self._redirect("/login?next=/")
        if self.path == "/login":
            return self._text(200, "<html>login</html>")
        if self.path == "/static/monitor.html":
            return self._redirect("/login?next=/static/monitor.html")
        if self.path == "/static/js/app.js":
            return self._text(200, "window.probiga=true;")
        if self.path == "/api/scheduler/tasks":
            if self.headers.get("X-ProBigA-Admin-Token") == "secret":
                return self._json(200, {"status": "ok", "tasks": []})
            return self._json(401, {"status": "error", "error": "admin_auth_required"})
        return self._json(404, {"status": "error"})

    def do_POST(self):  # noqa: N802
        if self.path == "/api/portfolio/refresh-prices":
            return self._json(401, {"status": "error", "error": "admin_auth_required"})
        return self._json(404, {"status": "error"})

    def log_message(self, format, *args):  # noqa: A002
        return None

    def _json(self, status_code: int, payload: dict):
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _text(self, status_code: int, payload: str):
        data = payload.encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, location: str):
        self.send_response(303)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()


def test_run_checks_passes_against_expected_production_shape():
    server = HTTPServer(("127.0.0.1", 0), _SecurityProbeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        results = check_production_security.run_checks(base_url, admin_token="secret", timeout_seconds=2)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert all(item.passed for item in results)
    assert not any(item.skipped for item in results)


def test_run_checks_skips_token_probe_when_token_is_absent():
    server = HTTPServer(("127.0.0.1", 0), _SecurityProbeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        results = check_production_security.run_checks(base_url, admin_token="", timeout_seconds=2)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    skipped = [item for item in results if item.skipped]
    assert len(skipped) == 1
    assert skipped[0].name == "admin read allowed with token"
    assert all(item.passed for item in results)
