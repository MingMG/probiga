from __future__ import annotations

import json
import os
import threading
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from integrations.qmt.worker import PROVIDER_ID, _connect, dispatch


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 18765


class QmtGatewayState:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.connection_port: int | None = None

    def connect(self) -> int | None:
        with self.lock:
            self.connection_port = _connect()
            return self.connection_port

    def call(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            if self.connection_port is None:
                self.connection_port = _connect()
            return dispatch(payload, self.connection_port)


class QmtGatewayHandler(BaseHTTPRequestHandler):
    server_version = "GuojinQmtGateway/1.0"

    @property
    def state(self) -> QmtGatewayState:
        return self.server.state  # type: ignore[attr-defined]

    def _authorized(self) -> bool:
        expected = (os.environ.get("QMT_GATEWAY_TOKEN") or "").strip()
        if not expected:
            return True
        return self.headers.get("X-QMT-Token", "") == expected

    def _write_json(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            # The caller timed out or closed the socket while QMT was still
            # working.  Treat it as an abandoned request; the gateway should
            # stay alive and avoid noisy traceback loops.
            return

    def do_GET(self) -> None:  # noqa: N802
        if not self._authorized():
            self._write_json(403, {"ok": False, "error": "forbidden"})
            return
        if self.path != "/health":
            self._write_json(404, {"ok": False, "error": "not found"})
            return
        self._write_json(
            200,
            {
                "ok": True,
                "provider": PROVIDER_ID,
                "connection_port": self.state.connection_port,
                "transport": "persistent_http",
            },
        )

    def do_POST(self) -> None:  # noqa: N802
        if not self._authorized():
            self._write_json(403, {"ok": False, "error": "forbidden"})
            return
        if self.path != "/call":
            self._write_json(404, {"ok": False, "error": "not found"})
            return
        try:
            content_length = int(self.headers.get("Content-Length", "0") or 0)
            if content_length <= 0 or content_length > 8 * 1024 * 1024:
                raise ValueError("invalid request size")
            payload = json.loads(self.rfile.read(content_length).decode("utf-8"))
            result = self.state.call(payload)
            result["ok"] = True
            result["transport"] = "persistent_http"
            self._write_json(200, result)
        except Exception as exc:
            self._write_json(
                200,
                {
                    "ok": False,
                    "error": f"{type(exc).__name__}: {exc}",
                    "traceback": traceback.format_exc(),
                    "transport": "persistent_http",
                },
            )

    def log_message(self, format: str, *args: Any) -> None:
        if os.environ.get("QMT_GATEWAY_ACCESS_LOG") == "1":
            super().log_message(format, *args)


class QmtGatewayServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address: tuple[str, int], state: QmtGatewayState):
        super().__init__(address, QmtGatewayHandler)
        self.state = state


def run_gateway(host: str | None = None, port: int | None = None) -> None:
    bind_host = host or (os.environ.get("QMT_GATEWAY_HOST") or DEFAULT_HOST)
    bind_port = port or int(os.environ.get("QMT_GATEWAY_PORT", str(DEFAULT_PORT)))
    if bind_host not in {"127.0.0.1", "localhost", "::1"}:
        raise RuntimeError("QMT gateway may only bind to a loopback address")
    state = QmtGatewayState()
    server = QmtGatewayServer((bind_host, bind_port), state)
    try:
        state.connect()
    except Exception:
        server.server_close()
        raise
    server.serve_forever(poll_interval=0.5)
