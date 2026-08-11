from __future__ import annotations

"""Keep a loopback-only reverse tunnel from production to local Guojin QMT.

The QMT gateway never listens on a public interface.  Production connects to
127.0.0.1:18765, and SSH forwards that socket to the Windows-side gateway.
"""

import argparse
import logging
import select
import socket
import sys
import threading
import time
from pathlib import Path

import paramiko

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.remote_support import production_ssh_client, production_ssh_connect_kwargs


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _bridge(channel, local_host: str, local_port: int) -> None:
    sock = socket.socket()
    try:
        sock.settimeout(10)
        sock.connect((local_host, local_port))
        sock.settimeout(None)
        while True:
            readers, _, _ = select.select([sock, channel], [], [], 1.0)
            if sock in readers:
                data = sock.recv(65536)
                if not data:
                    break
                channel.sendall(data)
            if channel in readers:
                data = channel.recv(65536)
                if not data:
                    break
                sock.sendall(data)
    except OSError:
        logging.getLogger("qmt_tunnel").debug("QMT tunnel channel closed", exc_info=True)
    finally:
        channel.close()
        sock.close()


def run_tunnel(
    *,
    remote_bind_host: str,
    remote_bind_port: int,
    local_host: str,
    local_port: int,
    keepalive_seconds: int,
    retry_min_seconds: float,
    retry_max_seconds: float,
) -> None:
    log = logging.getLogger("qmt_tunnel")
    retry_sleep = max(1.0, retry_min_seconds)
    while True:
        client = production_ssh_client(paramiko)
        transport = None
        try:
            client.connect(
                **production_ssh_connect_kwargs(
                    timeout=20,
                    banner_timeout=20,
                    auth_timeout=20,
                )
            )
            transport = client.get_transport()
            if transport is None:
                raise RuntimeError("SSH transport is unavailable")
            transport.set_keepalive(max(5, keepalive_seconds))
            transport.request_port_forward(remote_bind_host, remote_bind_port)
            retry_sleep = max(1.0, retry_min_seconds)
            log.info(
                "QMT reverse tunnel ready production %s:%s -> local %s:%s",
                remote_bind_host,
                remote_bind_port,
                local_host,
                local_port,
            )
            while transport.is_active():
                channel = transport.accept(timeout=10)
                if channel is None:
                    continue
                threading.Thread(
                    target=_bridge,
                    args=(channel, local_host, local_port),
                    daemon=True,
                    name="qmt-tunnel-bridge",
                ).start()
        except Exception as exc:
            log.warning("QMT reverse tunnel dropped: %s", exc)
            time.sleep(retry_sleep)
            retry_sleep = min(max(retry_sleep * 2, retry_min_seconds), retry_max_seconds)
        finally:
            try:
                if transport is not None:
                    transport.cancel_port_forward(remote_bind_host, remote_bind_port)
            except Exception:
                log.debug("Failed to cancel QMT remote port", exc_info=True)
            client.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Expose local QMT to production over a secure reverse SSH tunnel")
    parser.add_argument("--remote-bind-host", default="127.0.0.1")
    parser.add_argument("--remote-bind-port", type=int, default=18765)
    parser.add_argument("--local-host", default="127.0.0.1")
    parser.add_argument("--local-port", type=int, default=18765)
    parser.add_argument("--keepalive-seconds", type=int, default=10)
    parser.add_argument("--retry-min-seconds", type=float, default=3.0)
    parser.add_argument("--retry-max-seconds", type=float, default=30.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.remote_bind_host not in {"127.0.0.1", "localhost", "::1"}:
        raise SystemExit("QMT remote bind must stay on loopback")
    if args.local_host not in {"127.0.0.1", "localhost", "::1"}:
        raise SystemExit("QMT local gateway must stay on loopback")
    _configure_logging()
    run_tunnel(
        remote_bind_host=args.remote_bind_host,
        remote_bind_port=args.remote_bind_port,
        local_host=args.local_host,
        local_port=args.local_port,
        keepalive_seconds=max(5, args.keepalive_seconds),
        retry_min_seconds=max(1.0, args.retry_min_seconds),
        retry_max_seconds=max(args.retry_min_seconds, args.retry_max_seconds),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
