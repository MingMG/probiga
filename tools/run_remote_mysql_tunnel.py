from __future__ import annotations

import argparse
import logging
import os
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

from tools.remote_support import remote_host, remote_user, ssh_connect_kwargs


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _bridge_channel(channel, local_host: str, local_port: int) -> None:
    log = logging.getLogger("mysql_tunnel")
    sock = socket.socket()
    try:
        sock.settimeout(10)
        sock.connect((local_host, local_port))
        sock.settimeout(None)
    except Exception:
        channel.close()
        sock.close()
        return

    try:
        while True:
            readers, _, _ = select.select([sock, channel], [], [], 1.0)
            if sock in readers:
                data = sock.recv(32768)
                if not data:
                    break
                channel.sendall(data)
            if channel in readers:
                data = channel.recv(32768)
                if not data:
                    break
                sock.sendall(data)
    except OSError as exc:
        log.debug("Reverse tunnel bridge closed: %s", exc)
    finally:
        channel.close()
        sock.close()


def _run_tunnel(
    ssh_host: str,
    ssh_port: int,
    ssh_user: str,
    ssh_password: str,
    remote_bind_host: str,
    remote_bind_port: int,
    local_host: str,
    local_port: int,
    connect_timeout: int,
    keepalive_seconds: int,
    retry_min_seconds: float,
    retry_max_seconds: float,
) -> None:
    log = logging.getLogger("mysql_tunnel")
    retry_sleep = max(1.0, float(retry_min_seconds))
    while True:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        transport = None
        try:
            client.connect(**ssh_connect_kwargs(
                hostname=ssh_host,
                port=ssh_port,
                username=ssh_user,
                password=ssh_password,
                timeout=connect_timeout,
                banner_timeout=connect_timeout,
                auth_timeout=connect_timeout,
            ))
            transport = client.get_transport()
            if transport is None:
                raise RuntimeError("SSH transport not available")
            transport.set_keepalive(max(5, int(keepalive_seconds)))
            transport.request_port_forward(remote_bind_host, remote_bind_port)
            retry_sleep = max(1.0, float(retry_min_seconds))
            log.info(
                "Reverse tunnel ready %s:%s -> %s:%s",
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
                    target=_bridge_channel,
                    args=(channel, local_host, local_port),
                    daemon=True,
                    name="mysql-tunnel-bridge",
                ).start()
        except Exception as exc:
            log.exception("Reverse tunnel dropped: %s", exc)
            time.sleep(retry_sleep)
            retry_sleep = min(max(float(retry_max_seconds), retry_sleep), retry_sleep * 2)
        finally:
            try:
                if transport is not None:
                    transport.cancel_port_forward(remote_bind_host, remote_bind_port)
            except Exception as exc:
                log.warning("Failed to cancel reverse tunnel port forward: %s", exc)
            client.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Keep a reverse SSH tunnel from local MySQL to the production host")
    parser.add_argument("--ssh-host", default=remote_host())
    parser.add_argument("--ssh-port", type=int, default=22)
    parser.add_argument("--ssh-user", default=remote_user())
    parser.add_argument("--ssh-password")
    parser.add_argument("--remote-bind-host", default="127.0.0.1")
    parser.add_argument("--remote-bind-port", type=int, default=13306)
    parser.add_argument("--local-host", default="127.0.0.1")
    parser.add_argument("--local-port", type=int, default=3306)
    parser.add_argument("--connect-timeout", type=int, default=20)
    parser.add_argument("--keepalive-seconds", type=int, default=10)
    parser.add_argument("--retry-min-seconds", type=float, default=3.0)
    parser.add_argument("--retry-max-seconds", type=float, default=30.0)
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()
    ssh_password = args.ssh_password or os.environ.get("PROBIGA_REMOTE_SSH_PASSWORD")
    if not ssh_password:
        raise SystemExit("missing SSH password; pass it by option or PROBIGA_REMOTE_SSH_PASSWORD")

    _configure_logging()
    _run_tunnel(
        ssh_host=args.ssh_host,
        ssh_port=args.ssh_port,
        ssh_user=args.ssh_user,
        ssh_password=ssh_password,
        remote_bind_host=args.remote_bind_host,
        remote_bind_port=args.remote_bind_port,
        local_host=args.local_host,
        local_port=args.local_port,
        connect_timeout=args.connect_timeout,
        keepalive_seconds=args.keepalive_seconds,
        retry_min_seconds=args.retry_min_seconds,
        retry_max_seconds=args.retry_max_seconds,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
