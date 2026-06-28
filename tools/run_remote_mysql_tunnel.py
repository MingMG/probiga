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


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _bridge_channel(channel, local_host: str, local_port: int) -> None:
    sock = socket.socket()
    try:
        sock.connect((local_host, local_port))
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
) -> None:
    log = logging.getLogger("mysql_tunnel")
    while True:
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        transport = None
        try:
            client.connect(
                ssh_host,
                port=ssh_port,
                username=ssh_user,
                password=ssh_password,
                timeout=20,
            )
            transport = client.get_transport()
            if transport is None:
                raise RuntimeError("SSH transport not available")
            transport.set_keepalive(15)
            transport.request_port_forward(remote_bind_host, remote_bind_port)
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
            time.sleep(5)
        finally:
            try:
                if transport is not None:
                    transport.cancel_port_forward(remote_bind_host, remote_bind_port)
            except Exception:
                pass
            client.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Keep a reverse SSH tunnel from local MySQL to the production host")
    parser.add_argument("--ssh-host", required=True)
    parser.add_argument("--ssh-port", type=int, default=22)
    parser.add_argument("--ssh-user", required=True)
    parser.add_argument("--ssh-password")
    parser.add_argument("--remote-bind-host", default="127.0.0.1")
    parser.add_argument("--remote-bind-port", type=int, default=13306)
    parser.add_argument("--local-host", default="127.0.0.1")
    parser.add_argument("--local-port", type=int, default=3306)
    args = parser.parse_args()
    ssh_password = args.ssh_password or os.environ.get("PROBIGA_REMOTE_SSH_PASSWORD")
    if not ssh_password:
        parser.error("missing SSH password; pass it by option or PROBIGA_REMOTE_SSH_PASSWORD")

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
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
