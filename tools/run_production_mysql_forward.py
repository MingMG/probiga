from __future__ import annotations

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

from tools.remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_host,
    remote_user,
)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def _loopback(value: str, *, label: str) -> str:
    normalized = str(value).strip()
    if normalized not in {"127.0.0.1", "::1"}:
        raise ValueError(f"{label} must be an explicit loopback address")
    return normalized


def _relay(local_socket: socket.socket, channel: paramiko.Channel) -> None:
    try:
        while True:
            readers, _, _ = select.select([local_socket, channel], [], [], 1.0)
            if local_socket in readers:
                data = local_socket.recv(32768)
                if not data:
                    break
                channel.sendall(data)
            if channel in readers:
                data = channel.recv(32768)
                if not data:
                    break
                local_socket.sendall(data)
    except OSError:
        pass
    finally:
        channel.close()
        local_socket.close()


def _bridge_connection(
    transport: paramiko.Transport,
    local_socket: socket.socket,
    client_address: tuple[str, int],
    remote_host_name: str,
    remote_port: int,
) -> None:
    log = logging.getLogger("production_mysql_forward")
    channel = None
    try:
        channel = transport.open_channel(
            "direct-tcpip",
            (remote_host_name, remote_port),
            client_address,
        )
        if channel is None:
            raise RuntimeError("SSH direct-tcpip channel was not created")
        _relay(local_socket, channel)
    except Exception as exc:
        log.debug("MySQL forward connection closed: %s", exc)
        if channel is not None:
            channel.close()
        local_socket.close()


def _listener(local_host: str, local_port: int) -> socket.socket:
    family = socket.AF_INET6 if local_host == "::1" else socket.AF_INET
    listener = socket.socket(family, socket.SOCK_STREAM)
    try:
        listener.bind((local_host, local_port))
        listener.listen(64)
        listener.settimeout(1.0)
        return listener
    except Exception:
        listener.close()
        raise


def _run_forward(
    *,
    ssh_host: str,
    ssh_port: int,
    ssh_user: str,
    remote_host_name: str,
    remote_port: int,
    local_host: str,
    local_port: int,
    connect_timeout: int,
    keepalive_seconds: int,
    retry_min_seconds: float,
    retry_max_seconds: float,
) -> None:
    log = logging.getLogger("production_mysql_forward")
    local_host = _loopback(local_host, label="local bind host")
    remote_host_name = _loopback(remote_host_name, label="remote database endpoint")
    retry_sleep = max(1.0, float(retry_min_seconds))
    with _listener(local_host, local_port) as listener:
        log.info("Local MySQL listener reserved at %s:%s", local_host, local_port)
        while True:
            client = production_ssh_client(paramiko)
            try:
                client.connect(**production_ssh_connect_kwargs(
                    hostname=ssh_host,
                    port=ssh_port,
                    username=ssh_user,
                    timeout=connect_timeout,
                    banner_timeout=connect_timeout,
                    auth_timeout=connect_timeout,
                ))
                transport = client.get_transport()
                if transport is None:
                    raise RuntimeError("SSH transport not available")
                transport.set_keepalive(max(5, int(keepalive_seconds)))
                retry_sleep = max(1.0, float(retry_min_seconds))
                log.info(
                    "Production MySQL forward ready %s:%s -> %s:%s",
                    local_host,
                    local_port,
                    remote_host_name,
                    remote_port,
                )
                while transport.is_active():
                    try:
                        local_socket, address = listener.accept()
                    except TimeoutError:
                        continue
                    threading.Thread(
                        target=_bridge_connection,
                        args=(
                            transport,
                            local_socket,
                            address,
                            remote_host_name,
                            remote_port,
                        ),
                        daemon=True,
                        name="production-mysql-forward-bridge",
                    ).start()
                raise RuntimeError("SSH transport became inactive")
            except Exception as exc:
                log.exception("Production MySQL forward dropped: %s", exc)
            finally:
                client.close()
            time.sleep(retry_sleep)
            retry_sleep = min(max(float(retry_max_seconds), retry_sleep), retry_sleep * 2)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Keep the Windows loopback MySQL endpoint forwarded through the "
            "production SSH host to its Linux database relay"
        )
    )
    parser.add_argument("--ssh-host", default=remote_host())
    parser.add_argument("--ssh-port", type=int, default=22)
    parser.add_argument("--ssh-user", default=remote_user())
    parser.add_argument("--remote-host", default="127.0.0.1")
    parser.add_argument("--remote-port", type=int, default=13306)
    parser.add_argument("--local-host", default="127.0.0.1")
    parser.add_argument("--local-port", type=int, default=3306)
    parser.add_argument("--connect-timeout", type=int, default=20)
    parser.add_argument("--keepalive-seconds", type=int, default=10)
    parser.add_argument("--retry-min-seconds", type=float, default=3.0)
    parser.add_argument("--retry-max-seconds", type=float, default=30.0)
    args = parser.parse_args(argv)
    _loopback(args.local_host, label="local bind host")
    _loopback(args.remote_host, label="remote database endpoint")
    for name in ("ssh_port", "remote_port", "local_port"):
        value = getattr(args, name)
        if not 1 <= value <= 65535:
            parser.error(f"--{name.replace('_', '-')} must be between 1 and 65535")
    return args


def main() -> int:
    args = parse_args()
    _configure_logging()
    _run_forward(
        ssh_host=args.ssh_host,
        ssh_port=args.ssh_port,
        ssh_user=args.ssh_user,
        remote_host_name=args.remote_host,
        remote_port=args.remote_port,
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
