# -*- coding: utf-8 -*-
"""Start and inspect a resource-bounded production acceptance job."""
from __future__ import annotations

import argparse
import json
import re
import shlex
from typing import Any

import paramiko

from remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_pythonpath,
    remote_root,
)


def _unit_name(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_.-]+", "-", value.strip()).strip("-.").lower()
    if not cleaned:
        raise ValueError("job name must contain at least one safe character")
    return f"probiga-acceptance-{cleaned[:48]}"


def _connect() -> paramiko.SSHClient:
    client = production_ssh_client(paramiko)
    client.connect(**production_ssh_connect_kwargs())
    return client


def _run(client: paramiko.SSHClient, command: str, *, timeout: int = 120) -> dict[str, Any]:
    _stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
    out = stdout.read().decode("utf-8", errors="replace").strip()
    err = stderr.read().decode("utf-8", errors="replace").strip()
    status = stdout.channel.recv_exit_status()
    return {"returncode": status, "stdout": out, "stderr": err}


def _start(args: argparse.Namespace, client: paramiko.SSHClient) -> dict[str, Any]:
    if not args.command:
        raise ValueError("start requires a Python module/script and arguments after --")
    command = list(args.command)
    if command and command[0] == "--":
        command.pop(0)
    if not command:
        raise ValueError("start requires a Python module/script and arguments after --")

    root = remote_root()
    unit = _unit_name(args.name)
    python = f"{root}/venv/bin/python"
    tokens = [
        "systemd-run",
        "--quiet",
        "--collect",
        f"--unit={unit}",
        "--property=Type=exec",
        f"--property=WorkingDirectory={root}",
        f"--property=MemoryHigh={args.memory_high}",
        f"--property=MemoryMax={args.memory_max}",
        f"--property=CPUQuota={args.cpu_quota}",
        f"--property=RuntimeMaxSec={args.runtime_max}",
        "--property=TasksMax=64",
        "--property=Nice=10",
        f"--setenv=PYTHONPATH={remote_pythonpath(root)}",
        "--setenv=PROBIGA_ANALYSIS_KLINE_FEATURE_MODE=streaming",
        python,
        *command,
    ]
    result = _run(client, " ".join(shlex.quote(token) for token in tokens))
    result["unit"] = unit
    return result


def _status(args: argparse.Namespace, client: paramiko.SSHClient) -> dict[str, Any]:
    unit = _unit_name(args.name)
    properties = (
        "Id,LoadState,ActiveState,SubState,Result,ExecMainStatus,"
        "ExecMainStartTimestamp,ExecMainExitTimestamp,MemoryCurrent,MemoryPeak,"
        "CPUUsageNSec,TasksCurrent,RuntimeMaxUSec,ExecMainPID"
    )
    service = _run(
        client,
        f"systemctl show {shlex.quote(unit)} --property={shlex.quote(properties)}",
    )
    memory = _run(client, "free -m")
    scheduler = _run(
        client,
        "systemctl show probiga-scheduler "
        "--property=ActiveState,SubState,MemoryCurrent,MemoryPeak,TasksCurrent",
    )
    return {"unit": unit, "service": service, "server_memory": memory, "scheduler": scheduler}


def _journal(args: argparse.Namespace, client: paramiko.SSHClient) -> dict[str, Any]:
    unit = _unit_name(args.name)
    lines = max(1, min(int(args.lines), 2000))
    return {
        "unit": unit,
        "journal": _run(
            client,
            f"journalctl -u {shlex.quote(unit)} --no-pager -n {lines} -o cat",
        ),
    }


def _stop(args: argparse.Namespace, client: paramiko.SSHClient) -> dict[str, Any]:
    unit = _unit_name(args.name)
    stop = _run(client, f"systemctl stop {shlex.quote(unit)}", timeout=120)
    return {"unit": unit, "stop": stop, "status": _status(args, client)}


def _tune(args: argparse.Namespace, client: paramiko.SSHClient) -> dict[str, Any]:
    unit = _unit_name(args.name)
    properties = [
        f"MemoryHigh={args.memory_high}",
        f"MemoryMax={args.memory_max}",
        f"CPUQuota={args.cpu_quota}",
    ]
    command = " ".join(
        ["systemctl", "set-property", shlex.quote(unit), *(shlex.quote(item) for item in properties)]
    )
    tuned = _run(client, command, timeout=120)
    return {"unit": unit, "tune": tuned, "status": _status(args, client)}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("start", "status", "journal", "stop", "tune"))
    parser.add_argument("--name", required=True)
    parser.add_argument("--memory-high", default="450M")
    parser.add_argument("--memory-max", default="700M")
    parser.add_argument("--cpu-quota", default="70%")
    parser.add_argument("--runtime-max", default="7200")
    parser.add_argument("--lines", type=int, default=120)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.action == "start":
        # Fail before opening SSH.  The legacy launcher cannot prove the
        # active release venv and sealed adata identity as one unit.
        remote_pythonpath(remote_root())
    client = _connect()
    try:
        if args.action == "start":
            payload = _start(args, client)
        elif args.action == "status":
            payload = _status(args, client)
        elif args.action == "journal":
            payload = _journal(args, client)
        elif args.action == "stop":
            payload = _stop(args, client)
        else:
            payload = _tune(args, client)
    finally:
        client.close()
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    if args.action == "start" and int(payload.get("returncode") or 0) != 0:
        return int(payload["returncode"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
