# -*- coding: utf-8 -*-
"""Provision one AI bridge token into local and production .env files."""
from __future__ import annotations

import os
import posixpath
import secrets
import shlex
import tempfile
from datetime import datetime
from pathlib import Path

import paramiko

from remote_support import (
    production_ssh_client,
    production_ssh_connect_kwargs,
    remote_root,
)

ROOT = Path(__file__).resolve().parents[1]
NAME = "PROBIGA_AI_BRIDGE_TOKEN"


def _value(text: str) -> str:
    prefix = NAME + "="
    for line in text.splitlines():
        if line.startswith(prefix):
            return line[len(prefix) :].strip().strip('"').strip("'")
    return ""


def _updated(text: str, token: str) -> str:
    newline = "\r\n" if "\r\n" in text else "\n"
    lines = text.splitlines()
    replacement = f"{NAME}={token}"
    found = False
    for index, line in enumerate(lines):
        if line.startswith(NAME + "="):
            lines[index] = replacement
            found = True
    if not found:
        if lines and lines[-1].strip():
            lines.append("")
        lines.extend(["# ProBigA website-to-local AI bridge worker", replacement])
    return newline.join(lines) + newline


def _run(client: paramiko.SSHClient, command: str) -> None:
    _stdin, stdout, stderr = client.exec_command(command, timeout=30)
    error = stderr.read().decode("utf-8", errors="replace")
    if stdout.channel.recv_exit_status():
        raise RuntimeError(error[-2000:])


def main() -> int:
    local_path = ROOT / ".env"
    local_text = local_path.read_text(encoding="utf-8") if local_path.exists() else ""

    client = production_ssh_client(paramiko)
    client.connect(**production_ssh_connect_kwargs())
    remote_path = posixpath.join(remote_root(), ".env")
    try:
        sftp = client.open_sftp()
        try:
            with sftp.open(remote_path, "r") as remote_file:
                remote_text = remote_file.read().decode("utf-8")
        finally:
            sftp.close()

        local_token = _value(local_text)
        remote_token = _value(remote_text)
        if local_token and remote_token and not secrets.compare_digest(local_token, remote_token):
            raise RuntimeError("Local and production AI bridge tokens differ; refusing to overwrite either")
        token = local_token or remote_token or secrets.token_urlsafe(48)

        local_updated = _updated(local_text, token)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        file_descriptor, temporary_name = tempfile.mkstemp(prefix=".env.ai-bridge-", dir=str(local_path.parent))
        try:
            with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="") as temporary:
                temporary.write(local_updated)
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_name, local_path)
        finally:
            if os.path.exists(temporary_name):
                os.unlink(temporary_name)

        if not remote_token:
            remote_updated = _updated(remote_text, token).encode("utf-8")
            temporary_remote = remote_path + ".ai-bridge.tmp"
            backup = remote_path + ".before-ai-bridge-" + datetime.now().strftime("%Y%m%d_%H%M%S")
            sftp = client.open_sftp()
            try:
                with sftp.open(temporary_remote, "wb") as remote_file:
                    remote_file.write(remote_updated)
            finally:
                sftp.close()
            _run(
                client,
                " && ".join(
                    [
                        f"cp -a {shlex.quote(remote_path)} {shlex.quote(backup)}",
                        f"chmod --reference={shlex.quote(remote_path)} {shlex.quote(temporary_remote)}",
                        f"mv -f {shlex.quote(temporary_remote)} {shlex.quote(remote_path)}",
                    ]
                ),
            )
        print("AI bridge token configured locally and in production (value not displayed)")
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
