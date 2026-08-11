# -*- coding: utf-8 -*-
"""Atomically upload selected repository files to production with backups."""
from __future__ import annotations

import argparse
import os
import posixpath
import shlex
import subprocess
import sys
import uuid
from datetime import datetime
from pathlib import Path

import paramiko

from remote_support import remote_root, ssh_connect_kwargs


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BACKUP_RETENTION = 2


def _require_manual_deploy_authorization() -> None:
    raise SystemExit(
        "manual production upload is permanently disabled; "
        "use the pinned CI release workflow"
    )


def _verify_clean_tracked_main(files: list[str]) -> str:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=ROOT, check=True, capture_output=True,
            text=True, encoding="utf-8", errors="strict",
        ).stdout.strip()

    try:
        if git("status", "--porcelain"):
            raise SystemExit("manual production upload requires a clean working tree")
        if git("rev-parse", "--abbrev-ref", "HEAD") != "main":
            raise SystemExit("manual production upload requires the main branch")
        head = git("rev-parse", "HEAD")
        if git("rev-parse", "origin/main") != head:
            raise SystemExit("manual production upload requires HEAD == origin/main")
        tracked = set(git("ls-files", "--", *files).splitlines())
        missing = sorted(set(files) - tracked)
        if missing:
            raise SystemExit(f"manual upload files are not Git-tracked: {missing}")
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "tools" / "validate_production_release_boundary.py"),
                "--require-git-anchor",
                "--expected-git-sha",
                head,
            ],
            cwd=ROOT,
            check=True,
            timeout=120,
        )
        return head
    except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        raise SystemExit(f"manual production preflight failed: {exc}") from exc


def _positive_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _run(client: paramiko.SSHClient, command: str, timeout: int = 120) -> str:
    _stdin, stdout, stderr = client.exec_command(command, timeout=timeout)
    output = stdout.read().decode("utf-8", errors="replace").strip()
    error = stderr.read().decode("utf-8", errors="replace").strip()
    status = stdout.channel.recv_exit_status()
    if status:
        raise RuntimeError(f"remote command failed ({status}): {error[-4000:]}")
    return output


def _backup_prune_command(backup_parent: str, keep: int) -> str:
    """Build a remote command that retains only the newest backup directories."""
    if keep < 1:
        raise ValueError("backup retention must be positive")

    # Backup names use a sortable YYYYMMDD_HHMMSS suffix.  Restrict find to
    # direct children created by this tool so unrelated files are untouched.
    quoted_parent = shlex.quote(backup_parent)
    return (
        f"find {quoted_parent} -mindepth 1 -maxdepth 1 -type d "
        "-name 'acceptance_[0-9]*' -printf '%f\\n' | sort -r "
        f"| tail -n +{keep + 1} | while IFS= read -r name; do "
        f"rm -rf -- {quoted_parent}/\"$name\"; done"
    )


def _prune_backup_history(
    client: paramiko.SSHClient,
    backup_parent: str,
    keep: int = DEFAULT_BACKUP_RETENTION,
) -> None:
    _run(client, _backup_prune_command(backup_parent, keep))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", help="Repository-relative files to upload")
    parser.add_argument("--restart", action="append", default=[], help="systemd service to restart")
    parser.add_argument(
        "--backup-retention",
        type=_positive_int,
        default=DEFAULT_BACKUP_RETENTION,
        help=f"Number of newest remote backups to retain (default: {DEFAULT_BACKUP_RETENTION}).",
    )
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()
    _require_manual_deploy_authorization()

    files: list[tuple[str, Path]] = []
    for raw in args.files:
        relative = Path(raw.replace("\\", "/"))
        local = (ROOT / relative).resolve()
        try:
            normalized = local.relative_to(ROOT).as_posix()
        except ValueError as exc:
            raise SystemExit(f"file is outside repository: {raw}") from exc
        if not local.is_file():
            raise SystemExit(f"file does not exist: {local}")
        files.append((normalized, local))

    _verify_clean_tracked_main([relative for relative, _local in files])

    client = paramiko.SSHClient()
    known_hosts = os.environ.get("PROBIGA_SSH_KNOWN_HOSTS", "").strip()
    if not known_hosts:
        raise SystemExit("PROBIGA_SSH_KNOWN_HOSTS is required for production upload")
    known_hosts_path = Path(known_hosts).expanduser().resolve()
    if not known_hosts_path.is_file():
        raise SystemExit(f"SSH known-hosts file does not exist: {known_hosts_path}")
    client.load_host_keys(str(known_hosts_path))
    client.set_missing_host_key_policy(paramiko.RejectPolicy())
    connect_kwargs = ssh_connect_kwargs()
    if (
        connect_kwargs.get("username") == "root"
        and os.environ.get("PROBIGA_ALLOW_ROOT_PRODUCTION_DEPLOY", "").strip() != "1"
    ):
        raise SystemExit("root production upload is disabled")
    client.connect(**connect_kwargs)
    root = remote_root()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_parent = posixpath.join(root, ".codex_backups")
    backup_root = posixpath.join(backup_parent, f"acceptance_{stamp}")
    staged: list[tuple[str, str, str, bool]] = []
    try:
        # Prune before staging so an already large backup history does not
        # prevent this deployment from making its new safety copy.
        _run(client, f"mkdir -p {shlex.quote(backup_parent)}")
        _prune_backup_history(client, backup_parent, args.backup_retention)

        sftp = client.open_sftp()
        try:
            for relative, local in files:
                target = posixpath.join(root, relative)
                temporary = f"{target}.codex-{uuid.uuid4().hex}.tmp"
                _run(client, f"mkdir -p {shlex.quote(posixpath.dirname(target))}")
                sftp.put(str(local), temporary)
                if local.suffix == ".py":
                    _run(
                        client,
                        f"{shlex.quote(posixpath.join(root, 'venv/bin/python'))} -m py_compile {shlex.quote(temporary)}",
                    )
                try:
                    sftp.stat(target)
                    target_exists = True
                except FileNotFoundError:
                    target_exists = False
                staged.append((relative, target, temporary, target_exists))
        finally:
            sftp.close()

        _run(client, f"mkdir -p {shlex.quote(backup_root)}")
        for relative, target, temporary, target_exists in staged:
            backup = posixpath.join(backup_root, relative)
            _run(client, f"mkdir -p {shlex.quote(posixpath.dirname(backup))}")
            if target_exists:
                _run(client, f"cp -a {shlex.quote(target)} {shlex.quote(backup)}")
                _run(client, f"chmod --reference={shlex.quote(target)} {shlex.quote(temporary)}")
            _run(client, f"mv -f {shlex.quote(temporary)} {shlex.quote(target)}")

        # The new backup is now complete.  Keep only the configured number of
        # newest rollback copies, including this deployment's backup.
        _prune_backup_history(client, backup_parent, args.backup_retention)

        service_states = {}
        for service in args.restart:
            _run(client, f"systemctl restart {shlex.quote(service)}")
            service_states[service] = _run(client, f"systemctl is-active {shlex.quote(service)}")
        print(f"backup={backup_root}")
        for relative, _target, _temporary, _target_exists in staged:
            print(f"deployed={relative}")
        for service, state in service_states.items():
            print(f"service={service}:{state}")
        return 0
    finally:
        for _relative, _target, temporary, _target_exists in staged:
            try:
                _run(client, f"rm -f {shlex.quote(temporary)}", timeout=30)
            except Exception as exc:
                print(f"warning: failed to remove remote temporary file {temporary}: {exc}", file=sys.stderr)
        client.close()


if __name__ == "__main__":
    raise SystemExit(main())
