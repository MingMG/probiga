#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Unified operator entrypoint for common ProBigA maintenance checks."""
from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from server.common.process_env import build_child_env


@dataclass(frozen=True)
class OpCommand:
    description: str
    args: tuple[str, ...]
    timeout_seconds: int


COMMANDS: dict[str, OpCommand] = {
    "check-db": OpCommand("Show database table sizes and update times.", ("deploy/check_db.py",), 60),
    "check-tasks": OpCommand("Show scheduler task table status.", ("deploy/check_tasks.py",), 60),
    "check-running": OpCommand("Show running and recently completed scheduler tasks.", ("deploy/check_running.py",), 60),
    "security-check": OpCommand("Verify public/admin API protection.", ("tools/check_production_security.py",), 60),
    "quality-gate": OpCommand("Run the data quality gate.", ("tools/ensure_quality_gate.py",), 5 * 60),
    "run-all-changes": OpCommand("Run the curated post-change update workflow.", ("tools/run_all_changes.py",), 30 * 60),
}


def build_command(name: str) -> list[str]:
    spec = COMMANDS[name]
    return [sys.executable, *spec.args]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ProBigA operator command launcher.")
    parser.add_argument("command", nargs="?", choices=sorted(COMMANDS), help="Command alias to run.")
    parser.add_argument("--list", action="store_true", help="List available command aliases.")
    parser.add_argument("--timeout-seconds", type=int, default=0, help="Override the command timeout.")
    return parser.parse_args(argv)


def list_commands() -> None:
    for name in sorted(COMMANDS):
        print(f"{name:<18} {COMMANDS[name].description}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.list or not args.command:
        list_commands()
        return 0 if args.list else 2

    spec = COMMANDS[args.command]
    timeout = max(1, int(args.timeout_seconds or spec.timeout_seconds))
    cmd = build_command(args.command)
    print(f"RUN {' '.join(cmd)}", flush=True)
    try:
        result = subprocess.run(
            cmd,
            cwd=str(ROOT),
            env=build_child_env(ROOT),
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT {args.command} exceeded {timeout}s", file=sys.stderr, flush=True)
        return 124
    return int(result.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
