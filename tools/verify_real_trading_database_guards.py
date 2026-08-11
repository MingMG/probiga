#!/usr/bin/env python3
"""Actively verify that production database guards reject real-trading flags."""
from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.env_config import create_tool_engine, load_project_env
from tools.remote_support import (
    remote_pythonpath,
    remote_root,
    ssh_connect_kwargs,
)


def _is_production_root() -> bool:
    raw = str(ROOT).replace("\\", "/").rstrip("/")
    try:
        resolved = str(ROOT.resolve()).replace("\\", "/").rstrip("/")
    except OSError:
        resolved = raw
    production_root = remote_root()
    return raw == production_root or resolved == production_root


def _run_remote() -> int:
    import paramiko

    root = remote_root()
    command = (
        f"cd {shlex.quote(root)} && "
        f"PYTHONPATH={shlex.quote(remote_pythonpath(root))} "
        f"{shlex.quote(root + '/venv/bin/python')} "
        "tools/verify_real_trading_database_guards.py --local"
    )
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(**ssh_connect_kwargs())
    try:
        _stdin, stdout, stderr = client.exec_command(command, timeout=120)
        output = stdout.read().decode("utf-8", errors="replace").strip()
        error = stderr.read().decode("utf-8", errors="replace").strip()
        status = stdout.channel.recv_exit_status()
    finally:
        client.close()
    if output:
        print(output)
    if error:
        print(error, file=sys.stderr)
    return int(status)


def _expect_rejection(engine, *, name: str, statement: str, parameters: dict) -> dict:
    connection = engine.connect()
    transaction = connection.begin()
    try:
        try:
            connection.execute(text(statement), parameters)
        except DBAPIError as exc:
            transaction.rollback()
            message = str(getattr(exc, "orig", exc))
            return {"name": name, "rejected": True, "database_error": message[:500]}
        transaction.rollback()
        return {"name": name, "rejected": False, "database_error": None}
    finally:
        connection.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--local",
        action="store_true",
        help="Probe the current MYSQL_URL instead of production over SSH.",
    )
    args = parser.parse_args()
    if not args.local and not _is_production_root():
        return _run_remote()

    load_project_env()
    engine = create_tool_engine()
    try:
        with engine.connect() as connection:
            account_id = connection.execute(
                text("SELECT account_id FROM st_trade_account_v2 ORDER BY account_id LIMIT 1")
            ).scalar_one_or_none()
            execution_plan_id = connection.execute(
                text(
                    "SELECT execution_plan_id FROM st_execution_plan_v3 "
                    "ORDER BY created_at DESC LIMIT 1"
                )
            ).scalar_one_or_none()
        if not account_id or not execution_plan_id:
            payload = {
                "status": "BLOCK",
                "reason": "GUARD_TEST_TARGET_MISSING",
                "account_id_present": bool(account_id),
                "execution_plan_id_present": bool(execution_plan_id),
            }
            print(json.dumps(payload, ensure_ascii=False, indent=2))
            return 2

        checks = [
            _expect_rejection(
                engine,
                name="trade_account_update_guard",
                statement=(
                    "UPDATE st_trade_account_v2 SET real_trading_enabled = 1 "
                    "WHERE account_id = :target_id"
                ),
                parameters={"target_id": account_id},
            ),
            _expect_rejection(
                engine,
                name="execution_plan_update_guard",
                statement=(
                    "UPDATE st_execution_plan_v3 SET real_order_allowed = 1 "
                    "WHERE execution_plan_id = :target_id"
                ),
                parameters={"target_id": execution_plan_id},
            ),
        ]
        with engine.connect() as connection:
            account_enabled_count = int(
                connection.execute(
                    text(
                        "SELECT COUNT(*) FROM st_trade_account_v2 "
                        "WHERE real_trading_enabled <> 0"
                    )
                ).scalar_one()
            )
            plan_enabled_count = int(
                connection.execute(
                    text(
                        "SELECT COUNT(*) FROM st_execution_plan_v3 "
                        "WHERE real_order_allowed <> 0"
                    )
                ).scalar_one()
            )
        passed = (
            all(check["rejected"] for check in checks)
            and account_enabled_count == 0
            and plan_enabled_count == 0
        )
        payload = {
            "status": "PASS" if passed else "BLOCK",
            "checks": checks,
            "real_trading_enabled_count": account_enabled_count,
            "real_order_allowed_count": plan_enabled_count,
            "persistent_mutation": False,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0 if passed else 2
    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
