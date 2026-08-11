#!/usr/bin/env python3
"""Hold a MySQL 5.5 global read lock through final cutover acceptance.

The process acquires a named operator lock and ``FLUSH TABLES WITH READ LOCK``,
then continuously publishes a short-lived heartbeat.  It releases the lock
only for an explicit abort.  For a successful cutover, the controller first
writes ``MYSQL55_SERVICE_STOPPED`` to the stop file and then stops the legacy
service; loss of the guarded connection is accepted only in that state.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql
from pymysql.cursors import DictCursor

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.mysql55_to_mysql84_data_manifest import read_client_options  # noqa: E402


ACK = "I_CONFIRM_BUSINESS_WRITERS_STOPPED_AND_ACQUIRE_GLOBAL_READ_LOCK"
NAMED_LOCK = "probiga:mysql55:cutover-freeze"
ABORT = "ABORT"
SERVICE_STOPPED = "MYSQL55_SERVICE_STOPPED"
EXPECTED_VERSION = "5.5.20-log"


class FreezeError(RuntimeError):
    """The source freeze lock cannot be safely established or maintained."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _atomic_json(path: Path, payload: Mapping[str, Any], *, replace: bool) -> None:
    if not path.is_absolute() or (path.exists() and not replace):
        raise FreezeError("freeze evidence path must be absolute and new")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with temporary.open("x", encoding="utf-8", newline="\n") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _read_stop(path: Path) -> str | None:
    try:
        return path.read_text(encoding="ascii").strip()
    except FileNotFoundError:
        return None


def _source_state(connection) -> dict[str, Any]:
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT CONNECTION_ID() AS connection_id, @@version AS version, "
            "@@port AS port, @@server_id AS server_id, @@global.log_bin AS log_bin, "
            "@@global.binlog_format AS binlog_format"
        )
        row = cursor.fetchone() or {}
        cursor.execute("SHOW MASTER STATUS")
        master = cursor.fetchone() or {}
        cursor.execute(
            "SELECT COUNT(*) AS n FROM information_schema.PROCESSLIST "
            "WHERE ID <> CONNECTION_ID() AND STATE LIKE 'Waiting for global read lock%'"
        )
        blocked = cursor.fetchone() or {}
    result = {
        "connection_id": int(row.get("connection_id") or 0),
        "version": str(row.get("version") or ""),
        "port": int(row.get("port") or 0),
        "server_id": int(row.get("server_id") or 0),
        "log_bin": int(row.get("log_bin") or 0),
        "binlog_format": str(row.get("binlog_format") or "").upper(),
        "master_file": str(master.get("File") or ""),
        "master_position": int(master.get("Position") or 0),
        "blocked_writer_count": int(blocked.get("n") or 0),
    }
    if (
        result["version"] != EXPECTED_VERSION
        or result["port"] != 3306
        or result["server_id"] != 55
        or result["log_bin"] != 1
        or result["binlog_format"] != "STATEMENT"
        or not result["master_file"]
        or result["master_position"] < 4
    ):
        raise FreezeError("source identity/binlog state is not the approved MySQL 5.5 endpoint")
    return result


def _connect(option_file: Path):
    options = read_client_options(option_file)
    try:
        return pymysql.connect(
            **options,
            charset="utf8mb4",
            autocommit=True,
            cursorclass=DictCursor,
            connect_timeout=15,
            read_timeout=30,
            write_timeout=30,
        )
    finally:
        options.clear()


def hold(args: argparse.Namespace) -> int:
    if args.ack != ACK:
        raise FreezeError("exact global-read-lock acknowledgement is required")
    if (
        not args.ready_evidence.is_absolute()
        or not args.heartbeat.is_absolute()
        or not args.stop_file.is_absolute()
        or not args.final_evidence.is_absolute()
    ):
        raise FreezeError("freeze control/evidence paths must be absolute")
    if (
        args.ready_evidence.exists()
        or args.heartbeat.exists()
        or args.stop_file.exists()
        or args.final_evidence.exists()
    ):
        raise FreezeError("freeze control artifacts must be new")
    if not 2 <= args.heartbeat_seconds <= 30:
        raise FreezeError("heartbeat interval must be in 2..30 seconds")

    connection = _connect(args.source_option_file)
    named_lock = False
    table_lock = False
    expected_service_stop = False
    started = _utc_now()
    stopping = False

    def request_stop(_signum, _frame) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, request_stop)
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT GET_LOCK(%s, 0) AS acquired", (NAMED_LOCK,))
            row = cursor.fetchone() or {}
            if int(row.get("acquired") or 0) != 1:
                raise FreezeError("another cutover freeze guardian is active")
            named_lock = True
            cursor.execute("FLUSH TABLES WITH READ LOCK")
            table_lock = True
        state = _source_state(connection)
        ready = {
            "schema_version": 1,
            "tool": "hold_mysql55_cutover_lock",
            "status": "locked",
            "pid": os.getpid(),
            "started_at_utc": started,
            "locked_at_utc": _utc_now(),
            "source": state,
            "global_read_lock_held": True,
            "named_lock_held": True,
            "unlock_requires_explicit_abort": True,
        }
        _atomic_json(args.ready_evidence, ready, replace=False)

        while True:
            stop = _read_stop(args.stop_file)
            if stop not in {None, ABORT, SERVICE_STOPPED}:
                raise FreezeError("freeze stop file contains an invalid command")
            if stop == ABORT or stopping:
                with connection.cursor() as cursor:
                    cursor.execute("UNLOCK TABLES")
                    table_lock = False
                outcome = "aborted"
                break
            if stop == SERVICE_STOPPED:
                expected_service_stop = True
            try:
                state = _source_state(connection)
            except pymysql.MySQLError:
                if expected_service_stop:
                    outcome = "service_stopped"
                    table_lock = False
                    named_lock = False
                    break
                raise FreezeError("source lock connection was lost unexpectedly")
            heartbeat = {
                "schema_version": 1,
                "tool": "hold_mysql55_cutover_lock",
                "status": "locked",
                "pid": os.getpid(),
                "heartbeat_at_utc": _utc_now(),
                "source": state,
                "global_read_lock_held": True,
                "expected_service_stop": expected_service_stop,
                "blocked_writer_detected": state["blocked_writer_count"] > 0,
            }
            _atomic_json(args.heartbeat, heartbeat, replace=args.heartbeat.exists())
            if state["blocked_writer_count"] > 0:
                raise FreezeError("a business writer attempted to write after the global freeze")
            time.sleep(args.heartbeat_seconds)

        final = {
            **ready,
            "status": outcome,
            "finished_at_utc": _utc_now(),
            "global_read_lock_held": False,
            "named_lock_held": False,
            "service_stop_was_preannounced": expected_service_stop,
        }
        _atomic_json(args.final_evidence, final, replace=False)
        return 0
    finally:
        if table_lock:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("UNLOCK TABLES")
            except Exception:
                pass
        if named_lock:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT RELEASE_LOCK(%s)", (NAMED_LOCK,))
            except Exception:
                pass
        try:
            connection.close()
        except Exception:
            pass


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-option-file", type=Path, required=True)
    parser.add_argument("--ready-evidence", type=Path, required=True)
    parser.add_argument("--heartbeat", type=Path, required=True)
    parser.add_argument("--stop-file", type=Path, required=True)
    parser.add_argument("--final-evidence", type=Path, required=True)
    parser.add_argument("--heartbeat-seconds", type=int, default=5)
    parser.add_argument("--ack", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        return hold(args)
    except (FreezeError, OSError, ValueError, pymysql.MySQLError) as exc:
        try:
            if args.final_evidence.is_absolute() and not args.final_evidence.exists():
                _atomic_json(
                    args.final_evidence,
                    {
                        "schema_version": 1,
                        "tool": "hold_mysql55_cutover_lock",
                        "status": "failed",
                        "finished_at_utc": _utc_now(),
                        "failure": str(exc),
                        "global_read_lock_held": False,
                    },
                    replace=False,
                )
        except Exception:
            pass
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
