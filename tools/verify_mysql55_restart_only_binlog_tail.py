#!/usr/bin/env python3
"""Prove that a post-acceptance MySQL 5.5 binlog advance is restart-only.

This verifier is used only while the cutover freeze guardian holds the source
global read lock.  It accepts the narrow sequence emitted by clean MySQL 5.5
shutdown/startup cycles (Stop and Format_desc events) and rejects every Query,
row, transaction, rotation, or unknown event.  That lets an already sealed
all-table acceptance survive an automatic rollback restart without silently
accepting post-acceptance business writes.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pymysql
from pymysql.cursors import DictCursor

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.mysql55_to_mysql84_data_manifest import read_client_options  # noqa: E402


FORMAT = "probiga.mysql55_restart_only_binlog_tail"
EXPECTED_VERSION = "5.5.20-log"
FREEZE_LOCK = "probiga:mysql55:cutover-freeze"
_FILE_RE = re.compile(r"^(?P<prefix>[A-Za-z0-9_.-]*?)(?P<number>[0-9]{6})$")


class RestartTailError(RuntimeError):
    """The source advanced in a way that is not proven restart-only."""


@dataclass(frozen=True)
class Coordinate:
    file: str
    position: int


@dataclass(frozen=True)
class Event:
    file: str
    position: int
    event_type: str
    server_id: int
    end_position: int


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    if not path.is_absolute() or path.exists():
        raise RestartTailError("restart-tail evidence path must be absolute and new")
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


def _split_filename(value: str) -> tuple[str, int]:
    match = _FILE_RE.fullmatch(value)
    if match is None:
        raise RestartTailError(f"invalid binlog filename: {value}")
    return match.group("prefix"), int(match.group("number"))


def validate_restart_only(
    *,
    accepted: Coordinate,
    current: Coordinate,
    inventory: Sequence[tuple[str, int]],
    events_by_file: Mapping[str, Sequence[Event]],
    source_server_id: int,
) -> tuple[str, ...]:
    """Validate a contiguous sequence of shutdown/startup-only binlogs."""

    names = [name for name, _size in inventory]
    sizes = {name: int(size) for name, size in inventory}
    if accepted.file not in sizes or current.file not in sizes:
        raise RestartTailError("accepted/current binlog is absent from live inventory")
    accepted_index = names.index(accepted.file)
    current_index = names.index(current.file)
    if current_index <= accepted_index:
        raise RestartTailError("restart-only advance must move to a newer binlog file")
    selected = tuple(names[accepted_index : current_index + 1])
    accepted_prefix, accepted_number = _split_filename(accepted.file)
    for offset, name in enumerate(selected):
        prefix, number = _split_filename(name)
        if prefix != accepted_prefix or number != accepted_number + offset:
            raise RestartTailError("restart-only binlog sequence is not contiguous")

    for index, name in enumerate(selected):
        first = index == 0
        last = index == len(selected) - 1
        start = accepted.position if first else 4
        end = current.position if last else sizes[name]
        events = tuple(events_by_file.get(name, ()))
        expected_types = (
            ("Stop",)
            if first
            else (("Format_desc",) if last else ("Format_desc", "Stop"))
        )
        observed_types = tuple(event.event_type for event in events)
        if observed_types != expected_types:
            raise RestartTailError(
                f"binlog {name} contains non-restart events: {observed_types!r}"
            )
        expected_position = start
        for event in events:
            if event.file != name or event.position != expected_position:
                raise RestartTailError(f"binlog {name} event positions are not contiguous")
            if event.server_id != source_server_id:
                raise RestartTailError(f"binlog {name} has an unexpected server id")
            if event.end_position <= event.position:
                raise RestartTailError(f"binlog {name} has an invalid event boundary")
            expected_position = event.end_position
        if expected_position != end:
            raise RestartTailError(f"binlog {name} does not end at the approved boundary")
    return selected


def _quote_log_name(value: str) -> str:
    _split_filename(value)
    return "'" + value.replace("'", "''") + "'"


def verify(args: argparse.Namespace) -> dict[str, Any]:
    accepted_payload = json.loads(args.accepted_binlog_evidence.read_text("utf-8"))
    if (
        accepted_payload.get("status") != "success"
        or accepted_payload.get("mode") != "final-frozen"
    ):
        raise RestartTailError("accepted final binlog evidence is invalid")
    accepted_source = accepted_payload.get("source") or {}
    accepted_master = accepted_source.get("master") or {}
    accepted = Coordinate(
        str(accepted_master.get("file") or ""),
        int(accepted_master.get("position") or 0),
    )

    options = read_client_options(args.source_option_file)
    try:
        connection = pymysql.connect(
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
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT CONNECTION_ID() AS connection_id, @@version AS version, "
                "@@hostname AS hostname, @@server_id AS server_id, @@port AS port, "
                "@@global.log_bin AS log_bin, @@global.binlog_format AS binlog_format, "
                "IS_USED_LOCK(%s) AS freeze_guardian_connection_id",
                (FREEZE_LOCK,),
            )
            source = cursor.fetchone() or {}
            cursor.execute("SHOW MASTER STATUS")
            master = cursor.fetchone() or {}
            cursor.execute("SHOW BINARY LOGS")
            inventory_rows = tuple(cursor.fetchall())

            if (
                str(source.get("version") or "") != EXPECTED_VERSION
                or str(source.get("version") or "")
                != str(accepted_source.get("version") or "")
                or str(source.get("hostname") or "")
                != str(accepted_source.get("hostname") or "")
                or int(source.get("server_id") or 0)
                != int(accepted_source.get("server_id") or 0)
                or int(source.get("port") or 0) != 3306
                or int(source.get("log_bin") or 0) != 1
                or str(source.get("binlog_format") or "").upper() != "STATEMENT"
                or source.get("freeze_guardian_connection_id") is None
                or int(source.get("freeze_guardian_connection_id") or 0)
                == int(source.get("connection_id") or 0)
            ):
                raise RestartTailError(
                    "source identity or cutover freeze guardian is not approved"
                )

            current = Coordinate(
                str(master.get("File") or ""), int(master.get("Position") or 0)
            )
            inventory = tuple(
                (str(row.get("Log_name") or ""), int(row.get("File_size") or 0))
                for row in inventory_rows
            )
            names = [name for name, _size in inventory]
            if accepted.file not in names or current.file not in names:
                raise RestartTailError("accepted/current binlog is absent from inventory")
            selected_names = names[names.index(accepted.file) : names.index(current.file) + 1]
            events_by_file: dict[str, tuple[Event, ...]] = {}
            for index, name in enumerate(selected_names):
                start = accepted.position if index == 0 else 4
                cursor.execute(
                    f"SHOW BINLOG EVENTS IN {_quote_log_name(name)} FROM {int(start)}"
                )
                rows = tuple(cursor.fetchall())
                events_by_file[name] = tuple(
                    Event(
                        file=str(row.get("Log_name") or ""),
                        position=int(row.get("Pos") or 0),
                        event_type=str(row.get("Event_type") or ""),
                        server_id=int(row.get("Server_id") or 0),
                        end_position=int(row.get("End_log_pos") or 0),
                    )
                    for row in rows
                )
    finally:
        connection.close()

    selected = validate_restart_only(
        accepted=accepted,
        current=current,
        inventory=inventory,
        events_by_file=events_by_file,
        source_server_id=int(source["server_id"]),
    )
    result = {
        "format": FORMAT,
        "schema_version": 1,
        "status": "passed",
        "verified_at_utc": _utc_now(),
        "accepted_coordinate": asdict(accepted),
        "current_coordinate": asdict(current),
        "source": {
            "version": source["version"],
            "hostname": source["hostname"],
            "server_id": int(source["server_id"]),
            "port": int(source["port"]),
            "freeze_guardian_connection_id": int(
                source["freeze_guardian_connection_id"]
            ),
        },
        "verified_files": list(selected),
        "events": [
            asdict(event)
            for name in selected
            for event in events_by_file[name]
        ],
        "business_or_unknown_event_count": 0,
    }
    _atomic_json(args.evidence, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-option-file", type=Path, required=True)
    parser.add_argument("--accepted-binlog-evidence", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        verify(args)
    except (RestartTailError, OSError, ValueError, pymysql.MySQLError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
