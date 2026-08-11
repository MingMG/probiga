from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from tools import run_mysql55_to_mysql84_binlog_catchup as catchup


def _manifest(tmp_path: Path) -> Path:
    dump = tmp_path / "snapshot.sql"
    dump.write_bytes(b"snapshot")
    manifest = tmp_path / "snapshot.manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "status": "success",
                "binlog_coordinates_captured": True,
                "snapshot_binlog_coordinates": {
                    "file": "mysql-bin.000001",
                    "position": 6592943,
                },
                "source_preflight": {
                    "identity": {
                        "version": "5.5.20-log",
                        "hostname": "legacy-host",
                        "server_id": 55,
                    }
                },
                "artifacts": {
                    "dump": {
                        "path": str(dump.resolve()),
                        "bytes": dump.stat().st_size,
                        "sha256": hashlib.sha256(dump.read_bytes()).hexdigest(),
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return manifest


def test_load_snapshot_identity_binds_source_and_coordinate(tmp_path: Path):
    manifest = _manifest(tmp_path)

    identity = catchup.load_snapshot_identity(manifest.resolve())

    assert identity.coordinate == catchup.Coordinate("mysql-bin.000001", 6592943)
    assert identity.source_server_id == 55
    assert identity.manifest_sha256 == hashlib.sha256(manifest.read_bytes()).hexdigest()


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda value: value.update(status="failed"), "does not report success"),
        (
            lambda value: value.update(binlog_coordinates_captured=False),
            "did not capture",
        ),
        (
            lambda value: value["source_preflight"]["identity"].update(server_id=0),
            "not the configured source",
        ),
        (
            lambda value: value["snapshot_binlog_coordinates"].update(file="../bad"),
            "unsafe binlog coordinate",
        ),
    ),
)
def test_load_snapshot_identity_rejects_drift(tmp_path: Path, mutation, message: str):
    manifest = _manifest(tmp_path)
    value = json.loads(manifest.read_text(encoding="utf-8"))
    mutation(value)
    manifest.write_text(json.dumps(value), encoding="utf-8")

    with pytest.raises(catchup.CatchupError, match=message):
        catchup.load_snapshot_identity(manifest.resolve())


def test_build_segment_plan_handles_rotation_and_resume():
    inventory = (
        ("mysql-bin.000001", 1000),
        ("mysql-bin.000002", 2000),
        ("mysql-bin.000003", 3000),
    )

    plan = catchup.build_segment_plan(
        catchup.Coordinate("mysql-bin.000001", 500),
        catchup.Coordinate("mysql-bin.000003", 2500),
        inventory,
    )

    assert [(item.file, item.start_position, item.stop_position) for item in plan] == [
        ("mysql-bin.000001", 500, 1000),
        ("mysql-bin.000002", 4, 2000),
        ("mysql-bin.000003", 4, 2500),
    ]
    assert plan[0].cursor_after == catchup.Coordinate("mysql-bin.000002", 4)
    assert plan[-1].cursor_after == catchup.Coordinate("mysql-bin.000003", 2500)


def test_build_segment_plan_is_noop_at_same_coordinate():
    coordinate = catchup.Coordinate("mysql-bin.000001", 500)
    assert catchup.build_segment_plan(
        coordinate, coordinate, (("mysql-bin.000001", 1000),)
    ) == ()


def test_build_segment_plan_rejects_reversed_or_missing_coordinates():
    inventory = (("mysql-bin.000001", 1000), ("mysql-bin.000002", 2000))
    with pytest.raises(catchup.CatchupError, match="after"):
        catchup.build_segment_plan(
            catchup.Coordinate("mysql-bin.000002", 5),
            catchup.Coordinate("mysql-bin.000001", 900),
            inventory,
        )
    with pytest.raises(catchup.CatchupError, match="absent"):
        catchup.build_segment_plan(
            catchup.Coordinate("mysql-bin.000000", 4),
            catchup.Coordinate("mysql-bin.000001", 900),
            inventory,
        )


def _safe_mysqlbinlog_sql() -> bytes:
    return (
        b"/*!50530 SET @@SESSION.PSEUDO_SLAVE_MODE=1*/;\n"
        b"/*!32316 SET @OLD_SQL_LOG_BIN=@@SQL_LOG_BIN, SQL_LOG_BIN=0*/;\n"
        b"use `probiga`/*!*/;\n"
        b"BEGIN/*!*/;\n"
        b"INSERT INTO st_probe(id) VALUES (1)/*!*/;\n"
        b"COMMIT/*!*/;\n"
        b"/*!32316 SET SQL_LOG_BIN=@OLD_SQL_LOG_BIN*/;\n"
    )


def test_audit_extracted_sql_allows_business_dml(tmp_path: Path):
    path = tmp_path / "segment.sql"
    path.write_bytes(_safe_mysqlbinlog_sql())

    result = catchup.audit_extracted_sql(path)

    assert result["used_schemas"] == ["probiga"]
    assert result["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()


def test_audit_extracted_sql_allows_source_identifier_inside_sql(tmp_path: Path):
    path = tmp_path / "segment.sql"
    path.write_bytes(
        b"use `probiga`/*!*/;\n"
        b"CREATE TABLE st_probe (\n"
        b"  source VARCHAR(64) NOT NULL,\n"
        b"  value INT NOT NULL\n"
        b")/*!*/;\n"
        b"INSERT INTO st_probe(source, value) VALUES ('x', 1)\n"
        b"ON DUPLICATE KEY UPDATE\n"
        b"  source = VALUES(source),\n"
        b"  value = VALUES(value)/*!*/;\n"
    )

    result = catchup.audit_extracted_sql(path)

    assert result["used_schemas"] == ["probiga"]


def test_audit_extracted_sql_allows_idempotent_business_schema_creation(
    tmp_path: Path,
):
    path = tmp_path / "segment.sql"
    path.write_bytes(
        b"CREATE DATABASE IF NOT EXISTS `probiga` DEFAULT CHARACTER SET utf8mb4 "
        b"COLLATE utf8mb4_unicode_ci\n/*!*/;\n"
    )

    catchup.audit_extracted_sql(path)


def test_audit_extracted_sql_handles_a_large_non_admin_line(tmp_path: Path):
    path = tmp_path / "segment.sql"
    path.write_bytes(b"INSERT INTO probiga.st_probe VALUES ('" + b"x" * 1_000_000 + b"');\n")

    catchup.audit_extracted_sql(path)


def test_bounded_regex_search_detects_a_token_across_windows():
    payload = b"x" * (catchup._REGEX_SCAN_CHUNK - 8) + b" mysql.user "

    assert catchup._bounded_regex_search(catchup._SYSTEM_QUALIFIER_RE, payload)


@pytest.mark.parametrize(
    "payload",
    (
        b"use `mysql`/*!*/;\nUPDATE user SET x=1;\n",
        b"UPDATE mysql.user SET x=1;\n",
        b"CREATE USER bad@localhost;\n",
        b"SET GLOBAL read_only=0;\n",
        b"SET SESSION sql_log_bin=1;\n",
        b"source C:/bad.sql\n",
        b"CREATE DATABASE evil;\n",
        b"DROP DATABASE probiga;\n",
    ),
)
def test_audit_extracted_sql_rejects_admin_or_system_events(
    tmp_path: Path, payload: bytes
):
    path = tmp_path / "segment.sql"
    path.write_bytes(payload)

    with pytest.raises(catchup.CatchupError):
        catchup.audit_extracted_sql(path)


def test_checkpoint_is_bound_to_snapshot_and_target(tmp_path: Path):
    snapshot = catchup.load_snapshot_identity(_manifest(tmp_path).resolve())
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_text(
        json.dumps(
            {
                "format": catchup.CHECKPOINT_FORMAT,
                "status": "success",
                "snapshot_manifest_sha256": snapshot.manifest_sha256,
                "target_server_uuid": "f40c3202-9260-11f1-86ae-74d4dd7f8500",
                "cursor": {"file": "mysql-bin.000002", "position": 400},
            }
        ),
        encoding="utf-8",
    )

    assert catchup._load_checkpoint(
        checkpoint,
        snapshot=snapshot,
        target_uuid="f40c3202-9260-11f1-86ae-74d4dd7f8500",
    ) == catchup.Coordinate("mysql-bin.000002", 400)

    with pytest.raises(catchup.CatchupError, match="different target"):
        catchup._load_checkpoint(
            checkpoint,
            snapshot=snapshot,
            target_uuid="810354d6-9061-11f1-84ae-74d4dd7f8500",
        )


def test_final_frozen_requires_ack_and_idle_source():
    source = catchup.SourceObservation(
        version="5.5.20-log",
        version_comment="MySQL Community Server (GPL)",
        hostname="legacy-host",
        port=3306,
        server_id=55,
        log_bin=True,
        binlog_format="STATEMENT",
        read_only=False,
        connection_id=1,
        master=catchup.Coordinate("mysql-bin.000001", 100),
        binary_logs=(("mysql-bin.000001", 100),),
        active_non_sleep_sessions=0,
        active_transactions=0,
        observed_at_utc="2026-08-07T13:00:00+00:00",
        freeze_guardian_connection_id=2,
    )
    with pytest.raises(catchup.CatchupError, match="exact"):
        catchup._validate_mode("final-frozen", None, source)
    catchup._validate_mode("final-frozen", catchup.FINAL_FROZEN_ACK, source)

    busy = replace(source, active_transactions=1)
    with pytest.raises(catchup.CatchupError, match="active"):
        catchup._validate_mode("final-frozen", catchup.FINAL_FROZEN_ACK, busy)

    missing_guardian = replace(source, freeze_guardian_connection_id=None)
    with pytest.raises(catchup.CatchupError, match="guardian"):
        catchup._validate_mode(
            "final-frozen", catchup.FINAL_FROZEN_ACK, missing_guardian
        )
