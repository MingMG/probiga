from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.mysql55_to_mysql84_data_manifest import load_config
from tools.run_mysql84_final_acceptance import (
    AcceptanceError,
    _build_steps,
    _dump_sha256,
    _new_output,
    _validate_data_config,
    _validate_reviewed_migration_preflight_resume,
    validate_freeze_guard,
)


TARGET_UUID = "f40c3202-9260-11f1-86ae-74d4dd7f8500"
ZERO_DATES = [
    "probiga.jq_strategy_meta.created_at",
    "probiga.jq_strategy_meta.updated_at",
    "probiga.jq_strategy_picks.created_at",
    "probiga.st_daily_review.etl_sync_at",
    "probiga.st_portfolio_analysis_log.created_at",
    "probiga.st_portfolio_trans_log.created_at",
    "probiga.st_recommended_stocks.created_at",
    "probiga.st_user_portfolio.etl_sync_at",
]


def _write_data_config(path: Path, *, source_version: str = "5.5.20-log") -> None:
    path.write_text(
        json.dumps(
            {
                "format_version": 1,
                "schemas": ["biga", "probiga", "probiga_qmt_history"],
                "endpoints": {
                    "source": {
                        "version": source_version,
                        "port": 3306,
                        "server_uuid": None,
                        "legacy_identity_sha256": "a" * 64,
                        "require_tls": False,
                    },
                    "target": {
                        "version": "8.4.11",
                        "port": 33090,
                        "server_uuid": TARGET_UUID,
                        "legacy_identity_sha256": None,
                        "require_tls": True,
                    },
                },
                "execution": {"max_workers": 2},
                "counts": {"mode": "all", "tables": []},
                "boundaries": {
                    "primary_key_mode": "all",
                    "primary_key_tables": [],
                    "date_columns": {},
                },
                "aggregates": {},
                "hashes": {},
                "legacy_zero_date_columns": ZERO_DATES,
                "catalog_comparison": {
                    "mode": "exact",
                    "source_catalog_sha256": "b" * 64,
                    "target_catalog_sha256": "b" * 64,
                    "source_table_count": 0,
                    "target_table_count": 0,
                    "target_only_tables": [],
                    "target_extended_columns": {},
                },
            }
        ),
        encoding="utf-8",
    )


def test_data_config_requires_binlog_enabled_source_version(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    _write_data_config(path)
    config = load_config(path)
    _validate_data_config(config, target_uuid=TARGET_UUID, target_port=33090)

    _write_data_config(path, source_version="5.5.20")
    with pytest.raises(AcceptanceError, match="5.5.20-log"):
        _validate_data_config(
            load_config(path), target_uuid=TARGET_UUID, target_port=33090
        )


def test_dump_manifest_requires_successful_binlog_coordinate(tmp_path: Path) -> None:
    path = tmp_path / "dump.json"
    value = {
        "mode": "online-rehearsal",
        "binlog_coordinates_captured": True,
        "snapshot_binlog_coordinates": {"file": "mysql-bin.000001", "position": 4},
        "mysqldump": {"return_code": 0},
        "artifacts": {"dump": {"sha256": "b" * 64}},
    }
    path.write_text(json.dumps(value), encoding="utf-8")
    assert _dump_sha256(path) == "b" * 64
    value["snapshot_binlog_coordinates"] = None
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(AcceptanceError, match="binlog snapshot"):
        _dump_sha256(path)


def test_freeze_guard_requires_fresh_live_lock_heartbeat(tmp_path: Path) -> None:
    ready = tmp_path / "ready.json"
    heartbeat = tmp_path / "heartbeat.json"
    ready.write_text(
        json.dumps(
            {
                "tool": "hold_mysql55_cutover_lock",
                "status": "locked",
                "pid": os.getpid(),
                "global_read_lock_held": True,
                "named_lock_held": True,
            }
        ),
        encoding="utf-8",
    )
    heartbeat_value = {
        "tool": "hold_mysql55_cutover_lock",
        "status": "locked",
        "pid": os.getpid(),
        "heartbeat_at_utc": datetime.now(timezone.utc).isoformat(),
        "global_read_lock_held": True,
        "blocked_writer_detected": False,
        "source": {
            "version": "5.5.20-log",
            "port": 3306,
            "server_id": 55,
            "log_bin": 1,
            "binlog_format": "STATEMENT",
            "master_file": "mysql-bin.000001",
            "master_position": 42,
        },
    }
    heartbeat.write_text(json.dumps(heartbeat_value), encoding="utf-8")
    assert validate_freeze_guard(ready, heartbeat)["pid"] == os.getpid()
    heartbeat_value["blocked_writer_detected"] = True
    heartbeat.write_text(json.dumps(heartbeat_value), encoding="utf-8")
    with pytest.raises(AcceptanceError, match="safely holding"):
        validate_freeze_guard(ready, heartbeat)


def test_resume_allows_only_manifest_checkpoint_for_incomplete_step(
    tmp_path: Path,
) -> None:
    checkpoint = tmp_path / "source-data.checkpoint.json"
    output = tmp_path / "source-data.json"
    checkpoint.write_text("{}", encoding="utf-8")
    output.write_text("{}", encoding="utf-8")
    _new_output(checkpoint, resume=True, completed=False)
    with pytest.raises(AcceptanceError, match="untrusted"):
        _new_output(output, resume=True, completed=False)


def test_reviewed_migration_preflight_resume_requires_sealed_existing_ledgers(
    tmp_path: Path,
) -> None:
    failure_dir = tmp_path / "failure"
    failure_dir.mkdir()
    (failure_dir / "failed-10-restored-migrations.json").write_text(
        json.dumps(
            {
                "status": "failed",
                "failure": (
                    "V4 table column drift detected for "
                    "st_factor_definition_v4: reviewed"
                ),
            }
        ),
        encoding="utf-8",
    )
    (failure_dir / "failed-10-trigger-window.json").write_text(
        json.dumps(
            {
                "outcome": "child_failed",
                "named_lock_acquired": True,
                "named_lock_released": True,
                "production_activation_allowed": False,
                "child": {"started": True, "return_code": 2},
                "trust_transition": {
                    "enable_attempted": True,
                    "enabled_verified": True,
                    "restore_attempted": True,
                    "restore_primary_verified": True,
                    "restore_secondary_verified": True,
                },
            }
        ),
        encoding="utf-8",
    )
    plan = {
        "v2": [{"version": "v2-1", "status": "exists"}],
        "v3": [{"version": "v3-1", "status": "exists"}],
        "v4": [{"version": "v4-1", "status": "exists"}],
    }
    ledger = {
        "schema_migration_v2": [{"version": "v2-1"}],
        "schema_migration_v3": [{"version": "v3-1"}],
        "schema_migration_v4": [{"version": "v4-1"}],
    }
    preflight = tmp_path / "preflight.json"
    value = {
        "status": "plan_only",
        "mode": "final-frozen",
        "finished_at_utc": "2026-08-10T00:04:00+00:00",
        "target": {
            "server_uuid": TARGET_UUID,
            "port": 33090,
            "datadir": "F:\\target",
        },
        "schema_identity": {
            "server_uuid": TARGET_UUID,
            "port": 33090,
            "tls_cipher": "TLS_AES_256_GCM_SHA384",
        },
        "ledger_before": ledger,
        "ledger_after": ledger,
        "plan": plan,
    }
    preflight.write_text(json.dumps(value), encoding="utf-8")
    paths = {
        "reviewed_migration_preflight": preflight,
        "reviewed_migration_failure_dir": failure_dir,
    }
    current_plan = {
        "expected_target_uuid": TARGET_UUID,
        "expected_target_port": 33090,
        "expected_target_datadir": "F:\\target",
    }
    state = {
        "failed_at_utc": "2026-08-10T00:03:30Z",
    }

    result = _validate_reviewed_migration_preflight_resume(
        state=state,
        current_plan=current_plan,
        paths=paths,
    )
    assert result["version_counts"] == {"v2": 1, "v3": 1, "v4": 1}

    value["plan"]["v4"][0]["status"] = "would_apply"
    preflight.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(AcceptanceError, match="v4 migrations"):
        _validate_reviewed_migration_preflight_resume(
            state=state,
            current_plan=current_plan,
            paths=paths,
        )


def test_compatibility_repairs_run_before_data_capture_and_comparison(
    tmp_path: Path,
) -> None:
    paths = {
        name: tmp_path / name
        for name in (
            "python",
            "source_option",
            "dump_manifest",
            "binlog_dir",
            "mysqlbinlog",
            "mysql",
            "target_admin",
            "target_ca",
            "migration_option",
            "target_datadir",
            "data_config",
        )
    }
    paths["out"] = tmp_path
    args = SimpleNamespace(
        expected_target_uuid=TARGET_UUID,
        expected_target_port=33090,
        workers=2,
        resume=False,
        snapshot_id="frozen-snapshot",
        writes_frozen_at="2026-08-08T00:00:00+08:00",
        restored_artifact_sha256="a" * 64,
        change_id="MYSQL84-TEST",
    )

    names = [step.name for step in _build_steps(args, paths)]

    assert names.index("schema_semantic_audit") < names.index(
        "repair_fractional_datetime_compatibility"
    )
    assert names.index("repair_fractional_datetime_compatibility") < names.index(
        "materialize_datetime_defaults"
    )
    assert names.index("materialize_datetime_defaults") < names.index(
        "capture_frozen_source_data"
    )
    assert names.index("capture_quiescent_target_data") < names.index(
        "compare_business_data"
    )
