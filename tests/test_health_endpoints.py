from __future__ import annotations

import logging
import subprocess
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from sqlalchemy.exc import OperationalError

from server.api.routers import health


def test_release_git_inspection_marks_immutable_checkout_safe() -> None:
    assert health._release_git_command("rev-parse", "HEAD") == [
        "git",
        "-c",
        f"safe.directory={health.REPOSITORY_ROOT}",
        "rev-parse",
        "HEAD",
    ]


def test_scheduler_process_identity_requires_the_same_immutable_release(
    monkeypatch,
    tmp_path,
) -> None:
    sha = "a" * 40
    pid = 4321
    proc = tmp_path / str(pid)
    proc.mkdir()
    code_root = f"/opt/ProBigA-releases/{sha}"
    (proc / "environ").write_bytes(
        b"PROBIGA_EXPECTED_GIT_SHA=" + sha.encode() + b"\0"
        b"PROBIGA_BUILD_COMMIT_SHA=" + sha.encode() + b"\0"
        b"PROBIGA_CODE_ROOT=" + code_root.encode() + b"\0"
    )
    (proc / "cmdline").write_bytes(
        f"/var/lib/probiga/release-venvs/{sha}/bin/python\0"
        f"-P\0{code_root}/tools/run_scheduler_daemon.py\0".encode()
    )
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", sha)
    monkeypatch.delenv(
        "PROBIGA_STRATEGY_GOVERNANCE_MODE",
        raising=False,
    )
    monkeypatch.setattr(health, "_PROC_ROOT", tmp_path)

    payload = health._standalone_scheduler_release_identity(pid)

    assert payload == {
        "ready": True,
        "identity_mode": "SAME_BUILD_REQUIRED",
        "api_build_sha": sha,
        "expected_build_sha": sha,
        "expected_code_root": code_root,
        "observed_build_sha": sha,
        "observed_code_root": code_root,
        "same_build_as_api": True,
        "error_code": None,
    }


def test_scheduler_process_identity_rejects_an_old_scheduler_build(
    monkeypatch,
    tmp_path,
) -> None:
    expected_sha = "a" * 40
    old_sha = "b" * 40
    pid = 4321
    proc = tmp_path / str(pid)
    proc.mkdir()
    old_root = f"/opt/ProBigA-releases/{old_sha}"
    (proc / "environ").write_bytes(
        b"PROBIGA_EXPECTED_GIT_SHA=" + old_sha.encode() + b"\0"
        b"PROBIGA_BUILD_COMMIT_SHA=" + old_sha.encode() + b"\0"
        b"PROBIGA_CODE_ROOT=" + old_root.encode() + b"\0"
    )
    (proc / "cmdline").write_bytes(
        f"/var/lib/probiga/release-venvs/{old_sha}/bin/python\0"
        f"-P\0{old_root}/tools/run_scheduler_daemon.py\0".encode()
    )
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", expected_sha)
    monkeypatch.delenv(
        "PROBIGA_STRATEGY_GOVERNANCE_MODE",
        raising=False,
    )
    monkeypatch.setattr(health, "_PROC_ROOT", tmp_path)

    payload = health._standalone_scheduler_release_identity(pid)

    assert payload["ready"] is False
    assert payload["expected_build_sha"] == expected_sha
    assert payload["observed_build_sha"] == old_sha
    assert payload["observed_code_root"] == old_root
    assert payload["error_code"] == "scheduler_release_mismatch"


def test_scheduler_process_identity_rejects_expected_script_as_decoy_argument(
    monkeypatch,
    tmp_path,
) -> None:
    sha = "a" * 40
    pid = 4321
    proc = tmp_path / str(pid)
    proc.mkdir()
    code_root = f"/opt/ProBigA-releases/{sha}"
    (proc / "environ").write_bytes(
        b"PROBIGA_EXPECTED_GIT_SHA=" + sha.encode() + b"\0"
        b"PROBIGA_BUILD_COMMIT_SHA=" + sha.encode() + b"\0"
        b"PROBIGA_CODE_ROOT=" + code_root.encode() + b"\0"
    )
    (proc / "cmdline").write_bytes(
        f"/var/lib/probiga/release-venvs/{sha}/bin/python\0"
        f"-P\0/tmp/untrusted.py\0"
        f"{code_root}/tools/run_scheduler_daemon.py\0".encode()
    )
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", sha)
    monkeypatch.delenv(
        "PROBIGA_STRATEGY_GOVERNANCE_MODE",
        raising=False,
    )
    monkeypatch.setattr(health, "_PROC_ROOT", tmp_path)

    payload = health._standalone_scheduler_release_identity(pid)

    assert payload["ready"] is False
    assert payload["error_code"] == "scheduler_release_mismatch"


def test_scheduler_process_identity_rejects_any_deferred_process(
    monkeypatch,
    tmp_path,
) -> None:
    api_sha = "a" * 40
    scheduler_sha = "b" * 40
    pid = 4321
    proc = tmp_path / str(pid)
    proc.mkdir()
    scheduler_root = f"/opt/ProBigA-releases/{scheduler_sha}"
    (proc / "environ").write_bytes(
        b"PROBIGA_EXPECTED_GIT_SHA=" + scheduler_sha.encode() + b"\0"
        b"PROBIGA_BUILD_COMMIT_SHA=" + scheduler_sha.encode() + b"\0"
        b"PROBIGA_CODE_ROOT=" + scheduler_root.encode() + b"\0"
        b"DATABASE_URL=opaque-database-runtime-sentinel\0"
        b"PROBIGA_ADMIN_TOKEN=do-not-expose\0"
    )
    (proc / "cmdline").write_bytes(
        f"/var/lib/probiga/release-venvs/{scheduler_sha}/bin/python\0"
        f"-P\0{scheduler_root}/tools/run_scheduler_daemon.py\0".encode()
    )
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", api_sha)
    monkeypatch.setenv(
        "PROBIGA_STRATEGY_GOVERNANCE_MODE",
        "DEFERRED_DB",
    )
    monkeypatch.setattr(health, "_PROC_ROOT", tmp_path)

    payload = health._standalone_scheduler_release_identity(pid)

    assert payload == {
        "ready": False,
        "identity_mode": "FENCED_DEFERRED",
        "api_build_sha": api_sha,
        "expected_build_sha": api_sha,
        "expected_code_root": f"/opt/ProBigA-releases/{api_sha}",
        "observed_build_sha": None,
        "observed_code_root": None,
        "same_build_as_api": None,
        "error_code": "deferred_scheduler_process_present",
    }
    assert "opaque-database-runtime-sentinel" not in str(payload)
    assert "do-not-expose" not in str(payload)


def test_scheduler_process_identity_accepts_explicit_deferred_fence(
    monkeypatch,
) -> None:
    api_sha = "a" * 40
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", api_sha)
    monkeypatch.setenv(
        "PROBIGA_STRATEGY_GOVERNANCE_MODE",
        "DEFERRED_DB",
    )

    payload = health._standalone_scheduler_release_identity(0)

    assert payload == {
        "ready": True,
        "identity_mode": "FENCED_DEFERRED",
        "api_build_sha": api_sha,
        "expected_build_sha": api_sha,
        "expected_code_root": f"/opt/ProBigA-releases/{api_sha}",
        "observed_build_sha": None,
        "observed_code_root": None,
        "same_build_as_api": None,
        "error_code": None,
    }


def test_scheduler_status_verifies_deferred_inactive_disabled_fence(
    monkeypatch,
) -> None:
    sha = "a" * 40
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", sha)
    monkeypatch.setenv(
        "PROBIGA_STRATEGY_GOVERNANCE_MODE",
        "DEFERRED_DB",
    )

    def _run(command, **_kwargs):
        if command[1] == "is-active":
            return subprocess.CompletedProcess(
                command, 3, stdout="inactive\n", stderr=""
            )
        if command[1] == "is-enabled":
            return subprocess.CompletedProcess(
                command, 1, stdout="disabled\n", stderr=""
            )
        if command[1] == "show":
            return subprocess.CompletedProcess(
                command, 0, stdout="0\n", stderr=""
            )
        raise AssertionError(command)

    monkeypatch.setattr(health.subprocess, "run", _run)

    payload = health._standalone_scheduler_status()

    assert payload["verified"] is True
    assert payload["fenced"] is True
    assert payload["active"] is False
    assert payload["enabled"] is False
    assert payload["pid"] == 0
    assert payload["release_identity"]["identity_mode"] == "FENCED_DEFERRED"


def test_scheduler_status_rejects_live_process_during_deferred_database(
    monkeypatch,
) -> None:
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", "a" * 40)
    monkeypatch.setenv(
        "PROBIGA_STRATEGY_GOVERNANCE_MODE",
        "DEFERRED_DB",
    )

    def _run(command, **_kwargs):
        if command[1] == "is-active":
            return subprocess.CompletedProcess(
                command, 0, stdout="active\n", stderr=""
            )
        if command[1] == "is-enabled":
            return subprocess.CompletedProcess(
                command, 0, stdout="enabled\n", stderr=""
            )
        if command[1] == "show":
            return subprocess.CompletedProcess(
                command, 0, stdout="4321\n", stderr=""
            )
        raise AssertionError(command)

    monkeypatch.setattr(health.subprocess, "run", _run)

    payload = health._standalone_scheduler_status()

    assert payload["verified"] is False
    assert payload["fenced"] is False
    assert payload["error"] == "deferred_scheduler_not_inactive"
    assert payload["release_identity"]["error_code"] == (
        "deferred_scheduler_process_present"
    )


def test_release_git_inspection_reports_the_timed_out_stage(monkeypatch) -> None:
    expected_sha = "a" * 40
    calls: list[list[str]] = []

    def _run(command, **kwargs):
        calls.append(command)
        if command[-2:] == ["rev-parse", "HEAD"]:
            return subprocess.CompletedProcess(command, 0, stdout=expected_sha + "\n")
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected_sha)
    monkeypatch.setattr(health.subprocess, "run", _run)

    payload = health._deployed_git_revision()

    assert payload["actual_git_sha"] == expected_sha
    assert payload["matches_expected"] is True
    assert payload["inspection_status"] == "error"
    assert payload["inspection_error_code"] == "probe_timeout"
    assert payload["inspection_error_stage"] == "tracked_status"
    assert payload["code_worktree_clean"] is False
    assert all(call.count("--untracked-files=no") <= 1 for call in calls)


def test_release_git_inspection_scans_untracked_files_once(
    monkeypatch,
    tmp_path,
) -> None:
    expected_sha = "a" * 40
    commands: list[list[str]] = []

    def _run(command, **kwargs):
        commands.append(command)
        if command[-2:] == ["rev-parse", "HEAD"]:
            output = expected_sha + "\n"
        elif "status" in command:
            output = ""
        else:
            output = (
                f"100644 {'b' * 40} 0\tserver/api/routers/health.py\0"
                "server/rogue.py\0"
            )
        return subprocess.CompletedProcess(command, 0, stdout=output)

    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected_sha)
    monkeypatch.setattr(health, "REPOSITORY_ROOT", tmp_path)
    monkeypatch.setattr(health.subprocess, "run", _run)

    payload = health._deployed_git_revision()

    untracked_commands = [command for command in commands if "--others" in command]
    status_command = next(command for command in commands if "status" in command)
    assert len(untracked_commands) == 1
    assert "--untracked-files=no" in status_command
    assert payload["inspection_status"] == "ok"
    assert payload["tracked_worktree_clean"] is True
    assert payload["untracked_executable_count"] == 1
    assert payload["code_worktree_clean"] is False


def test_release_identity_failure_exposes_sanitized_diagnostics(monkeypatch) -> None:
    _stub_production_health_dependencies(monkeypatch)
    monkeypatch.setattr(
        health,
        "_deployed_git_revision",
        lambda: {
            "deployment_mode": "production",
            "expected_sha_configured": True,
            "matches_expected": True,
            "code_worktree_clean": False,
            "tracked_worktree_clean": False,
            "inspection_status": "error",
            "inspection_error_code": "probe_timeout",
            "inspection_error_stage": "tracked_status",
            "inspection_durations_ms": {"tracked_status": 15001},
            "tracked_change_count": None,
            "untracked_executable_count": 1,
            "untracked_root_shadow_count": 1,
            "untracked_executable_paths": ["private/secret.py"],
            "untracked_root_shadow_paths": ["private/secret.py"],
        },
    )
    monkeypatch.setattr(
        health,
        "_primary_database_readiness",
        lambda: {"status": "ok", "ready": True},
    )
    monkeypatch.setattr(
        health,
        "_scheduler_script_policy_readiness",
        lambda: {"status": "ok", "ready": True},
    )

    with pytest.raises(HTTPException) as exc:
        health.health()

    assert exc.value.status_code == 503
    assert exc.value.detail == {
        "code": "release_identity_check_failed",
        "message": "deployed checkout differs from the pinned clean release revision",
        "reason": "inspection_failed",
        "head_matches_expected": True,
        "worktree_clean": False,
        "probe_stage": "tracked_status",
        "probe_error": "probe_timeout",
        "probe_durations_ms": {"tracked_status": 15001},
        "tracked_change_count": None,
        "untracked_executable_count": 1,
        "untracked_import_shadow_count": 1,
    }
    assert "private" not in str(exc.value.detail)


def test_primary_database_readiness_executes_round_trip(monkeypatch) -> None:
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.scalar_one.return_value = 1
    monkeypatch.setattr(health, "get_engine", lambda: engine)

    payload = health._primary_database_readiness()

    assert payload == {"status": "ok", "ready": True}
    assert str(connection.execute.call_args.args[0]) == "SELECT 1"


def test_primary_database_readiness_sanitizes_connection_failure(
    monkeypatch,
    caplog,
) -> None:
    engine = MagicMock()
    engine.connect.side_effect = OperationalError(
        "SELECT 1",
        {},
        ConnectionError("secret connection detail"),
    )
    monkeypatch.setattr(health, "get_engine", lambda: engine)

    with caplog.at_level(logging.ERROR, logger=health.__name__):
        payload = health._primary_database_readiness()

    assert payload == {
        "status": "error",
        "ready": False,
        "error": "primary database readiness probe failed",
        "error_code": "database_readiness_probe_failed",
    }
    assert "secret" not in str(payload)
    assert "secret connection detail" not in caplog.text
    assert "SELECT 1" not in caplog.text
    assert "OperationalError" in caplog.text


def test_adata_release_failure_never_exposes_private_paths_or_secrets(
    monkeypatch,
) -> None:
    private_source = "/srv/private/customer-a/adata"
    monkeypatch.setenv(health.ADATA_SOURCE_ENV, private_source)
    monkeypatch.setenv(health.ADATA_GIT_SHA_ENV, "a" * 40)
    monkeypatch.setenv(health.ADATA_TREE_SHA_ENV, "b" * 64)

    def fail_validation(*_args, **_kwargs):
        raise health.AdataReleaseError(
            "mysql://private-host:3306/secret at " + private_source
        )

    monkeypatch.setattr(
        health,
        "validate_adata_release_source",
        fail_validation,
    )

    payload = health._deployed_adata_revision()

    assert payload["verified"] is False
    assert payload["source_configured"] is True
    assert payload["error_code"] == "release_validation_failed"
    assert "source_dir" not in payload
    assert "private-host" not in str(payload)
    assert "customer-a" not in str(payload)
    assert "secret" not in str(payload)


def test_table_freshness_failure_exposes_only_stable_error_code(
    monkeypatch,
) -> None:
    engine = MagicMock()
    engine.connect.side_effect = OperationalError(
        "SELECT secret FROM private_host",
        {},
        ConnectionError("password=do-not-expose"),
    )
    monkeypatch.setattr(health, "get_engine", lambda: engine)

    payload = health._table_freshness(
        "sm_index_current",
        "index_code",
        fresh_window_seconds=30,
    )

    assert payload == {
        "table": "sm_index_current",
        "status": "error",
        "error": "database freshness probe failed",
        "error_code": "database_probe_failed",
    }
    assert "private_host" not in str(payload)
    assert "do-not-expose" not in str(payload)


def _stub_production_health_dependencies(monkeypatch) -> None:
    monkeypatch.setattr(
        health,
        "_deployed_git_revision",
        lambda: {
            "deployment_mode": "production",
            "expected_sha_configured": True,
            "matches_expected": True,
            "code_worktree_clean": True,
        },
    )
    monkeypatch.setattr(
        health,
        "_deployed_adata_revision",
        lambda: {"verified": True},
    )
    monkeypatch.setattr(health, "admin_auth_status", lambda: {"ready": True})
    monkeypatch.setattr(
        health,
        "_strategy_funding_schema_readiness",
        lambda: {"status": "ok", "ready": True},
    )
    monkeypatch.setattr(
        health,
        "scheduler_runtime_info",
        lambda: {
            "embedded_scheduler_enabled": False,
            "embedded_scheduler_running": False,
        },
    )
    monkeypatch.setattr(health, "scheduler_authority_contract", lambda: {})
    monkeypatch.setattr(
        health,
        "_standalone_scheduler_status",
        lambda: {
            "verified": True,
            "active": True,
            "enabled": True,
            "pid": 4321,
        },
    )
    monkeypatch.setattr(
        health,
        "_standalone_scheduler_heartbeat_readiness",
        lambda _pid: {"status": "ok", "ready": True},
    )
    monkeypatch.setattr(
        health,
        "_detached_job_log_readiness",
        lambda: {"status": "ok", "ready": True},
    )


def test_production_health_fails_closed_when_database_probe_fails(
    monkeypatch,
) -> None:
    _stub_production_health_dependencies(monkeypatch)
    monkeypatch.setattr(
        health,
        "_primary_database_readiness",
        lambda: {
            "status": "error",
            "ready": False,
            "error": "primary database readiness probe failed",
            "error_code": "database_readiness_probe_failed",
        },
    )
    monkeypatch.setattr(
        health,
        "_scheduler_script_policy_readiness",
        lambda: {"status": "ok", "ready": True},
    )

    with pytest.raises(HTTPException, match="primary database readiness") as exc:
        health.health()

    assert exc.value.status_code == 503


def test_production_health_fails_closed_when_scheduler_policy_fails(
    monkeypatch,
) -> None:
    _stub_production_health_dependencies(monkeypatch)
    monkeypatch.setattr(
        health,
        "_primary_database_readiness",
        lambda: {"status": "ok", "ready": True},
    )
    monkeypatch.setattr(
        health,
        "_scheduler_script_policy_readiness",
        lambda: {
            "status": "error",
            "ready": False,
            "error": "scheduler script policy readiness probe failed",
            "error_code": "scheduler_script_policy_probe_failed",
        },
    )

    with pytest.raises(HTTPException, match="scheduler script policy") as exc:
        health.health()

    assert exc.value.status_code == 503


def test_detached_job_log_readiness_creates_fsyncs_and_removes_probe(
    monkeypatch,
    tmp_path,
) -> None:
    code_root = tmp_path / "code"
    log_root = tmp_path / "jobs"
    code_root.mkdir()
    monkeypatch.setattr(health, "REPOSITORY_ROOT", code_root)
    monkeypatch.setenv("PROBIGA_JOB_LOG_ROOT", str(log_root))

    payload = health._detached_job_log_readiness()

    assert payload == {"status": "ok", "ready": True}
    assert log_root.is_dir()
    assert list(log_root.iterdir()) == []


def test_detached_job_log_readiness_sanitizes_failure(
    monkeypatch,
    caplog,
) -> None:
    monkeypatch.setattr(
        health,
        "_detached_job_log_root",
        lambda **_kwargs: (_ for _ in ()).throw(
            RuntimeError("private-path password=secret")
        ),
    )

    with caplog.at_level(logging.ERROR, logger=health.__name__):
        payload = health._detached_job_log_readiness()

    assert payload == {
        "status": "error",
        "ready": False,
        "error": "detached job log readiness probe failed",
        "error_code": "detached_job_log_readiness_failed",
    }
    assert "private-path" not in str(payload)


def test_production_health_fails_closed_when_job_log_root_is_not_writable(
    monkeypatch,
) -> None:
    _stub_production_health_dependencies(monkeypatch)
    monkeypatch.setattr(
        health,
        "_primary_database_readiness",
        lambda: {"status": "ok", "ready": True},
    )
    monkeypatch.setattr(
        health,
        "_scheduler_script_policy_readiness",
        lambda: {"status": "ok", "ready": True},
    )
    monkeypatch.setattr(
        health,
        "_detached_job_log_readiness",
        lambda: {"status": "error", "ready": False},
    )

    with pytest.raises(HTTPException, match="detached job log readiness") as exc:
        health.health()

    assert exc.value.status_code == 503


def test_production_health_fails_closed_when_scheduler_heartbeat_is_stale(
    monkeypatch,
) -> None:
    _stub_production_health_dependencies(monkeypatch)
    monkeypatch.setattr(
        health,
        "_primary_database_readiness",
        lambda: {"status": "ok", "ready": True},
    )
    monkeypatch.setattr(
        health,
        "_scheduler_script_policy_readiness",
        lambda: {"status": "ok", "ready": True},
    )
    monkeypatch.setattr(
        health,
        "_standalone_scheduler_heartbeat_readiness",
        lambda _pid: {"status": "error", "ready": False},
    )

    with pytest.raises(HTTPException, match="scheduler heartbeat") as exc:
        health.health()

    assert exc.value.status_code == 503


def test_strategy_funding_schema_readiness_exposes_only_frozen_contract(
    monkeypatch,
) -> None:
    from server.engine import strategy_governance

    engine = MagicMock()
    monkeypatch.setattr(health, "get_engine", lambda: engine)
    monkeypatch.setattr(
        health,
        "validate_strategy_funding_checkpoint_schema",
        lambda _connection: {
            "table_count": 2,
            "tables": health._EXPECTED_FUNDING_TABLE_COUNTS,
            "trigger_count": 4,
            "contract_hash": health.FUNDING_CHECKPOINT_SCHEMA_CONTRACT_HASH,
            "rolling_history_storage": (
                "ADDRESSABLE_APPEND_ONLY_DAILY_FACT_CHAIN"
            ),
            "checkpoint_target_average_bytes": (
                health.FUNDING_CHECKPOINT_TARGET_AVG_BYTES
            ),
            "checkpoint_total_target_bytes": (
                health.FUNDING_CHECKPOINT_TOTAL_TARGET_BYTES
            ),
            "checkpoint_total_hard_bytes": (
                health.FUNDING_CHECKPOINT_TOTAL_HARD_BYTES
            ),
            "batch_max_rows": health.FUNDING_CHECKPOINT_BATCH_MAX_ROWS,
            "batch_max_bytes": health.FUNDING_CHECKPOINT_BATCH_MAX_BYTES,
            "manifest_max_bytes": health.FUNDING_CHECKPOINT_MANIFEST_MAX_BYTES,
            "audit_max_bytes": health.FUNDING_CHECKPOINT_AUDIT_MAX_BYTES,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )
    monkeypatch.setattr(
        strategy_governance,
        "validate_governance_append_only_triggers",
        lambda _connection: {
            "trigger_count": 38,
            "trigger_names": sorted(
                strategy_governance.EXPECTED_GOVERNANCE_APPEND_ONLY_TRIGGER_NAMES
            ),
            "contract_hash": (
                health._EXPECTED_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
            ),
        },
    )
    monkeypatch.setattr(
        strategy_governance,
        "validate_metric_input_review_triggers",
        lambda _connection: {
            "trigger_count": 2,
            "trigger_names": sorted(
                health._EXPECTED_METRIC_REVIEW_TRIGGER_NAMES
            ),
            "contract_hash": health._EXPECTED_METRIC_REVIEW_CONTRACT_HASH,
        },
    )

    payload = health._strategy_funding_schema_readiness()

    assert payload["status"] == "ok"
    assert payload["ready"] is True
    assert payload["table_count"] == 2
    assert payload["trigger_count"] == 4
    assert payload["governance_append_only_trigger_count"] == 38
    assert payload["governance_metric_review_trigger_count"] == 2
    assert payload["governance_trigger_count"] == 40
    assert payload["governance_append_only_contract_hash"] == (
        health._EXPECTED_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
    )
    assert payload["governance_metric_review_contract_hash"] == (
        health._EXPECTED_METRIC_REVIEW_CONTRACT_HASH
    )
    assert payload["budgets"]["batch_max_rows"] == 100
    assert "tables" not in payload


def test_strategy_funding_schema_readiness_rejects_governance_trigger_drift(
    monkeypatch,
) -> None:
    from server.engine import strategy_governance

    engine = MagicMock()
    monkeypatch.setattr(health, "get_engine", lambda: engine)
    monkeypatch.setattr(
        health,
        "validate_strategy_funding_checkpoint_schema",
        lambda _connection: {
            "table_count": 2,
            "tables": health._EXPECTED_FUNDING_TABLE_COUNTS,
            "trigger_count": 4,
            "contract_hash": health._EXPECTED_FUNDING_SCHEMA_CONTRACT_HASH,
            "rolling_history_storage": (
                "ADDRESSABLE_APPEND_ONLY_DAILY_FACT_CHAIN"
            ),
            "checkpoint_target_average_bytes": 8192,
            "checkpoint_total_target_bytes": 8388608,
            "checkpoint_total_hard_bytes": 16777216,
            "batch_max_rows": 100,
            "batch_max_bytes": 4194304,
            "manifest_max_bytes": 1048576,
            "audit_max_bytes": 131072,
            "automatic_real_order_submission": False,
            "real_order_authority": False,
        },
    )
    monkeypatch.setattr(
        strategy_governance,
        "validate_governance_append_only_triggers",
        lambda _connection: {
            "trigger_count": 37,
            "trigger_names": [],
            "contract_hash": (
                health._EXPECTED_GOVERNANCE_APPEND_ONLY_CONTRACT_HASH
            ),
        },
    )
    monkeypatch.setattr(
        strategy_governance,
        "validate_metric_input_review_triggers",
        lambda _connection: {
            "trigger_count": 2,
            "trigger_names": sorted(
                health._EXPECTED_METRIC_REVIEW_TRIGGER_NAMES
            ),
            "contract_hash": health._EXPECTED_METRIC_REVIEW_CONTRACT_HASH,
        },
    )

    payload = health._strategy_funding_schema_readiness()

    assert payload == {
        "status": "error",
        "ready": False,
        "error": "strategy funding schema contract is incomplete",
        "error_code": "funding_schema_contract_incomplete",
    }


def test_scheduler_policy_probe_log_does_not_expose_exception_text(
    monkeypatch,
    caplog,
) -> None:
    private = "mysql://user:" + "password@private-host:3306/secret"
    monkeypatch.setattr(
        health,
        "resolve_scheduler_script",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError(private)),
    )

    with caplog.at_level(logging.ERROR, logger=health.__name__):
        payload = health._scheduler_script_policy_readiness()

    assert payload["error_code"] == "scheduler_script_policy_probe_failed"
    assert private not in str(payload)
    assert private not in caplog.text
    assert "OSError" in caplog.text


def test_strategy_funding_schema_readiness_sanitizes_metadata_failure(
    monkeypatch,
) -> None:
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    monkeypatch.setattr(health, "get_engine", lambda: engine)

    def fail_validation(_connection):
        assert _connection is connection
        raise RuntimeError(
            "mysql://private-host:3306/secret password=do-not-expose"
        )

    monkeypatch.setattr(
        health,
        "validate_strategy_funding_checkpoint_schema",
        fail_validation,
    )

    payload = health._strategy_funding_schema_readiness()

    assert payload == {
        "status": "error",
        "ready": False,
        "error": "strategy funding schema validation failed",
        "error_code": "funding_schema_validation_failed",
    }
    assert "private-host" not in str(payload)
    assert "do-not-expose" not in str(payload)


def test_production_health_fails_closed_when_funding_schema_is_not_exact(
    monkeypatch,
) -> None:
    _stub_production_health_dependencies(monkeypatch)
    monkeypatch.setattr(
        health,
        "_primary_database_readiness",
        lambda: {"status": "ok", "ready": True},
    )
    monkeypatch.setattr(
        health,
        "_scheduler_script_policy_readiness",
        lambda: {"status": "ok", "ready": True},
    )
    monkeypatch.setattr(
        health,
        "_strategy_funding_schema_readiness",
        lambda: {
            "status": "error",
            "ready": False,
            "error_code": "funding_schema_validation_failed",
        },
    )

    with pytest.raises(HTTPException, match="strategy funding schema readiness") as exc:
        health.health()

    assert exc.value.status_code == 503


def test_qmt_capabilities_uses_external_collector_without_local_sdk(monkeypatch):
    from integrations.qmt import bridge
    from integrations.qmt import diagnostics

    monkeypatch.setattr(bridge, "is_probe_runtime_configured", lambda: False)
    monkeypatch.setattr(
        health,
        "_get_qmt_live_runtime_config",
        lambda: {"enabled": True, "poll_seconds": 10},
    )

    def _unexpected_local_probe(*args, **kwargs):
        raise AssertionError("external collector mode must not probe local QMT SDK")

    monkeypatch.setattr(diagnostics, "capabilities", _unexpected_local_probe)

    payload = health.health_qmt_capabilities()

    assert payload["status"] == "external_windows_collector"
    assert payload["qmt_live_runtime"]["enabled"] is True
    assert payload["rows"] == []


def test_qmt_capabilities_force_keeps_explicit_local_probe(monkeypatch):
    from integrations.qmt import bridge
    from integrations.qmt import diagnostics

    monkeypatch.setattr(bridge, "is_probe_runtime_configured", lambda: False)
    monkeypatch.setattr(
        health,
        "_get_qmt_live_runtime_config",
        lambda: {"enabled": True},
    )
    monkeypatch.setattr(
        health,
        "get_gj_qmt_config",
        lambda: {"ping_timeout": 8},
    )
    monkeypatch.setattr(
        diagnostics,
        "capabilities",
        lambda *, timeout, force: {
            "ok": True,
            "status": "ok",
            "timeout": timeout,
            "force": force,
        },
    )

    payload = health.health_qmt_capabilities(force=True)

    assert payload == {"ok": True, "status": "ok", "timeout": 12, "force": True}


def test_qmt_core_probe_uses_external_collector_without_local_sdk(monkeypatch):
    from integrations.qmt import bridge
    from integrations.qmt import diagnostics

    monkeypatch.setattr(bridge, "is_probe_runtime_configured", lambda: False)
    monkeypatch.setattr(
        health,
        "_get_qmt_live_runtime_config",
        lambda: {"enabled": True, "poll_seconds": 10},
    )

    def _unexpected_local_probe(*args, **kwargs):
        raise AssertionError("external collector mode must not probe local QMT SDK")

    monkeypatch.setattr(diagnostics, "core_probe", _unexpected_local_probe)

    payload = health.health_qmt_core_probe()

    assert payload["status"] == "external_windows_collector"
    assert payload["qmt_live_runtime"]["enabled"] is True
    assert payload["probes"] == []
