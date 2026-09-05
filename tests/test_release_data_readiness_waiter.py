# -*- coding: utf-8 -*-
from datetime import datetime
import json
from pathlib import Path
import re
import stat
from types import SimpleNamespace

import pytest

from tools import wait_release_data_readiness as waiter


BUILD_SHA = "a" * 40


class _Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def monotonic(self) -> float:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.value += seconds


def _ready_proof() -> dict:
    return {
        "status": "READY",
        "build_sha": BUILD_SHA,
        "phase": "post_activation_data_readiness",
        "validated_at": "2026-08-27 08:00:00",
        "task_count": len(
            waiter.ensure_quality_gate.RELEASE_DATA_READINESS_TASK_TYPES
        ),
    }


def test_waiter_retries_read_only_gate_then_returns_exact_ready_proof() -> None:
    clock = _Clock()
    active_checks = []
    progress = []
    validator_calls = []

    def validate(engine, build_sha, now):
        validator_calls.append((engine, build_sha, now))
        if len(validator_calls) < 3:
            raise RuntimeError("one exact-build task has not completed")
        return _ready_proof()

    result = waiter.wait_for_release_data_readiness(
        object(),
        expected_build_sha=BUILD_SHA,
        timeout_seconds=30,
        poll_seconds=5,
        validator=validate,
        active_release_check=active_checks.append,
        wall_clock=lambda: datetime(2026, 8, 27, 2, 30),
        monotonic_clock=clock.monotonic,
        sleeper=clock.sleep,
        emit=progress.append,
    )

    assert result["status"] == "READY"
    assert result["retryable"] is False
    assert result["attempts"] == 3
    assert result["elapsed_seconds"] == 10
    assert result["proof"] == _ready_proof()
    assert active_checks == [BUILD_SHA, BUILD_SHA, BUILD_SHA]
    assert [item["status"] for item in progress] == ["NOT_READY", "NOT_READY"]
    assert all(call[1] == BUILD_SHA for call in validator_calls)


def test_waiter_timeout_is_data_blocked_and_never_fakes_ready() -> None:
    clock = _Clock()

    def not_ready(_engine, _build_sha, _now):
        raise RuntimeError("canonical QMT input window is incomplete")

    result = waiter.wait_for_release_data_readiness(
        object(),
        expected_build_sha=BUILD_SHA,
        timeout_seconds=10,
        poll_seconds=5,
        validator=not_ready,
        active_release_check=lambda _sha: None,
        wall_clock=lambda: datetime(2026, 8, 27, 2, 30),
        monotonic_clock=clock.monotonic,
        sleeper=clock.sleep,
    )

    assert result == {
        "schema": "probiga.release-data-readiness-wait.v1",
        "status": "DATA_BLOCKED",
        "build_sha": BUILD_SHA,
        "attempts": 3,
        "elapsed_seconds": 10,
        "retryable": True,
        "last_error": "canonical QMT input window is incomplete",
    }


def test_waiter_stops_before_select_when_active_release_changes() -> None:
    called = False

    def validate(_engine, _build_sha, _now):
        nonlocal called
        called = True
        return _ready_proof()

    with pytest.raises(waiter.ActiveReleaseChangedError):
        waiter.wait_for_release_data_readiness(
            object(),
            expected_build_sha=BUILD_SHA,
            timeout_seconds=0,
            poll_seconds=1,
            validator=validate,
            active_release_check=lambda _sha: (_ for _ in ()).throw(
                waiter.ActiveReleaseChangedError("release changed")
            ),
        )

    assert called is False


def test_active_release_health_requires_exact_expected_and_actual_sha() -> None:
    payload = {
        "status": "ok",
        "release_revision": {
            "deployment_mode": "production",
            "matches_expected": True,
            "expected_git_sha": BUILD_SHA,
            "actual_git_sha": BUILD_SHA,
        },
    }
    waiter._assert_active_release(BUILD_SHA, health_loader=lambda: payload)

    payload["release_revision"]["actual_git_sha"] = "b" * 40
    with pytest.raises(waiter.ActiveReleaseChangedError):
        waiter._assert_active_release(BUILD_SHA, health_loader=lambda: payload)


def test_local_runtime_identity_is_immutable_production_checkout() -> None:
    environ = {
        "PROBIGA_DEPLOYMENT_MODE": "production",
        "PROBIGA_BUILD_COMMIT_SHA": BUILD_SHA,
        "PROBIGA_EXPECTED_GIT_SHA": BUILD_SHA,
        "PROBIGA_CODE_ROOT": f"/opt/ProBigA-releases/{BUILD_SHA}",
    }
    waiter._validate_local_runtime_identity(BUILD_SHA, environ)

    environ["PROBIGA_CODE_ROOT"] = "/opt/ProBigA"
    with pytest.raises(waiter.ActiveReleaseChangedError):
        waiter._validate_local_runtime_identity(BUILD_SHA, environ)


def _production_health() -> dict:
    return {
        "status": "ok",
        "release_revision": {
            "deployment_mode": "production",
            "matches_expected": True,
            "expected_git_sha": BUILD_SHA,
            "actual_git_sha": BUILD_SHA,
        },
    }


def test_public_receipt_removes_raw_errors_and_requires_exact_task_count() -> None:
    secret = "internal-error-marker-not-for-public-status"
    public = waiter._public_status_payload(
        {
            "status": "NOT_READY",
            "build_sha": BUILD_SHA,
            "attempts": 2,
            "elapsed_seconds": 60,
            "retryable": True,
            "last_error": secret,
        }
    )

    encoded = json.dumps(public, sort_keys=True)
    assert secret not in encoded
    assert "last_error" not in public
    assert public["reason_code"] == "release_data_not_ready"
    assert len(public["reason_sha256"]) == 64

    proof = _ready_proof()
    proof["task_count"] -= 1
    with pytest.raises(ValueError, match="proof identity"):
        waiter._public_status_payload(
            {
                "status": "READY",
                "build_sha": BUILD_SHA,
                "attempts": 1,
                "elapsed_seconds": 0,
                "retryable": False,
                "proof": proof,
            }
        )


def test_protected_environment_requires_root_service_group_boundary(monkeypatch) -> None:
    parent_info = SimpleNamespace(
        st_mode=stat.S_IFDIR | 0o755,
        st_uid=0,
    )
    file_info = SimpleNamespace(
        st_mode=stat.S_IFREG | 0o640,
        st_uid=0,
        st_gid=42,
        st_nlink=1,
    )

    class _Node:
        def __init__(self, info, parent=None):
            self._info = info
            self.parent = parent

        def lstat(self):
            return self._info

    parent = _Node(parent_info)
    path = _Node(file_info, parent=parent)
    loaded = []
    monkeypatch.setattr(
        waiter,
        "os",
        SimpleNamespace(
            name="posix",
            getegid=lambda: 42,
            getgroups=lambda: [7],
        ),
    )
    monkeypatch.setattr(waiter, "load_project_env", loaded.append)

    waiter._load_protected_production_env(path)
    assert loaded == [path]

    file_info.st_mode = stat.S_IFREG | 0o644
    with pytest.raises(waiter.ProductionRuntimeConfigError):
        waiter._load_protected_production_env(path)


def test_remote_snapshot_reads_only_health_and_sanitized_public_receipt() -> None:
    command = waiter._production_status_snapshot_command(BUILD_SHA)

    assert "http://127.0.0.1/api/health" in command
    assert "/var/lib/probiga/release-data-readiness" in command
    assert "/usr/bin/flock" in command
    assert "tools/wait_release_data_readiness.py" not in command
    assert "MYSQL_URL" not in command
    assert "/opt/ProBigA/.env" not in command

    pending = waiter._parse_remote_status_snapshot(
        json.dumps(_production_health()) + "\n{}\n",
        expected_build_sha=BUILD_SHA,
    )
    assert pending["status"] == "NOT_READY"
    assert pending["reason_code"] == "observer_pending"

    ready = waiter._public_status_payload(
        {
            "status": "READY",
            "build_sha": BUILD_SHA,
            "attempts": 3,
            "elapsed_seconds": 120,
            "retryable": False,
            "proof": _ready_proof(),
        }
    )
    ready["updated_at"] = "2026-08-27T08:00:00+08:00"
    parsed = waiter._parse_remote_status_snapshot(
        json.dumps(_production_health()) + "\n" + json.dumps(ready) + "\n",
        expected_build_sha=BUILD_SHA,
        decision_time=datetime(2026, 8, 27, 8, 0, tzinfo=waiter._SHANGHAI),
    )
    assert parsed == ready

    with_secret = {**ready, "mysql_url": "mysql://should-not-be-returned"}
    with pytest.raises(RuntimeError, match="fields differ"):
        waiter._parse_remote_status_snapshot(
            json.dumps(_production_health())
            + "\n"
            + json.dumps(with_secret)
            + "\n",
            expected_build_sha=BUILD_SHA,
            decision_time=datetime(
                2026, 8, 27, 8, 0, tzinfo=waiter._SHANGHAI
            ),
        )

    stale = waiter._public_status_payload(
        {
            "status": "NOT_READY",
            "build_sha": BUILD_SHA,
            "attempts": 4,
            "elapsed_seconds": 180,
            "retryable": True,
            "last_error": "not ready",
        }
    )
    stale["updated_at"] = "2026-08-27T07:00:00+08:00"
    with pytest.raises(RuntimeError, match="stale"):
        waiter._parse_remote_status_snapshot(
            json.dumps(_production_health()) + "\n" + json.dumps(stale) + "\n",
            expected_build_sha=BUILD_SHA,
            decision_time=datetime(2026, 8, 27, 8, 0, tzinfo=waiter._SHANGHAI),
        )

    ready["updated_at"] = "2026-08-27T07:00:00+08:00"
    with pytest.raises(RuntimeError, match="stale"):
        waiter._parse_remote_status_snapshot(
            json.dumps(_production_health()) + "\n" + json.dumps(ready) + "\n",
            expected_build_sha=BUILD_SHA,
            decision_time=datetime(2026, 8, 27, 8, 0, tzinfo=waiter._SHANGHAI),
        )


def test_remote_waiter_retries_data_blocked_receipt_until_ready(monkeypatch) -> None:
    captured = {}

    class _Client:
        def connect(self, **kwargs):
            captured["connect"] = kwargs

        def close(self):
            captured["closed"] = True

    receipts = iter(
        [
            {
                "status": "DATA_BLOCKED",
                "build_sha": BUILD_SHA,
                "attempts": 361,
                "elapsed_seconds": 21600,
                "retryable": True,
                "reason_code": "release_data_not_ready",
            },
            {
                "status": "READY",
                "build_sha": BUILD_SHA,
                "attempts": 2,
                "elapsed_seconds": 60,
                "retryable": False,
            },
        ]
    )
    client = _Client()
    monkeypatch.setattr(waiter, "load_project_env", lambda: None)
    monkeypatch.setattr(waiter, "production_ssh_client", lambda: client)
    monkeypatch.setattr(
        waiter,
        "production_ssh_connect_kwargs",
        lambda **kwargs: {"host": "prod", **kwargs},
    )
    monkeypatch.setattr(
        waiter,
        "_remote_status_snapshot",
        lambda _client, _build_sha: next(receipts),
    )
    monkeypatch.setattr(waiter.time, "sleep", lambda _seconds: None)
    args = waiter._parser().parse_args(
        [
            "--expected-build-sha",
            BUILD_SHA,
            "--timeout-seconds",
            "30",
            "--poll-seconds",
            "7",
        ]
    )

    assert waiter._run_remote(args) == 0
    assert captured["closed"] is True


def test_local_observer_uses_non_restarting_fatal_exit_codes(monkeypatch) -> None:
    args = SimpleNamespace(
        expected_build_sha=BUILD_SHA,
        status_file="",
        timeout_seconds=0,
        poll_seconds=1,
    )
    monkeypatch.setattr(
        waiter,
        "_validate_local_runtime_identity",
        lambda _sha: (_ for _ in ()).throw(
            waiter.ActiveReleaseChangedError("release changed")
        ),
    )
    assert waiter._run_local(args) == 3

    monkeypatch.setattr(waiter, "_validate_local_runtime_identity", lambda _sha: None)
    monkeypatch.setattr(
        waiter,
        "_load_protected_production_env",
        lambda: (_ for _ in ()).throw(
            waiter.ProductionRuntimeConfigError("metadata differs")
        ),
    )
    assert waiter._run_local(args) == 4


def test_waiter_source_has_no_database_write_statements() -> None:
    source = waiter.__file__
    text = open(source, encoding="utf-8").read().upper()
    for token in ("INSERT INTO", "UPDATE ", "DELETE FROM", "ALTER TABLE", "CREATE TABLE"):
        assert token not in text


def test_deploy_starts_hardened_observer_only_after_success_without_waiting() -> None:
    root = Path(__file__).resolve().parents[1]
    deploy = (root / "deploy" / "production_deploy.sh").read_text(
        encoding="utf-8"
    )

    assert "--validate-release-data-readiness" not in deploy
    function_source = deploy.split(
        "start_release_data_readiness_observer() {", 1
    )[1].split("\n}\nprepared_governance_snapshot()", 1)[0]
    assert "--no-block" in function_source
    assert "--collect" in function_source
    assert "--uid=\"$SERVICE_USER\"" in function_source
    assert "--property=ProtectSystem=strict" in function_source
    assert "--property=\"ReadWritePaths=$status_file\"" in function_source
    assert "--property=Restart=always" in function_source
    assert "--property=RestartSec=300" in function_source
    assert "RestartPreventExitStatus=3 4" in function_source
    assert "--timeout-seconds 21600" in function_source
    assert "--status-file \"$status_file\"" in function_source
    assert "MYSQL_URL" not in function_source
    assert "PASSWORD" not in function_source
    assert "production_release_command" not in function_source
    assert "install -o \"$SERVICE_USER\" -g \"$service_group\" -m 0644" in function_source
    assert "root:$service_group" in function_source

    # Normal activation launches the observer only after the durable receipt,
    # successful-deploy fence, finalized-journal removal, and trap teardown.
    success_index = deploy.rindex("DEPLOY_SUCCEEDED=1")
    finalized_index = deploy.index(
        "activation_snapshot_remove_finalized_before_deploy", success_index
    )
    trap_index = deploy.index("trap - ERR TERM INT HUP", finalized_index)
    observer_calls = list(re.finditer(
        r'if \[ "\$RELEASE_DATA_VALIDATION_BLOCKING" -eq 1 \] && \\\n'
        r'\s*! start_release_data_readiness_observer; then',
        deploy,
    ))
    observer_index = next(
        match.start() for match in observer_calls if match.start() > trap_index
    )
    assert success_index < finalized_index < trap_index < observer_index
    assert "readonly RELEASE_DATA_VALIDATION_BLOCKING=1" in deploy
    # Definition plus preserved-no-receipt, idempotent and full activation.
    # Each call retains the explicit production data-validation gate. A
    # DEPLOYED_CODE_ONLY_DEGRADED release is intentionally not observed as if
    # it were an active, data-ready production release.
    assert len(observer_calls) == 3
    assert deploy.count("start_release_data_readiness_observer") == 4
