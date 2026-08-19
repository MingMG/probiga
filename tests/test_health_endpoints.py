from __future__ import annotations

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
) -> None:
    engine = MagicMock()
    engine.connect.side_effect = OperationalError(
        "SELECT 1",
        {},
        ConnectionError("secret connection detail"),
    )
    monkeypatch.setattr(health, "get_engine", lambda: engine)

    payload = health._primary_database_readiness()

    assert payload == {
        "status": "error",
        "ready": False,
        "error": "OperationalError",
    }
    assert "secret" not in str(payload)


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
        lambda: {"verified": True, "active": True, "enabled": True},
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
            "error": "OperationalError",
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
            "error": "SchedulerScriptPolicyError",
        },
    )

    with pytest.raises(HTTPException, match="scheduler script policy") as exc:
        health.health()

    assert exc.value.status_code == 503


def test_qmt_capabilities_uses_external_collector_without_local_sdk(monkeypatch):
    from integrations.qmt import bridge
    from integrations.qmt import diagnostics

    monkeypatch.setattr(bridge, "is_configured", lambda: False)
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

    monkeypatch.setattr(bridge, "is_configured", lambda: False)
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
