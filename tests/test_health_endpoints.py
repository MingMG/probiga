from __future__ import annotations

from server.api.routers import health


def test_release_git_inspection_marks_immutable_checkout_safe() -> None:
    assert health._release_git_command("rev-parse", "HEAD") == [
        "git",
        "-c",
        f"safe.directory={health.REPOSITORY_ROOT}",
        "rev-parse",
        "HEAD",
    ]


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
