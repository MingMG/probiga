from __future__ import annotations

from subprocess import CompletedProcess
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from server.api.routers import health as health_router

_REAL_DEPLOYED_ADATA_REVISION = health_router._deployed_adata_revision
_REAL_ROOT_SHADOW_SCANNER = health_router._untracked_root_shadow_files
_REAL_STANDALONE_SCHEDULER_STATUS = health_router._standalone_scheduler_status


@pytest.fixture(autouse=True)
def _verified_non_database_release_dependencies(monkeypatch):
    monkeypatch.setattr(
        health_router,
        "_untracked_root_shadow_files",
        lambda: (),
    )
    monkeypatch.setattr(
        health_router,
        "_deployed_adata_revision",
        lambda: {"configured": True, "verified": True, "read_only": True},
    )
    monkeypatch.setattr(
        health_router,
        "admin_auth_status",
        lambda: {"ready": True},
    )
    monkeypatch.setattr(
        health_router,
        "scheduler_runtime_info",
        lambda: {
            "embedded_scheduler_enabled": True,
            "embedded_scheduler_running": True,
        },
    )
    monkeypatch.setattr(
        health_router,
        "_standalone_scheduler_status",
        lambda: {
            "verified": True,
            "active": False,
            "state": "inactive",
            "enabled": False,
            "enablement_state": "disabled",
            "error": None,
        },
    )


def test_health_reports_and_enforces_pinned_revision(monkeypatch) -> None:
    expected = "a" * 40
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)
    monkeypatch.setenv("PROBIGA_IN_APP_DEPLOY_ENABLED", "0")
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")

    def clean_checkout(args, **_kwargs):
        stdout = expected + "\n" if args[1:3] == ["rev-parse", "HEAD"] else ""
        return CompletedProcess(args, 0, stdout, "")

    monkeypatch.setattr(health_router.subprocess, "run", clean_checkout)

    result = health_router.health()
    assert result["status"] == "ok"
    assert result["release_revision"]["matches_expected"] is True
    assert result["release_revision"]["tracked_worktree_clean"] is True
    assert result["release_revision"]["code_worktree_clean"] is True
    assert result["scheduler_runtime"]["embedded_scheduler_running"] is True
    assert result["standalone_scheduler"]["active"] is False
    assert result["standalone_scheduler"]["enabled"] is False
    assert result["in_app_deploy_enabled"] is False

    def wrong_revision(args, **_kwargs):
        stdout = "b" * 40 + "\n" if args[1:3] == ["rev-parse", "HEAD"] else ""
        return CompletedProcess(args, 0, stdout, "")

    monkeypatch.setattr(health_router.subprocess, "run", wrong_revision)
    with pytest.raises(HTTPException) as captured:
        health_router.health()
    assert captured.value.status_code == 503


def test_production_health_requires_expected_revision(monkeypatch) -> None:
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.delenv("PROBIGA_EXPECTED_GIT_SHA", raising=False)
    monkeypatch.setattr(
        health_router.subprocess,
        "run",
        lambda args, **_kwargs: CompletedProcess(args, 0, "", ""),
    )

    with pytest.raises(HTTPException) as captured:
        health_router.health()
    assert captured.value.status_code == 503


def test_production_health_requires_verified_adata_release(monkeypatch) -> None:
    expected = "b" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)
    monkeypatch.setattr(
        health_router,
        "_deployed_adata_revision",
        lambda: {"configured": False, "verified": False, "read_only": False},
    )
    monkeypatch.setattr(
        health_router.subprocess,
        "run",
        lambda args, **_kwargs: CompletedProcess(
            args,
            0,
            expected + "\n" if args[1:3] == ["rev-parse", "HEAD"] else "",
            "",
        ),
    )

    with pytest.raises(HTTPException) as captured:
        health_router.health()
    assert captured.value.status_code == 503


def test_adata_health_rejects_import_origin_outside_verified_source(
    tmp_path,
    monkeypatch,
) -> None:
    source = (tmp_path / "sealed").resolve()
    source.mkdir()
    outside = (tmp_path / "mutable" / "adata" / "__init__.py").resolve()
    outside.parent.mkdir(parents=True)
    outside.write_text("", encoding="utf-8")
    monkeypatch.setenv(health_router.ADATA_SOURCE_ENV, str(source))
    monkeypatch.setenv(health_router.ADATA_GIT_SHA_ENV, "a" * 40)
    monkeypatch.setenv(health_router.ADATA_TREE_SHA_ENV, "b" * 64)
    monkeypatch.setattr(
        health_router,
        "validate_adata_release_source",
        lambda *_args, **_kwargs: {
            "git_sha": "a" * 40,
            "tree_sha256": "b" * 64,
            "source_dir": str(source),
            "read_only": True,
        },
    )
    monkeypatch.setattr(
        health_router,
        "ensure_adata_import_path",
        lambda _root: source,
    )
    monkeypatch.setattr(
        health_router.importlib.util,
        "find_spec",
        lambda _name: SimpleNamespace(origin=str(outside)),
    )

    result = _REAL_DEPLOYED_ADATA_REVISION()
    assert result["verified"] is False
    assert "outside" in str(result["error"])


def test_production_health_requires_ready_admin_auth(monkeypatch) -> None:
    expected = "b" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)
    monkeypatch.setattr(
        health_router,
        "admin_auth_status",
        lambda: {"ready": False},
    )
    monkeypatch.setattr(
        health_router.subprocess,
        "run",
        lambda args, **_kwargs: CompletedProcess(
            args,
            0,
            expected + "\n" if args[1:3] == ["rev-parse", "HEAD"] else "",
            "",
        ),
    )

    with pytest.raises(HTTPException) as captured:
        health_router.health()
    assert captured.value.status_code == 503


def test_health_rejects_dirty_tracked_checkout_at_same_sha(monkeypatch) -> None:
    expected = "c" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)

    def dirty_checkout(args, **_kwargs):
        stdout = (
            expected + "\n"
            if args[1:3] == ["rev-parse", "HEAD"]
            else " M server/api/main.py\n"
        )
        return CompletedProcess(args, 0, stdout, "")

    monkeypatch.setattr(health_router.subprocess, "run", dirty_checkout)
    with pytest.raises(HTTPException) as captured:
        health_router.health()
    assert captured.value.status_code == 503


def test_health_rejects_untracked_executable_at_same_sha(monkeypatch) -> None:
    expected = "d" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)

    def untracked_code(args, **_kwargs):
        if args[1:3] == ["rev-parse", "HEAD"]:
            stdout = expected + "\n"
        elif args[1:4] == ["ls-files", "--others", "--exclude-standard"]:
            stdout = "sitecustomize.py\n"
        else:
            stdout = ""
        return CompletedProcess(args, 0, stdout, "")

    monkeypatch.setattr(health_router.subprocess, "run", untracked_code)
    with pytest.raises(HTTPException) as captured:
        health_router.health()
    assert captured.value.status_code == 503


def test_health_rejects_ignored_root_bytecode_shadow(monkeypatch) -> None:
    expected = "d" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)
    monkeypatch.setattr(
        health_router,
        "_untracked_root_shadow_files",
        lambda: ("uvicorn.pyc",),
    )

    def clean_git(args, **_kwargs):
        stdout = expected + "\n" if args[1:3] == ["rev-parse", "HEAD"] else ""
        return CompletedProcess(args, 0, stdout, "")

    monkeypatch.setattr(health_router.subprocess, "run", clean_git)
    with pytest.raises(HTTPException) as captured:
        health_router.health()
    assert captured.value.status_code == 503


def test_health_rejects_recursive_ignored_bytecode_shadow(monkeypatch) -> None:
    expected = "d" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)
    monkeypatch.setattr(
        health_router,
        "_untracked_root_shadow_files",
        lambda: ("server/api/__pycache__/main.cpython-314.pyc",),
    )
    monkeypatch.setattr(
        health_router.subprocess,
        "run",
        lambda args, **_kwargs: CompletedProcess(
            args,
            0,
            expected + "\n" if args[1:3] == ["rev-parse", "HEAD"] else "",
            "",
        ),
    )

    with pytest.raises(HTTPException) as captured:
        health_router.health()
    assert captured.value.status_code == 503


def test_root_shadow_scanner_recurses_strategies_and_versions(
    tmp_path,
    monkeypatch,
) -> None:
    strategy_cache = tmp_path / "strategies" / "__pycache__"
    version_cache = tmp_path / "versions" / "release" / "__pycache__"
    extension_package = tmp_path / "numpy"
    strategy_cache.mkdir(parents=True)
    version_cache.mkdir(parents=True)
    extension_package.mkdir()
    (strategy_cache / "runner.cpython-314.pyc").write_bytes(b"pyc")
    (version_cache / "manifest.cpython-314.pyo").write_bytes(b"pyo")
    (extension_package / "__init__.cpython-314-x86_64-linux-gnu.so").write_bytes(
        b"so"
    )
    monkeypatch.setattr(health_router, "REPOSITORY_ROOT", tmp_path)
    monkeypatch.setattr(
        health_router.subprocess,
        "run",
        lambda args, **_kwargs: CompletedProcess(args, 0, "", ""),
    )

    found = _REAL_ROOT_SHADOW_SCANNER()

    assert "strategies/__pycache__/runner.cpython-314.pyc" in found
    assert "versions/release/__pycache__/manifest.cpython-314.pyo" in found
    assert "numpy/__init__.cpython-314-x86_64-linux-gnu.so" in found


def test_standalone_scheduler_status_accepts_only_known_inactive_state(
    monkeypatch,
) -> None:
    def systemctl(args, **_kwargs):
        if args[1] == "is-active":
            return CompletedProcess(args, 3, "inactive\n", "")
        if args[1] == "is-enabled":
            return CompletedProcess(args, 1, "disabled\n", "")
        raise AssertionError(args)

    monkeypatch.setattr(
        health_router.subprocess,
        "run",
        systemctl,
    )

    assert _REAL_STANDALONE_SCHEDULER_STATUS() == {
        "verified": True,
        "active": False,
        "state": "inactive",
        "enabled": False,
        "enablement_state": "disabled",
        "error": None,
    }


def test_standalone_scheduler_status_reports_enabled_inactive_unit(
    monkeypatch,
) -> None:
    def systemctl(args, **_kwargs):
        if args[1] == "is-active":
            return CompletedProcess(args, 3, "inactive\n", "")
        if args[1] == "is-enabled":
            return CompletedProcess(args, 0, "enabled\n", "")
        raise AssertionError(args)

    monkeypatch.setattr(health_router.subprocess, "run", systemctl)

    status = _REAL_STANDALONE_SCHEDULER_STATUS()

    assert status["verified"] is True
    assert status["active"] is False
    assert status["enabled"] is True
    assert status["error"] == "standalone_scheduler_enabled"


def test_standalone_scheduler_status_accepts_missing_unit(monkeypatch) -> None:
    def systemctl(args, **_kwargs):
        if args[1] == "is-active":
            return CompletedProcess(args, 4, "unknown\n", "")
        if args[1] == "is-enabled":
            return CompletedProcess(args, 1, "", "not found")
        if args[1] == "show":
            return CompletedProcess(args, 0, "not-found\n", "")
        raise AssertionError(args)

    monkeypatch.setattr(health_router.subprocess, "run", systemctl)

    status = _REAL_STANDALONE_SCHEDULER_STATUS()

    assert status["verified"] is True
    assert status["active"] is False
    assert status["enabled"] is False
    assert status["enablement_state"] == "not-found"


@pytest.mark.parametrize(
    ("enabled", "running"),
    [(False, False), (True, False)],
)
def test_production_health_requires_embedded_scheduler_running(
    monkeypatch,
    enabled,
    running,
) -> None:
    expected = "1" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)
    monkeypatch.setattr(
        health_router,
        "scheduler_runtime_info",
        lambda: {
            "embedded_scheduler_enabled": enabled,
            "embedded_scheduler_running": running,
        },
    )
    monkeypatch.setattr(
        health_router.subprocess,
        "run",
        lambda args, **_kwargs: CompletedProcess(
            args,
            0,
            expected + "\n" if args[1:3] == ["rev-parse", "HEAD"] else "",
            "",
        ),
    )

    with pytest.raises(HTTPException) as captured:
        health_router.health()

    assert captured.value.status_code == 503
    assert "embedded scheduler" in str(captured.value.detail)


def test_production_health_rejects_active_standalone_scheduler(monkeypatch) -> None:
    expected = "2" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)
    monkeypatch.setattr(
        health_router,
        "_standalone_scheduler_status",
        lambda: {
            "verified": True,
            "active": True,
            "state": "active",
            "enabled": False,
            "enablement_state": "disabled",
            "error": None,
        },
    )
    monkeypatch.setattr(
        health_router.subprocess,
        "run",
        lambda args, **_kwargs: CompletedProcess(
            args,
            0,
            expected + "\n" if args[1:3] == ["rev-parse", "HEAD"] else "",
            "",
        ),
    )

    with pytest.raises(HTTPException) as captured:
        health_router.health()

    assert captured.value.status_code == 503
    assert "standalone scheduler" in str(captured.value.detail)


def test_production_health_rejects_enabled_inactive_standalone_scheduler(
    monkeypatch,
) -> None:
    expected = "4" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)
    monkeypatch.setattr(
        health_router,
        "_standalone_scheduler_status",
        lambda: {
            "verified": True,
            "active": False,
            "state": "inactive",
            "enabled": True,
            "enablement_state": "enabled",
            "error": "standalone_scheduler_enabled",
        },
    )
    monkeypatch.setattr(
        health_router.subprocess,
        "run",
        lambda args, **_kwargs: CompletedProcess(
            args,
            0,
            expected + "\n" if args[1:3] == ["rev-parse", "HEAD"] else "",
            "",
        ),
    )

    with pytest.raises(HTTPException) as captured:
        health_router.health()

    assert captured.value.status_code == 503
    assert "disablement" in str(captured.value.detail)


def test_production_health_fails_closed_when_standalone_state_is_unknown(
    monkeypatch,
) -> None:
    expected = "3" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)
    monkeypatch.setattr(
        health_router,
        "_standalone_scheduler_status",
        lambda: {
            "verified": False,
            "active": None,
            "state": None,
            "enabled": None,
            "enablement_state": None,
            "error": "FileNotFoundError",
        },
    )
    monkeypatch.setattr(
        health_router.subprocess,
        "run",
        lambda args, **_kwargs: CompletedProcess(
            args,
            0,
            expected + "\n" if args[1:3] == ["rev-parse", "HEAD"] else "",
            "",
        ),
    )

    with pytest.raises(HTTPException) as captured:
        health_router.health()

    assert captured.value.status_code == 503
    assert "standalone scheduler" in str(captured.value.detail)


def test_health_scopes_cleanliness_to_code_and_config_not_runtime_data(
    monkeypatch,
) -> None:
    expected = "e" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)
    calls = []

    def capture(args, **_kwargs):
        calls.append(args)
        stdout = expected + "\n" if args[1:3] == ["rev-parse", "HEAD"] else ""
        return CompletedProcess(args, 0, stdout, "")

    monkeypatch.setattr(health_router.subprocess, "run", capture)
    assert health_router.health()["status"] == "ok"
    status = next(args for args in calls if args[1:3] == ["status", "--porcelain"])
    assert "--untracked-files=all" in status
    assert "scripts" in status
    assert "strategies" in status
    assert "data" not in status


@pytest.mark.parametrize(
    "status_line",
    [
        "?? strategies/stock_strategy_v2.json\n",
        "?? scripts/sync_realtime_quotes.py\n",
        " M scripts/sync_realtime_quotes.py\n",
    ],
)
def test_health_rejects_protected_config_and_script_drift(
    monkeypatch,
    status_line,
) -> None:
    expected = "f" * 40
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", expected)

    def protected_drift(args, **_kwargs):
        if args[1:3] == ["rev-parse", "HEAD"]:
            stdout = expected + "\n"
        elif args[1:3] == ["status", "--porcelain"]:
            stdout = status_line
        else:
            stdout = ""
        return CompletedProcess(args, 0, stdout, "")

    monkeypatch.setattr(health_router.subprocess, "run", protected_drift)
    with pytest.raises(HTTPException) as captured:
        health_router.health()
    assert captured.value.status_code == 503
