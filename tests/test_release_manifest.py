from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from server.api.routers import health
from server.common import release_manifest


BUILD_SHA = "a" * 40
TREE_SHA = "b" * 64


def _manifest():
    return release_manifest.build_release_manifest(
        release_id=BUILD_SHA,
        source_tree_hash=TREE_SHA,
        migration_version="c" * 64,
        built_at="2026-09-02T01:02:03Z",
        artifact_components={
            "release_id": BUILD_SHA,
            "tree": TREE_SHA,
            "wheel": "d" * 64,
        },
    )


def test_release_manifest_is_sealed_read_only_and_does_not_need_git(
    monkeypatch,
    tmp_path,
):
    payload = _manifest()
    path = release_manifest.write_release_manifest(tmp_path, payload)
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD_SHA)
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", BUILD_SHA)
    monkeypatch.setenv("PROBIGA_RELEASE_TREE_SHA256", TREE_SHA)

    verified = release_manifest.verify_runtime_release_manifest(tmp_path)

    assert path.name == "probiga.release.json"
    assert verified["verified"] is True
    assert verified["read_only"] is True
    assert verified["manifest"] == payload


def test_release_manifest_rejects_tampering_and_duplicate_keys(tmp_path):
    path = release_manifest.release_manifest_path(tmp_path)
    path.write_text(
        '{"schema":"probiga.release-manifest.v1","schema":"forged"}',
        encoding="utf-8",
    )
    with pytest.raises(release_manifest.ReleaseManifestError, match="duplicate"):
        release_manifest.load_release_manifest(tmp_path)

    payload = _manifest()
    payload["artifact_hash"] = "0" * 64
    with pytest.raises(release_manifest.ReleaseManifestError, match="seal"):
        release_manifest.validate_release_manifest(payload)


def test_database_registry_is_idempotent_and_rejects_same_release_drift():
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    release_manifest.privileged_migrate_release_manifest_schema(engine)
    payload = _manifest()

    first = release_manifest.register_runtime_release_manifest(engine, payload)
    second = release_manifest.register_runtime_release_manifest(engine, payload)
    assert first == second == payload

    with engine.begin() as connection:
        connection.execute(
            text(
                f"UPDATE {release_manifest.REGISTRY_TABLE} "
                "SET artifact_hash=:artifact_hash WHERE release_id=:release_id"
            ),
            {"artifact_hash": "e" * 64, "release_id": BUILD_SHA},
        )
    with pytest.raises(
        release_manifest.ReleaseManifestError,
        match="database release manifest identity differs",
    ):
        release_manifest.register_runtime_release_manifest(engine, payload)


def test_health_uses_manifest_without_invoking_git(monkeypatch, tmp_path):
    release_manifest.write_release_manifest(tmp_path, _manifest())
    monkeypatch.setattr(health, "REPOSITORY_ROOT", tmp_path)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD_SHA)
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", BUILD_SHA)
    monkeypatch.setenv("PROBIGA_RELEASE_TREE_SHA256", TREE_SHA)

    def git_must_not_run(*_args, **_kwargs):
        raise AssertionError("runtime Git inspection is forbidden with a manifest")

    monkeypatch.setattr(health.subprocess, "run", git_must_not_run)
    result = health._deployed_git_revision()

    assert result["identity_source"] == "release_manifest"
    assert result["actual_git_sha"] == BUILD_SHA
    assert result["matches_expected"] is True
    assert result["code_worktree_clean"] is True
    assert result["inspection_status"] == "ok"


def test_production_health_missing_manifest_fails_without_git(monkeypatch, tmp_path):
    monkeypatch.setattr(health, "REPOSITORY_ROOT", tmp_path)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", BUILD_SHA)
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", BUILD_SHA)

    def git_must_not_run(*_args, **_kwargs):
        raise AssertionError("production runtime Git inspection is forbidden")

    monkeypatch.setattr(health.subprocess, "run", git_must_not_run)
    result = health._deployed_git_revision()

    assert result["identity_source"] == "release_manifest"
    assert result["inspection_error_code"] == "manifest_missing"
    assert result["matches_expected"] is False


def test_release_manifest_cli_is_wired_into_production_deploy():
    source = (
        Path(__file__).resolve().parents[1]
        / "deploy"
        / "production_deploy.sh"
    ).read_text(encoding="utf-8")
    assert "server.common.release_manifest write" in source
    assert '--root "$CODE_VALIDATION_ROOT"' in source
    assert 'test -r "$CODE_VALIDATION_ROOT/probiga.release.json"' in source
