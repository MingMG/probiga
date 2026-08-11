from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

from server.common import adata_release
from server.common.process_env import build_child_env


GIT_SHA = "a" * 40


def _source(tmp_path: Path) -> tuple[Path, str]:
    source = (tmp_path / "release-source").resolve()
    package = source / "adata"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    (package / "module.py").write_text("VALUE = 2\n", encoding="utf-8")
    sealed = adata_release.seal_adata_release_source(source, GIT_SHA)
    return source, str(sealed["tree_sha256"])


def test_sealed_release_detects_source_tampering(tmp_path: Path) -> None:
    source, tree_sha = _source(tmp_path)
    result = adata_release.validate_adata_release_source(
        source,
        expected_git_sha=GIT_SHA,
        expected_tree_sha256=tree_sha,
    )
    assert result["tree_sha256"] == tree_sha

    (source / "adata" / "module.py").write_text("VALUE = 999\n", encoding="utf-8")
    with pytest.raises(adata_release.AdataReleaseError, match="tree hash"):
        adata_release.validate_adata_release_source(
            source,
            expected_git_sha=GIT_SHA,
            expected_tree_sha256=tree_sha,
        )


def test_production_resolver_requires_complete_release_identity(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.delenv(adata_release.ADATA_SOURCE_ENV, raising=False)
    monkeypatch.delenv(adata_release.ADATA_GIT_SHA_ENV, raising=False)
    monkeypatch.delenv(adata_release.ADATA_TREE_SHA_ENV, raising=False)

    with pytest.raises(adata_release.AdataReleaseError, match="required"):
        adata_release.resolve_adata_source(tmp_path)


def test_production_rejects_mutable_nested_checkout(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repository = (tmp_path / "repo").resolve()
    source = repository / "adata"
    package = source / "adata"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("VALUE = 1\n", encoding="utf-8")
    sealed = adata_release.seal_adata_release_source(source, GIT_SHA)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv(adata_release.ADATA_SOURCE_ENV, str(source))
    monkeypatch.setenv(adata_release.ADATA_GIT_SHA_ENV, GIT_SHA)
    monkeypatch.setenv(
        adata_release.ADATA_TREE_SHA_ENV,
        str(sealed["tree_sha256"]),
    )
    monkeypatch.setattr(adata_release.os, "access", lambda *_args: False)

    with pytest.raises(adata_release.AdataReleaseError, match="mutable nested"):
        adata_release.resolve_adata_source(repository)


def test_import_path_replaces_mutable_checkout_but_preserves_empty_entry(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repository = (tmp_path / "repo").resolve()
    mutable = repository / "adata"
    mutable.mkdir(parents=True)
    source, _tree_sha = _source(tmp_path)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    monkeypatch.setenv(adata_release.ADATA_SOURCE_ENV, str(source))
    original = ["", str(mutable), "sentinel"]
    monkeypatch.setattr(sys, "path", original.copy())

    resolved = adata_release.ensure_adata_import_path(repository)

    assert resolved == source
    assert sys.path[0] == str(source)
    assert "" in sys.path
    assert str(mutable) not in sys.path


def test_read_only_validation_checks_parent_directory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source, tree_sha = _source(tmp_path)

    monkeypatch.setattr(
        adata_release.os,
        "access",
        lambda path, mode: Path(path) == source.parent and mode == os.W_OK,
    )
    with pytest.raises(adata_release.AdataReleaseError, match="writable"):
        adata_release.validate_adata_release_source(
            source,
            expected_git_sha=GIT_SHA,
            expected_tree_sha256=tree_sha,
            require_read_only=True,
        )


def test_read_only_validation_checks_nested_directories(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source, tree_sha = _source(tmp_path)
    writable_child = source / "adata"
    monkeypatch.setattr(
        adata_release.os,
        "access",
        lambda path, mode: Path(path) == writable_child and mode == os.W_OK,
    )

    with pytest.raises(adata_release.AdataReleaseError, match="writable"):
        adata_release.validate_adata_release_source(
            source,
            expected_git_sha=GIT_SHA,
            expected_tree_sha256=tree_sha,
            require_read_only=True,
        )


def test_child_environment_prioritizes_verified_source_and_filters_mutable_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repository = (tmp_path / "repo").resolve()
    mutable = repository / "adata"
    mutable.mkdir(parents=True)
    source, _tree_sha = _source(tmp_path)
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "development")
    monkeypatch.setenv(adata_release.ADATA_SOURCE_ENV, str(source))
    monkeypatch.setenv(
        "PYTHONPATH",
        os.pathsep.join((str(mutable), "inherited-safe")),
    )

    env = build_child_env(
        repository,
        extra_python_paths=(mutable, "extra-safe"),
    )
    paths = env["PYTHONPATH"].split(os.pathsep)

    assert paths[:2] == [str(source), str(repository)]
    assert str(mutable) not in paths
    assert "inherited-safe" in paths
    assert "extra-safe" in paths
