from __future__ import annotations

from pathlib import Path

import pytest

from server.common import process_env
from server.common.adata_release import AdataReleaseError


def test_windows_qmt_edge_child_env_does_not_require_linux_adata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "qmt_windows_edge")
    shared_path = tmp_path / "shared"
    monkeypatch.setenv(
        "PYTHONPATH",
        str(tmp_path / "adata")
        + process_env.os.pathsep
        + str(shared_path),
    )
    monkeypatch.setattr(process_env, "_windows_qmt_edge_runtime", lambda: True)
    monkeypatch.setattr(
        process_env,
        "resolve_adata_source",
        lambda _root: (_ for _ in ()).throw(
            AssertionError("Linux adata release must not be resolved")
        ),
    )

    env = process_env.build_child_env(tmp_path)

    python_paths = env["PYTHONPATH"].split(process_env.os.pathsep)
    assert python_paths[0] == str(tmp_path)
    assert str(tmp_path / "adata") not in python_paths
    assert str(shared_path) in python_paths


def test_other_production_roles_still_require_sealed_adata(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("PROBIGA_DEPLOYMENT_MODE", "production")
    monkeypatch.setenv("PROBIGA_SCHEDULER_EXECUTOR_ROLE", "linux_standalone")
    monkeypatch.delenv("PROBIGA_ADATA_SOURCE_DIR", raising=False)
    monkeypatch.delenv("PROBIGA_EXPECTED_ADATA_SHA", raising=False)
    monkeypatch.delenv("PROBIGA_EXPECTED_ADATA_TREE_SHA256", raising=False)

    with pytest.raises(
        AdataReleaseError,
        match="production adata release source and both expected hashes",
    ):
        process_env.build_child_env(tmp_path)
