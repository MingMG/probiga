from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import subprocess
import sys
from types import SimpleNamespace

import pytest

from tools import run_qmt_windows_edge_release_bootstrap as bootstrap


PRIOR = "a" * 40
TARGET = "b" * 40


def seal(build=PRIOR):
    return {"attested_build_sha": build,
            "trigger_inventory_server_uuid": "11111111-2222-3333-4444-555555555555",
            "trigger_inventory_seal_database": "probiga",
            "trigger_inventory_contract_hash": "c" * 64,
            "trigger_inventory_table_comment": "old-frozen-contract"}


@pytest.mark.parametrize("prior", [PRIOR, "632e4e8" + "1" * 33])
def test_prior_reader_executes_retained_contract_code_not_candidate_contract(tmp_path, monkeypatch, prior):
    # An actual isolated child interprets the prior contract. The parent would
    # reject it under its changed frozen contract, so that validator must not run.
    from server.engine import strategy_governance
    monkeypatch.setattr(strategy_governance, "validate_privileged_trigger_migration_seal",
                        lambda *_args, **_kwargs: pytest.fail("candidate contract interpreted prior seal"))
    code = tmp_path / prior
    for directory in ("server", "server/common", "server/engine", "tools"):
        folder = code / directory
        folder.mkdir(parents=True, exist_ok=True)
        (folder / "__init__.py").write_text("")
    (code / "server/common/release_manifest.py").write_text(
        "def verify_runtime_release_manifest(root): return {'verified': True}\n")
    (code / "server/engine/strategy_governance.py").write_text(
        "import os\n"
        "def validate_privileged_trigger_migration_seal(connection, *, expected_build_sha):\n"
        f"    assert expected_build_sha == {prior!r}\n"
        "    assert os.environ['PROBIGA_BUILD_COMMIT_SHA'] == expected_build_sha\n"
        "    assert os.environ['PROBIGA_EXPECTED_GIT_SHA'] == expected_build_sha\n"
        f"    return {seal(prior)!r}\n")
    (code / "tools/run_qmt_windows_edge_release_bootstrap.py").write_text(
        "from contextlib import nullcontext\n"
        "class Engine:\n"
        "    def connect(self): return nullcontext(None)\n"
        "    def dispose(self): pass\n"
        "def _create_recovery_runtime_engine(): return Engine()\n")
    monkeypatch.setattr(bootstrap, "_require_activation_grant_root", lambda: None)
    monkeypatch.setenv("PROBIGA_BUILD_COMMIT_SHA", TARGET)
    monkeypatch.setenv("PROBIGA_EXPECTED_GIT_SHA", TARGET)
    child_env = {**os.environ, "PROBIGA_BUILD_COMMIT_SHA": prior, "PROBIGA_EXPECTED_GIT_SHA": prior}
    monkeypatch.setattr(bootstrap, "_retained_contract_runtime", lambda build: (code, Path(sys.executable), child_env))
    assert bootstrap._read_retained_contract_seal(prior) == seal(prior)
    assert os.environ["PROBIGA_BUILD_COMMIT_SHA"] == TARGET
    assert os.environ["PROBIGA_EXPECTED_GIT_SHA"] == TARGET


@pytest.mark.parametrize("output", [json.dumps(seal(TARGET)), "not json", json.dumps({**seal(), "password": "secret"})])
def test_reader_rejects_wrong_contract_or_malformed_output_without_echo(monkeypatch, output):
    monkeypatch.setattr(bootstrap, "_require_activation_grant_root", lambda: None)
    monkeypatch.setattr(bootstrap, "_retained_contract_runtime", lambda build: (Path("."), Path(sys.executable), {}))
    monkeypatch.setattr(bootstrap.subprocess, "run", lambda *_args, **_kwargs: SimpleNamespace(stdout=output))
    with pytest.raises(RuntimeError, match="^RECOVERY_BLOCKED: retained contract attestation failed$"):
        bootstrap._read_retained_contract_seal(PRIOR)


def test_reader_sanitizes_child_database_failure(monkeypatch):
    monkeypatch.setattr(bootstrap, "_require_activation_grant_root", lambda: None)
    monkeypatch.setattr(bootstrap, "_retained_contract_runtime", lambda build: (Path("."), Path(sys.executable), {}))
    def fail(*args, **kwargs):
        raise subprocess.CalledProcessError(2, args[0], stderr="mysql://password@host")
    monkeypatch.setattr(bootstrap.subprocess, "run", fail)
    with pytest.raises(RuntimeError) as error:
        bootstrap._read_retained_contract_seal(PRIOR)
    assert str(error.value) == "RECOVERY_BLOCKED: retained contract attestation failed"


@pytest.mark.parametrize("kind,owner,mode,links", [
    (stat.S_IFLNK, 0, 0o644, 1), (stat.S_IFREG, 1000, 0o644, 1),
    (stat.S_IFREG, 0, 0o666, 1), (stat.S_IFREG, 0, 0o644, 2),
])
def test_retained_path_rejects_mutable_or_indirect_artifact(kind, owner, mode, links):
    path = SimpleNamespace(parents=[], lstat=lambda: SimpleNamespace(
        st_mode=kind | mode, st_uid=owner, st_nlink=links))
    with pytest.raises(RuntimeError):
        bootstrap._retained_root_path(path)


@pytest.mark.parametrize("build", ["", "0" * 40, "A" * 40, "../" + PRIOR])
def test_retained_runtime_requires_canonical_build_before_filesystem(build):
    with pytest.raises(RuntimeError, match="build is invalid"):
        bootstrap._retained_contract_runtime(build)
