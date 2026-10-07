from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from tools import install_qmt_simulation_entries as installer


BUILD = "a" * 40
SOURCE_ROOT = installer.ROOT


@pytest.fixture
def release(monkeypatch, tmp_path):
    root = tmp_path / "exact-production"
    committed = {}
    for name in (installer.TEMPLATE, installer.RUNNER):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        committed[name] = (SOURCE_ROOT / name).read_bytes()
        path.write_bytes(committed[name])
    monkeypatch.setattr(installer, "ROOT", root)
    monkeypatch.setattr(installer.subprocess, "check_output",
                        lambda args, **kwargs: BUILD if "rev-parse" in args else "")
    def artifact(*, root, source_path, build_sha):
        assert build_sha == BUILD
        encoded = committed[Path(source_path).relative_to(root).as_posix()]
        return {"source_bytes": encoded, "source_sha256": hashlib.sha256(encoded).hexdigest(),
                "git_blob": hashlib.sha1(encoded).hexdigest()}
    monkeypatch.setattr(installer, "git_strategy_artifact", artifact)
    return root, tmp_path / "QMT"


def test_read_only_missing_proof_creates_no_directory(release):
    root, qmt = release
    result = installer.validate_installed_entries(qmt_home=qmt, expected_build_sha=BUILD)
    assert result["status"] == "NOT_READY"
    assert result["entry_count"] == 14
    assert len(result["errors"]) == 15
    assert not qmt.exists()
    assert result["catalog_registration"] == "NOT_ATTESTED"
    assert result["database_writes"] is False and result["qmt_calls"] is False


def test_exact_roster_hash_names_and_runner_have_read_only_ready_proof(release):
    root, qmt = release
    installed = installer.install_entries(qmt_home=qmt, expected_build_sha=BUILD)
    before = {path: (path.read_bytes(), path.stat().st_mtime_ns) for path in (qmt / "python").iterdir()}
    result = installer.validate_installed_entries(qmt_home=qmt, expected_build_sha=BUILD)
    assert result["status"] == "READY" and result["errors"] == []
    assert result["entry_count"] == 14
    assert all(path.read_bytes() == data and path.stat().st_mtime_ns == stamp
               for path, (data, stamp) in before.items())
    assert {item["strategy_key"] for item in installed["entries"]} == {
        item["strategy_key"] for item in installer.strategy_catalog()["strategies"]
        + installer.strategy_catalog()["combinations"]}
    assert all(item["qmt_name"] == "PROBIGA模拟_" + item["name"] for item in installed["entries"])
    assert installed["runner_sha256"] == hashlib.sha256((root / installer.RUNNER).read_bytes()).hexdigest()
    assert result["catalog_registration"] == "NOT_ATTESTED"


@pytest.mark.parametrize("fault", ["missing_entry", "changed_entry", "missing_manifest", "old_build",
                                   "wrong_roster", "wrong_name", "wrong_runner_hash", "unsafe_flag", "non_boolean_flag"])
def test_readiness_rejects_missing_old_changed_or_counterfeit_artifacts(release, fault):
    _, qmt = release
    installed = installer.install_entries(qmt_home=qmt, expected_build_sha=BUILD)
    manifest_path = Path(installed["manifest_path"])
    if fault == "missing_entry":
        Path(installed["entries"][0]["path"]).unlink()
    elif fault == "changed_entry":
        Path(installed["entries"][0]["path"]).write_bytes(b"# QMT default sample must not count as installed\n")
    elif fault == "missing_manifest":
        manifest_path.unlink()
    else:
        manifest = json.loads(manifest_path.read_text("utf8"))
        if fault == "old_build":
            manifest["build_sha"] = "b" * 40
        elif fault == "wrong_roster":
            manifest["entries"].pop()
        elif fault == "wrong_name":
            manifest["entries"][0]["qmt_name"] = "not-original-strategy"
        elif fault == "wrong_runner_hash":
            manifest["runner_sha256"] = "0" * 64
        elif fault == "non_boolean_flag":
            manifest["simulation_only"] = 1
        else:
            manifest["automatic_real_order_submission"] = True
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf8")
    result = installer.validate_installed_entries(qmt_home=qmt, expected_build_sha=BUILD)
    assert result["status"] == "NOT_READY" and result["errors"]
    assert result["database_writes"] is False and result["qmt_calls"] is False


def test_apply_rejects_dirty_release_before_writing(release, monkeypatch):
    _, qmt = release
    monkeypatch.setattr(installer.subprocess, "check_output",
                        lambda args, **kwargs: BUILD if "rev-parse" in args else " M tools/run_qmt_strategy_daily.py")
    with pytest.raises(RuntimeError, match="exact checkout"):
        installer.install_entries(qmt_home=qmt, expected_build_sha=BUILD)
    assert not qmt.exists()


def test_apply_rejects_runner_bytes_not_from_exact_commit(release):
    root, qmt = release
    (root / installer.RUNNER).write_bytes(b"# uncommitted alternate runner\n")
    with pytest.raises(RuntimeError, match="source differs from Git"):
        installer.install_entries(qmt_home=qmt, expected_build_sha=BUILD)
    assert not qmt.exists()


def test_operator_cli_defaults_to_read_only_validation(release, monkeypatch, capsys):
    from tools import env_config
    _, qmt = release
    monkeypatch.setattr(env_config, "load_project_env", lambda: None)
    monkeypatch.setattr(installer, "resolve_big_qmt_home", lambda **kwargs: qmt)
    assert installer.main(["--expected-build-sha", BUILD]) == 4
    assert json.loads(capsys.readouterr().out)["status"] == "NOT_READY"
    assert not qmt.exists()


def test_operator_cli_apply_without_exact_activation_never_writes(release, monkeypatch):
    from types import SimpleNamespace
    from tools import env_config, run_qmt_windows_edge_release_bootstrap as bootstrap
    _, qmt = release
    disposed = []
    monkeypatch.setattr(env_config, "load_project_env", lambda: None)
    monkeypatch.setattr(env_config, "create_tool_engine", lambda: SimpleNamespace(dispose=lambda: disposed.append(True)))
    monkeypatch.setattr(installer, "os", SimpleNamespace(name="nt", environ={"PROBIGA_SCHEDULER_EXECUTOR_ROLE": "qmt_windows_edge"}))
    monkeypatch.setattr(bootstrap, "read_release_activation", lambda *_a, **_k: {"status": "PENDING"})
    monkeypatch.setattr(installer, "resolve_big_qmt_home", lambda **kwargs: pytest.fail("No target lookup before activation"))
    with pytest.raises(RuntimeError, match="exact activation grant"):
        installer.main(["--expected-build-sha", BUILD, "--apply"])
    assert disposed == [True] and not qmt.exists()
