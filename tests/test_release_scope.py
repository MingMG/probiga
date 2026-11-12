"""Exercise release scope against real Git histories, never the working tree."""

import importlib.util
import json
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys

import pytest


TOOL = Path(__file__).resolve().parents[1] / "tools/classify_release_scope.py"
SPEC = importlib.util.spec_from_file_location("release_scope_policy", TOOL)
policy = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = policy
SPEC.loader.exec_module(policy)


class Repo:
    def __init__(self, root):
        self.root = root
        root.mkdir()
        self.git("init", "--initial-branch=main")
        self.git("config", "user.email", "scope-test@example.invalid")
        self.git("config", "user.name", "Scope test")
        self.git("config", "core.autocrlf", "false")
        self.write(
            "server/api/main.py",
            'from server.api.routers import health\n'
            'app.include_router(health.router, prefix="/api")\n',
        )
        self.write("server/static/index.html", "initial")
        self.write("server/common/config.py", "SETTING = 1\n")
        self.write("docs/readme.md", "documentation")
        self.base = self.commit()

    def git(self, *args, input=None):
        result = subprocess.run(
            [policy._git_executable(), "-C", str(self.root), *args],
            env=policy._git_environment(), input=input,
            capture_output=True, check=True,
        )
        return result.stdout.decode("utf-8").strip()

    def write(self, path, text):
        file = self.root / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(text, encoding="utf-8", newline="\n")

    def commit(self):
        self.git("add", "--all")
        self.git("commit", "--no-gpg-sign", "-m", "release scope fixture")
        return self.git("rev-parse", "HEAD")

    def classify(self, target=None, base=None, windows_base=None):
        return policy.classify_release_scope(
            repository=self.root, base_sha=base or self.base,
            target_sha=target or self.git("rev-parse", "HEAD"),
            windows_base_sha=windows_base,
        )


@pytest.fixture
def repo(tmp_path):
    return Repo(tmp_path / "repository")


@pytest.mark.parametrize("path", [
    "server/api/routers/auth.py",
    "server/common/config.py",
    "server/static/js/app.js",
    "deploy/production_deploy.sh",
    "tools/classify_release_scope.py",
    "requirements.txt",
    ".env.example",
])
def test_application_and_deployment_changes_are_linux(repo, path):
    repo.write(path, "changed\n")
    result = repo.classify(repo.commit())
    assert result["schema"] == "probiga.release-scope.v4"
    assert result["scope"] == "LINUX"
    assert result["reason_codes"] == ["code_only_delta"]
    assert result["base_contract_sha256"] == result["contract_sha256"]


@pytest.mark.parametrize("path", policy._WINDOWS_RUNTIME_FILES)
def test_windows_runtime_body_change_is_not_a_schema_change(repo, path):
    repo.write(path, "REVISION = 1\n\ndef read():\n    return 1\n")
    windows = repo.commit()
    repo.write(path, "REVISION = 1\n\ndef read():\n    return 2\n")
    result = repo.classify(repo.commit(), base=windows, windows_base=windows)
    assert result["scope"] == "COORDINATED"
    assert result["reason_codes"] == ["windows_runtime_changed"]
    assert result["windows_runtime_changed_paths"] == [path]
    assert result["base_contract_sha256"] == result["contract_sha256"]


def _missed_windows_runtime_release(
    repo, path="server/common/qmt_datadir_minute_coverage.py",
):
    repo.write(path, "REVISION = 1\n\ndef persisted_matrix():\n    return 1\n")
    windows = repo.commit()
    repo.write(path, "REVISION = 1\n\ndef persisted_matrix():\n    return 2\n")
    linux = repo.commit()
    repo.write("tools/classify_release_scope.py", "POLICY_IMPLEMENTATION = 2\n")
    return windows, linux, repo.commit()


@pytest.mark.parametrize("path", [
    "tools/import_qmt_datadir_daily.py",
    "server/common/historical_daily_bar_evidence.py",
    "server/common/official_historical_daily_evidence.py",
    "server/common/qmt_daily_market_truth.py",
    "server/common/qmt_datadir_daily_coverage.py",
    "server/common/qmt_datadir_minute_coverage.py",
    "server/common/qmt_announcement_pit.py",
    "tools/sync_qmt_announcement_pit.py",
    "server/common/qmt_edge_release_receipt.py",
    "tools/run_qmt_windows_edge_release_bootstrap.py",
])
def test_deployed_windows_base_catches_a_runtime_change_already_in_linux(repo, path):
    windows, linux, target = _missed_windows_runtime_release(repo, path)
    historical = repo.classify(target, base=linux)
    assert historical["scope"] == "LINUX"
    assert historical["windows_base_sha"] is None
    result = repo.classify(target, base=linux, windows_base=windows)
    assert result["scope"] == "COORDINATED"
    assert result["reason_codes"] == ["windows_runtime_changed"]
    assert result["windows_base_sha"] == windows
    assert result["windows_runtime_changed_paths"] == [path]
    assert result["base_contract_sha256"] == result["contract_sha256"]
    # Once the artifact reached Windows, another Linux repository commit does
    # not force that unchanged runtime to reload.
    assert repo.classify(target, base=linux, windows_base=linux)["scope"] == "LINUX"


def test_linux_function_change_preserves_unchanged_windows_artifacts(repo):
    repo.write("server/common/qmt_datadir_minute_coverage.py", "READER = 1\n")
    windows = repo.commit()
    repo.write("server/common/pit_facts.py", "def read():\n    return 1\n")
    linux = repo.commit()
    repo.write("server/common/pit_facts.py", "def read():\n    return 2\n")
    result = repo.classify(repo.commit(), base=linux, windows_base=windows)
    assert result["scope"] == "LINUX"
    assert result["reason_codes"] == ["code_only_delta"]
    assert result["windows_runtime_changed_paths"] == []


def test_deploy_passes_the_guarded_windows_base_and_targets_the_new_artifact(repo, tmp_path):
    windows, linux, target = _missed_windows_runtime_release(repo)
    git_bash = Path("C:/Program Files/Git/bin/bash.exe")
    bash = str(git_bash) if git_bash.is_file() else shutil.which("bash")
    if not bash:
        pytest.skip("Bash unavailable")
    script = (TOOL.parents[1] / "deploy/production_deploy.sh").read_text(encoding="utf-8")
    bodies = dict(re.findall(r"(?ms)^([a-zA-Z_][a-zA-Z0-9_]*)\(\) \{\n(.*?)^[ \t]*\}\s*$", script))
    tools_root, artifacts = tmp_path / "scope-tools", tmp_path / "artifacts"
    (tools_root / target).mkdir(parents=True)
    artifacts.mkdir()
    values = {
        "BOOTSTRAP_PYTHON": Path(sys.executable).as_posix(),
        "COMPONENT_CLASSIFIER": TOOL.as_posix(),
        "CODE_GIT_CACHE": (repo.root / ".git").as_posix(),
        "COMPONENT_TOOL_ROOT": tools_root.as_posix(),
        "RELEASE_ARTIFACT_ROOT": artifacts.as_posix(),
        "PREVIOUS_SHA": linux, "EXPECTED_SHA": target,
        "TARGET_WINDOWS_SHA": target, "TARGET_CONTRACT_SHA": target,
        "STRICT_WINDOWS_MANIFEST": "0", "RELEASE_MODE": "full", "I_KNOW_DDL_CHANGED": "0",
    }
    harness = "set -eu\n" + "\n".join(f"{key}={shlex.quote(value)}" for key, value in values.items())
    harness += f"""
controlled_guard_component_sha() {{ printf '%s\\n' {windows}; }}
log_release() {{ printf '%s\\n' "$*"; }}
log_release_warning() {{ printf '%s\\n' "$*"; }}
log_release_error() {{ printf '%s\\n' "$*" >&2; }}
resolve_release_execution_scope() {{
{bodies['resolve_release_execution_scope']}
}}
classify_component_release() {{
{bodies['classify_component_release']}
}}
classify_component_release
printf 'RESULT:%s:%s:%s\\n' "$RELEASE_SCOPE" "$RELEASE_EXECUTION_SCOPE" "$TARGET_WINDOWS_SHA"
"""
    completed = subprocess.run([bash, "-c", harness], capture_output=True, text=True, timeout=60)
    assert completed.returncode == 0, completed.stdout + completed.stderr
    assert f"RESULT:COORDINATED:COORDINATED:{target}" in completed.stdout
    report = json.loads((tools_root / target / "scope.json").read_text())
    assert report["windows_base_sha"] == windows
    assert report["reason_codes"] == ["windows_runtime_changed"]


def test_windows_comparison_base_requires_a_full_ancestor_commit(repo):
    repo.write("server/static/index.html", "branch one")
    windows = repo.commit()
    repo.git("checkout", "--detach", repo.base)
    repo.write("server/static/index.html", "branch two")
    target = repo.commit()
    with pytest.raises(policy.ScopeError, match="^windows_base_not_ancestor$"):
        repo.classify(target, windows_base=windows)
    with pytest.raises(policy.ScopeError, match="^full_commit_sha_required$"):
        repo.classify(target, windows_base=windows[:12])


def test_qmt_strategy_source_change_is_coordinated(repo):
    # The Linux API hashes this source from its own checkout and requires the
    # Windows bridge to report the same SHA-256 and Git blob, so a Linux-only
    # release of a change here can never converge both endpoints.
    path = "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py"
    repo.write(path, "BRIDGE = 1\n")
    base = repo.commit()
    repo.write(path, "BRIDGE = 1\n\n\ndef read():\n    return 2\n")
    result = repo.classify(repo.commit(), base=base)
    assert result["scope"] == "COORDINATED"
    assert result["reason_codes"] == ["schema_contract_changed"]
    assert result["base_contract_sha256"] != result["contract_sha256"]


def test_deleting_account_auth_code_is_linux(repo):
    repo.write("server/auth/service.py", "LOGIN = True\n")
    mid = repo.commit()
    (repo.root / "server/auth/service.py").unlink()
    result = repo.classify(repo.commit(), base=mid)
    assert result["scope"] == "LINUX"
    assert "server/auth/service.py" in result["changed_paths"]


def test_docs_tests_only_and_no_change_are_linux(repo):
    assert repo.classify()["scope"] == "LINUX"
    repo.write("docs/readme.md", "new docs")
    repo.write("tests/test_new.py", "assert True\n")
    result = repo.classify(repo.commit())
    assert result["scope"] == "LINUX"


def test_python_ddl_is_coordinated_and_code_edits_stay_linux(repo):
    repo.write(
        "server/db/migrations.py",
        'SQL = "CREATE TABLE example (id INT PRIMARY KEY);"\n',
    )
    schema = repo.commit()
    assert repo.classify(schema)["scope"] == "COORDINATED"
    repo.write(
        "server/db/migrations.py",
        'SQL = "CREATE TABLE example (id INT PRIMARY KEY);"\n'
        "UNRELATED = True\n",
    )
    result = repo.classify(repo.commit(), base=schema)
    assert result["scope"] == "LINUX"
    assert result["base_contract_sha256"] == result["contract_sha256"]


def test_auxiliary_stage_index_change_is_coordinated_and_body_change_is_linux(repo):
    path = "server/common/auxiliary_runtime_schema.py"
    before = (
        '_QMT_DATADIR_IMPORT_DDL = ("CREATE TABLE IF NOT EXISTS '
        'qmt_datadir_minute_stage (run_id VARCHAR(64), trade_date DATE, '
        'stock_code VARCHAR(16), trade_time DATETIME, '
        'KEY idx_qdms_run_date (run_id, trade_date))",)\n'
        '\n\ndef validate_qmt_datadir_import_runtime_schema(engine):\n'
        '    return engine\n'
    )
    repo.write(path, before)
    base = repo.commit()
    after = before.replace(
        "KEY idx_qdms_run_date (run_id, trade_date)",
        "KEY idx_qdms_run_date_code_time (run_id, trade_date, stock_code, trade_time)",
    )
    repo.write(path, after)
    indexed = repo.commit()
    result = repo.classify(indexed, base=base)
    assert result["scope"] == "COORDINATED"
    assert result["reason_codes"] == ["schema_contract_changed"]
    assert result["base_contract_sha256"] != result["contract_sha256"]

    repo.write(path, after.replace("return engine", "return bool(engine)"))
    result = repo.classify(repo.commit(), base=indexed)
    assert result["scope"] == "LINUX"
    assert result["reason_codes"] == ["code_only_delta"]
    assert result["base_contract_sha256"] == result["contract_sha256"]


def test_sql_file_ddl_is_coordinated(repo):
    repo.write("db/schema.sql", "ALTER TABLE example ADD COLUMN name VARCHAR(20);\n")
    assert repo.classify(repo.commit())["scope"] == "COORDINATED"


CONTRACT_MODULE = "server/common/scheduler_args.py"


def test_contract_docstring_and_comment_change_is_linux(repo):
    repo.write(CONTRACT_MODULE, '"""Revision."""\nREVISION = 1\n')
    first = repo.commit()
    repo.write(CONTRACT_MODULE, '"""Revision, clarified."""\nREVISION = 1  # same\n')
    result = repo.classify(repo.commit(), base=first)
    assert result["scope"] == "LINUX"
    assert result["base_contract_sha256"] == result["contract_sha256"]


def test_contract_function_body_change_is_linux(repo):
    repo.write(CONTRACT_MODULE, "REVISION = 1\n\n\ndef helper(value):\n    return value\n")
    first = repo.commit()
    repo.write(
        CONTRACT_MODULE,
        "REVISION = 1\n\n\ndef helper(value):\n    # traced\n    return value + 1\n",
    )
    result = repo.classify(repo.commit(), base=first)
    assert result["scope"] == "LINUX"
    assert result["base_contract_sha256"] == result["contract_sha256"]


@pytest.mark.parametrize("path", [
    "server/api/scheduler_runtime.py",
    "server/common/qmt_edge_release_receipt.py",
    "tools/qmt_host_ownership_contract.py",
    "tools/update_qmt_windows_edge.ps1",
])
def test_contract_declaration_change_is_coordinated(repo, path):
    repo.write(path, "CONTRACT_REVISION = 1\n")
    first = repo.commit()
    repo.write(path, "CONTRACT_REVISION = 2\n")
    result = repo.classify(repo.commit(), base=first)
    assert result["scope"] == "COORDINATED"
    expected_reasons = ["schema_contract_changed"]
    if path in policy._WINDOWS_RUNTIME_FILES:
        expected_reasons.append("windows_runtime_changed")
    assert result["reason_codes"] == expected_reasons


def test_contract_binding_addition_and_removal_are_coordinated(repo):
    repo.write(CONTRACT_MODULE, "ALPHA = 1\n")
    first = repo.commit()
    repo.write(CONTRACT_MODULE, "ALPHA = 1\nBETA = 2\n")
    second = repo.commit()
    assert repo.classify(second, base=first)["scope"] == "COORDINATED"
    repo.write(CONTRACT_MODULE, "ALPHA = 1\n")
    third = repo.commit()
    assert repo.classify(third, base=second)["scope"] == "COORDINATED"


def test_contract_same_value_with_different_spelling_is_linux(repo):
    repo.write(CONTRACT_MODULE, "TASK_TYPES = frozenset({'a', 'b'})\n")
    first = repo.commit()
    repo.write(CONTRACT_MODULE, "TASK_TYPES = frozenset({'b', 'a'})\n")
    result = repo.classify(repo.commit(), base=first)
    assert result["scope"] == "LINUX"
    assert result["base_contract_sha256"] == result["contract_sha256"]


def test_contract_mapping_order_change_is_coordinated(repo):
    repo.write(CONTRACT_MODULE, "READY = {'a': 1, 'b': 2}\n")
    first = repo.commit()
    repo.write(CONTRACT_MODULE, "READY = {'b': 2, 'a': 1}\n")
    assert repo.classify(repo.commit(), base=first)["scope"] == "COORDINATED"


def test_contract_module_level_mapping_mutation_is_coordinated(repo):
    repo.write(CONTRACT_MODULE, "DEPENDENCIES = {}\nDEPENDENCIES['a'] = ('x',)\n")
    first = repo.commit()
    repo.write(CONTRACT_MODULE, "DEPENDENCIES = {}\nDEPENDENCIES['a'] = ('y',)\n")
    result = repo.classify(repo.commit(), base=first)
    assert result["scope"] == "COORDINATED"


def test_contract_multi_target_binding_rename_is_coordinated(repo):
    repo.write(CONTRACT_MODULE, "ALPHA = BETA = 1\n")
    first = repo.commit()
    repo.write(CONTRACT_MODULE, "GAMMA = DELTA = 1\n")
    assert repo.classify(repo.commit(), base=first)["scope"] == "COORDINATED"


def test_contract_export_list_is_not_part_of_the_contract(repo):
    repo.write(CONTRACT_MODULE, "REVISION = 1\n__all__ = ['REVISION']\n")
    first = repo.commit()
    repo.write(CONTRACT_MODULE, "REVISION = 1\n__all__ = ['REVISION', 'helper']\n")
    result = repo.classify(repo.commit(), base=first)
    assert result["scope"] == "LINUX"


def test_deleted_contract_file_is_coordinated(repo):
    repo.write(CONTRACT_MODULE, "REVISION = 1\n")
    first = repo.commit()
    (repo.root / CONTRACT_MODULE).unlink()
    result = repo.classify(repo.commit(), base=first)
    assert result["scope"] == "COORDINATED"


def test_unparsable_target_contract_file_aborts(repo):
    repo.write(CONTRACT_MODULE, "def broken(:\n")
    with pytest.raises(policy.ScopeError, match="^contract_parse_failed$"):
        repo.classify(repo.commit())


def test_empty_target_contract_file_aborts(repo):
    repo.write(CONTRACT_MODULE, "import os\n\n\ndef helper():\n    return 1\n")
    with pytest.raises(policy.ScopeError, match="^contract_extraction_empty$"):
        repo.classify(repo.commit())


def test_duplicate_target_contract_binding_aborts(repo):
    repo.write(CONTRACT_MODULE, "REVISION = 1\nREVISION = 2\n")
    with pytest.raises(policy.ScopeError, match="^contract_symbol_duplicate$"):
        repo.classify(repo.commit())


def test_unsupported_target_contract_value_aborts(repo):
    repo.write(CONTRACT_MODULE, "PHASE = 1j\n")
    with pytest.raises(policy.ScopeError, match="^contract_value_unsupported$"):
        repo.classify(repo.commit())


def test_unparsable_base_contract_file_stays_lenient(repo):
    # The base tree cannot be repaired by this release; a broken base must still
    # classify (as a contract change) instead of aborting the deploy.
    repo.write(CONTRACT_MODULE, "def broken(:\n")
    broken = repo.commit()
    repo.write(CONTRACT_MODULE, "REVISION = 1\n")
    result = repo.classify(repo.commit(), base=broken)
    assert result["scope"] == "COORDINATED"


def test_same_sha_recompute_is_lenient_and_stable(repo):
    repo.write(CONTRACT_MODULE, "def broken(:\n")
    broken = repo.commit()
    first = repo.classify(broken, base=broken)
    second = repo.classify(broken, base=broken)
    assert first == second
    assert first["scope"] == "LINUX"
    assert first["base_contract_sha256"] == first["contract_sha256"]


def test_every_policy_python_file_declares_bindings():
    root = TOOL.parents[1]
    assert policy._CONTRACT_PYTHON_FILES
    assert policy._CONTRACT_WHOLE_FILE_FILES
    for path in policy._CONTRACT_PYTHON_FILES:
        record = policy._contract_file_record(
            path, (root / path).read_bytes(), strict=True,
        )
        assert record["bindings"], path


def test_symlinks_and_gitlinks_fail_even_under_docs(repo):
    blob = repo.git("hash-object", "-w", "--stdin", input=b"../../secret")
    repo.git("update-index", "--add", "--cacheinfo", "120000", blob, "docs/link")
    repo.git("commit", "--no-gpg-sign", "-m", "symlink")
    with pytest.raises(policy.ScopeError, match="^unsafe_tree_entry$"):
        repo.classify()
    repo.git("reset", "--hard", repo.base)
    repo.git("update-index", "--add", "--cacheinfo", "160000", repo.base, "docs/submodule")
    repo.git("commit", "--no-gpg-sign", "-m", "gitlink")
    with pytest.raises(policy.ScopeError, match="^unsafe_tree_entry$"):
        repo.classify()


def test_nonancestor_and_abbreviations_are_rejected(repo):
    repo.write("server/static/index.html", "first")
    first = repo.commit()
    repo.git("checkout", "--detach", repo.base)
    repo.write("server/static/index.html", "second")
    second = repo.commit()
    with pytest.raises(policy.ScopeError, match="^base_not_ancestor$"):
        repo.classify(second, first)
    with pytest.raises(policy.ScopeError, match="^full_commit_sha_required$"):
        repo.classify(second[:12])


def test_dirty_working_tree_is_not_part_of_the_contract(repo):
    repo.write("server/common/config.py", "uncommitted_shared_change()\n")
    result = repo.classify()
    assert result["changed_paths"] == []
    assert result["scope"] == "LINUX"


def test_bare_mirror_cli_matches_checkout(repo, tmp_path):
    repo.write("server/static/index.html", "updated")
    target = repo.commit()
    mirror = tmp_path / "mirror.git"
    repo.git("clone", "--bare", str(repo.root), str(mirror))
    output = subprocess.run([
        sys.executable, "-I", str(TOOL), "--git-dir", str(mirror),
        "--base-sha", repo.base, "--target-sha", target,
    ], check=True, capture_output=True, text=True)
    assert json.loads(output.stdout) == repo.classify(target)


def test_errors_do_not_echo_repository_path_or_git_stderr(tmp_path):
    secret_path = tmp_path / "sensitive-secret-path"
    secret_path.mkdir()
    output = subprocess.run([
        sys.executable, "-I", str(TOOL), "--repository", str(secret_path),
        "--base-sha", "1" * 40, "--target-sha", "2" * 40,
    ], capture_output=True, text=True)
    assert output.returncode == 2
    assert output.stdout == ""
    assert "sensitive-secret-path" not in output.stderr
    assert json.loads(output.stderr)["error"] == "git_read_failed"


@pytest.mark.parametrize("name", [b"..", b".git", b"bad\\path", b"absolute:drive", b"trailing."])
def test_handcrafted_git_tree_paths_are_rejected_without_checkout(repo, name):
    blob = repo.git("hash-object", "-w", "--stdin", input=b"untrusted content")
    tree = repo.git("hash-object", "--literally", "-t", "tree", "-w", "--stdin",
                    input=b"100644 " + name + b"\0" + bytes.fromhex(blob))
    target = repo.git("commit-tree", tree, "-p", repo.base, "-m", "crafted tree")
    with pytest.raises(policy.ScopeError, match="^unsafe_tree_entry$"):
        repo.classify(target)


def test_invalid_cli_inputs_are_not_echoed():
    output = subprocess.run([sys.executable, "-I", str(TOOL), "--private-secret-argument"],
                            capture_output=True, text=True)
    assert output.returncode == 2
    assert "private-secret" not in output.stderr
    assert json.loads(output.stderr)["error"] == "invalid_arguments"
