"""Exercise release scope against real Git histories, never the working tree."""

import importlib.util
import json
from pathlib import Path
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

    def classify(self, target=None, base=None):
        return policy.classify_release_scope(
            repository=self.root, base_sha=base or self.base,
            target_sha=target or self.git("rev-parse", "HEAD"),
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
    "integrations/bigqmt/qmt_strategy/probiga_big_qmt_bridge.py",
])
def test_application_and_deployment_changes_are_linux(repo, path):
    repo.write(path, "changed\n")
    result = repo.classify(repo.commit())
    assert result["schema"] == "probiga.release-scope.v4"
    assert result["scope"] == "LINUX"
    assert result["reason_codes"] == ["code_only_delta"]
    assert result["base_contract_sha256"] == result["contract_sha256"]


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
    assert result["reason_codes"] == ["schema_contract_changed"]


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
