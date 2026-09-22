"""Exercise release scope against real Git histories, never the working tree."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


TOOL = Path(__file__).resolve().parents[1] / "tools/classify_release_scope.py"
SPEC = importlib.util.spec_from_file_location("release_scope_policy", TOOL)
policy = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = policy
SPEC.loader.exec_module(policy)

MAIN = 'from server.api.routers import health\napp.include_router(health.router, prefix="/api")\n'
JOURNAL_MAIN = (
    'from server.api.routers import health, trading_day\n'
    'app.include_router(health.router, prefix="/api")\n'
    'app.include_router(trading_day.router, prefix="/api")\n'
)


class Repo:
    def __init__(self, root):
        self.root = root
        root.mkdir()
        self.git("init", "--initial-branch=main")
        self.git("config", "user.email", "scope-test@example.invalid")
        self.git("config", "user.name", "Scope test")
        self.git("config", "core.autocrlf", "false")
        self.write("server/api/main.py", MAIN)
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


def test_reviewed_ui_journal_and_router_wiring_preserve_contract(repo):
    repo.write("server/static/js/trading-day.js", "window.TradingDay = {};\n")
    repo.write("server/api/routers/trading_day.py", "PRIVATE_ROUTER = True\n")
    repo.write("server/common/trading_day_store.py", "PRIVATE_STORE = True\n")
    repo.write("server/api/main.py", JOURNAL_MAIN)
    result = repo.classify(repo.commit())
    assert result["schema"] == "probiga.release-scope.v1"
    assert result["scope"] == "LINUX"
    assert result["base_contract_sha256"] == result["contract_sha256"]
    assert len(result["contract_sha256"]) == 64
    assert result["changed_paths"] == sorted([
        "server/static/js/trading-day.js", "server/api/routers/trading_day.py",
        "server/common/trading_day_store.py", "server/api/main.py",
    ])


def test_read_only_data_monitor_is_a_reviewed_linux_private_module(repo):
    repo.write("server/api/data_monitor.py", "READ_ONLY_MONITOR = True\n")
    result = repo.classify(repo.commit())
    assert result["scope"] == "LINUX"
    assert result["base_contract_sha256"] == result["contract_sha256"]
    assert result["changed_paths"] == ["server/api/data_monitor.py"]


def test_complete_delta_cannot_hide_earlier_shared_change(repo):
    repo.write("server/common/config.py", "SETTING = 2\n")
    shared = repo.commit()
    repo.write("server/static/index.html", "new page")
    target = repo.commit()
    assert repo.classify(target, shared)["scope"] == "LINUX"
    result = repo.classify(target)
    assert result["scope"] == "COORDINATED"
    assert "server/common/config.py" in result["changed_paths"]
    assert result["base_contract_sha256"] != result["contract_sha256"]


@pytest.mark.parametrize("path", [
    "tools/classify_release_scope.py", "deploy/production_deploy.sh",
    "server/common/release_manifest.py", "requirements.txt", "configuration.yml",
    "server/api/routers/new_private_router.py", "server/jobs/new_job.py",
    "integrations/qmt/protocol.py", "db/schema.sql", "unknown-file",
])
def test_unknown_and_cross_endpoint_paths_require_coordination(repo, path):
    repo.write(path, "changed\n")
    result = repo.classify(repo.commit())
    assert result["scope"] == "COORDINATED"
    assert "unreviewed_runtime_path" in result["reason_codes"]
    assert result["base_contract_sha256"] != result["contract_sha256"]


@pytest.mark.parametrize("main", [
    JOURNAL_MAIN + "start_new_runtime()\n",
    JOURNAL_MAIN.replace('prefix="/api")\n', 'prefix="/elsewhere")\n'),
    JOURNAL_MAIN.replace("health, trading_day", "health, trading_day as alias"),
    JOURNAL_MAIN + 'app.include_router(trading_day.router, prefix="/api")\n',
    JOURNAL_MAIN.replace('trading_day.router, prefix="/api"', 'trading_day.router, prefix="/api", dependencies=[danger()]'),
    JOURNAL_MAIN.replace("app.include_router(trading_day", "other.include_router(trading_day"),
    JOURNAL_MAIN + "this is invalid Python !\n",
    'from server.api.routers import health, trading_day\napp.include_router(health.router, prefix="/api")\n',
])
def test_main_py_is_not_an_unrestricted_allowlist(repo, main):
    repo.write("server/api/main.py", main)
    result = repo.classify(repo.commit())
    assert result["scope"] == "COORDINATED"
    assert result["base_contract_sha256"] != result["contract_sha256"]


def test_ast_ignores_formatting_and_permits_separate_router_import(repo):
    repo.write("server/api/main.py", MAIN + "\n# explanation\n"
               "from server.api.routers import trading_day\n"
               "app.include_router(\n    trading_day.router, prefix='/api',\n)\n")
    assert repo.classify(repo.commit())["scope"] == "LINUX"


def test_docs_tests_only_and_no_change_are_linux(repo):
    assert repo.classify()["scope"] == "LINUX"
    repo.write("docs/readme.md", "new docs")
    repo.write("tests/test_new.py", "assert True\n")
    result = repo.classify(repo.commit())
    assert result["scope"] == "LINUX"
    assert result["base_contract_sha256"] == result["contract_sha256"]


@pytest.mark.parametrize("path", ["docs/readme.md", "server/static/index.html"])
def test_deletions_and_renames_are_never_linux(repo, path):
    old = repo.root / path
    old.rename(old.with_name("renamed-" + old.name))
    result = repo.classify(repo.commit())
    assert result["scope"] == "COORDINATED"
    assert "deleted_or_renamed_path" in result["reason_codes"]


def test_symlinks_and_gitlinks_fail_even_under_excluded_directories(repo):
    blob = repo.git("hash-object", "-w", "--stdin", input=b"../../secret")
    repo.git("update-index", "--add", "--cacheinfo", "120000", blob, "server/static/link")
    repo.git("commit", "--no-gpg-sign", "-m", "symlink")
    with pytest.raises(policy.ScopeError, match="^unsafe_tree_entry$"):
        repo.classify()
    repo.git("reset", "--hard", repo.base)
    repo.git("update-index", "--add", "--cacheinfo", "160000", repo.base, "docs/submodule")
    repo.git("commit", "--no-gpg-sign", "-m", "gitlink")
    with pytest.raises(policy.ScopeError, match="^unsafe_tree_entry$"):
        repo.classify()


def test_file_mode_change_is_not_linux(repo):
    repo.git("update-index", "--chmod=+x", "server/static/index.html")
    repo.git("commit", "--no-gpg-sign", "-m", "executable")
    assert repo.classify()["scope"] == "COORDINATED"


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


def test_environment_replace_refs_and_repository_helpers_are_ignored(repo, monkeypatch, tmp_path):
    repo.write("server/common/config.py", "SETTING = 2\n")
    target = repo.commit()
    repo.git("replace", repo.base, target)
    marker = tmp_path / "executed"
    helper = f'{sys.executable} -c "open({str(marker)!r}, \'w\').write(\'executed\')"'
    repo.git("config", "diff.external", helper)
    repo.git("config", "core.fsmonitor", helper)
    monkeypatch.setenv("GIT_OBJECT_DIRECTORY", str(tmp_path / "nonexistent"))
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.fsmonitor")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", helper)
    monkeypatch.setenv("GIT_EXTERNAL_DIFF", helper)
    result = repo.classify(target)
    assert result["scope"] == "COORDINATED"
    assert result["base_contract_sha256"] != result["contract_sha256"]
    assert not marker.exists()


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


@pytest.mark.parametrize("path", ["../escape", "/absolute", "docs/../escape", "docs\\escape", "C:drive", "docs/secret\nfile"])
def test_crafted_tree_paths_fail_closed(monkeypatch, tmp_path, path):
    git = policy.Git(repository=tmp_path)
    record = f"100644 blob {'a' * 40}\t{path}\0".encode("utf-8")
    monkeypatch.setattr(git, "run", lambda *args: record)
    with pytest.raises(policy.ScopeError, match="^unsafe_tree_entry$"):
        git.tree("1" * 40)
