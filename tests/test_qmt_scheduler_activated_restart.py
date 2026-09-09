"""An activated release remains restartable while main moves ahead."""
import json
import os
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize(
    "case",
    ["activated_prior", "activated_head", "pending", "wrong_build", "no_grant",
     "writes", "unmerged", "missing", "new_target_with_old_grant",
     "selector_failed", "no_request", "dirty", "wrong_origin", "missing_main_ref",
     "wrong_branch"],
)
def test_scheduler_restart_requires_merged_activated_release_in_ps5_file(case, tmp_path):
    ps = Path(os.environ.get("SystemRoot", "")) / "System32/WindowsPowerShell/v1.0/powershell.exe"
    git = shutil.which("git")
    if not ps.is_file() or not git:
        pytest.skip("Windows PowerShell 5.1 and Git are required")
    repo = tmp_path / "repo"
    repo.mkdir()

    def run_git(*args):
        return subprocess.run(
            [git, "-C", str(repo), *args], check=True,
            capture_output=True, text=True, timeout=15,
        ).stdout.strip()

    run_git("init", "-q", "-b", "main")
    run_git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-qm", "prior")
    prior = run_git("rev-parse", "HEAD")
    run_git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-qm", "latest")
    latest = run_git("rev-parse", "HEAD")
    if case == "activated_head":
        prior = latest
    payload = {
        "mode": "check-activation", "status": "READY", "build_sha": prior,
        "activation_granted": True, "database_writes": False,
    }
    native_exit = 0
    if case == "pending":
        payload.update(status="PENDING", activation_granted=False)
        native_exit = 4
    elif case == "wrong_build":
        payload["build_sha"] = latest
    elif case == "no_grant":
        payload["activation_granted"] = False
    elif case == "writes":
        payload["database_writes"] = True
    elif case == "unmerged":
        prior, latest = latest, prior
        payload["build_sha"] = prior
    run_git("checkout", "-q", "-B", "main", prior)
    run_git("remote", "add", "origin", "https://github.com/MingMG/probiga.git")
    if case != "missing_main_ref":
        run_git("update-ref", "refs/remotes/origin/main", latest)
    if case == "dirty":
        (repo / "unexpected.txt").write_text("uncommitted", encoding="utf-8")
    elif case == "wrong_origin":
        run_git("remote", "set-url", "origin", "https://example.invalid/other.git")
    elif case == "wrong_branch":
        run_git("branch", "-m", "other")
    selection = {
        "mode": "select-update-target", "status": "SELECTED", "build_sha": prior,
        "target_build_sha": latest if case == "new_target_with_old_grant" else prior,
        "database_writes": False, "writer_authorized": False,
    }
    if case == "no_request":
        selection.update(status="NO_REQUEST", target_build_sha=None)
    native = tmp_path / "check-activation.cmd"
    native.write_text(
        '@echo off\nif "%~3"=="--select-update-target" goto selected\necho '
        + json.dumps(payload, separators=(",", ":"))
        + f"\nexit /b {native_exit}\n:selected\necho "
        + json.dumps(selection, separators=(",", ":"))
        + f"\nexit /b {4 if case == 'selector_failed' else 0}\n", encoding="ascii",
    )
    if case == "missing":
        native = tmp_path / "absent.exe"
    source = (ROOT / "tools/run_local_scheduler_task.ps1").read_text(encoding="utf-8")
    helper = source[source.index("function Invoke-Git("):source.index("# A Windows Scheduled Task")]
    gate = source[source.index("# A Windows Scheduled Task"):source.index("$env:PROBIGA_JOB_LOG_ROOT =")]
    assert "--is-ancestor" in gate
    assert "$BuildSha -cne $TargetSha" not in source
    quote = lambda value: "'" + str(value).replace("'", "''") + "'"
    script = tmp_path / "restart.ps1"
    script.write_text(
        "$ErrorActionPreference='Stop'\nSet-StrictMode -Version Latest\n"
        f"$ExpectedRoot={quote(repo)}\n$PythonExe={quote(native)}\n"
        "$ExpectedOrigin='https://github.com/MingMG/probiga.git'\n"
        f"$RealGit={quote(git)}\n"
        "$global:NetworkAttempts=0\nfunction git {\n"
        "  if (($args -contains 'fetch') -or ($args -contains 'ls-remote')) {\n"
        "    $global:NetworkAttempts++; throw 'GitHub is unavailable'\n  }\n"
        "  & $RealGit @args\n}\n"
        "$global:LASTEXITCODE=0\n"
        + helper + "\n$Started=$false\n$Failure=''\ntry {\n"
        + gate + "\n$Started=$true\n} catch { $Failure=$_.Exception.Message }\n"
        + "[ordered]@{started=$Started;failure=$Failure;build=$env:PROBIGA_BUILD_COMMIT_SHA;network_attempts=$global:NetworkAttempts} | ConvertTo-Json -Compress\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        [str(ps), "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", str(script)],
        capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=15,
    )
    assert result.returncode == 0, result.stderr
    observed = json.loads(result.stdout.strip())
    assert observed["network_attempts"] == 0, observed
    assert observed["started"] is (case in {"activated_prior", "activated_head"}), observed
    if case in {"activated_prior", "activated_head"}:
        assert observed["build"] == prior
    else:
        assert observed["failure"]
