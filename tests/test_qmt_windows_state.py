"""Fixed Windows state preparation; only temp directories and mocked launchers."""
from __future__ import annotations

import base64
import json
import os
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace

import pytest

from tools import run_qmt_windows_edge_release_bootstrap as bootstrap
from tools import run_guojin_qmt_full_market_history_2024 as history_job


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "tools/initialize_qmt_windows_state.ps1"
SHA = "a" * 40
SCOPES = (
    "qmt-local-gap-repair", "qmt-model-reload", "scheduler", "jobs",
    "qmt-full-market-history",
)


def _literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def _ps(tmp_path, body):
    if os.name != "nt" or not shutil.which("powershell.exe"):
        pytest.skip("Windows PowerShell 5.1 required")
    program_data = tmp_path / "ProgramData"
    program_data.mkdir(exist_ok=True)
    environment = os.environ.copy()
    environment["ProgramData"] = str(program_data)
    # The test runner may inherit PowerShell 7 module paths. Exercise the
    # actual Windows PowerShell 5.1 security module, not a different edition.
    environment["PSModulePath"] = str(Path(shutil.which("powershell.exe")).parent / "Modules")
    script = (
        "$ErrorActionPreference='Stop'; Set-StrictMode -Version Latest\n"
        "[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)\n"
        f". {_literal(HELPER)}\n" + body
    )
    completed = subprocess.run(
        [shutil.which("powershell.exe"), "-NoLogo", "-NoProfile", "-NonInteractive",
         "-EncodedCommand", base64.b64encode(script.encode("utf-16-le")).decode("ascii")],
        env=environment, capture_output=True, text=True, encoding="utf-8", timeout=30,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr
    return json.loads(completed.stdout)


def test_shared_initializer_creates_all_scopes_with_protected_acl_and_reuses_them(tmp_path):
    result = _ps(tmp_path, """
$Existing = @(Get-QmtWindowsStatePaths)[0]
[IO.Directory]::CreateDirectory($Existing) | Out-Null
[IO.File]::WriteAllText((Join-Path $Existing 'keep.txt'), 'preserved')
$Prepared = @(Initialize-QmtWindowsStateDirectories)
function Set-Acl { throw 'already-correct ACL must not be rewritten' }
$Again = @(Initialize-QmtWindowsStateDirectories)
$Evidence = @($Again | ForEach-Object {
    $Acl = Get-Acl -LiteralPath $_
    @{name=[IO.Path]::GetFileName($_); protected=$Acl.AreAccessRulesProtected;
      sids=@($Acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier]) |
        ForEach-Object {$_.IdentityReference.Value})}
})
@{first=$Prepared; second=$Again; evidence=$Evidence;
  retained=[IO.File]::ReadAllText((Join-Path $Existing 'keep.txt'))} | ConvertTo-Json -Depth 5 -Compress
""")
    assert result["first"] == result["second"]
    assert result["retained"] == "preserved"
    assert [row["name"] for row in result["evidence"]] == list(SCOPES)
    for row in result["evidence"]:
        assert row["protected"] is True
        assert "S-1-5-18" in row["sids"]
        assert "S-1-5-32-544" in row["sids"]
        assert len(row["sids"]) in (2, 3)


def test_initializer_preflights_every_scope_before_any_creation(tmp_path):
    result = _ps(tmp_path, """
$script:Reads = @()
function Assert-DeployStateDirectoryAccess {
    param($Path)
    $script:Reads += $Path
    if ($Path.EndsWith('qmt-full-market-history')) { throw 'ACL_DENIED' }
}
$Failure = ''
try { Initialize-QmtWindowsStateDirectories | Out-Null }
catch { $Failure = $_.Exception.Message }
@{failure=$Failure; reads=$Reads; created=(Test-Path (Join-Path $env:ProgramData 'ProBigA'))} |
    ConvertTo-Json -Compress
""")
    assert result["failure"] == "ACL_DENIED"
    assert len(result["reads"]) == 5
    assert result["created"] is False


@pytest.mark.parametrize("component", ["ProBigA", "scheduler"])
def test_initializer_rejects_reparse_ancestor_before_mutation(tmp_path, component):
    result = _ps(tmp_path, f"""
[IO.Directory]::CreateDirectory((Join-Path $env:ProgramData 'ProBigA\\scheduler')) | Out-Null
function Get-Item {{
    param($LiteralPath, [switch]$Force, $ErrorAction)
    $Item = Microsoft.PowerShell.Management\\Get-Item -LiteralPath $LiteralPath -Force
    if ($Item.Name -ceq '{component}') {{
        return [pscustomobject]@{{PSIsContainer=$true; Attributes=[IO.FileAttributes]::ReparsePoint}}
    }}
    return $Item
}}
function Set-Acl {{ throw 'MUTATED' }}
$Failure = ''
try {{ Initialize-QmtWindowsStateDirectories | Out-Null }}
catch {{ $Failure = $_.Exception.Message }}
@{{failure=$Failure; created=(Test-Path (Join-Path $env:ProgramData 'ProBigA\\qmt-full-market-history'))}} |
    ConvertTo-Json -Compress
""")
    assert result["failure"] == "QMT_STATE_UNSAFE_DIRECTORY"
    assert result["created"] is False


@pytest.mark.parametrize("failure", ["set_acl", "readback"])
def test_initializer_acl_failure_never_reports_ready(tmp_path, failure):
    result = _ps(tmp_path, f"""
$Path = @(Get-QmtWindowsStatePaths)[0]
[IO.Directory]::CreateDirectory($Path) | Out-Null
function Set-Acl {{
    param($LiteralPath, $AclObject, $ErrorAction)
    {'throw "ACL_DENIED"' if failure == 'set_acl' else '# Simulate an ineffective ACL write.'}
}}
$Failure = ''
try {{ Initialize-QmtWindowsStateDirectories | Out-Null }}
catch {{ $Failure = $_.Exception.Message }}
@{{failure=$Failure; created=(Test-Path (Join-Path $env:ProgramData 'ProBigA\\qmt-full-market-history'))}} |
    ConvertTo-Json -Compress
""")
    assert result["failure"] == ("ACL_DENIED" if failure == "set_acl" else "QMT_STATE_ACL_READBACK_INVALID")
    assert result["created"] is False


def test_helper_import_is_read_only_and_programdata_must_be_absolute(tmp_path):
    result = _ps(tmp_path, """
$Created = Test-Path (Join-Path $env:ProgramData 'ProBigA')
$env:ProgramData = 'relative'
$Failure = ''
try { Initialize-QmtWindowsStateDirectories | Out-Null }
catch { $Failure = $_.Exception.Message }
@{failure=$Failure; created=$Created} | ConvertTo-Json -Compress
""")
    assert result == {"failure": "QMT_STATE_PROGRAMDATA_INVALID", "created": False}


@pytest.mark.parametrize("change", ["none", "root", "sha", "branch"])
def test_standalone_entry_binds_local_main_root_and_build_before_state_mutation(tmp_path, change):
    if os.name != "nt" or not shutil.which("powershell.exe") or not shutil.which("git.exe"):
        pytest.skip("Windows PowerShell 5.1 and Git required")
    repository = tmp_path / "production fixture"
    tools_dir = repository / "tools"
    tools_dir.mkdir(parents=True)
    shutil.copyfile(HELPER, tools_dir / HELPER.name)
    shutil.copyfile(ROOT / "tools/deploy_preflight.ps1", tools_dir / "deploy_preflight.ps1")
    environment = os.environ.copy()
    for key in list(environment):
        if key.upper().startswith("GIT_"):
            environment.pop(key)
    environment.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=str(tmp_path / "no-global-config"))

    def git(*args):
        return subprocess.run(
            [shutil.which("git.exe"), "-C", str(repository), *args], env=environment,
            check=True, capture_output=True, text=True, timeout=10,
        ).stdout.strip()

    git("init", "--quiet", "--initial-branch=main")
    git("add", "tools")
    git("-c", "user.name=State fixture", "-c", "user.email=fixture@example.invalid", "commit", "--quiet", "-m", "state fixture")
    build_sha = git("rev-parse", "HEAD")
    if change == "branch":
        git("checkout", "-q", "-b", "codex/fixture")
    program_data = tmp_path / "ProgramData"
    program_data.mkdir()
    environment["ProgramData"] = str(program_data)
    environment["PSModulePath"] = str(Path(shutil.which("powershell.exe")).parent / "Modules")
    completed = subprocess.run(
        [shutil.which("powershell.exe"), "-NoLogo", "-NoProfile", "-NonInteractive",
         "-File", str(tools_dir / HELPER.name),
         "-StateInitializationRoot", str(tmp_path if change == "root" else repository),
         "-StateInitializationBuildSha", "b" * 40 if change == "sha" else build_sha],
        env=environment, capture_output=True, text=True, encoding="utf-8", timeout=30,
    )
    if change == "none":
        assert completed.returncode == 0, completed.stderr
        receipt = json.loads(completed.stdout)
        assert receipt["status"] == "ready"
        assert receipt["build_sha"] == build_sha
        assert receipt["production_root"] == str(repository)
        assert [Path(path).name for path in receipt["state_roots"]] == list(SCOPES)
    else:
        assert completed.returncode != 0
        assert "QMT_STATE_RELEASE_" in completed.stderr
        assert not (program_data / "ProBigA").exists()


def _state_receipt(program_data):
    return {
        "schema": "probiga.qmt-windows-state.v1", "status": "ready",
        "build_sha": SHA, "production_root": str(bootstrap.ROOT),
        "state_roots": [str(program_data / "ProBigA" / name) for name in SCOPES],
    }


@pytest.mark.parametrize("failure", [None, "timeout", "exit", "invalid_json", "missing_scope", "foreign_build", "foreign_root"])
def test_bootstrap_initializer_has_fixed_command_and_strict_readback(monkeypatch, tmp_path, failure):
    windows = tmp_path / "Windows"
    program_data = tmp_path / "ProgramData"
    monkeypatch.setenv("SystemRoot", str(windows))
    monkeypatch.setenv("ProgramData", str(program_data))
    calls = []

    def run(command, **kwargs):
        calls.append((command, kwargs))
        assert kwargs == {
            "check": True, "capture_output": True, "text": True, "encoding": "utf-8",
            "timeout": 60, "creationflags": getattr(subprocess, "CREATE_NO_WINDOW", 0),
        }
        if failure == "timeout":
            raise subprocess.TimeoutExpired(command, 60, output="sensitive diagnostic")
        if failure == "exit":
            raise subprocess.CalledProcessError(1, command, stderr="sensitive diagnostic")
        receipt = _state_receipt(program_data)
        if failure == "missing_scope":
            receipt["state_roots"].pop()
        elif failure == "foreign_build":
            receipt["build_sha"] = "b" * 40
        elif failure == "foreign_root":
            receipt["production_root"] = str(tmp_path / "other")
        return SimpleNamespace(stdout="invalid" if failure == "invalid_json" else json.dumps(receipt))

    monkeypatch.setattr(bootstrap.subprocess, "run", run)
    if failure:
        with pytest.raises(RuntimeError, match="state initialization") as caught:
            bootstrap._initialize_windows_state_directories(SHA)
        assert "sensitive" not in str(caught.value)
    else:
        bootstrap._initialize_windows_state_directories(SHA)
    assert len(calls) == 1
    assert calls[0][0] == [
        str(windows / "System32/WindowsPowerShell/v1.0/powershell.exe"),
        "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
        "-File", str(HELPER), "-StateInitializationRoot", str(bootstrap.ROOT),
        "-StateInitializationBuildSha", SHA,
    ]


def test_ordinary_history_job_refuses_missing_state_without_creating_it(monkeypatch, tmp_path):
    state = tmp_path / "missing-state"
    monkeypatch.setattr(history_job, "ROOT", tmp_path / "code")
    with pytest.raises(RuntimeError, match="pre-created real directory"):
        history_job._validated_runtime_paths(
            state_root=str(state), lock_path=str(state / "history.lock"),
            log_path=str(state / "history.jsonl"),
        )
    assert not state.exists()


def test_formal_callers_use_shared_initializer_after_grant_and_before_scheduler_start():
    updater = (ROOT / "tools/update_qmt_windows_edge.ps1").read_text(encoding="utf-8")
    registration = (ROOT / "tools/register_qmt_windows_edge_scheduler_task.ps1").read_text(encoding="utf-8")
    final = updater[updater.rindex("Confirm-QmtReleaseActivation $CurrentSha"):]
    assert final.index("initialize_qmt_windows_state.ps1") < final.index("Start-EdgeScheduler $CurrentSha")
    equal = updater[updater.index("if ($CurrentSha -ceq $TargetSha)"):]
    assert equal.index("Confirm-QmtReleaseActivation $TargetSha") < equal.index("initialize_qmt_windows_state.ps1") < equal.index("--check-ready")
    assert "Initialize-ProtectedStateDirectory" not in registration
    assert "Initialize-QmtWindowsStateDirectories" in registration
    assert "-RunLevel Limited" in registration
    assert "Assert-DeployAdministrator" in registration
