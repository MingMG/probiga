"""Executable Windows PowerShell 5.1 fault injection; no production task/network.

Git's proxy selection is exercised with real Git and a loopback-only rejecting
HTTP proxy. A compiled child executable supplies deterministic I/O and hangs.
All Git configuration and child environment changes live in pytest's temp tree.
"""

from __future__ import annotations

import base64
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import socketserver
import subprocess
import threading
import time

import pytest


ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "tools/deploy_preflight.ps1"
ORIGIN = "https://github.com/MingMG/probiga.git"
pytestmark = pytest.mark.skipif(os.name != "nt", reason="Windows PowerShell 5.1 required")


def _literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@pytest.fixture(scope="module")
def powershell() -> str:
    path = shutil.which("powershell.exe")
    if not path:
        pytest.skip("Windows PowerShell 5.1 is required")
    return path


def _ps(powershell: str, body: str, env: dict[str, str], timeout: int = 25) -> dict:
    script = (
        "$ErrorActionPreference='Stop'; Set-StrictMode -Version Latest\n"
        "[Console]::OutputEncoding=[Text.UTF8Encoding]::new($false)\n"
        "if ($PSVersionTable.PSEdition -cne 'Desktop' -or "
        "$PSVersionTable.PSVersion.Major -ne 5) { throw 'PS 5.1 required' }\n"
        f". {_literal(HELPER)}\n" + body
    )
    result = subprocess.run(
        [powershell, "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand",
         base64.b64encode(script.encode("utf-16-le")).decode("ascii")],
        env=env, capture_output=True, text=True, encoding="utf-8", timeout=timeout,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


@pytest.fixture
def repo(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    git = shutil.which("git.exe")
    if not git:
        pytest.skip("Git for Windows is required")
    env = os.environ.copy()
    for key in list(env):
        if key.upper().startswith("GIT_") or key.upper() in {
            "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
        }:
            env.pop(key)
    env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=str(tmp_path / "global.gitconfig"),
               GIT_TERMINAL_PROMPT="0", GCM_INTERACTIVE="Never")
    Path(env["GIT_CONFIG_GLOBAL"]).write_text("[http]\n\tproxy = http://127.0.0.1:9\n", encoding="utf-8")
    repository = tmp_path / "repo with spaces"
    repository.mkdir()
    for args in (("init", "--quiet"), ("remote", "add", "origin", ORIGIN)):
        subprocess.run([git, "-C", str(repository), *args], env=env, check=True,
                       capture_output=True, timeout=10)
    return repository, env


def _git_config(repo: tuple[Path, dict[str, str]], key: str, value: str) -> None:
    root, env = repo
    subprocess.run([shutil.which("git.exe"), "-C", str(root), "config", key, value],
                   env=env, check=True, capture_output=True, timeout=10)


def _config_bytes(repo: tuple[Path, dict[str, str]]) -> tuple[bytes, bytes]:
    root, env = repo
    return ((root / ".git/config").read_bytes(), Path(env["GIT_CONFIG_GLOBAL"]).read_bytes())


@contextmanager
def _rejecting_proxy():
    requests: list[str] = []

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            self.request.settimeout(3)
            requests.append(self.rfile.readline().decode("ascii", errors="replace").strip())
            self.wfile.write(b"HTTP/1.1 502 Local fixture refused CONNECT\r\n"
                             b"Content-Length: 0\r\nConnection: close\r\n\r\n")

    with socketserver.TCPServer(("127.0.0.1", 0), Handler) as server:
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        try:
            yield f"http://127.0.0.1:{server.server_address[1]}", requests
        finally:
            server.shutdown()
            worker.join(timeout=3)


@pytest.mark.parametrize("selection", ["url", "remote", "invocation"])
def test_real_git_effective_proxy_overrides_conflicting_config(powershell, repo, selection):
    root, env = repo
    with _rejecting_proxy() as (proxy, requests):
        env.update(HTTPS_PROXY="http://127.0.0.1:9")
        _git_config(repo, f"http.{ORIGIN}.proxy", proxy if selection == "url" else "http://127.0.0.1:9")
        if selection != "url":
            _git_config(repo, "remote.origin.proxy", proxy if selection == "remote" else "http://127.0.0.1:9")
        if selection == "invocation":
            # An explicit proxy must also beat an inherited bypass.
            env["NO_PROXY"] = "github.com"
        before = _config_bytes(repo)
        result = _ps(powershell, f"""
$Before = [Environment]::GetEnvironmentVariables('Process') | ConvertTo-Json -Compress
$Failure = ''
try {{ Invoke-DeployGit -Root {_literal(root)} -Arguments @('ls-remote','origin','refs/heads/main') `
    -Stage 'fixture.remote' -TimeoutSeconds 5 -GitHubProxy {_literal(proxy if selection == 'invocation' else '')} | Out-Null }}
catch {{ $Failure = $_.Exception.Message }}
$After = [Environment]::GetEnvironmentVariables('Process') | ConvertTo-Json -Compress
@{{ failure=$Failure; environment_unchanged=($Before -ceq $After) }} | ConvertTo-Json -Compress
""", env)
        assert requests == ["CONNECT github.com:443 HTTP/1.1"]
        assert "stage=fixture.remote reason=PROXY" in result["failure"]
        assert "502" in result["failure"]
        assert result["environment_unchanged"]
        assert _config_bytes(repo) == before


def test_explicit_direct_is_process_scoped_and_beats_url_and_remote_config(powershell, repo):
    root, env = repo
    _git_config(repo, f"http.{ORIGIN}.proxy", "http://127.0.0.1:7890")
    _git_config(repo, "remote.origin.proxy", "http://127.0.0.1:7891")
    env.update(HTTP_PROXY="http://127.0.0.1:7892", HTTPS_PROXY="http://127.0.0.1:7893")
    before = _config_bytes(repo)
    result = _ps(powershell, f"""
$Before = [Environment]::GetEnvironmentVariables('Process') | ConvertTo-Json -Compress
$Url = Invoke-DeployGit -Root {_literal(root)} -GitHubProxy direct `
    -Arguments @('config','--get-urlmatch','http.proxy',{_literal(ORIGIN)})
$Remote = Invoke-DeployGit -Root {_literal(root)} -GitHubProxy direct `
    -Arguments @('config','--get','remote.origin.proxy')
$After = [Environment]::GetEnvironmentVariables('Process') | ConvertTo-Json -Compress
@{{ url=$Url; remote=$Remote; environment_unchanged=($Before -ceq $After) }} | ConvertTo-Json -Compress
""", env)
    assert result == {"url": "", "remote": "", "environment_unchanged": True}
    assert _config_bytes(repo) == before


@pytest.fixture(scope="module")
def fake_git(powershell, tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("fake-git")
    executable = root / "git.exe"
    source = r'''
using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
public class FakeGit {
    public static int Main(string[] args) {
        if (args.Length == 1 && args[0] == "sleep-child") { Thread.Sleep(60000); return 0; }
        string mode = Environment.GetEnvironmentVariable("FAKE_GIT_MODE");
        if (mode == "sleep") {
            var child = Process.Start(new ProcessStartInfo {
                FileName = Process.GetCurrentProcess().MainModule.FileName,
                Arguments = "sleep-child", UseShellExecute = false, CreateNoWindow = true });
            File.WriteAllText(Environment.GetEnvironmentVariable("FAKE_GIT_PID_FILE"),
                Process.GetCurrentProcess().Id + "," + child.Id);
            Thread.Sleep(60000); return 0;
        }
        if (mode == "fail") {
            Console.Error.WriteLine(File.ReadAllText(Environment.GetEnvironmentVariable("FAKE_GIT_ERROR_FILE")));
            return 128;
        }
        foreach (string arg in args) Console.WriteLine("ARG:" + Convert.ToBase64String(Encoding.UTF8.GetBytes(arg)));
        foreach (string key in new[] { "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "GIT_TRACE", "GIT_TRACE_CURL",
                "GIT_CURL_VERBOSE", "GIT_SSL_NO_VERIFY", "GIT_TERMINAL_PROMPT", "GCM_INTERACTIVE" }) {
            string value = Environment.GetEnvironmentVariable(key);
            Console.WriteLine("ENV:" + key + ":" + (value == null ? "<absent>" : value));
        }
        return 0;
    }
}
'''
    _ps(powershell, f"""
Add-Type -TypeDefinition @'
{source}
'@ -OutputAssembly {_literal(executable)} -OutputType ConsoleApplication
@{{compiled=$true}} | ConvertTo-Json -Compress
""", os.environ.copy())
    return executable


def _fake_lookup(executable: Path) -> str:
    # Override discovery only; the helper still launches a real native process.
    return f"function Get-Command {{ [pscustomobject]@{{ Source={_literal(executable)} }} }}\n"


def test_child_environment_and_argument_quoting_do_not_escape_to_caller(powershell, repo, fake_git):
    root, env = repo
    env.update(HTTP_PROXY="http://127.0.0.1:7800", HTTPS_PROXY="http://127.0.0.1:7801",
               NO_PROXY="github.com", GIT_TRACE="1", GIT_TRACE_CURL="1", GIT_CURL_VERBOSE="1",
               GIT_SSL_NO_VERIFY="1", GIT_TERMINAL_PROMPT="1", GCM_INTERACTIVE="Always")
    before = _config_bytes(repo)
    arguments = ["check", "path with spaces\\", 'embedded"quote', "$(not-a-command)"]
    result = _ps(powershell, _fake_lookup(fake_git) + f"""
$Before = [Environment]::GetEnvironmentVariables('Process') | ConvertTo-Json -Compress
$Output = Invoke-DeployGit -Root {_literal(root)} -GitHubProxy http://127.0.0.1:7900 `
    -Arguments @({','.join(_literal(arg) for arg in arguments)})
$After = [Environment]::GetEnvironmentVariables('Process') | ConvertTo-Json -Compress
@{{output=$Output; environment_unchanged=($Before -ceq $After)}} | ConvertTo-Json -Compress
""", env)
    lines = result["output"].splitlines()
    actual_args = [base64.b64decode(line[4:]).decode() for line in lines if line.startswith("ARG:")]
    child_env = dict(line[4:].split(":", 1) for line in lines if line.startswith("ENV:"))
    assert actual_args[-len(arguments):] == arguments
    assert f"http.{ORIGIN}.proxy=http://127.0.0.1:7900" in actual_args
    assert "remote.origin.proxy=http://127.0.0.1:7900" in actual_args
    assert "http.sslVerify=true" in actual_args
    for name in ("GIT_TRACE", "GIT_TRACE_CURL", "GIT_CURL_VERBOSE", "GIT_SSL_NO_VERIFY"):
        assert child_env[name] == "<absent>"
    assert child_env["NO_PROXY"] in ("", "<absent>")
    assert child_env["GIT_TERMINAL_PROMPT"] == "0"
    assert child_env["GCM_INTERACTIVE"] == "Never"
    assert child_env["HTTPS_PROXY"] == env["HTTPS_PROXY"]
    assert result["environment_unchanged"]
    assert _config_bytes(repo) == before


@pytest.mark.parametrize("diagnostic,reason", [
    ("fatal: Could not resolve proxy: broken-proxy", "PROXY"),
    ("fatal: Authentication failed for repository", "AUTHENTICATION"),
    ("fatal: SSL certificate problem", "TLS_OR_HOST_KEY"),
    ("fatal: Could not resolve host: github.com", "DNS"),
    ("fatal: Connection timed out", "TIMEOUT"),
    ("fatal: Failed to connect to host", "NETWORK"),
    ("fatal: detected dubious ownership in repository", "PERMISSION"),
    ("fatal: not a git repository", "REPOSITORY"),
])
def test_failures_report_stage_reason_and_redact_stderr(powershell, repo, fake_git, diagnostic, reason):
    root, env = repo
    error_file = root.parent / "stderr.txt"
    error_file.write_text(diagnostic + "\nhttps://user:passphrase@github.com/repo?auth=queryvalue\n"
                          "Authorization: Bearer headervalue\npassword=passvalue "
                          "token=tokenvalue github_pat_patvalue", encoding="utf-8")
    env.update(FAKE_GIT_MODE="fail", FAKE_GIT_ERROR_FILE=str(error_file))
    before = _config_bytes(repo)
    result = _ps(powershell, _fake_lookup(fake_git) + f"""
$Before = [Environment]::GetEnvironmentVariables('Process') | ConvertTo-Json -Compress
$Failure = ''
try {{ Invoke-DeployGit -Root {_literal(root)} -GitHubProxy http://127.0.0.1:7900 `
    -Arguments @('fetch','origin','main') -Stage 'fixture.fetch' | Out-Null }}
catch {{ $Failure = $_.Exception.Message }}
$After = [Environment]::GetEnvironmentVariables('Process') | ConvertTo-Json -Compress
@{{failure=$Failure; environment_unchanged=($Before -ceq $After)}} | ConvertTo-Json -Compress
""", env)
    message = result["failure"]
    assert f"stage=fixture.fetch reason={reason} exit=128" in message
    assert "[REDACTED]" in message
    for secret in ("user", "passphrase", "queryvalue", "headervalue", "passvalue", "tokenvalue", "patvalue"):
        assert secret not in message
    assert "\n" not in message
    assert result["environment_unchanged"]
    assert _config_bytes(repo) == before


def test_timeout_terminates_only_launched_process_tree_and_keeps_parent_config(powershell, repo, fake_git):
    root, env = repo
    pid_file = root.parent / "child-pids.txt"
    env.update(FAKE_GIT_MODE="sleep", FAKE_GIT_PID_FILE=str(pid_file), HTTPS_PROXY="http://127.0.0.1:7800")
    before = _config_bytes(repo)
    started = time.monotonic()
    result = _ps(powershell, _fake_lookup(fake_git) + f"""
$Before = [Environment]::GetEnvironmentVariables('Process') | ConvertTo-Json -Compress
$Failure = ''
try {{ Invoke-DeployGit -Root {_literal(root)} -Arguments @('fetch','origin','main') `
    -Stage 'fixture.timeout' -TimeoutSeconds 1 -GitHubProxy http://127.0.0.1:7900 | Out-Null }}
catch {{ $Failure = $_.Exception.Message }}
$Alive = @()
foreach ($ChildPid in ([IO.File]::ReadAllText({_literal(pid_file)}) -split ',')) {{
    try {{ $P = [Diagnostics.Process]::GetProcessById([int]$ChildPid); if (!$P.HasExited) {{ $Alive += $ChildPid }}; $P.Dispose() }}
    catch [ArgumentException] {{ }}
}}
$After = [Environment]::GetEnvironmentVariables('Process') | ConvertTo-Json -Compress
@{{failure=$Failure; alive=$Alive; environment_unchanged=($Before -ceq $After)}} | ConvertTo-Json -Compress
""", env)
    assert time.monotonic() - started < 12
    assert "stage=fixture.timeout reason=TIMEOUT timeout_seconds=1" in result["failure"]
    assert result["alive"] == []
    assert result["environment_unchanged"]
    assert _config_bytes(repo) == before


def test_actual_windows_token_task_accesscheck_handles_allow_readonly_and_deny(powershell, repo):
    _, env = repo
    result = _ps(powershell, """
Assert-DeployTaskAccess @()
$Identity = [Security.Principal.WindowsIdentity]::GetCurrent()
try { $Sid = $Identity.User.Value } finally { $Identity.Dispose() }
@{
    allow=[ProBigADeployAccess]::CanManage("O:SYG:SYD:(A;;FA;;;$Sid)")
    readonly=[ProBigADeployAccess]::CanManage("O:SYG:SYD:(A;;FR;;;$Sid)")
    deny=[ProBigADeployAccess]::CanManage("O:SYG:SYD:(D;;FW;;;$Sid)(A;;FA;;;WD)")
} | ConvertTo-Json -Compress
""", env)
    assert result == {"allow": True, "readonly": False, "deny": False}


def test_task_query_success_does_not_allow_later_mutation_when_dacl_denies(powershell, repo):
    root, env = repo
    marker = root.parent / "mutation-must-not-happen"
    result = _ps(powershell, f"""
Assert-DeployTaskAccess @()
$script:Queries = 0
$Task = [pscustomobject]@{{}}
$Task | Add-Member ScriptMethod GetSecurityDescriptor {{ param($Flags) $script:Queries++; return 'O:SYG:SYD:(A;;FR;;;WD)' }}
$Folder = [pscustomobject]@{{Task=$Task}}
$Folder | Add-Member ScriptMethod GetTask {{ param($Name) return $this.Task }}
$Service = [pscustomobject]@{{Folder=$Folder}}
$Service | Add-Member ScriptMethod Connect {{}}
$Service | Add-Member ScriptMethod GetFolder {{ param($Name) return $this.Folder }}
function New-Object {{ param([string]$ComObject) if ($ComObject -cne 'Schedule.Service') {{ throw 'unexpected COM request' }}; return $Service }}
$Failure = ''
try {{ Assert-DeployTaskAccess @('Fixture task'); [IO.File]::WriteAllText({_literal(marker)}, 'mutated') }}
catch {{ $Failure = $_.Exception.Message }}
@{{failure=$Failure; queries=$script:Queries}} | ConvertTo-Json -Compress
""", env)
    assert result["queries"] == 1
    assert "stage=permissions.task reason=TASK_ACCESS_DENIED task=Fixture task" in result["failure"]
    assert not marker.exists()


def test_directory_write_preflight_failure_prevents_later_mutation(powershell, repo):
    root, env = repo
    marker = root.parent / "mutation-must-not-happen"
    result = _ps(powershell, f"""
$Failure = ''
try {{ Assert-DeployDirectoryWritable {_literal(root / 'missing-parent')} 'fixture.write';
    [IO.File]::WriteAllText({_literal(marker)}, 'mutated') }}
catch {{ $Failure = $_.Exception.Message }}
Assert-DeployDirectoryWritable {_literal(root)} 'fixture.write'
@{{failure=$Failure; leftovers=@(Get-ChildItem -LiteralPath {_literal(root)} -Filter '.deploy-preflight-*').Count}} | ConvertTo-Json -Compress
""", env)
    assert "stage=fixture.write reason=WRITE_ACCESS_DENIED" in result["failure"]
    assert result["leftovers"] == 0
    assert not marker.exists()


def test_native_start_failure_retains_stage_and_original_cause(powershell, repo):
    root, env = repo
    missing = root / "definitely-missing-git.exe"
    before = _config_bytes(repo)
    result = _ps(powershell, _fake_lookup(missing) + f"""
$Failure = ''
try {{ Invoke-DeployGit -Root {_literal(root)} -Arguments @('--version') -Stage 'fixture.start' | Out-Null }}
catch {{ $Failure = $_.Exception.Message }}
@{{failure=$Failure}} | ConvertTo-Json -Compress
""", env)
    assert "stage=fixture.start reason=PROCESS_START_OR_IO detail=" in result["failure"]
    # .NET reports a failed Start call in the original diagnostic. Accessing
    # HasExited on a process which never started must not replace that error.
    assert '"Start"' in result["failure"]
    assert "HasExited" not in result["failure"]
    assert _config_bytes(repo) == before


@pytest.mark.parametrize("lock_name", ["index.lock", "HEAD.lock", "refs/heads/main.lock", "packed-refs.lock"])
def test_repository_busy_is_detected_before_forward_stop_and_retains_lock(powershell, repo, lock_name):
    root, env = repo
    lock = root / ".git" / lock_name
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_bytes(b"existing writer lock; never remove automatically\n")
    before = lock.read_bytes()
    # Bind the executable fault injection to the actual forward-switch order.
    updater = (ROOT / "tools/update_qmt_windows_edge.ps1").read_text(encoding="utf-8")
    check = updater.index("    Assert-DeployRepositoryIdle $ExpectedRoot $GitTimeoutSeconds")
    assert check < updater.index('    Invoke-Git @("fetch", "--prune", "origin", "main")', check)
    assert check < updater.index('    Stop-EdgeScheduler\n    Invoke-Git @("merge", "--ff-only", $TargetSha)')
    recovery = (ROOT / "tools/resume_qmt_prior_edge.ps1").read_text(encoding="utf-8")
    prepare = recovery.split("function Prepare-ProductionGit() {", 1)[1].split("\n}", 1)[0]
    assert prepare.index("Assert-DeployRepositoryIdle") < prepare.index("@('fetch'")
    forward = recovery.split("function Invoke-ForwardRecovery() {", 1)[1].split("\n}", 1)[0]
    assert forward.index("Prepare-ProductionGit") < forward.index("Enter-TaskGate")
    result = _ps(powershell, f"""
$Stopped = $false; $Failure = ''
try {{ Assert-DeployRepositoryIdle {_literal(root)} 5; $Stopped = $true }}
catch {{ $Failure = $_.Exception.Message }}
@{{failure=$Failure; stopped=$Stopped}} | ConvertTo-Json -Compress
""", env)
    assert f"stage=repository.locks reason=REPOSITORY_BUSY lock={lock_name}" in result["failure"]
    assert result["stopped"] is False
    assert lock.read_bytes() == before


@pytest.mark.parametrize("can_write_dacl", [False, True])
def test_registration_checks_actual_state_directory_write_dac_before_stopping(powershell, repo, can_write_dacl):
    root, env = repo
    program_data = root.parent / "ProgramData"
    for name in ("qmt-local-gap-repair", "qmt-model-reload", "scheduler", "jobs", "qmt-full-market-history"):
        (program_data / "ProBigA" / name).mkdir(parents=True)
    env["ProgramData"] = str(program_data)
    source = (ROOT / "tools/register_qmt_windows_edge_scheduler_task.ps1").read_text(encoding="utf-8")
    # Execute the real registration pre-stop block. Only task discovery/stops
    # and returned ACL data are substituted; AccessCheck uses the actual token.
    pre_stop = source[source.index("$ExistingNames ="):source.index('$UserName = "$env:USERDOMAIN')]
    assert pre_stop.index("Assert-QmtWindowsStateDirectories") < pre_stop.index("Stop-ExistingTask $UpdateTaskName")
    result = _ps(powershell, f"""
. {_literal(ROOT / 'tools/initialize_qmt_windows_state.ps1')}
Assert-DeployTaskAccess @()
$TaskName = 'Fixture scheduler'; $UpdateTaskName = 'Fixture updater'
$script:Stops = @(); $script:AclReads = @()
$Identity = [Security.Principal.WindowsIdentity]::GetCurrent()
try {{ $Sid = $Identity.User.Value }} finally {{ $Identity.Dispose() }}
$script:RestrictedSddl = "O:S-1-5-7G:SYD:(A;;0x1201bf;;;$Sid)"
$script:AllowSddl = "O:S-1-5-7G:SYD:(A;;FA;;;$Sid)"
function Get-ScheduledTask {{ @() }}
function Stop-ExistingTask {{ param($Name) $script:Stops += $Name }}
function Get-Acl {{
    param($LiteralPath)
    $script:AclReads += $LiteralPath
    $Descriptor = [Security.AccessControl.DirectorySecurity]::new()
    $Sddl = $script:AllowSddl
    if ({'$false' if can_write_dacl else '$true'} -and $LiteralPath.EndsWith('qmt-model-reload')) {{ $Sddl = $script:RestrictedSddl }}
    $Descriptor.SetSecurityDescriptorSddlForm($Sddl)
    return $Descriptor
}}
$Failure = ''
try {{
{pre_stop}
}} catch {{ $Failure = $_.Exception.Message }}
@{{failure=$Failure; stops=$script:Stops; acl_reads=$script:AclReads;
    can_write=[ProBigADeployAccess]::CanAccess($script:RestrictedSddl, 0x1201bf);
    can_write_dacl=[ProBigADeployAccess]::CanAccess($script:RestrictedSddl, 0x1601bf)}} | ConvertTo-Json -Compress
""", env)
    assert result["can_write"] is True
    assert result["can_write_dacl"] is False
    if can_write_dacl:
        assert result["failure"] == ""
        assert result["stops"] == ["Fixture updater", "Fixture scheduler"]
        assert len(result["acl_reads"]) == 5
    else:
        assert "stage=permissions.state-directory reason=STATE_DIRECTORY_ACCESS_DENIED" in result["failure"]
        assert "qmt-model-reload" in result["failure"]
        assert result["stops"] == []
        assert len(result["acl_reads"]) == 2
