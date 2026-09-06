param(
    [Parameter(Mandatory = $true)] [ValidateNotNullOrEmpty()]
    [string]$ProductionRoot,
    [Parameter(Mandatory = $true)] [ValidatePattern("^[0-9a-fA-F]{40}$")]
    [string]$PriorBuildSha,
    [Parameter(Mandatory = $true)] [ValidatePattern("^[0-9a-fA-F]{40}$")]
    [string]$TargetBuildSha,
    [ValidateRange(60, 3600)] [int]$BootstrapTimeoutSeconds = 1200,
    [ValidateRange(60, 3600)] [int]$TransitionTimeoutSeconds = 1800
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest
if ($PSVersionTable.PSEdition -cne "Desktop") {
    throw "prior-edge recovery requires Windows PowerShell 5.1"
}
foreach ($Module in @("ScheduledTasks", "CimCmdlets")) {
    $Manifest = Join-Path $PSHOME "Modules\$Module\$Module.psd1"
    if (!(Test-Path -LiteralPath $Manifest -PathType Leaf)) {
        throw "required Windows PowerShell module is unavailable"
    }
    $ManifestItem = Get-Item -LiteralPath $Manifest -Force
    if (($ManifestItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "required Windows PowerShell module is not an ordinary file"
    }
    Import-Module -Name $Manifest -Force -ErrorAction Stop
}
$SchedulerTaskName = "ProBigA QMT Windows Edge Scheduler"
$UpdaterTaskName = "ProBigA QMT Windows Edge Updater"
$ExpectedOrigin = "https://github.com/MingMG/probiga.git"
$RecoveryProtocol = "probiga.qmt-edge-precutover-recovery.v1"
$ControllerRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
$ProductionRoot = [IO.Path]::GetFullPath($ProductionRoot)
$PriorBuildSha = $PriorBuildSha.ToLowerInvariant()
$TargetBuildSha = $TargetBuildSha.ToLowerInvariant()
$WindowsRoot = [Environment]::GetFolderPath([Environment+SpecialFolder]::Windows)
$PowerShellExe = Join-Path $WindowsRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
$WScriptExe = Join-Path $WindowsRoot "System32\wscript.exe"
$PythonExe = Join-Path $ProductionRoot ".venv\Scripts\python.exe"
$QmtPythonExe = Join-Path $ProductionRoot "runtime\qmt-py313\Scripts\python.exe"
$DaemonScript = Join-Path $ProductionRoot "tools\run_scheduler_daemon.py"
$BootstrapTool = Join-Path $ProductionRoot "tools\run_qmt_windows_edge_release_bootstrap.py"
$WrapperScript = Join-Path $ProductionRoot "tools\run_local_scheduler_task.ps1"
$UpdaterLauncher = Join-Path $ProductionRoot "tools\run_hidden_qmt_updater.vbs"
$ProgramDataRoot = [IO.Path]::GetFullPath($env:ProgramData)
$StateRoot = [IO.Path]::GetFullPath((Join-Path $ProgramDataRoot "ProBigA\scheduler"))
$RuntimePath = Join-Path $StateRoot "scheduler-runtime.json"
$ShutdownRequestPath = Join-Path $StateRoot "scheduler-shutdown-request.json"
$ShutdownReceiptPath = Join-Path $StateRoot "scheduler-shutdown-receipt.json"

function Invoke-Git([string]$Root, [string[]]$Arguments) {
    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $global:LASTEXITCODE = -1
        try {
            $Output = & git -C $Root @Arguments 2>$null
            $ExitCode = $global:LASTEXITCODE
        } catch { $Output = @(); $ExitCode = -1 }
    } finally { $ErrorActionPreference = $PreviousPreference }
    if ($ExitCode -ne 0) { throw "prior-edge recovery Git identity check failed" }
    return (($Output -join "`n").Trim())
}

function Test-Ancestor([string]$Ancestor, [string]$Descendant) {
    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $global:LASTEXITCODE = -1
        try {
            & git -C $ControllerRoot merge-base --is-ancestor $Ancestor $Descendant 2>$null
            $ExitCode = $global:LASTEXITCODE
        } catch { $ExitCode = -1 }
    } finally { $ErrorActionPreference = $PreviousPreference }
    return $ExitCode -eq 0
}

function Assert-Directory([string]$Path, [string]$Label) {
    if ($Path -notmatch "^[A-Za-z]:[\\/]" -or !(Test-Path -LiteralPath $Path -PathType Container)) {
        throw "$Label is not an absolute local directory"
    }
    if (((Get-Item -LiteralPath $Path -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "$Label cannot be a reparse point"
    }
}

function Assert-Roots() {
    Assert-Directory $ControllerRoot "controller root"
    Assert-Directory $ProductionRoot "prior production root"
    if ($ControllerRoot -ieq $ProductionRoot) { throw "controller and production roots must differ" }

    # Establish the local trust boundary before any fetch invokes a remote or helper.
    $ControllerTop = Invoke-Git $ControllerRoot @("rev-parse", "--show-toplevel")
    $ControllerOrigin = Invoke-Git $ControllerRoot @("remote", "get-url", "origin")
    if ([IO.Path]::GetFullPath($ControllerTop) -ine $ControllerRoot -or $ControllerOrigin -ine $ExpectedOrigin) {
        throw "controller repository binding differs"
    }
    Invoke-Git $ControllerRoot @("fetch", "--prune", "origin", "main") | Out-Null
    $ControllerHead = (Invoke-Git $ControllerRoot @("rev-parse", "HEAD")).ToLower()
    $ControllerMain = (Invoke-Git $ControllerRoot @("rev-parse", "origin/main")).ToLower()
    $ControllerDirty = Invoke-Git $ControllerRoot @("status", "--porcelain", "--untracked-files=normal")
    if ($ControllerHead -cne $TargetBuildSha -or $ControllerMain -cne $TargetBuildSha -or $ControllerDirty) {
        throw "controller is not a clean merged exact-main release"
    }

    $ProductionTop = Invoke-Git $ProductionRoot @("rev-parse", "--show-toplevel")
    $ProductionOrigin = Invoke-Git $ProductionRoot @("remote", "get-url", "origin")
    $ProductionBranch = Invoke-Git $ProductionRoot @("symbolic-ref", "--short", "HEAD")
    $ProductionHead = (Invoke-Git $ProductionRoot @("rev-parse", "HEAD")).ToLower()
    $ProductionDirty = Invoke-Git $ProductionRoot @("status", "--porcelain", "--untracked-files=normal")
    if (
        [IO.Path]::GetFullPath($ProductionTop) -ine $ProductionRoot -or
        $ProductionOrigin -ine $ExpectedOrigin -or $ProductionBranch -cne "main" -or
        $ProductionHead -cne $PriorBuildSha -or $ProductionDirty -or
        $PriorBuildSha -ceq $TargetBuildSha -or !(Test-Ancestor $PriorBuildSha $TargetBuildSha)
    ) { throw "prior production checkout is not the clean ancestor release" }
}

function Invoke-JsonTool([string[]]$Arguments) {
    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $global:LASTEXITCODE = -1
        try {
            $Output = & $PythonExe -P $BootstrapTool @Arguments 2>&1
            $ExitCode = $global:LASTEXITCODE
        } catch { $Output = @(); $ExitCode = -1 }
    } finally { $ErrorActionPreference = $PreviousPreference }
    try { $Payload = (($Output -join "`n").Trim()) | ConvertFrom-Json -ErrorAction Stop }
    catch { throw "prior-edge recovery proof is malformed" }
    return [pscustomobject]@{ ExitCode = $ExitCode; Payload = $Payload }
}

function Get-Daemons() {
    return @(Get-CimInstance Win32_Process -ErrorAction Stop | Where-Object {
        [string]$_.CommandLine -like "*run_scheduler_daemon.py*"
    })
}

function Assert-NoDaemon() {
    if (@(Get-Daemons).Count -ne 0) { throw "another Windows scheduler daemon is active" }
}

function Assert-TaskBindings($Scheduler, $Updater) {
    $SchedulerArgs = "-WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File `"$WrapperScript`" -RegisteredRoot `"$ProductionRoot`""
    $UpdaterArgs = "//B //NoLogo `"$UpdaterLauncher`" `"$ProductionRoot`""
    if (
        @($Scheduler.Actions).Count -ne 1 -or @($Updater.Actions).Count -ne 1 -or
        $Scheduler.Actions[0].Execute -ine $PowerShellExe -or
        $Scheduler.Actions[0].WorkingDirectory -ine $ProductionRoot -or
        $Scheduler.Actions[0].Arguments -cne $SchedulerArgs -or
        $Updater.Actions[0].Execute -ine $WScriptExe -or
        $Updater.Actions[0].WorkingDirectory -ine $ProductionRoot -or
        $Updater.Actions[0].Arguments -cne $UpdaterArgs
    ) { throw "registered Windows edge task binding differs" }
}

function Enter-TaskGate() {
    $Scheduler = Get-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
    $Updater = Get-ScheduledTask -TaskName $UpdaterTaskName -ErrorAction Stop
    Assert-TaskBindings $Scheduler $Updater
    if ($Scheduler.State -eq "Running" -or $Updater.State -eq "Running") {
        throw "Windows edge tasks are not idle"
    }
    $Gate = [pscustomobject]@{
        Scheduler = $false; Updater = $false
        SchedulerWasEnabled = [bool]$Scheduler.Settings.Enabled
        UpdaterWasEnabled = [bool]$Updater.Settings.Enabled
    }
    try {
        if ($Gate.UpdaterWasEnabled) {
            Disable-ScheduledTask -TaskName $UpdaterTaskName -ErrorAction Stop | Out-Null
            $Gate.Updater = $true
        }
        if ($Gate.SchedulerWasEnabled) {
            Disable-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop | Out-Null
            $Gate.Scheduler = $true
        }
        $Scheduler = Get-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
        $Updater = Get-ScheduledTask -TaskName $UpdaterTaskName -ErrorAction Stop
        Assert-TaskBindings $Scheduler $Updater
        if (
            [bool]$Scheduler.Settings.Enabled -or [bool]$Updater.Settings.Enabled -or
            $Scheduler.State -eq "Running" -or $Updater.State -eq "Running"
        ) { throw "Windows edge task gate is not exclusive" }
        Assert-NoDaemon
        return $Gate
    } catch { Exit-TaskGate $Gate; throw }
}

function Exit-TaskGate($Gate) {
    if ($null -eq $Gate) { return }
    # Refuse to enable a task whose action was changed during the recovery window.
    $Scheduler = Get-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
    $Updater = Get-ScheduledTask -TaskName $UpdaterTaskName -ErrorAction Stop
    Assert-TaskBindings $Scheduler $Updater
    if ($Gate.Scheduler) {
        Enable-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop | Out-Null
        $Gate.Scheduler = $false
    }
    if ($Gate.Updater) {
        Enable-ScheduledTask -TaskName $UpdaterTaskName -ErrorAction Stop | Out-Null
        $Gate.Updater = $false
    }
    $Scheduler = Get-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
    $Updater = Get-ScheduledTask -TaskName $UpdaterTaskName -ErrorAction Stop
    Assert-TaskBindings $Scheduler $Updater
    if (
        [bool]$Scheduler.Settings.Enabled -ne $Gate.SchedulerWasEnabled -or
        [bool]$Updater.Settings.Enabled -ne $Gate.UpdaterWasEnabled
    ) {
        throw "Windows edge task enabled state was not restored"
    }
}

function Assert-PriorReady() {
    $Activation = Invoke-JsonTool @("--check-activation", "--expected-build-sha", $PriorBuildSha, "--compact")
    if (
        $Activation.ExitCode -ne 0 -or [string]$Activation.Payload.mode -cne "check-activation" -or
        [string]$Activation.Payload.status -cne "READY" -or
        [string]$Activation.Payload.build_sha -cne $PriorBuildSha -or
        $Activation.Payload.activation_granted -ne $true -or $Activation.Payload.database_writes -ne $false
    ) { throw "prior activation is not ready" }
    $Selection = Invoke-JsonTool @("--select-update-target", "--expected-build-sha", $PriorBuildSha, "--compact")
    if (
        $Selection.ExitCode -ne 0 -or [string]$Selection.Payload.mode -cne "select-update-target" -or
        [string]$Selection.Payload.build_sha -cne $PriorBuildSha -or
        [string]$Selection.Payload.status -cne "SELECTED" -or
        [string]$Selection.Payload.target_build_sha -cne $PriorBuildSha -or
        $Selection.Payload.database_writes -ne $false -or $Selection.Payload.writer_authorized -ne $false
    ) { throw "prior build is not the selected writer" }
    $Strategy = Invoke-JsonTool @("--check-strategy", "--expected-build-sha", $PriorBuildSha, "--compact")
    if (
        $Strategy.ExitCode -ne 0 -or [string]$Strategy.Payload.mode -cne "check-strategy" -or
        [string]$Strategy.Payload.status -cne "READY" -or
        [string]$Strategy.Payload.expected_build_sha -cne $PriorBuildSha -or
        $Strategy.Payload.database_writes -ne $false
    ) { throw "prior strategy model is not the exact release" }
}

function Read-OwnedRuntime($Daemon) {
    if (!(Test-Path -LiteralPath $RuntimePath -PathType Leaf)) { return $null }
    $Item = Get-Item -LiteralPath $RuntimePath -Force
    if (($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "scheduler runtime path is a reparse point"
    }
    try {
        $Runtime = Get-Content -LiteralPath $RuntimePath -Raw | ConvertFrom-Json -ErrorAction Stop
        $Heartbeat = [DateTimeOffset]::Parse([string]$Runtime.heartbeat_at_utc).ToUniversalTime()
    } catch { throw "scheduler runtime identity is malformed" }
    $Age = ([DateTimeOffset]::UtcNow - $Heartbeat).TotalSeconds
    if (
        [int]$Runtime.schema_version -ne 1 -or [string]$Runtime.instance_id -notmatch "^[0-9a-fA-F-]{36}$" -or
        [int]$Runtime.pid -ne [int]$Daemon.Id -or
        ([string]$Runtime.build_sha).ToLower() -cne $PriorBuildSha -or $Age -lt -10 -or $Age -gt 15
    ) { return $null }
    return $Runtime
}

function Wait-OwnedRuntime($Daemon, [int]$TimeoutSeconds = 30) {
    $Deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        if ($Daemon.HasExited) { throw "owned prior daemon exited before runtime identity" }
        $Runtime = Read-OwnedRuntime $Daemon
        if ($null -ne $Runtime) { return $Runtime }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $Deadline)
    throw "owned prior daemon runtime identity timed out"
}

function Assert-OwnedBootstrap($Proof, $Daemon) {
    $Identity = $Proof.identity.current
    if (
        [string]$Proof.mode -cne "bootstrap" -or [string]$Proof.status -cne "inserted" -or
        $Proof.database_writes -ne $true -or $Proof.qmt_calls -ne $true -or
        [string]$Proof.expected_build_sha -cne $PriorBuildSha -or
        [string]$Proof.release_receipt.status -cne "AVAILABLE" -or
        [int]$Identity.pid -ne [int]$Daemon.Id -or [string]$Identity.build_sha -cne $PriorBuildSha -or
        [string]$Identity.host_name -cne [Net.Dns]::GetHostName() -or
        [string]$Identity.instance_id -cne ([string]$Identity.host_name + "-" + [int]$Daemon.Id)
    ) { throw "prior release bootstrap identity differs" }
}

function Test-ExactContext($Payload, $Daemon) {
    try {
        $Context = $Payload.context
        return (
            [string]$Payload.mode -ceq "check-transition" -and [string]$Payload.status -ceq "PENDING" -and
            [string]$Payload.build_sha -ceq $PriorBuildSha -and
            [string]$Payload.target_build_sha -ceq $TargetBuildSha -and
            $Payload.database_writes -eq $false -and $Payload.writer_authorized -eq $false -and
            [string]$Context.schema -ceq "probiga.qmt-edge-precutover-context.v1" -and
            [string]$Context.protocol -ceq $RecoveryProtocol -and
            [string]$Context.build_sha -ceq $TargetBuildSha -and
            [string]$Context.prior_build_sha -ceq $PriorBuildSha -and
            [int]$Context.prior_pid -eq [int]$Daemon.Id -and
            [string]$Context.prior_host_name -ceq [Net.Dns]::GetHostName() -and
            [string]$Context.prior_instance_id -ceq ([string]$Context.prior_host_name + "-" + [int]$Daemon.Id) -and
            $Context.prior_running -eq $true -and
            [string]$Context.deployment_attempt_id -cmatch "^[0-9a-f]{32}$"
        )
    } catch { return $false }
}

function Wait-Context($Daemon) {
    $Deadline = (Get-Date).AddSeconds($TransitionTimeoutSeconds)
    do {
        if ($Daemon.HasExited) { throw "prior daemon exited before handoff" }
        $Selection = Invoke-JsonTool @("--select-update-target", "--expected-build-sha", $PriorBuildSha, "--compact")
        if (
            $Selection.ExitCode -ne 0 -or
            [string]$Selection.Payload.mode -cne "select-update-target" -or
            [string]$Selection.Payload.build_sha -cne $PriorBuildSha -or
            $Selection.Payload.database_writes -ne $false -or
            $Selection.Payload.writer_authorized -ne $false
        ) { throw "release target selection failed closed" }
        $Status = [string]$Selection.Payload.status
        $Selected = [string]$Selection.Payload.target_build_sha
        if ($Status -ceq "SELECTED" -and $Selected -ceq $TargetBuildSha) {
            $Transition = Invoke-JsonTool @(
                "--check-transition", "--expected-build-sha", $PriorBuildSha,
                "--target-build-sha", $TargetBuildSha, "--compact"
            )
            if ($Transition.ExitCode -ne 4 -or !(Test-ExactContext $Transition.Payload $Daemon)) {
                throw "protected handoff context differs"
            }
            return $Transition.Payload.context
        }
        if (!(($Status -ceq "NO_REQUEST" -and !$Selected) -or
            ($Status -ceq "SELECTED" -and $Selected -ceq $PriorBuildSha))) {
            throw "unexpected protected release target is active"
        }
        Start-Sleep -Seconds 2
    } while ((Get-Date) -lt $Deadline)
    throw "protected handoff timed out"
}

function Request-Shutdown($Runtime, $Daemon) {
    $RequestUid = [Guid]::NewGuid().ToString()
    $Request = [ordered]@{
        schema_version = 1; request_uid = $RequestUid
        instance_id = [string]$Runtime.instance_id; pid = [int]$Daemon.Id
        build_sha = $PriorBuildSha; requested_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
    }
    if (Test-Path -LiteralPath $ShutdownRequestPath) {
        $Item = Get-Item -LiteralPath $ShutdownRequestPath -Force
        if ($Item.PSIsContainer -or ($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "shutdown request path is not an ordinary file"
        }
    }
    $Temporary = "$ShutdownRequestPath.$PID.tmp"
    try {
        [IO.File]::WriteAllText($Temporary, (($Request | ConvertTo-Json -Compress) + "`n"), [Text.UTF8Encoding]::new($false))
        Move-Item -LiteralPath $Temporary -Destination $ShutdownRequestPath -Force
    } finally { Remove-Item $Temporary -Force -ErrorAction SilentlyContinue }
    return $RequestUid
}

function Wait-Shutdown([string]$RequestUid, $Runtime, $Daemon) {
    $Deadline = (Get-Date).AddSeconds(90)
    do {
        if ($Daemon.HasExited -and (Test-Path -LiteralPath $ShutdownReceiptPath -PathType Leaf)) {
            $Item = Get-Item -LiteralPath $ShutdownReceiptPath -Force
            if (($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw "shutdown receipt path is a reparse point"
            }
            try { $Receipt = Get-Content -LiteralPath $ShutdownReceiptPath -Raw | ConvertFrom-Json -ErrorAction Stop }
            catch { throw "shutdown receipt is malformed" }
            if (
                [string]$Receipt.status -ceq "stopped" -and [string]$Receipt.request_uid -ceq $RequestUid -and
                [string]$Receipt.instance_id -ceq [string]$Runtime.instance_id -and
                [int]$Receipt.pid -eq [int]$Daemon.Id -and
                ([string]$Receipt.build_sha).ToLower() -ceq $PriorBuildSha
            ) { return }
            throw "shutdown receipt identity differs"
        }
        Start-Sleep -Milliseconds 250
    } while ((Get-Date) -lt $Deadline)
    throw "prior daemon did not stop gracefully"
}

function Remove-OwnedRuntime($Runtime, $Daemon) {
    if ($null -eq $Runtime -or !$Daemon.HasExited -or !(Test-Path -LiteralPath $RuntimePath -PathType Leaf)) { return }
    $Item = Get-Item -LiteralPath $RuntimePath -Force
    if (($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { return }
    try { $Current = Get-Content -LiteralPath $RuntimePath -Raw | ConvertFrom-Json -ErrorAction Stop }
    catch { return }
    if (
        [string]$Current.instance_id -ceq [string]$Runtime.instance_id -and
        [int]$Current.pid -eq [int]$Daemon.Id -and
        ([string]$Current.build_sha).ToLower() -ceq $PriorBuildSha
    ) { Remove-Item -LiteralPath $RuntimePath -Force }
}

# Same containment primitive as the installed wrapper: closing this handle kills
# the daemon, bootstrap process, and all descendants on every exit path.
Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
public sealed class ProBigAPriorEdgeJob : IDisposable {
    private IntPtr handle;
    [StructLayout(LayoutKind.Sequential)] private struct Basic {
        public long A, B; public uint Flags; public UIntPtr C, D;
        public uint E; public UIntPtr F; public uint G, H;
    }
    [StructLayout(LayoutKind.Sequential)] private struct IO { public ulong A, B, C, D, E, F; }
    [StructLayout(LayoutKind.Sequential)] private struct Extended {
        public Basic Basic; public IO Io; public UIntPtr A, B, C, D;
    }
    [DllImport("kernel32.dll")] private static extern IntPtr CreateJobObject(IntPtr attributes, string name);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool SetInformationJobObject(IntPtr job, int cls, ref Extended info, uint len);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
    [DllImport("kernel32.dll")] private static extern bool CloseHandle(IntPtr h);
    public ProBigAPriorEdgeJob() {
        handle = CreateJobObject(IntPtr.Zero, null);
        if (handle == IntPtr.Zero) throw new Win32Exception();
        var info = new Extended(); info.Basic.Flags = 0x00002000;
        uint size = (uint)Marshal.SizeOf(info);
        if (!SetInformationJobObject(handle, 9, ref info, size)) {
            int error = Marshal.GetLastWin32Error(); CloseHandle(handle);
            handle = IntPtr.Zero; throw new Win32Exception(error);
        }
    }
    public void Assign(Process p) {
        if (!AssignProcessToJobObject(handle, p.Handle)) throw new Win32Exception(Marshal.GetLastWin32Error());
    }
    public void Dispose() {
        if (handle != IntPtr.Zero) { CloseHandle(handle); handle = IntPtr.Zero; }
    }
}
'@

function Invoke-Recovery() {
    Assert-Roots
    foreach ($Path in @(
        $PowerShellExe, $WScriptExe, $PythonExe, $QmtPythonExe, $DaemonScript,
        $BootstrapTool, $WrapperScript, $UpdaterLauncher, (Join-Path $ProductionRoot ".env")
    )) {
        if (!(Test-Path -LiteralPath $Path -PathType Leaf)) { throw "prior-edge recovery dependency is missing" }
    }
    Assert-Directory $StateRoot "scheduler state root"
    if (!$StateRoot.StartsWith($ProgramDataRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        throw "scheduler state root escapes ProgramData"
    }
    Assert-NoDaemon
    $Gate = $null; $Job = $null; $Daemon = $null; $Runtime = $null
    $Failure = $null; $CleanupFailure = $null
    try {
        # ACL/UAC failure occurs here, before any QMT call, daemon, or DB write.
        $Gate = Enter-TaskGate
        foreach ($Name in @("PYTHONHOME", "PYTHONPATH", "PYTHONSTARTUP", "PYTHONINSPECT")) {
            Remove-Item "Env:$Name" -ErrorAction SilentlyContinue
        }
        $env:VIRTUAL_ENV = Join-Path $ProductionRoot ".venv"
        $env:PROBIGA_CODE_ROOT = $ProductionRoot
        $env:PROBIGA_DEPLOYMENT_MODE = "production"
        $env:PROBIGA_SCHEDULER_EXECUTOR_ROLE = "qmt_windows_edge"
        $env:PROBIGA_BUILD_COMMIT_SHA = $PriorBuildSha
        $env:PROBIGA_EXPECTED_GIT_SHA = $PriorBuildSha
        $env:QMT_PYTHON = $QmtPythonExe
        Assert-PriorReady

        $Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
        $Job = [ProBigAPriorEdgeJob]::new()
        $Daemon = Start-Process -FilePath $PythonExe -ArgumentList @("-P", ('"' + $DaemonScript + '"')) `
            -WorkingDirectory $ProductionRoot -WindowStyle Hidden `
            -RedirectStandardOutput (Join-Path $StateRoot "resume-$Stamp.out.log") `
            -RedirectStandardError (Join-Path $StateRoot "resume-$Stamp.err.log") -PassThru
        $Job.Assign($Daemon)
        # The existing bootstrap reads shared DB heartbeats. Bind this newly
        # created child locally before it can consume or append any receipt.
        $Runtime = Wait-OwnedRuntime $Daemon

        $BootstrapOut = Join-Path $StateRoot "resume-bootstrap-$Stamp.json"
        $Bootstrap = Start-Process -FilePath $PythonExe -ArgumentList @(
            "-P", ('"' + $BootstrapTool + '"'), "--bootstrap", "--expected-build-sha", $PriorBuildSha,
            "--heartbeat-timeout-seconds", "240", "--compact"
        ) -WorkingDirectory $ProductionRoot -WindowStyle Hidden -RedirectStandardOutput $BootstrapOut `
            -RedirectStandardError (Join-Path $StateRoot "resume-bootstrap-$Stamp.err.log") -PassThru
        $Job.Assign($Bootstrap)
        if (!$Bootstrap.WaitForExit($BootstrapTimeoutSeconds * 1000)) { throw "prior release bootstrap timed out" }
        if ($Bootstrap.ExitCode -ne 0) { throw "prior release bootstrap failed" }
        try {
            $Proof = Get-Content $BootstrapOut -Raw | ConvertFrom-Json -ErrorAction Stop
        } catch { throw "prior release bootstrap proof is malformed" }
        Assert-OwnedBootstrap $Proof $Daemon
        $Runtime = Read-OwnedRuntime $Daemon
        if ($null -eq $Runtime) { throw "local daemon identity differs" }
        [ordered]@{ status="PRIOR_EDGE_READY"; prior_build_sha=$PriorBuildSha; target_build_sha=$TargetBuildSha; prior_pid=[int]$Daemon.Id } |
            ConvertTo-Json -Compress | Write-Output

        $Context = Wait-Context $Daemon
        $RequestUid = Request-Shutdown $Runtime $Daemon
        Wait-Shutdown $RequestUid $Runtime $Daemon
        [ordered]@{ status="PRIOR_EDGE_STOPPED"; deployment_attempt_id=[string]$Context.deployment_attempt_id; prior_pid=[int]$Daemon.Id } |
            ConvertTo-Json -Compress | Write-Output
    } catch { $Failure = $_ }
    finally {
        if ($null -ne $Job) { $Job.Dispose() }
        if ($null -ne $Daemon -and !$Daemon.HasExited) {
            try {
                Stop-Process -Id $Daemon.Id -Force -ErrorAction Stop
                $Daemon.WaitForExit(15000) | Out-Null
            } catch { $CleanupFailure = $_ }
        }
        if ($null -ne $Daemon -and $Daemon.HasExited) { Remove-OwnedRuntime $Runtime $Daemon }
        try { Assert-NoDaemon; Exit-TaskGate $Gate }
        catch { $CleanupFailure = $_ }
    }
    if ($null -ne $CleanupFailure) { throw "recovery cleanup failed; Windows edge tasks remain gated" }
    if ($null -ne $Failure) { throw $Failure }
}

try { Invoke-Recovery; exit 0 }
catch { [Console]::Error.WriteLine($_.Exception.Message); exit 1 }
