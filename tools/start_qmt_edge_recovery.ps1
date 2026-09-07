param(
    [Parameter(Mandatory = $true)] [ValidateNotNullOrEmpty()] [string]$ProductionRoot,
    [Parameter(Mandatory = $true)] [ValidatePattern('^[0-9a-fA-F]{40}$')] [string]$PriorBuildSha,
    [Parameter(Mandatory = $true)] [ValidatePattern('^[0-9a-fA-F]{40}$')] [string]$TargetBuildSha,
    [ValidateRange(60, 3600)] [int]$BootstrapTimeoutSeconds = 1200,
    [ValidateRange(60, 3600)] [int]$TransitionTimeoutSeconds = 1800,
    [switch]$ForwardOnlyHandoff,
    [string]$GitHubProxy = '',
    [ValidateRange(5, 300)] [int]$GitTimeoutSeconds = 45,
    # Internal transport only: no script, command, or result path is accepted.
    [Parameter(DontShow = $true)] [switch]$ElevatedChild,
    [Parameter(DontShow = $true)] [ValidatePattern('^[0-9a-f]{32}$')] [string]$LaunchId
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if ($PSVersionTable.PSEdition -cne 'Desktop') { throw 'recovery launcher requires Windows PowerShell 5.1' }
$ControllerRoot = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
if ($ProductionRoot -notmatch '^[A-Za-z]:[\\/]') { throw 'production root must be an absolute local directory' }
$ProductionRoot = [IO.Path]::GetFullPath($ProductionRoot).TrimEnd('\')
$PriorBuildSha = $PriorBuildSha.ToLowerInvariant()
$TargetBuildSha = $TargetBuildSha.ToLowerInvariant()
$LauncherScript = Join-Path $PSScriptRoot 'start_qmt_edge_recovery.ps1'
$ControllerScript = Join-Path $PSScriptRoot 'resume_qmt_prior_edge.ps1'
$PowerShellExe = Join-Path ([Environment]::GetFolderPath([Environment+SpecialFolder]::Windows)) 'System32\WindowsPowerShell\v1.0\powershell.exe'
. (Join-Path $PSScriptRoot 'deploy_preflight.ps1')
Assert-DeployProxy $GitHubProxy

function ConvertTo-LauncherArgument([string]$Value) {
    # Start-Process joins ArgumentList; quote each scalar for the native parser.
    $Escaped = [regex]::Replace($Value, '(\\*)"', '$1$1\"')
    $Escaped = [regex]::Replace($Escaped, '(\\+)$', '$1$1')
    return '"' + $Escaped + '"'
}

function Get-RecoveryArguments([switch]$ForElevatedChild) {
    $ScriptPath = $ControllerScript
    if ($ForElevatedChild) { $ScriptPath = $LauncherScript }
    $Values = @('-NoProfile', '-NonInteractive', '-File', $ScriptPath,
        '-ProductionRoot', $ProductionRoot, '-PriorBuildSha', $PriorBuildSha,
        '-TargetBuildSha', $TargetBuildSha,
        '-BootstrapTimeoutSeconds', [string]$BootstrapTimeoutSeconds,
        '-TransitionTimeoutSeconds', [string]$TransitionTimeoutSeconds,
        '-GitTimeoutSeconds', [string]$GitTimeoutSeconds)
    if ($GitHubProxy) { $Values += @('-GitHubProxy', $GitHubProxy) }
    if ($ForwardOnlyHandoff) { $Values += '-ForwardOnlyHandoff' }
    if ($ForElevatedChild) { $Values += @('-ElevatedChild', '-LaunchId', $LaunchId) }
    return (($Values | ForEach-Object { ConvertTo-LauncherArgument $_ }) -join ' ')
}

function Assert-LauncherDirectory([string]$Path) {
    $Current = [IO.Path]::GetFullPath($Path)
    while ($Current) {
        $Item = Get-Item -LiteralPath $Current -Force -ErrorAction Stop
        if (!$Item.PSIsContainer -or ($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw 'launcher path must contain only ordinary local directories'
        }
        $Current = [IO.Path]::GetDirectoryName($Current)
    }
}

function Assert-LauncherRepository() {
    Assert-LauncherDirectory $ControllerRoot
    Assert-LauncherDirectory $ProductionRoot
    if ($ControllerRoot -ieq $ProductionRoot) { throw 'controller and production roots must differ' }
    $Git = @{ Root = $ControllerRoot; TimeoutSeconds = $GitTimeoutSeconds; GitHubProxy = $GitHubProxy }
    $Top = Invoke-DeployGit @Git -Arguments @('rev-parse', '--show-toplevel') -Stage 'launcher.controller-root'
    $Origin = Invoke-DeployGit @Git -Arguments @('remote', 'get-url', 'origin') -Stage 'launcher.controller-origin'
    if ([IO.Path]::GetFullPath($Top) -ine $ControllerRoot -or $Origin -ine 'https://github.com/MingMG/probiga.git') {
        throw 'launcher.controller-identity: repository binding differs'
    }
    $Dirty = Invoke-DeployGit @Git -Arguments @('status', '--porcelain', '--untracked-files=normal') -Stage 'launcher.controller-status'
    if ($Dirty) { throw 'launcher.controller-status: controller must be clean before requesting elevation' }
    Invoke-DeployGit @Git -Arguments @('fetch', '--prune', 'origin', 'main') -Stage 'launcher.remote-main' | Out-Null
    $HeadSha = Invoke-DeployGit @Git -Arguments @('rev-parse', 'HEAD') -Stage 'launcher.controller-head'
    $MainSha = Invoke-DeployGit @Git -Arguments @('rev-parse', 'origin/main') -Stage 'launcher.controller-main'
    if ($HeadSha -ine $TargetBuildSha -or $MainSha -ine $TargetBuildSha) {
        throw 'launcher.controller-release: controller must be the clean merged exact-main target'
    }
    foreach ($Path in @($LauncherScript, $ControllerScript)) {
        $Item = Get-Item -LiteralPath $Path -Force -ErrorAction Stop
        if ($Item.PSIsContainer -or ($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw 'launcher.controller-file: controller must be an ordinary tracked file'
        }
    }
}

function Write-LaunchState([string]$Status, [string]$Stage, [string]$Reason = '', [int]$ExitCode = -1, [int]$ControllerPid = 0) {
    foreach ($Path in @($StatePath, $HistoryPath)) {
        if (Test-Path -LiteralPath $Path) {
            $Item = Get-Item -LiteralPath $Path -Force
            if ($Item.PSIsContainer -or ($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'launcher evidence path is not an ordinary file'
            }
        }
    }
    $Record = [ordered]@{
        schema = 'probiga.qmt-edge-launch.v1'; launch_id = $LaunchId
        status = $Status; stage = $Stage; updated_at_utc = [DateTimeOffset]::UtcNow.ToString('o')
        launcher_pid = $PID; controller_pid = $ControllerPid; controller_started = ($ControllerPid -gt 0)
        exit_code = $ExitCode; reason = $Reason; prior_build_sha = $PriorBuildSha; target_build_sha = $TargetBuildSha
        stdout = $StdoutPath; stderr = $StderrPath
    }
    $Json = ($Record | ConvertTo-Json -Compress) + "`n"
    $Temporary = "$StatePath.$PID.tmp"
    try {
        [IO.File]::WriteAllText($Temporary, $Json, [Text.UTF8Encoding]::new($false))
        Move-Item -LiteralPath $Temporary -Destination $StatePath -Force
        [IO.File]::AppendAllText($HistoryPath, $Json, [Text.UTF8Encoding]::new($false))
    } finally { Remove-Item -LiteralPath $Temporary -Force -ErrorAction SilentlyContinue }
    if ($Status -in @('FAILED', 'CANCELLED') -or ($Status -ceq 'COMPLETED' -and $ExitCode -ne 0)) {
        [Console]::Error.WriteLine("recovery_launcher status=$Status stage=$Stage exit=$ExitCode reason=$Reason evidence=$StatePath")
    }
}

function Enter-LaunchMutex([string]$Name) {
    $Mutex = [Threading.Mutex]::new($false, $Name)
    $Acquired = $false
    try {
        try { $Acquired = $Mutex.WaitOne(0) }
        catch [Threading.AbandonedMutexException] { $Acquired = $true }
        if (!$Acquired) { throw 'launcher.already-active: confirmation or recovery is already in progress; no second UAC prompt was opened' }
        return $Mutex
    } catch { $Mutex.Dispose(); throw }
}

function Exit-LaunchMutex($Mutex) {
    if ($null -ne $Mutex) { try { $Mutex.ReleaseMutex() } finally { $Mutex.Dispose() } }
}

function Test-UacCancelled($Exception) {
    while ($null -ne $Exception) {
        if ($Exception -is [ComponentModel.Win32Exception] -and $Exception.NativeErrorCode -eq 1223) { return $true }
        $Exception = $Exception.InnerException
    }
    return $false
}

function New-ControllerJob() {
    if (!('ProBigARecoveryLauncherJob' -as [type])) {
        # The controller owns its daemon/bootstrap job. This outer job owns only
        # our controller tree, so closing the elevated host cannot orphan it.
        Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
public sealed class ProBigARecoveryLauncherJob : IDisposable {
    private IntPtr handle;
    [StructLayout(LayoutKind.Sequential)] private struct Basic {
        public long A, B; public uint Flags; public UIntPtr C, D;
        public uint E; public UIntPtr F; public uint G, H;
    }
    [StructLayout(LayoutKind.Sequential)] private struct IO { public ulong A, B, C, D, E, F; }
    [StructLayout(LayoutKind.Sequential)] private struct Extended { public Basic Basic; public IO Io; public UIntPtr A, B, C, D; }
    [DllImport("kernel32.dll", SetLastError=true)] private static extern IntPtr CreateJobObject(IntPtr attributes, string name);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool SetInformationJobObject(IntPtr job, int cls, ref Extended info, uint len);
    [DllImport("kernel32.dll", SetLastError=true)] private static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
    [DllImport("kernel32.dll")] private static extern bool CloseHandle(IntPtr h);
    public ProBigARecoveryLauncherJob() {
        handle = CreateJobObject(IntPtr.Zero, null);
        if (handle == IntPtr.Zero) throw new Win32Exception();
        var info = new Extended(); info.Basic.Flags = 0x00002000;
        if (!SetInformationJobObject(handle, 9, ref info, (uint)Marshal.SizeOf(info))) {
            int error = Marshal.GetLastWin32Error(); CloseHandle(handle); handle = IntPtr.Zero; throw new Win32Exception(error);
        }
    }
    public void Assign(Process process) {
        if (!AssignProcessToJobObject(handle, process.Handle)) throw new Win32Exception(Marshal.GetLastWin32Error());
    }
    public void Dispose() { if (handle != IntPtr.Zero) { CloseHandle(handle); handle = IntPtr.Zero; } }
}
'@
    }
    return [ProBigARecoveryLauncherJob]::new()
}

function Invoke-ElevatedRecovery() {
    $Gate = $null; $Controller = $null; $Job = $null
    try {
        Assert-DeployAdministrator
        $Gate = Enter-LaunchMutex ($MutexName + '.execution')
        Write-LaunchState 'STARTED' 'elevated-preflight'
        # Repeat in the actual elevated environment; never trust a parent receipt.
        Assert-LauncherRepository
        foreach ($Path in @($StdoutPath, $StderrPath)) {
            if (Test-Path -LiteralPath $Path) { throw 'launcher evidence already exists for this launch' }
        }
        $Job = New-ControllerJob
        $Controller = Start-Process -FilePath $PowerShellExe -ArgumentList (Get-RecoveryArguments) `
            -WorkingDirectory $ControllerRoot -WindowStyle Hidden -PassThru `
            -RedirectStandardOutput $StdoutPath -RedirectStandardError $StderrPath
        $null = $Controller.Handle
        $Job.Assign($Controller)
        Write-LaunchState 'STARTED' 'controller' '' -1 $Controller.Id
        $Controller.WaitForExit()
        $Controller.Refresh()
        $Code = [int]$Controller.ExitCode
        $Reason = ''
        if ($Code -ne 0) { $Reason = 'controller failed; inspect the stage-specific stderr evidence' }
        Write-LaunchState 'COMPLETED' 'controller-exited' $Reason $Code $Controller.Id
        return $Code
    } catch {
        $ControllerPid = 0
        if ($null -ne $Controller) { $ControllerPid = $Controller.Id }
        Write-LaunchState 'FAILED' 'elevated-preflight-or-launch' (Protect-DeployDiagnostic $_.Exception.Message) 1 $ControllerPid
        return 1
    } finally {
        try {
            if ($null -ne $Job) { $Job.Dispose() }
            if ($null -ne $Controller -and !$Controller.HasExited) {
                # Also cover assignment failure before the process entered the job.
                $Controller.Kill()
                $Controller.WaitForExit()
            }
        } finally { Exit-LaunchMutex $Gate }
    }
}

function Invoke-RecoveryLaunch() {
    $Gate = $null; $Child = $null; $ExecutionGate = $null
    try {
        $Gate = Enter-LaunchMutex ($MutexName + '.prompt')
        # A child retains its own gate if the original launcher window is closed.
        $ExecutionGate = Enter-LaunchMutex ($MutexName + '.execution')
        # Keep the unowned handle alive across UAC. The existing user-created
        # object remains usable by the same user at either integrity level.
        $ExecutionGate.ReleaseMutex()
        Assert-LauncherRepository
        Write-LaunchState 'WAITING_CONFIRMATION' 'uac'
        $Child = Start-Process -FilePath $PowerShellExe -Verb RunAs `
            -ArgumentList (Get-RecoveryArguments -ForElevatedChild) `
            -WorkingDirectory $ControllerRoot -WindowStyle Hidden -PassThru
        $null = $Child.Handle
        $Child.WaitForExit()
        $Child.Refresh()
        $Completion = Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json -ErrorAction Stop
        if ($Completion.launch_id -cne $LaunchId -or $Completion.status -cne 'COMPLETED' -or
            [int]$Completion.launcher_pid -ne $Child.Id -or !$Completion.controller_started) {
            if ($Completion.launch_id -ceq $LaunchId -and $Completion.status -ceq 'FAILED') { return 1 }
            Write-LaunchState 'FAILED' 'child-exited-without-completion' 'elevated host exited before a controller completion receipt; controller completion is unconfirmed' 1
            return 1
        }
        if ([int]$Child.ExitCode -ne [int]$Completion.exit_code) {
            Write-LaunchState 'FAILED' 'child-exit-mismatch' 'elevated host exit differs from the controller receipt' 1
            return 1
        }
        return [int]$Completion.exit_code
    } catch {
        $Status = 'FAILED'; $Stage = 'preflight-or-launch'; $Code = 1
        if (Test-UacCancelled $_.Exception) { $Status = 'CANCELLED'; $Stage = 'uac'; $Code = 1223 }
        Write-LaunchState $Status $Stage (Protect-DeployDiagnostic $_.Exception.Message) $Code
        return $Code
    } finally {
        if ($null -ne $ExecutionGate) { $ExecutionGate.Dispose() }
        Exit-LaunchMutex $Gate
    }
}

if (!$ElevatedChild) { $LaunchId = [Guid]::NewGuid().ToString('N') }
elseif (!$LaunchId) { throw 'elevated child requires a launch identity' }
Assert-LauncherDirectory $ControllerRoot
Assert-LauncherDirectory $ProductionRoot
# runtime/ is already ignored, so recording a launch cannot dirty the release.
$EvidenceRoot = Join-Path $ControllerRoot 'runtime\deploy'
foreach ($Directory in @((Join-Path $ControllerRoot 'runtime'), $EvidenceRoot)) {
    if (!(Test-Path -LiteralPath $Directory)) { New-Item -ItemType Directory -Path $Directory | Out-Null }
    Assert-LauncherDirectory $Directory
}
$StatePath = Join-Path $EvidenceRoot "edge-recovery-$LaunchId.json"
$HistoryPath = Join-Path $EvidenceRoot "edge-recovery-$LaunchId.events.jsonl"
$StdoutPath = Join-Path $EvidenceRoot "edge-recovery-$LaunchId.stdout.jsonl"
$StderrPath = Join-Path $EvidenceRoot "edge-recovery-$LaunchId.stderr.txt"
$Hasher = [Security.Cryptography.SHA256]::Create()
try { $RootHash = [BitConverter]::ToString($Hasher.ComputeHash([Text.Encoding]::UTF8.GetBytes($ProductionRoot.ToLowerInvariant()))).Replace('-', '') }
finally { $Hasher.Dispose() }
$MutexName = 'Global\ProBigA.QmtEdgeRecovery.' + $RootHash
Write-Output "Recovery evidence: $StatePath"
if ($ElevatedChild) { $Result = Invoke-ElevatedRecovery }
else { $Result = Invoke-RecoveryLaunch }
exit $Result
