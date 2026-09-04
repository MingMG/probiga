param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$ProductionRoot
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$TaskName = "ProBigA QMT Windows Edge Scheduler"
$UpdateTaskName = "ProBigA QMT Windows Edge Updater"
$ExpectedOrigin = "https://github.com/MingMG/probiga.git"
$WindowsRoot = [System.Environment]::GetFolderPath(
    [System.Environment+SpecialFolder]::Windows
)
$PowerShellExe = Join-Path $WindowsRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
$WScriptExe = Join-Path $WindowsRoot "System32\wscript.exe"
if ($ProductionRoot -notmatch "^[A-Za-z]:[\\/]") {
    throw "QMT Windows edge production root must be an absolute local path"
}
$ExpectedRoot = [System.IO.Path]::GetFullPath($ProductionRoot)
$Root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
if ($Root -ine $ExpectedRoot) {
    throw "QMT Windows edge installer differs from the registered production root"
}
$RootItem = Get-Item -LiteralPath $Root -Force
if (
    !$RootItem.PSIsContainer -or
    ($RootItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0
) {
    throw "QMT Windows edge production root must be an ordinary directory"
}

$Wrapper = Join-Path $Root "tools\run_local_scheduler_task.ps1"
$Updater = Join-Path $Root "tools\update_qmt_windows_edge.ps1"
$UpdaterLauncher = Join-Path $Root "tools\run_hidden_qmt_updater.vbs"
$StrategyReloader = Join-Path $Root "tools\reload_big_qmt_strategy.ps1"
$Daemon = Join-Path $Root "tools\run_scheduler_daemon.py"
$PythonExe = Join-Path $Root ".venv\Scripts\python.exe"
$QmtPythonExe = Join-Path $Root "runtime\qmt-py313\Scripts\python.exe"
$EnvFile = Join-Path $Root ".env"
foreach ($Path in @(
    $Wrapper, $Updater, $UpdaterLauncher, $StrategyReloader, $Daemon,
    $PythonExe, $QmtPythonExe, $EnvFile
)) {
    if (!(Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "QMT Windows edge dependency is missing: $Path"
    }
    $DependencyItem = Get-Item -LiteralPath $Path -Force
    if (
        ($DependencyItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) `
            -ne 0
    ) {
        throw "QMT Windows edge dependency cannot be a reparse point: $Path"
    }
}

function Invoke-Git([string[]]$Arguments) {
    # Windows PowerShell 5 surfaces a native program's stderr as a terminating
    # NativeCommandError when ErrorActionPreference is Stop, even when git exits
    # zero (notably fetch progress).  The exit code is the authority here; keep
    # stderr out of the success output and fail closed on every non-zero code.
    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $Output = & git -C $ExpectedRoot @Arguments 2>$null
        $ExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousPreference
    }
    if ($ExitCode -ne 0) {
        throw "git command failed: git $($Arguments -join ' ')"
    }
    return @($Output)
}

# Registration starts the writer immediately, so the installer must enforce
# the same exact-main identity as the updater before it creates any state or
# scheduled task.  A dirty, detached, stale, or conversation worktree is not
# an eligible production QMT edge checkout.
$TopLevel = ((Invoke-Git @("rev-parse", "--show-toplevel")) -join "").Trim()
if ([System.IO.Path]::GetFullPath($TopLevel) -ine $ExpectedRoot) {
    throw "QMT Windows edge Git top level differs from production root"
}
$Origin = ((Invoke-Git @("remote", "get-url", "origin")) -join "").Trim()
if ($Origin -ine $ExpectedOrigin) {
    throw "QMT Windows edge origin differs from the production repository"
}
Invoke-Git @("fetch", "--prune", "origin", "main") | Out-Null
$Branch = ((Invoke-Git @("symbolic-ref", "--short", "HEAD")) -join "").Trim()
if ($Branch -cne "main") {
    throw "QMT Windows edge checkout must remain on main"
}
$Dirty = ((
    Invoke-Git @("status", "--porcelain", "--untracked-files=normal")
) -join "`n").Trim()
if ($Dirty) {
    throw "QMT Windows edge checkout is dirty; registration refused"
}
$CurrentSha = ((Invoke-Git @("rev-parse", "HEAD")) -join "").Trim().ToLowerInvariant()
$TargetSha = ((
    Invoke-Git @("rev-parse", "origin/main")
) -join "").Trim().ToLowerInvariant()
if (
    $CurrentSha -notmatch "^[0-9a-f]{40}$" -or
    $TargetSha -notmatch "^[0-9a-f]{40}$" -or
    $CurrentSha -cne $TargetSha
) {
    throw "QMT Windows edge checkout must equal origin/main exactly"
}

function Stop-ExistingTask([string]$Name) {
    $Existing = Get-ScheduledTask -TaskName $Name -ErrorAction SilentlyContinue
    if ($null -eq $Existing -or $Existing.State -ne "Running") {
        return
    }
    Stop-ScheduledTask -TaskName $Name -ErrorAction Stop
    $Deadline = (Get-Date).AddSeconds(120)
    do {
        $Current = Get-ScheduledTask -TaskName $Name -ErrorAction Stop
        if ($Current.State -ne "Running") {
            return
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $Deadline)
    throw "existing QMT Windows task did not stop before root migration: $Name"
}

# If an older task is still bound to the interactive/user checkout, drain it
# before replacing either definition.  A failed migration therefore leaves
# the edge stopped instead of running mixed roots.
Stop-ExistingTask $UpdateTaskName
Stop-ExistingTask $TaskName

$UserName = "$env:USERDOMAIN\$env:USERNAME"
$ProgramDataRoot = [System.IO.Path]::GetFullPath($env:ProgramData)
$Identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
function Initialize-ProtectedStateDirectory([string]$RelativePath) {
    $StateRoot = [System.IO.Path]::GetFullPath(
        (Join-Path $ProgramDataRoot $RelativePath)
    )
    if (!$StateRoot.StartsWith(
        $ProgramDataRoot + [System.IO.Path]::DirectorySeparatorChar
    )) {
        throw "QMT Windows state root escapes ProgramData"
    }
    New-Item -ItemType Directory -Path $StateRoot -Force | Out-Null
    $StateItem = Get-Item -LiteralPath $StateRoot -Force
    if (($StateItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "QMT Windows state root cannot be a reparse point"
    }
    $Acl = [System.Security.AccessControl.DirectorySecurity]::new()
    $Acl.SetAccessRuleProtection($true, $false)
    $Inheritance = [System.Security.AccessControl.InheritanceFlags](
        [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
        [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
    )
    $Propagation = [System.Security.AccessControl.PropagationFlags]::None
    foreach ($SidValue in @(
        "S-1-5-18", "S-1-5-32-544", $Identity.User.Value
    )) {
        $Sid = [System.Security.Principal.SecurityIdentifier]::new($SidValue)
        $Rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
            $Sid,
            [System.Security.AccessControl.FileSystemRights]::FullControl,
            $Inheritance,
            $Propagation,
            [System.Security.AccessControl.AccessControlType]::Allow
        )
        $Acl.AddAccessRule($Rule)
    }
    Set-Acl -LiteralPath $StateRoot -AclObject $Acl
    return $StateRoot
}

$StateRoots = @(
    (Initialize-ProtectedStateDirectory "ProBigA\qmt-local-gap-repair"),
    (Initialize-ProtectedStateDirectory "ProBigA\qmt-model-reload"),
    (Initialize-ProtectedStateDirectory "ProBigA\scheduler"),
    (Initialize-ProtectedStateDirectory "ProBigA\jobs")
)

$SchedulerArgument = (
    "-WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass " +
    "-File `"$Wrapper`" -RegisteredRoot `"$ExpectedRoot`""
)
$UpdaterArgument = (
    "//B //NoLogo `"$UpdaterLauncher`" `"$ExpectedRoot`""
)
$Action = New-ScheduledTaskAction `
    -Execute $PowerShellExe `
    -Argument $SchedulerArgument `
    -WorkingDirectory $ExpectedRoot
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $UserName
$Principal = New-ScheduledTaskPrincipal `
    -UserId $UserName `
    -LogonType Interactive `
    -RunLevel Limited
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -RestartCount 20 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Principal $Principal `
    -Settings $Settings `
    -Force | Out-Null

$UpdateAction = New-ScheduledTaskAction `
    -Execute $WScriptExe `
    -Argument $UpdaterArgument `
    -WorkingDirectory $ExpectedRoot
$UpdateTrigger = New-ScheduledTaskTrigger `
    -Once `
    -At ((Get-Date).AddMinutes(1)) `
    -RepetitionInterval (New-TimeSpan -Minutes 5)
$UpdateSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 45)
Register-ScheduledTask `
    -TaskName $UpdateTaskName `
    -Action $UpdateAction `
    -Trigger $UpdateTrigger `
    -Principal $Principal `
    -Settings $UpdateSettings `
    -Force | Out-Null

$Registered = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
$RegisteredUpdater = Get-ScheduledTask -TaskName $UpdateTaskName -ErrorAction Stop
if (
    $Registered.TaskName -ne $TaskName -or
    $RegisteredUpdater.TaskName -ne $UpdateTaskName -or
    @($Registered.Actions).Count -ne 1 -or
    @($RegisteredUpdater.Actions).Count -ne 1 -or
    $Registered.Actions[0].Execute -ine $PowerShellExe -or
    $RegisteredUpdater.Actions[0].Execute -ine $WScriptExe -or
    $Registered.Actions[0].WorkingDirectory -ine $ExpectedRoot -or
    $RegisteredUpdater.Actions[0].WorkingDirectory -ine $ExpectedRoot -or
    $Registered.Actions[0].Arguments -cne $SchedulerArgument -or
    $RegisteredUpdater.Actions[0].Arguments -cne $UpdaterArgument
) {
    throw "QMT Windows edge scheduled task registration/root binding differs"
}
Start-ScheduledTask -TaskName $TaskName
Write-Host "Registered scheduler/updater and started: $TaskName"
