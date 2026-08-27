param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$RegisteredRoot
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$SchedulerTaskName = "ProBigA QMT Windows Edge Scheduler"
$UpdateTaskName = "ProBigA QMT Windows Edge Updater"
$ExpectedOrigin = "https://github.com/MingMG/probiga.git"
$WindowsRoot = [System.Environment]::GetFolderPath(
    [System.Environment+SpecialFolder]::Windows
)
$PowerShellExe = Join-Path $WindowsRoot "System32\WindowsPowerShell\v1.0\powershell.exe"
if ($RegisteredRoot -notmatch "^[A-Za-z]:[\\/]") {
    throw "scheduler registered root must be an absolute local path"
}
$ExpectedRoot = [System.IO.Path]::GetFullPath($RegisteredRoot)
$DerivedRoot = [System.IO.Path]::GetFullPath(
    (Split-Path -Parent $PSScriptRoot)
)
if ($DerivedRoot -ine $ExpectedRoot) {
    throw "scheduler task wrapper differs from its registered production root"
}
$RootItem = Get-Item -LiteralPath $DerivedRoot -Force
if (
    !$RootItem.PSIsContainer -or
    ($RootItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0
) {
    throw "scheduler production root must be an ordinary directory"
}

$PythonExe = Join-Path $ExpectedRoot ".venv\Scripts\python.exe"
$QmtPythonExe = Join-Path $ExpectedRoot "runtime\qmt-py313\Scripts\python.exe"
$DaemonScript = Join-Path $ExpectedRoot "tools\run_scheduler_daemon.py"
$UpdaterScript = Join-Path $ExpectedRoot "tools\update_qmt_windows_edge.ps1"
$WrapperScript = Join-Path $ExpectedRoot "tools\run_local_scheduler_task.ps1"
$EnvFile = Join-Path $ExpectedRoot ".env"
$ProgramDataRoot = [System.IO.Path]::GetFullPath($env:ProgramData)
$SchedulerStateRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $ProgramDataRoot "ProBigA\scheduler")
)
$JobLogRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $ProgramDataRoot "ProBigA\jobs")
)
foreach ($StatePath in @($SchedulerStateRoot, $JobLogRoot)) {
    if (!$StatePath.StartsWith(
        $ProgramDataRoot + [System.IO.Path]::DirectorySeparatorChar
    )) {
        throw "scheduler state root escapes ProgramData"
    }
    if (!(Test-Path -LiteralPath $StatePath -PathType Container)) {
        throw "scheduler state root was not installed: $StatePath"
    }
    $StateItem = Get-Item -LiteralPath $StatePath -Force
    if (($StateItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "scheduler state root cannot be a reparse point: $StatePath"
    }
}
foreach ($Path in @(
    $PythonExe,
    $QmtPythonExe,
    $DaemonScript,
    $UpdaterScript,
    $WrapperScript,
    $EnvFile
)) {
    if (!(Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "scheduler release dependency is missing: $Path"
    }
    $DependencyItem = Get-Item -LiteralPath $Path -Force
    if (
        ($DependencyItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) `
            -ne 0
    ) {
        throw "scheduler release dependency cannot be a reparse point: $Path"
    }
}

$SchedulerArgument = (
    "-WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass " +
    "-File `"$WrapperScript`" -RegisteredRoot `"$ExpectedRoot`""
)
$UpdaterArgument = (
    "-WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass " +
    "-File `"$UpdaterScript`" -RegisteredRoot `"$ExpectedRoot`""
)
$Registered = Get-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
$RegisteredUpdater = Get-ScheduledTask -TaskName $UpdateTaskName -ErrorAction Stop
if (
    @($Registered.Actions).Count -ne 1 -or
    @($RegisteredUpdater.Actions).Count -ne 1 -or
    $Registered.Actions[0].Execute -ine $PowerShellExe -or
    $RegisteredUpdater.Actions[0].Execute -ine $PowerShellExe -or
    $Registered.Actions[0].WorkingDirectory -ine $ExpectedRoot -or
    $RegisteredUpdater.Actions[0].WorkingDirectory -ine $ExpectedRoot -or
    $Registered.Actions[0].Arguments -cne $SchedulerArgument -or
    $RegisteredUpdater.Actions[0].Arguments -cne $UpdaterArgument
) {
    throw "scheduler registered production root binding differs"
}

function Invoke-Git([string[]]$Arguments) {
    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $Output = & git -C $ExpectedRoot @Arguments 2>$null
        $ExitCode = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousPreference
    }
    if ($ExitCode -ne 0) {
        throw "scheduler release identity check failed"
    }
    return @($Output)
}

# A Windows Scheduled Task is a new process and does not inherit the updater's
# temporary environment.  Freeze the child build identity from the verified
# checkout on every daemon start; otherwise build-bound QMT publishers would
# either be permanently DATA_BLOCKED or could accept an ambient stale SHA.
$TopLevel = ((Invoke-Git @("rev-parse", "--show-toplevel")) -join "").Trim()
if ([System.IO.Path]::GetFullPath($TopLevel) -ine $ExpectedRoot) {
    throw "scheduler Git top level differs from registered production root"
}
$Origin = ((Invoke-Git @("remote", "get-url", "origin")) -join "").Trim()
if ($Origin -ine $ExpectedOrigin) {
    throw "scheduler origin differs from the production repository"
}
Invoke-Git @("fetch", "--prune", "origin", "main") | Out-Null
$Branch = ((
    Invoke-Git @("symbolic-ref", "--short", "HEAD")
) -join "").Trim()
$BuildSha = ((
    Invoke-Git @("rev-parse", "HEAD")
) -join "").Trim().ToLowerInvariant()
$TargetSha = ((
    Invoke-Git @("rev-parse", "origin/main")
) -join "").Trim().ToLowerInvariant()
$Dirty = ((
    Invoke-Git @("status", "--porcelain", "--untracked-files=normal")
) -join "`n").Trim()
if (
    $Branch -cne "main" -or
    $BuildSha -notmatch "^[0-9a-f]{40}$" -or
    $TargetSha -notmatch "^[0-9a-f]{40}$" -or
    $BuildSha -cne $TargetSha -or
    $Dirty
) {
    throw "QMT Windows scheduler checkout is not a clean exact-main release"
}

$env:PROBIGA_JOB_LOG_ROOT = $JobLogRoot
$env:PROBIGA_API_SCHEDULER_POLL_SECONDS = "60"
$env:PROBIGA_SCHEDULER_EXECUTOR_ROLE = "qmt_windows_edge"
$env:PROBIGA_BUILD_COMMIT_SHA = $BuildSha
$env:PROBIGA_EXPECTED_GIT_SHA = $BuildSha
$env:QMT_PYTHON = $QmtPythonExe

$Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$StdOutPath = Join-Path $SchedulerStateRoot "scheduler_task-$Stamp.out.log"
$StdErrPath = Join-Path $SchedulerStateRoot "scheduler_task-$Stamp.err.log"
$Process = Start-Process `
    -FilePath $PythonExe `
    -ArgumentList @("-P", ('"' + $DaemonScript + '"')) `
    -WorkingDirectory $ExpectedRoot `
    -WindowStyle Hidden `
    -RedirectStandardOutput $StdOutPath `
    -RedirectStandardError $StdErrPath `
    -Wait `
    -PassThru
exit $Process.ExitCode
