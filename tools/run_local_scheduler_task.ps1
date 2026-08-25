$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$ExpectedRoot = [System.IO.Path]::GetFullPath("E:\My Code\ProBigA")
$DerivedRoot = [System.IO.Path]::GetFullPath(
    (Split-Path -Parent $PSScriptRoot)
)
if ($DerivedRoot -ine $ExpectedRoot) {
    throw "scheduler task wrapper is outside the production workspace"
}

$PythonExe = Join-Path $ExpectedRoot ".venv\Scripts\python.exe"
$DaemonScript = Join-Path $ExpectedRoot "tools\run_scheduler_daemon.py"
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
if (!(Test-Path -LiteralPath $PythonExe -PathType Leaf)) {
    throw "scheduler Python runtime is missing"
}
if (!(Test-Path -LiteralPath $DaemonScript -PathType Leaf)) {
    throw "scheduler daemon entry point is missing"
}

$env:PROBIGA_JOB_LOG_ROOT = $JobLogRoot
$env:PROBIGA_API_SCHEDULER_POLL_SECONDS = "60"
$env:PROBIGA_SCHEDULER_EXECUTOR_ROLE = "qmt_windows_edge"

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
