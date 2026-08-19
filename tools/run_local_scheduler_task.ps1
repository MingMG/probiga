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
$DataDir = Join-Path $ExpectedRoot "data"
if (!(Test-Path -LiteralPath $PythonExe -PathType Leaf)) {
    throw "scheduler Python runtime is missing"
}
if (!(Test-Path -LiteralPath $DaemonScript -PathType Leaf)) {
    throw "scheduler daemon entry point is missing"
}
if (!(Test-Path -LiteralPath $DataDir -PathType Container)) {
    New-Item -ItemType Directory -Path $DataDir | Out-Null
}

$Stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$StdOutPath = Join-Path $DataDir "scheduler_task-$Stamp.out.log"
$StdErrPath = Join-Path $DataDir "scheduler_task-$Stamp.err.log"
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
