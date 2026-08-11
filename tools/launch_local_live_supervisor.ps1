$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "data"
if (!(Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir | Out-Null
}

$SupervisorScript = Join-Path $Root "tools\run_local_live_supervisor.ps1"
$StdOutPath = Join-Path $DataDir "local_live_supervisor.out.log"
$StdErrPath = Join-Path $DataDir "local_live_supervisor.err.log"

$qmtAutoRestart = ([string]$env:QMT_CLIENT_AUTO_RESTART).Trim().ToLowerInvariant() -in @(
    "1", "true", "yes", "on"
)
if ($qmtAutoRestart) {
    # The supervisor owns the trading-session window, retry interval and daily
    # attempt cap. Do not launch the legacy unbounded watchdog.
    Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') guarded QMT recovery enabled"
} else {
    Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') QMT client auto-restart disabled"
}

# run_local_live_supervisor.ps1 owns a named mutex, so launching is safe even
# when Win32_Process/WMI is unavailable; a duplicate exits immediately.
Start-Process -FilePath "powershell.exe" `
    -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$SupervisorScript`"" `
    -WorkingDirectory $Root `
    -WindowStyle Hidden `
    -RedirectStandardOutput $StdOutPath `
    -RedirectStandardError $StdErrPath
