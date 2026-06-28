$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "data"
if (!(Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir | Out-Null
}

$SupervisorScript = Join-Path $Root "tools\run_local_live_supervisor.ps1"
$StdOutPath = Join-Path $DataDir "local_live_supervisor.out.log"
$StdErrPath = Join-Path $DataDir "local_live_supervisor.err.log"

$running = Get-CimInstance Win32_Process | Where-Object {
    $_.Name -eq "powershell.exe" -and $_.CommandLine -like "*run_local_live_supervisor.ps1*"
}
if ($running) {
    exit 0
}

Start-Process -FilePath "powershell.exe" `
    -ArgumentList "-NoProfile -ExecutionPolicy Bypass -File `"$SupervisorScript`"" `
    -WorkingDirectory $Root `
    -WindowStyle Hidden `
    -RedirectStandardOutput $StdOutPath `
    -RedirectStandardError $StdErrPath
