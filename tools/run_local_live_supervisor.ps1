$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "data"
if (!(Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir | Out-Null
}

function Get-SupervisorProcesses {
    Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "powershell.exe" -and
        $_.CommandLine -like "*run_local_live_supervisor.ps1*" -and
        $_.ProcessId -ne $PID
    }
}

if (Get-SupervisorProcesses) {
    Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') supervisor already running; exit duplicate"
    exit 0
}

$StartScript = Join-Path $Root "tools\start_local_live_services.ps1"
Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') local live supervisor started"

while ($true) {
    try {
        & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $StartScript
    }
    catch {
        Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') supervisor ensure failed: $($_.Exception.Message)"
    }
    Start-Sleep -Seconds 30
}
