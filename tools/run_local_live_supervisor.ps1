$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "data"
if (!(Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir | Out-Null
}

$createdNew = $false
$supervisorMutex = [System.Threading.Mutex]::new(
    $true,
    "Local\ProBigA.LocalLiveSupervisor",
    [ref]$createdNew
)
if (!$createdNew) {
    $supervisorMutex.Dispose()
    Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') supervisor already running; exit duplicate"
    exit 0
}

$StartScript = Join-Path $Root "tools\start_local_live_services.ps1"
Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') local live supervisor started"

try {
    while ($true) {
        try {
            & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $StartScript
        }
        catch {
            Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') supervisor ensure failed: $($_.Exception.Message)"
        }
        # Five-second supervision keeps the 30-second heartbeat SLA meaningful:
        # recovery begins on the first check after the threshold is crossed.
        Start-Sleep -Seconds 5
    }
}
finally {
    if ($createdNew) {
        $supervisorMutex.ReleaseMutex()
    }
    $supervisorMutex.Dispose()
}
