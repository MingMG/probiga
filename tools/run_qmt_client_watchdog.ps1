$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "data"
if (!(Test-Path -LiteralPath $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir | Out-Null
}

$qmtAutoRestart = ([string]$env:QMT_CLIENT_AUTO_RESTART).Trim().ToLowerInvariant() -in @(
    "1", "true", "yes", "on"
)
if (!$qmtAutoRestart) {
    Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') QMT client auto-restart disabled; watchdog exits"
    exit 0
}

function Resolve-QmtClientPath {
    $explicitCandidates = @()
    if ($env:GJ_QMT_EXE) { $explicitCandidates += $env:GJ_QMT_EXE }
    if ($env:QMT_CLIENT_EXE) { $explicitCandidates += $env:QMT_CLIENT_EXE }
    if ($env:GJ_QMT_HOME) {
        $explicitCandidates += (Join-Path $env:GJ_QMT_HOME "bin.x64\XtItClient.exe")
        $explicitCandidates += (Join-Path $env:GJ_QMT_HOME "bin.x64\XtMiniQmt.exe")
    }
    if ($env:QMT_HOME) {
        $explicitCandidates += (Join-Path $env:QMT_HOME "bin.x64\XtItClient.exe")
        $explicitCandidates += (Join-Path $env:QMT_HOME "bin.x64\XtMiniQmt.exe")
    }
    foreach ($candidate in $explicitCandidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate)) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }

    foreach ($driveRoot in @("D:\", "C:\")) {
        if (!(Test-Path -LiteralPath $driveRoot)) { continue }
        foreach ($folder in Get-ChildItem -LiteralPath $driveRoot -Directory -ErrorAction SilentlyContinue) {
            foreach ($exeName in @("XtItClient.exe", "XtMiniQmt.exe")) {
                $candidate = Join-Path $folder.FullName "bin.x64\$exeName"
                if (Test-Path -LiteralPath $candidate) {
                    return (Resolve-Path -LiteralPath $candidate).Path
                }
            }
        }
    }
    return $null
}

$createdNew = $false
$watchdogMutex = [System.Threading.Mutex]::new(
    $true,
    "Local\ProBigA.QmtClientWatchdog",
    [ref]$createdNew
)
if (!$createdNew) {
    $watchdogMutex.Dispose()
    exit 0
}

$qmtPath = Resolve-QmtClientPath
if (!$qmtPath) {
    Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') QMT client executable not found; watchdog exits"
    $watchdogMutex.ReleaseMutex()
    $watchdogMutex.Dispose()
    exit 1
}
$qmtWorkingDirectory = Split-Path -Parent $qmtPath
$eventLogPath = Join-Path $DataDir "qmt_client_watchdog.events.log"
$heartbeatPath = Join-Path $DataDir "qmt_client_watchdog.heartbeat"
$runtimeProcessNames = @("XtMiniQmt", "XtItClient")
$minimumBackoffSeconds = 30
$maximumBackoffSeconds = 900
$configuredBackoff = 0
if ([int]::TryParse(
    [string]$env:QMT_CLIENT_MIN_BACKOFF_SECONDS,
    [ref]$configuredBackoff
)) {
    $minimumBackoffSeconds = [Math]::Max(15, $configuredBackoff)
}
if ([int]::TryParse(
    [string]$env:QMT_CLIENT_MAX_BACKOFF_SECONDS,
    [ref]$configuredBackoff
)) {
    $maximumBackoffSeconds = [Math]::Max(
        $minimumBackoffSeconds,
        $configuredBackoff
    )
}
$lastStartAttempt = [DateTime]::MinValue
$attemptCount = 0
$startedMessage = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') QMT client watchdog started"
Add-Content -LiteralPath $eventLogPath -Value $startedMessage -Encoding UTF8
Write-Output $startedMessage

try {
    while ($true) {
        [System.IO.File]::WriteAllText(
            $heartbeatPath,
            (Get-Date -Format "yyyy-MM-dd HH:mm:ss"),
            [System.Text.UTF8Encoding]::new($false)
        )
        $now = Get-Date
        $isWeekday = $now.DayOfWeek -notin @(
            [System.DayOfWeek]::Saturday,
            [System.DayOfWeek]::Sunday
        )
        $inStartWindow = (
            $now.TimeOfDay -ge [TimeSpan]::FromHours(6.5) -and
            $now.TimeOfDay -le [TimeSpan]::FromHours(23)
        )
        # Treat either current executable as live. During the guarded window,
        # retry forever with bounded exponential backoff.
        $running = @(Get-Process -Name $runtimeProcessNames -ErrorAction SilentlyContinue)
        if ($running.Count -gt 0) {
            $attemptCount = 0
        }
        $power = [Math]::Min(10, [Math]::Max(0, $attemptCount - 1))
        $retryDelaySeconds = [int][Math]::Min(
            $maximumBackoffSeconds,
            $minimumBackoffSeconds * [Math]::Pow(2, $power)
        )
        $restartAllowed = (
            ((Get-Date) - $lastStartAttempt).TotalSeconds -ge
            $retryDelaySeconds
        )
        if (
            $running.Count -eq 0 -and
            $isWeekday -and
            $inStartWindow -and
            $restartAllowed
        ) {
            try {
                $lastStartAttempt = Get-Date
                $attemptCount += 1
                $started = Start-Process -FilePath $qmtPath `
                    -WorkingDirectory $qmtWorkingDirectory `
                    -WindowStyle Minimized `
                    -PassThru
                $message = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') restarted QMT client pid=$($started.Id) attempt=$attemptCount next_backoff=${retryDelaySeconds}s no_daily_limit"
                Add-Content -LiteralPath $eventLogPath -Value $message -Encoding UTF8
                Write-Output $message
            }
            catch {
                $message = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') failed to restart QMT client: $($_.Exception.Message)"
                Add-Content -LiteralPath $eventLogPath -Value $message -Encoding UTF8
                Write-Output $message
            }
        }
        Start-Sleep -Seconds 10
    }
}
finally {
    if ($createdNew) {
        $watchdogMutex.ReleaseMutex()
    }
    $watchdogMutex.Dispose()
}
