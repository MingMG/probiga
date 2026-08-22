$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$DataDir = Join-Path $Root "data"
if (!(Test-Path $DataDir)) {
    New-Item -ItemType Directory -Path $DataDir | Out-Null
}

# The supervisor is long-lived, so inherited environment values can be stale
# after an operational source switch.  Reload only service-control switches on
# every pass; Python jobs continue to load their complete settings from .env.
$serviceSwitchNames = @(
    "BIG_QMT_BRIDGE_ENABLED",
    "BIG_QMT_STRATEGY_AUTO_RECOVER",
    "BIG_QMT_RECOVERY_MIN_BACKOFF_SECONDS",
    "BIG_QMT_RECOVERY_MAX_BACKOFF_SECONDS",
    "BIG_QMT_CONSUMER_STARTUP_GRACE_SECONDS",
    "BIG_QMT_CONSUMER_FAILURE_GRACE_SECONDS",
    "BIG_QMT_CONSUMER_FAILURE_CHECKS",
    "BIG_QMT_CONSUMER_MAX_SAMPLE_GAP_SECONDS",
    "LEGACY_MINIQMT_ENABLED",
    "QMT_CLIENT_AUTO_RESTART",
    "QMT_CLIENT_MIN_BACKOFF_SECONDS",
    "QMT_CLIENT_MAX_BACKOFF_SECONDS",
    "QMT_ALERT_WEBHOOK_URL",
    "WECOM_WEBHOOK_URL",
    "LIVE_QUOTE_POLL_ENABLED"
)
$envFile = Join-Path $Root ".env"
if (Test-Path -LiteralPath $envFile) {
    foreach ($line in Get-Content -LiteralPath $envFile) {
        $trimmed = ([string]$line).Trim()
        if (!$trimmed -or $trimmed.StartsWith("#") -or !$trimmed.Contains("=")) {
            continue
        }
        $parts = $trimmed.Split("=", 2)
        $name = $parts[0].Trim()
        if ($name -in $serviceSwitchNames) {
            [Environment]::SetEnvironmentVariable($name, $parts[1].Trim(), "Process")
        }
    }
}

function Test-QmtClientAutoRestart {
    return ([string]$env:QMT_CLIENT_AUTO_RESTART).Trim().ToLowerInvariant() -in @(
        "1", "true", "yes", "on"
    )
}

function Get-QmtRetryDelaySeconds {
    param(
        [int]$ConsecutiveFailures,
        [int]$MinimumSeconds = 30,
        [int]$MaximumSeconds = 900
    )
    $power = [Math]::Min(10, [Math]::Max(0, $ConsecutiveFailures - 1))
    return [int][Math]::Min(
        $MaximumSeconds,
        $MinimumSeconds * [Math]::Pow(2, $power)
    )
}

function Write-QmtAlert {
    param(
        [string]$Component,
        [string]$Status,
        [string]$Message
    )
    $payload = [ordered]@{
        timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        component = $Component
        status = $Status
        message = $Message
    }
    $payload |
        ConvertTo-Json -Compress |
        Add-Content -LiteralPath (
            Join-Path $DataDir "qmt_health_alerts.jsonl"
        ) -Encoding UTF8
    $webhook = if ($env:QMT_ALERT_WEBHOOK_URL) {
        $env:QMT_ALERT_WEBHOOK_URL
    }
    else {
        $env:WECOM_WEBHOOK_URL
    }
    if ($webhook) {
        try {
            $body = @{
                msgtype = "markdown"
                markdown = @{
                    content = (
                        "### ProBigA QMT $Status`n" +
                        "> component: $Component`n" +
                        "> time: $($payload.timestamp)`n" +
                        "> $Message"
                    )
                }
            } | ConvertTo-Json -Depth 5
            Invoke-RestMethod `
                -Uri $webhook `
                -Method Post `
                -ContentType "application/json" `
                -Body $body `
                -TimeoutSec 10 | Out-Null
        }
        catch {
            Write-Warning "QMT alert delivery failed: $($_.Exception.Message)"
        }
    }
}

function Test-QmtAutoStartWindow {
    $now = Get-Date
    if ($now.DayOfWeek -in @(
        [System.DayOfWeek]::Saturday,
        [System.DayOfWeek]::Sunday
    )) {
        return $false
    }
    $start = [TimeSpan]::FromHours(6.5)
    $end = [TimeSpan]::FromHours(23)
    return $now.TimeOfDay -ge $start -and $now.TimeOfDay -le $end
}

function Test-LegacyMiniQmtEnabled {
    return ([string]$env:LEGACY_MINIQMT_ENABLED).Trim().ToLowerInvariant() -in @(
        "1", "true", "yes", "on"
    )
}

function Test-LiveQuoteRuntimeEnabled {
    return ([string]$env:LIVE_QUOTE_POLL_ENABLED).Trim().ToLowerInvariant() -in @(
        "1", "true", "yes", "on"
    )
}

function Test-BigQmtBridgeEnabled {
    return ([string]$env:BIG_QMT_BRIDGE_ENABLED).Trim().ToLowerInvariant() -in @(
        "1", "true", "yes", "on"
    )
}

function Test-BigQmtStrategyAutoRecover {
    $configured = ([string]$env:BIG_QMT_STRATEGY_AUTO_RECOVER).Trim()
    if (!$configured) {
        # Strategy GUI automation is deliberately independent from terminal
        # recovery.  Clicking through QMT windows can change MainWindowTitle
        # and must never be enabled implicitly by QMT_CLIENT_AUTO_RESTART.
        return $false
    }
    return $configured.ToLowerInvariant() -in @("1", "true", "yes", "on")
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

    $candidates = @(
        "D:\QMT\bin.x64\XtMiniQmt.exe",
        "C:\QMT\bin.x64\XtMiniQmt.exe"
    )
    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }
    $running = Get-CimInstance Win32_Process | Where-Object {
        $_.Name -eq "XtMiniQmt.exe" -and $_.ExecutablePath
    } | Select-Object -First 1
    if ($running -and $running.ExecutablePath -and (Test-Path -LiteralPath $running.ExecutablePath)) {
        return $running.ExecutablePath
    }
    return $null
}

function Resolve-PythonPath {
    if ($env:PROBIGA_PYTHON_EXE -and (Test-Path -LiteralPath $env:PROBIGA_PYTHON_EXE)) {
        return (Resolve-Path -LiteralPath $env:PROBIGA_PYTHON_EXE).Path
    }
    $venvPython = Join-Path $Root ".venv\Scripts\python.exe"
    if (Test-Path -LiteralPath $venvPython) {
        return (Resolve-Path -LiteralPath $venvPython).Path
    }
    $cmd = Get-Command python -ErrorAction SilentlyContinue
    if ($cmd -and $cmd.Source) {
        return $cmd.Source
    }
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py -and $py.Source) {
        return $py.Source
    }
    $candidates = @(
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python313\python.exe"),
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python312\python.exe"),
        (Join-Path $env:LOCALAPPDATA "Programs\Python\Python311\python.exe")
    )
    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate)) {
            return (Resolve-Path -LiteralPath $candidate).Path
        }
    }
    throw "python.exe not found"
}

function Get-QmtProcesses {
    # XtMiniQmt.exe is the launcher in current QMT builds; the logged-in
    # terminal continues as XtItClient.exe after the launcher exits.
    @(Get-Process -Name "XtMiniQmt", "XtItClient" -ErrorAction SilentlyContinue)
}

function Test-QmtClientLoggedIn {
    param($Proc)
    if (!$Proc -or $Proc.ProcessName -ne "XtItClient") {
        return $false
    }
    # Guojin QMT prefixes the main-window title with the logged-in account.
    # A bare "Guojin QMT Trading Terminal" title is only the login shell and
    # must not suppress guarded recovery on the following trading morning.
    return (
        $Proc.MainWindowHandle -ne [IntPtr]::Zero -and
        ([string]$Proc.MainWindowTitle) -match "^\s*\d+\s*-\s*.+QMT"
    )
}

function Test-PythonLauncherProcess {
    param($Proc)
    $exe = [string]$Proc.ExecutablePath
    $venvLauncher = Join-Path $Root ".venv\Scripts\python.exe"
    $qmtLauncher = Join-Path $Root "runtime\qmt-py313\Scripts\python.exe"
    return ($exe -ieq $venvLauncher) -or ($exe -ieq $qmtLauncher)
}

function Get-ServiceKeyFromScriptName {
    param([string]$ScriptName)
    switch -Wildcard ($ScriptName) {
        "*run_big_qmt_bridge.py*" { return "big_qmt_bridge" }
        "*run_guojin_qmt_gateway.py*" { return "qmt_gateway" }
        "*run_qmt_live_runtime.py*" { return "qmt_live" }
        "*run_remote_qmt_tunnel.py*" { return "qmt_tunnel" }
        "*run_remote_mysql_tunnel.py*" { return "mysql_tunnel" }
        default { throw "Unknown managed service script: $ScriptName" }
    }
}

function Get-ManagedPidPath {
    param([string]$ServiceKey)
    return (Join-Path $DataDir "$ServiceKey.pid")
}

function Get-ManagedProcess {
    param([string]$ServiceKey)
    $pidPath = Get-ManagedPidPath $ServiceKey
    if (!(Test-Path -LiteralPath $pidPath)) {
        return $null
    }
    try {
        $parts = ([string](Get-Content -LiteralPath $pidPath -Raw)).Trim().Split("|", 3)
        $processId = [int]$parts[0]
        $expectedStart = if ($parts.Count -ge 2) { [long]$parts[1] } else { 0 }
        $proc = Get-Process -Id $processId -ErrorAction Stop
        $actualStart = $proc.StartTime.ToUniversalTime().ToFileTimeUtc()
        if ($proc.ProcessName -notlike "python*" -or ($expectedStart -gt 0 -and $actualStart -ne $expectedStart)) {
            throw "stale PID record"
        }
        return $proc
    }
    catch {
        Remove-Item -LiteralPath $pidPath -Force -ErrorAction SilentlyContinue
        return $null
    }
}

function Set-ManagedProcess {
    param([string]$ServiceKey, [string]$ScriptName, $Proc)
    $pidPath = Get-ManagedPidPath $ServiceKey
    $started = $Proc.StartTime.ToUniversalTime().ToFileTimeUtc()
    Set-Content -LiteralPath $pidPath -Value "$($Proc.Id)|$started|$ScriptName" -Encoding Ascii
}

function Stop-ManagedProcess {
    param([string]$ServiceKey)
    $proc = Get-ManagedProcess $ServiceKey
    if ($proc) {
        if ($ServiceKey -eq "big_qmt_bridge") {
            # The bridge can own a delegated ETF subprocess.  Killing only the
            # venv launcher leaves that child orphaned and its scheduler lease
            # stuck at running, so terminate this verified process tree.
            & taskkill.exe /PID $proc.Id /T /F 2>$null | Out-Null
            if (Get-Process -Id $proc.Id -ErrorAction SilentlyContinue) {
                Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            }
        }
        else {
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        }
    }
    Remove-Item -LiteralPath (Get-ManagedPidPath $ServiceKey) -Force -ErrorAction SilentlyContinue
}

$script:ServiceProcessInventoryLoaded = $false
$script:ServiceProcessInventory = @()
$script:ServiceProcessInventoryError = $null

function Reset-ServiceProcessInventory {
    $script:ServiceProcessInventoryLoaded = $false
    $script:ServiceProcessInventory = @()
    $script:ServiceProcessInventoryError = $null
}

function Get-ServiceProcesses {
    if (!$script:ServiceProcessInventoryLoaded) {
        $script:ServiceProcessInventoryLoaded = $true
        try {
            # One constrained WMI snapshot per supervisor pass is enough.
            # Repeated full Win32_Process scans caused provider memory/RPC
            # failures on the always-on workstation.
            $script:ServiceProcessInventory = @(
                Get-CimInstance Win32_Process -Filter "Name = 'python.exe'"
            )
        }
        catch {
            $script:ServiceProcessInventoryError = $_.Exception
        }
    }
    if ($script:ServiceProcessInventoryError) {
        throw $script:ServiceProcessInventoryError
    }
    $script:ServiceProcessInventory | Where-Object {
        $_.Name -eq "python.exe" -and (
            (
                $_.CommandLine -like "*run_guojin_qmt_gateway.py*" -and
                $_.ExecutablePath -like "*runtime\qmt-py313\Scripts\python.exe"
            ) -or
            $_.CommandLine -like "*run_big_qmt_bridge.py*" -or
            $_.CommandLine -like "*run_qmt_live_runtime.py*" -or
            $_.CommandLine -like "*run_remote_qmt_tunnel.py*" -or
            $_.CommandLine -like "*run_remote_mysql_tunnel.py*"
        ) -and -not (Test-PythonLauncherProcess $_)
    }
}

function Get-ServiceKey {
    param($Proc)
    if ($Proc.CommandLine -like "*run_big_qmt_bridge.py*") {
        return "big_qmt_bridge"
    }
    if ($Proc.CommandLine -like "*run_guojin_qmt_gateway.py*") {
        return "qmt_gateway"
    }
    if ($Proc.CommandLine -like "*run_qmt_live_runtime.py*") {
        return "qmt_live"
    }
    if ($Proc.CommandLine -like "*run_remote_qmt_tunnel.py*") {
        return "qmt_tunnel"
    }
    if ($Proc.CommandLine -like "*run_remote_mysql_tunnel.py*") {
        return "mysql_tunnel"
    }
    return "unknown"
}

function Stop-DuplicateProcesses {
    # Managed services use PID records with process start-time validation.
    # This avoids the unstable Win32_Process provider on the always-on host.
    return
}

function Stop-LegacyQmtServices {
    Stop-ManagedProcess "qmt_gateway"
    Stop-ManagedProcess "qmt_tunnel"
}

function Stop-PublicQuoteService {
    Stop-ManagedProcess "qmt_live"
}

function Ensure-Process {
    param(
        [string]$PythonExe,
        [string]$ScriptName,
        [string]$ArgLine,
        [string]$StdOutPath,
        [string]$StdErrPath
    )
    $serviceKey = Get-ServiceKeyFromScriptName $ScriptName
    if (Get-ManagedProcess $serviceKey) {
        return
    }
    $proc = Start-Process -FilePath $PythonExe `
        -ArgumentList $ArgLine `
        -WorkingDirectory $Root `
        -WindowStyle Hidden `
        -RedirectStandardOutput $StdOutPath `
        -RedirectStandardError $StdErrPath `
        -PassThru
    Set-ManagedProcess $serviceKey $ScriptName $proc
    Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') started $serviceKey pid=$($proc.Id)"
}

function Ensure-QmtClient {
    $running = @(Get-QmtProcesses)
    $loggedIn = $running |
        Where-Object { Test-QmtClientLoggedIn $_ } |
        Select-Object -First 1
    $statePath = Join-Path $DataDir "qmt_client_autostart.state.json"
    if ($loggedIn) {
        $previousState = $null
        if (Test-Path -LiteralPath $statePath) {
            try {
                $previousState = Get-Content `
                    -LiteralPath $statePath `
                    -Raw |
                    ConvertFrom-Json
            }
            catch {
                $previousState = $null
            }
        }
        if (
            $previousState -and
            [string]$previousState.status -ne "healthy"
        ) {
            Write-QmtAlert `
                "qmt_client_login" `
                "RECOVERED" `
                "Guojin QMT login recovered; retry counter reset."
        }
        @{
            status = "healthy"
            consecutive_failures = 0
            attempts = 0
            recovered_at = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
            client_pid = [int]$loggedIn.Id
        } |
            ConvertTo-Json |
            Set-Content -LiteralPath $statePath -Encoding UTF8
        return
    }
    if (!(Test-QmtAutoStartWindow)) {
        return
    }
    $state = $null
    if (Test-Path -LiteralPath $statePath) {
        try {
            $state = Get-Content -LiteralPath $statePath -Raw | ConvertFrom-Json
        }
        catch {
            $state = $null
        }
    }
    $failures = if ($state) {
        [int]$state.consecutive_failures
    }
    else {
        0
    }
    $nextAttempt = [DateTime]::MinValue
    if ($state) {
        [DateTime]::TryParse(
            [string]$state.next_attempt_at,
            [ref]$nextAttempt
        ) | Out-Null
    }
    $minimumBackoff = 30
    $maximumBackoff = 900
    try {
        if ($env:QMT_CLIENT_MIN_BACKOFF_SECONDS) {
            $minimumBackoff = [Math]::Max(
                15,
                [int]$env:QMT_CLIENT_MIN_BACKOFF_SECONDS
            )
        }
        if ($env:QMT_CLIENT_MAX_BACKOFF_SECONDS) {
            $maximumBackoff = [Math]::Max(
                $minimumBackoff,
                [int]$env:QMT_CLIENT_MAX_BACKOFF_SECONDS
            )
        }
    }
    catch {
        Write-Warning "Invalid QMT retry backoff; using 30s..900s."
        $minimumBackoff = 30
        $maximumBackoff = 900
    }
    if (
        $running.Count -eq 0 -and
        $state -and
        [string]$state.status -eq "login_unverified"
    ) {
        # Title diagnostics never attempted a restart, so they must not inflate
        # the process-missing restart backoff if QMT later exits for real.
        $failures = 0
        $nextAttempt = [DateTime]::MinValue
    }
    if (
        (Get-Date) -lt $nextAttempt -and
        !($running.Count -eq 0 -and [string]$state.status -eq "login_unverified")
    ) {
        return
    }
    if ($running.Count -gt 0) {
        # MainWindowTitle is only a diagnostic signal.  QMT can keep receiving
        # quotes and trade pushes while a dialog/editor owns the main window,
        # so a title mismatch is never sufficient authority to kill it.  Keep
        # the live process untouched and retry the diagnostic with backoff.
        $failures += 1
        $delaySeconds = Get-QmtRetryDelaySeconds `
            $failures `
            $minimumBackoff `
            $maximumBackoff
        $nextAttempt = (Get-Date).AddSeconds($delaySeconds)
        $processIds = @($running | ForEach-Object { [int]$_.Id })
        @{
            status = "login_unverified"
            consecutive_failures = $failures
            attempts = if ($state) { [int]$state.attempts } else { 0 }
            checked_at = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
            next_attempt_at = $nextAttempt.ToString("yyyy-MM-dd HH:mm:ss")
            retry_delay_seconds = $delaySeconds
            client_pids = $processIds
        } | ConvertTo-Json | Set-Content -LiteralPath $statePath -Encoding UTF8
        if (!$state -or [string]$state.status -ne "login_unverified") {
            Write-QmtAlert `
                "qmt_client_login" `
                "DEGRADED" `
                (
                    "QMT process is running, but login could not be verified from " +
                    "the window title. Leaving the client untouched; checking again " +
                    "in $delaySeconds seconds."
                )
        }
        return
    }
    $clientPath = Resolve-QmtClientPath
    if (!$clientPath) {
        throw "QMT client executable not found"
    }
    $failures += 1
    $delaySeconds = Get-QmtRetryDelaySeconds `
        $failures `
        $minimumBackoff `
        $maximumBackoff
    $nextAttempt = (Get-Date).AddSeconds($delaySeconds)
    @{
        status = "retrying"
        consecutive_failures = $failures
        attempts = $failures
        last_attempt = (Get-Date -Format "yyyy-MM-dd HH:mm:ss")
        next_attempt_at = $nextAttempt.ToString(
            "yyyy-MM-dd HH:mm:ss"
        )
        retry_delay_seconds = $delaySeconds
        executable = [System.IO.Path]::GetFileName($clientPath)
    } | ConvertTo-Json | Set-Content -LiteralPath $statePath -Encoding UTF8
    Write-QmtAlert `
        "qmt_client_login" `
        "RETRYING" `
        (
            "QMT is not logged in. Persistent retry $failures started; " +
            "next retry in $delaySeconds seconds with no daily attempt limit."
        )
    $workingDir = Split-Path -Parent $clientPath
    Start-Process -FilePath $clientPath `
        -WorkingDirectory $workingDir `
        -WindowStyle Minimized
    Start-Sleep -Seconds 15
}

$python = Resolve-PythonPath
$qmtPython = Join-Path $Root "runtime\qmt-py313\Scripts\python.exe"

# QMT terminal recovery is opt-in, trading-session bounded and rate limited.
if (
    (Test-QmtClientAutoRestart) -and
    ((Test-BigQmtBridgeEnabled) -or (Test-LegacyMiniQmtEnabled))
) {
    try {
        Ensure-QmtClient
    }
    catch {
        Write-Warning "Unable to recover Guojin QMT client: $($_.Exception.Message)"
    }
}

Stop-DuplicateProcesses
if (Test-BigQmtBridgeEnabled) {
    Stop-PublicQuoteService
    $existingBridge = Get-ManagedProcess "big_qmt_bridge"
    if ($existingBridge) {
        $bridgeScriptPath = Join-Path $Root "tools\run_big_qmt_bridge.py"
        if (
            (Test-Path -LiteralPath $bridgeScriptPath) -and
            (Get-Item -LiteralPath $bridgeScriptPath).LastWriteTimeUtc -gt
                $existingBridge.StartTime.ToUniversalTime()
        ) {
            Stop-ManagedProcess "big_qmt_bridge"
            Write-QmtAlert `
                "qmt_snapshot_consumer" `
                "RESTARTING" `
                "Bridge source changed after process start; loading the new collector."
            $existingBridge = $null
        }
    }
    if ($existingBridge) {
        # A cold consumer must first load the watchlist and persist a full
        # market snapshot before it can publish its first sync receipt.  The
        # supervisor runs every five seconds; checking the receipt immediately
        # used to kill that healthy startup repeatedly, so it never became
        # ready.  Give the process a bounded, configurable startup window.
        $consumerStartupGraceSeconds = 300
        $consumerFailureGraceSeconds = 180
        $consumerFailureChecks = 3
        $consumerMaxSampleGapSeconds = 45
        $consumerFailureStatePath = Join-Path `
            $DataDir `
            "big_qmt_consumer_health.state.json"
        try {
            if ($env:BIG_QMT_CONSUMER_STARTUP_GRACE_SECONDS) {
                $consumerStartupGraceSeconds = [Math]::Max(
                    30,
                    [int]$env:BIG_QMT_CONSUMER_STARTUP_GRACE_SECONDS
                )
            }
            if ($env:BIG_QMT_CONSUMER_FAILURE_GRACE_SECONDS) {
                $consumerFailureGraceSeconds = [Math]::Max(
                    60,
                    [int]$env:BIG_QMT_CONSUMER_FAILURE_GRACE_SECONDS
                )
            }
            if ($env:BIG_QMT_CONSUMER_FAILURE_CHECKS) {
                $consumerFailureChecks = [Math]::Max(
                    2,
                    [int]$env:BIG_QMT_CONSUMER_FAILURE_CHECKS
                )
            }
            if ($env:BIG_QMT_CONSUMER_MAX_SAMPLE_GAP_SECONDS) {
                $consumerMaxSampleGapSeconds = [Math]::Max(
                    15,
                    [int]$env:BIG_QMT_CONSUMER_MAX_SAMPLE_GAP_SECONDS
                )
            }
        }
        catch {
            Write-Warning "Invalid Big QMT consumer recovery threshold; using defaults."
            $consumerStartupGraceSeconds = 300
            $consumerFailureGraceSeconds = 180
            $consumerFailureChecks = 3
            $consumerMaxSampleGapSeconds = 45
        }
        $consumerAgeSeconds = ((Get-Date) - $existingBridge.StartTime).TotalSeconds
        if ($consumerAgeSeconds -ge $consumerStartupGraceSeconds) {
            try {
                $healthJson = & $python `
                    (Join-Path $Root "tools\check_big_qmt_end_to_end_health.py") `
                    --json 2>$null
                $health = ([string]$healthJson) | ConvertFrom-Json
                if (
                    !$health.healthy -and
                    $health.checks.strategy_heartbeat -and
                    $health.checks.full_market_snapshot -and
                    !$health.checks.sync_receipt
                ) {
                    $failureState = $null
                    if (Test-Path -LiteralPath $consumerFailureStatePath) {
                        try {
                            $failureState = Get-Content `
                                -LiteralPath $consumerFailureStatePath `
                                -Raw |
                                ConvertFrom-Json
                        }
                        catch {
                            $failureState = $null
                        }
                    }
                    $now = Get-Date
                    $firstFailureAt = $now
                    $failureCount = 1
                    $consumerStartedAt = $existingBridge.StartTime.ToUniversalTime().ToString("o")
                    if (
                        $failureState -and
                        [int]$failureState.consumer_pid -eq [int]$existingBridge.Id -and
                        [string]$failureState.consumer_started_at -eq $consumerStartedAt
                    ) {
                        $parsedFirstFailureAt = [DateTime]::MinValue
                        $parsedLastFailureAt = [DateTime]::MinValue
                        if (
                            [DateTime]::TryParse(
                            [string]$failureState.first_failure_at,
                            [ref]$parsedFirstFailureAt
                            ) -and
                            [DateTime]::TryParse(
                                [string]$failureState.last_failure_at,
                                [ref]$parsedLastFailureAt
                            ) -and
                            ($now - $parsedLastFailureAt).TotalSeconds -le
                                $consumerMaxSampleGapSeconds
                        ) {
                            $firstFailureAt = $parsedFirstFailureAt
                            $failureCount = [int]$failureState.consecutive_failures + 1
                        }
                    }
                    $failureAgeSeconds = ($now - $firstFailureAt).TotalSeconds
                    @{
                        status = "degraded"
                        consumer_pid = [int]$existingBridge.Id
                        consumer_started_at = $consumerStartedAt
                        consecutive_failures = $failureCount
                        first_failure_at = $firstFailureAt.ToString(
                            "yyyy-MM-dd HH:mm:ss"
                        )
                        last_failure_at = $now.ToString("yyyy-MM-dd HH:mm:ss")
                        failure_age_seconds = [int]$failureAgeSeconds
                    } |
                        ConvertTo-Json |
                        Set-Content `
                            -LiteralPath $consumerFailureStatePath `
                            -Encoding UTF8
                    if ($failureCount -eq 1) {
                        Write-QmtAlert `
                            "qmt_snapshot_consumer" `
                            "DEGRADED" `
                            (
                                "Sync receipt is stale or mismatched; waiting for " +
                                "persistent failure before restarting the consumer."
                            )
                    }
                    if (
                        $failureCount -ge $consumerFailureChecks -and
                        $failureAgeSeconds -ge $consumerFailureGraceSeconds
                    ) {
                        Stop-ManagedProcess "big_qmt_bridge"
                        Remove-Item `
                            -LiteralPath $consumerFailureStatePath `
                            -Force `
                            -ErrorAction SilentlyContinue
                        Write-QmtAlert `
                            "qmt_snapshot_consumer" `
                            "RESTARTING" `
                            (
                                "Sync receipt stayed unhealthy for " +
                                "$([int]$failureAgeSeconds)s across $failureCount " +
                                "checks. Restarting consumer."
                            )
                    }
                }
                else {
                    if (
                        (Test-Path -LiteralPath $consumerFailureStatePath) -and
                        $health.checks.sync_receipt
                    ) {
                        Write-QmtAlert `
                            "qmt_snapshot_consumer" `
                            "RECOVERED" `
                            "Sync receipt recovered; consumer restart guard reset."
                    }
                    Remove-Item `
                        -LiteralPath $consumerFailureStatePath `
                        -Force `
                        -ErrorAction SilentlyContinue
                }
            }
            catch {
                # Unknown health must break the consecutive-failure series;
                # isolated failures separated by probe errors are not continuous.
                Remove-Item `
                    -LiteralPath $consumerFailureStatePath `
                    -Force `
                    -ErrorAction SilentlyContinue
                Write-Warning "Unable to evaluate Big QMT end-to-end health: $($_.Exception.Message)"
            }
        }
    }
    Ensure-Process `
        -PythonExe $python `
        -ScriptName "run_big_qmt_bridge.py" `
        -ArgLine "tools/run_big_qmt_bridge.py" `
        -StdOutPath (Join-Path $DataDir "big_qmt_bridge.out.log") `
        -StdErrPath (Join-Path $DataDir "big_qmt_bridge.err.log")

    if (Test-BigQmtStrategyAutoRecover) {
        $strategyRecoveryScript = Join-Path $Root "tools\ensure_big_qmt_strategy_running.ps1"
        $strategyMinBackoffSeconds = 30
        $strategyMaxBackoffSeconds = 900
        try {
            if ($env:BIG_QMT_RECOVERY_MIN_BACKOFF_SECONDS) {
                $strategyMinBackoffSeconds = [Math]::Max(
                    15,
                    [int]$env:BIG_QMT_RECOVERY_MIN_BACKOFF_SECONDS
                )
            }
            if ($env:BIG_QMT_RECOVERY_MAX_BACKOFF_SECONDS) {
                $strategyMaxBackoffSeconds = [Math]::Max(
                    $strategyMinBackoffSeconds,
                    [int]$env:BIG_QMT_RECOVERY_MAX_BACKOFF_SECONDS
                )
            }
        }
        catch {
            Write-Warning "Invalid Big QMT strategy backoff; using 30s..900s."
            $strategyMinBackoffSeconds = 30
            $strategyMaxBackoffSeconds = 900
        }
        try {
            $quotedRecoveryScript = '"' + $strategyRecoveryScript + '"'
            $recoveryProcess = Start-Process `
                -FilePath "powershell.exe" `
                -ArgumentList @(
                    "-NoProfile",
                    "-ExecutionPolicy", "Bypass",
                    "-File", $quotedRecoveryScript,
                    "-MinimumBackoffSeconds", $strategyMinBackoffSeconds,
                    "-MaximumBackoffSeconds", $strategyMaxBackoffSeconds
                ) `
                -WorkingDirectory $Root `
                -WindowStyle Hidden `
                -PassThru
            if (!$recoveryProcess.WaitForExit(150000)) {
                Stop-Process `
                    -Id $recoveryProcess.Id `
                    -Force `
                    -ErrorAction SilentlyContinue
                Write-Warning (
                    "Big QMT strategy recovery exceeded 150 seconds and " +
                    "was terminated so the supervisor can continue."
                )
            }
        }
        catch {
            Write-Warning "Unable to recover Big QMT strategy: $($_.Exception.Message)"
        }
    }
}

# Public quote polling is a fallback.  Never let it overwrite a healthy Big
# QMT primary snapshot when both switches were accidentally enabled.
if ((Test-LiveQuoteRuntimeEnabled) -and !(Test-BigQmtBridgeEnabled)) {
    Ensure-Process `
        -PythonExe $python `
        -ScriptName "run_qmt_live_runtime.py" `
        -ArgLine "tools/run_qmt_live_runtime.py" `
        -StdOutPath (Join-Path $DataDir "live_quote_runtime.out.log") `
        -StdErrPath (Join-Path $DataDir "live_quote_runtime.err.log")
}

if (!(Test-LegacyMiniQmtEnabled)) {
    Stop-LegacyQmtServices
} elseif (!(Test-Path -LiteralPath $qmtPython)) {
    Write-Warning "Skip legacy miniQMT services: runtime not found at $qmtPython"
} else {
    try {
        if (Test-QmtClientAutoRestart) {
            Ensure-QmtClient
        }

        Ensure-Process `
            -PythonExe $qmtPython `
            -ScriptName "run_guojin_qmt_gateway.py" `
            -ArgLine "tools/run_guojin_qmt_gateway.py" `
            -StdOutPath (Join-Path $DataDir "qmt_gateway.out.log") `
            -StdErrPath (Join-Path $DataDir "qmt_gateway.err.log")

        Start-Sleep -Seconds 2

        Ensure-Process `
            -PythonExe $python `
            -ScriptName "run_remote_qmt_tunnel.py" `
            -ArgLine "tools/run_remote_qmt_tunnel.py" `
            -StdOutPath (Join-Path $DataDir "qmt_tunnel.out.log") `
            -StdErrPath (Join-Path $DataDir "qmt_tunnel.err.log")
    } catch {
        Write-Warning "Skip legacy miniQMT services: $($_.Exception.Message)"
    }
}

$sshKeyFile = $env:PROBIGA_REMOTE_SSH_KEY_FILE
$sshHost = $env:PROBIGA_REMOTE_SSH_HOST
$sshUser = $env:PROBIGA_REMOTE_SSH_USER
$sshKnownHosts = $env:PROBIGA_SSH_KNOWN_HOSTS
if ($sshKeyFile -and $sshHost -and $sshUser -and $sshKnownHosts) {
    Ensure-Process `
        -PythonExe $python `
        -ScriptName "run_remote_mysql_tunnel.py" `
        -ArgLine "tools/run_remote_mysql_tunnel.py --remote-bind-port 13306 --local-port 3306" `
        -StdOutPath (Join-Path $DataDir "mysql_tunnel.out.log") `
        -StdErrPath (Join-Path $DataDir "mysql_tunnel.err.log")
} else {
    Write-Warning "Skip remote MySQL tunnel: configure SSH host, user, key file, and known-hosts file."
}

Stop-DuplicateProcesses
