param(
    [ValidateSet('all', 'big_qmt_bridge')][string]$ServiceScope = 'all',
    [ValidateSet('Ensure', 'Restart', 'RetireDuplicate')][string]$ConsumerAction = 'Ensure',
    [string]$RegisteredRoot = '',
    [string]$ExpectedBuildSha = '',
    [int]$ExpectedProcessId = 0,
    [long]$ExpectedStartFileTimeUtc = 0,
    [string]$ExpectedProcessBuildSha = '',
    [string]$DuplicateRoot = ''
)

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
    # Recheck the persisted desired state after waiting for consumer control.
    # A retiring checkout must not be resurrected by an already-started pass.
    $configured = [string]$env:BIG_QMT_BRIDGE_ENABLED
    $switchPath = Join-Path $Root '.env'
    if (Test-Path -LiteralPath $switchPath) {
        foreach ($line in [IO.File]::ReadAllLines($switchPath)) {
            if ($line -match '^[ \t]*BIG_QMT_BRIDGE_ENABLED[ \t]*=(.*)$') {
                $configured = $Matches[1].Trim()
            }
        }
    }
    return $configured.Trim().ToLowerInvariant() -in @(
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
        "*run_production_mysql_forward.py*" { return "mysql_forward" }
        default { throw "Unknown managed service script: $ScriptName" }
    }
}

function Get-ManagedPidPath {
    param([string]$ServiceKey)
    return (Join-Path $DataDir "$ServiceKey.pid")
}

function Enter-BigQmtConsumerControl {
    $bytes = [Text.Encoding]::UTF8.GetBytes([IO.Path]::GetFullPath($Root).TrimEnd('\', '/').ToLowerInvariant())
    $hash = [Security.Cryptography.SHA256]::Create()
    try { $identity = ([BitConverter]::ToString($hash.ComputeHash($bytes))).Replace('-', '') }
    finally { $hash.Dispose() }
    $mutex = [Threading.Mutex]::new($false, "Global\ProBigA.BigQmtConsumer.$identity")
    $owned = $false
    try {
        try { $owned = $mutex.WaitOne(10000) }
        catch [Threading.AbandonedMutexException] { $owned = $true }
        if (!$owned) { throw 'BigQMT consumer control is busy; no action taken' }
        return $mutex
    }
    catch { $mutex.Dispose(); throw }
}

function Exit-BigQmtConsumerControl {
    param($Mutex)
    try { $Mutex.ReleaseMutex() }
    finally { $Mutex.Dispose() }
}

function Assert-BigQmtConsumerIdentity {
    param([int]$ExpectedProcessId, [long]$ExpectedStartFileTimeUtc, [string]$ExpectedBuildSha)
    if ($ExpectedProcessId -le 0 -or $ExpectedStartFileTimeUtc -le 0 -or
        $ExpectedBuildSha -cnotmatch '^[0-9a-f]{40}$') {
        throw 'BigQMT consumer action requires exact PID, birth and recorded build'
    }
    $parts = ([IO.File]::ReadAllText((Get-ManagedPidPath 'big_qmt_bridge'))).Trim().Split('|', 4)
    if ($parts.Count -ne 4 -or $parts[0] -cne [string]$ExpectedProcessId -or
        $parts[1] -cne [string]$ExpectedStartFileTimeUtc -or $parts[2] -cne 'run_big_qmt_bridge.py' -or
        $parts[3] -cne $ExpectedBuildSha) {
        throw 'BigQMT consumer managed identity changed; no action taken'
    }
    $actual = Get-Process -Id $ExpectedProcessId -ErrorAction Stop
    if ($actual.ProcessName -notlike 'python*' -or
        $actual.StartTime.ToUniversalTime().ToFileTimeUtc() -ne $ExpectedStartFileTimeUtc) {
        throw 'BigQMT consumer PID was reused; no action taken'
    }
    return $actual
}

function New-PrivateConsumerEnvFile {
    param([string]$Path, [Security.AccessControl.FileSecurity]$Security)
    # Same CreateNew + explicit ACL primitive as secondary_edge/export_cold_package.
    return [IO.FileStream]::new($Path, [IO.FileMode]::CreateNew,
        [Security.AccessControl.FileSystemRights]::WriteData, [IO.FileShare]::None,
        4096, [IO.FileOptions]::WriteThrough, $Security)
}

function Disable-BigQmtConsumer {
    # A permanent desired-state change for Stop/Retire, never a release hold.
    $path = Join-Path $Root '.env'
    $bytes = [IO.File]::ReadAllBytes($path)
    $bom = $bytes.Length -ge 3 -and $bytes[0] -eq 239 -and $bytes[1] -eq 187 -and $bytes[2] -eq 191
    $encoding = [Text.UTF8Encoding]::new($bom, $true)
    $original = [IO.File]::ReadAllText($path, $encoding)
    $pattern = '(?m)^[ \t]*BIG_QMT_BRIDGE_ENABLED[ \t]*=[^\r\n]*'
    $matches = [regex]::Matches($original, $pattern)
    if ($matches.Count -gt 1) { throw 'Duplicate BIG_QMT_BRIDGE_ENABLED declarations; no state changed' }
    $replacement = if ($matches.Count -eq 1) {
        [regex]::Replace($original, $pattern, 'BIG_QMT_BRIDGE_ENABLED=false')
    } else {
        $original + $(if ($original.EndsWith("`n")) { '' } else { "`r`n" }) + "BIG_QMT_BRIDGE_ENABLED=false`r`n"
    }
    if ($replacement -ceq $original) { return }
    $temporary = $path + '.consumer-' + [Guid]::NewGuid().ToString('N')
    try {
        $security = [IO.File]::GetAccessControl($path)
        $stream = New-PrivateConsumerEnvFile $temporary $security
        try {
            $payload = [byte[]]($encoding.GetPreamble() + $encoding.GetBytes($replacement))
            $stream.Write($payload, 0, $payload.Length)
            $stream.Flush($true)
        }
        finally { $stream.Dispose() }
        [IO.File]::Replace($temporary, $path, [NullString]::Value)
    }
    finally { if (Test-Path -LiteralPath $temporary) { Remove-Item -LiteralPath $temporary -Force } }
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
    param([string]$ServiceKey, [string]$ScriptName, $Proc, [string]$BuildSha = "")
    $pidPath = Get-ManagedPidPath $ServiceKey
    $started = $Proc.StartTime.ToUniversalTime().ToFileTimeUtc()
    Set-Content -LiteralPath $pidPath -Value "$($Proc.Id)|$started|$ScriptName|$BuildSha" -Encoding Ascii
}

function Get-ConsumerCheckoutBuild {
    # Read the Windows checkout, not origin/main or the Linux release. A Linux
    # only publication deliberately retains this component and its process.
    $build = ((& git -C $Root rev-parse HEAD 2>$null) -join "").Trim().ToLowerInvariant()
    if ($LASTEXITCODE -ne 0 -or $build -notmatch "^[0-9a-f]{40}$" -or $build -eq ("0" * 40)) {
        throw "BigQMT consumer checkout build is unavailable"
    }
    return $build
}

function Get-BuildBoundConsumer {
    param([string]$BuildSha)
    if ($BuildSha -cnotmatch "^[0-9a-f]{40}$" -or $BuildSha -eq ("0" * 40)) {
        throw "BigQMT consumer checkout build is invalid"
    }
    $proc = Get-ManagedProcess "big_qmt_bridge"
    if (!$proc) { return $null }
    $parts = ([string](Get-Content -LiteralPath (Get-ManagedPidPath "big_qmt_bridge") -Raw)).Trim().Split("|", 4)
    if ($parts.Count -eq 4 -and $parts[3] -ceq $BuildSha) {
        return $proc
    }
    # Only the verified Python process tree is replaced. A native QMT model
    # with unchanged source can remain live through this app-build rollover.
    Stop-ManagedProcess "big_qmt_bridge"
    Write-Host "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') restarting BigQMT consumer for checkout $BuildSha"
    return $null
}

function Stop-ManagedProcess {
    param([string]$ServiceKey, [int]$ExpectedProcessId = 0,
        [long]$ExpectedStartFileTimeUtc = 0, [string]$ExpectedBuildSha = '')
    if ($ExpectedProcessId -gt 0) {
        if ($ServiceKey -ne 'big_qmt_bridge') { throw 'Exact consumer identity is not valid for another service' }
        $null = Assert-BigQmtConsumerIdentity $ExpectedProcessId $ExpectedStartFileTimeUtc $ExpectedBuildSha
    }
    $proc = Get-ManagedProcess $ServiceKey
    if ($proc) {
        if ($ServiceKey -eq "big_qmt_bridge") {
            # The bridge can own a delegated ETF subprocess.  Killing only the
            # venv launcher leaves that child orphaned and its scheduler lease
            # stuck at running, so terminate this verified process tree.
            $children = @()
            $tree = @($proc.Id)
            $inventory = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" -OperationTimeoutSec 5)
            do {
                $next = @($inventory | Where-Object { $_.ParentProcessId -in $tree -and $_.ProcessId -notin $tree })
                $children += $next
                $tree += @($next.ProcessId)
                if ($tree.Count -gt 64) { throw 'Unexpected consumer process tree; no stop attempted' }
            } while ($next.Count)
            $stopper = Start-Process -FilePath (Join-Path $env:SystemRoot 'System32\taskkill.exe') `
                -ArgumentList @('/PID', [string]$proc.Id, '/T', '/F') -WindowStyle Hidden -PassThru
            if (!$stopper.WaitForExit(10000)) {
                # This is our own taskkill helper, never the consumer/native process.
                # It must be terminal before releasing the consumer control mutex.
                if (!$stopper.HasExited) { $stopper.Kill() }
                if (!$stopper.WaitForExit(2000)) {
                    throw 'BigQMT stop helper did not terminate; inspect before another action'
                }
                throw 'BigQMT consumer tree stop deadline; inspect before another action'
            }
            if (Get-Process -Id $proc.Id -ErrorAction SilentlyContinue) {
                Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            }
            if (Get-Process -Id $proc.Id -ErrorAction SilentlyContinue) {
                throw 'BigQMT consumer tree did not stop; keep its managed identity'
            }
            foreach ($child in $children) {
                $remaining = Get-Process -Id $child.ProcessId -ErrorAction SilentlyContinue
                if ($remaining -and $remaining.StartTime.ToUniversalTime().ToFileTimeUtc() -eq
                    $child.CreationDate.ToUniversalTime().ToFileTimeUtc()) {
                    throw 'BigQMT maintenance child did not stop; keep its managed identity'
                }
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
            $_.CommandLine -like "*run_production_mysql_forward.py*" -or
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
    if ($Proc.CommandLine -like "*run_production_mysql_forward.py*") {
        return "mysql_forward"
    }
    if ($Proc.CommandLine -like "*run_remote_mysql_tunnel.py*") {
        return "superseded_mysql_reverse_tunnel"
    }
    return "unknown"
}

function Stop-SupersededMysqlReverseTunnels {
    # The authoritative MySQL instance now lives behind the production host's
    # loopback relay. The former Windows->Linux reverse tunnel would publish a
    # stopped local database and can also steal the sole remote bind, so no
    # such process may coexist with the production local forward.
    $legacy = @(Get-ServiceProcesses | Where-Object {
        $_.CommandLine -like "*run_remote_mysql_tunnel.py*"
    })
    foreach ($proc in $legacy) {
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction SilentlyContinue
    }
    Remove-Item -LiteralPath (Get-ManagedPidPath "mysql_tunnel") -Force -ErrorAction SilentlyContinue
    if ($legacy.Count -gt 0) {
        Reset-ServiceProcessInventory
        Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') stopped superseded MySQL reverse tunnel count=$($legacy.Count)"
    }
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
        [string]$StdErrPath,
        [string]$ExpectedBuildSha = ""
    )
    $serviceKey = Get-ServiceKeyFromScriptName $ScriptName
    $buildEnvironment = @{}
    if ($serviceKey -eq "big_qmt_bridge") {
        if ($ExpectedBuildSha -cnotmatch "^[0-9a-f]{40}$" -or $ExpectedBuildSha -eq ("0" * 40)) {
            throw "BigQMT consumer startup requires its checkout build"
        }
        $ArgLine += " --expected-build-sha $ExpectedBuildSha"
    }
    if (Get-ManagedProcess $serviceKey) {
        return
    }
    try {
        if ($serviceKey -eq "big_qmt_bridge") {
            # The long-lived supervisor may have inherited a prior release's
            # declarations. Bind the newly launched interpreter to the actual
            # Windows checkout; the child independently verifies HEAD and the
            # activated scheduler identity before publishing reference data.
            foreach ($name in @("PROBIGA_BUILD_COMMIT_SHA", "PROBIGA_SCHEDULER_BUILD_SHA", "PROBIGA_EXPECTED_GIT_SHA", "EXPECTED_GIT_SHA")) {
                $buildEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
                [Environment]::SetEnvironmentVariable($name, $ExpectedBuildSha, "Process")
            }
            $runtimeEnvironment = @{
                PROBIGA_DEPLOYMENT_MODE = "production"
                PROBIGA_SCHEDULER_EXECUTOR_ROLE = "qmt_windows_edge"
                PROBIGA_CODE_ROOT = $Root
                QMT_PYTHON = (Join-Path $Root "runtime\qmt-py313\Scripts\python.exe")
            }
            foreach ($name in $runtimeEnvironment.Keys) {
                $buildEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
                [Environment]::SetEnvironmentVariable($name, $runtimeEnvironment[$name], "Process")
            }
        }
        $proc = Start-Process -FilePath $PythonExe `
            -ArgumentList $ArgLine `
            -WorkingDirectory $Root `
            -WindowStyle Hidden `
            -RedirectStandardOutput $StdOutPath `
            -RedirectStandardError $StdErrPath `
            -PassThru
    }
    finally {
        foreach ($name in $buildEnvironment.Keys) {
            [Environment]::SetEnvironmentVariable($name, $buildEnvironment[$name], "Process")
        }
    }
    Set-ManagedProcess $serviceKey $ScriptName $proc $ExpectedBuildSha
    Write-Output "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') started $serviceKey pid=$($proc.Id)"
}

function Assert-ConsumerProductionRoot {
    param([string]$RegisteredRoot)
    if ($RegisteredRoot -notmatch '^[A-Za-z]:[\\/]') { throw 'Consumer control requires its registered production root' }
    $registered = [IO.Path]::GetFullPath($RegisteredRoot).TrimEnd('\', '/')
    if ([IO.Path]::GetFullPath($Root).TrimEnd('\', '/') -ine $registered) {
        throw 'Consumer controller differs from its registered production root'
    }
    $item = Get-Item -LiteralPath $registered -Force
    if (!$item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
        throw 'Consumer production root must be an ordinary directory'
    }
    $task = Get-ScheduledTask -TaskName 'ProBigA QMT Windows Edge Scheduler' -ErrorAction Stop
    $arguments = [string]($task.Actions.Arguments -join ' ')
    if ($arguments -notmatch '-RegisteredRoot\s+"([^"]+)"' -or
        [IO.Path]::GetFullPath($Matches[1]).TrimEnd('\', '/') -ine $registered) {
        throw 'Consumer production root differs from the registered Windows authority'
    }
}

function Invoke-BigQmtConsumerScope {
    param([string]$Action, [string]$RegisteredRoot, [string]$ExpectedBuildSha,
        [int]$ExpectedProcessId, [long]$ExpectedStartFileTimeUtc,
        [string]$ExpectedProcessBuildSha, [string]$DuplicateRoot = '')
    Assert-ConsumerProductionRoot $RegisteredRoot
    if ($Action -eq 'RetireDuplicate') {
        if ($DuplicateRoot -notmatch '^[A-Za-z]:[\\/]') { throw 'Duplicate consumer root must be explicit and absolute' }
        $target = [IO.Path]::GetFullPath($DuplicateRoot).TrimEnd('\', '/')
        if ($target -ieq [IO.Path]::GetFullPath($Root).TrimEnd('\', '/')) {
            throw 'RetireDuplicate cannot disable the authoritative production root'
        }
        $item = Get-Item -LiteralPath $target -Force
        if (!$item.PSIsContainer -or ($item.Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw 'Duplicate consumer root must be an ordinary directory'
        }
        $Root = $target
        $DataDir = Join-Path $Root 'data'
    }
    $control = Enter-BigQmtConsumerControl
    try {
        if ($Action -eq 'RetireDuplicate') {
            if ($ExpectedProcessId -le 0 -or $ExpectedStartFileTimeUtc -le 0 -or
                $ExpectedProcessBuildSha -cnotmatch '^[0-9a-f]{40}$') {
                throw 'Duplicate retirement requires its exact original identity'
            }
            if (!(Test-Path -LiteralPath (Get-ManagedPidPath 'big_qmt_bridge')) -and !(Test-BigQmtBridgeEnabled)) {
                $prior = Get-Process -Id $ExpectedProcessId -ErrorAction SilentlyContinue
                if (!$prior -or $prior.StartTime.ToUniversalTime().ToFileTimeUtc() -ne $ExpectedStartFileTimeUtc) {
                    return @{ status = 'COMPLETE'; action = $Action; consumer_root = $Root; changed = $false; disabled = $true }
                }
                throw 'Duplicate retirement has no managed record but its original process is still alive'
            }
            $null = Assert-BigQmtConsumerIdentity $ExpectedProcessId $ExpectedStartFileTimeUtc $ExpectedProcessBuildSha
            Disable-BigQmtConsumer
            Stop-ManagedProcess 'big_qmt_bridge' -ExpectedProcessId $ExpectedProcessId `
                -ExpectedStartFileTimeUtc $ExpectedStartFileTimeUtc -ExpectedBuildSha $ExpectedProcessBuildSha
            return @{ status = 'COMPLETE'; action = $Action; consumer_root = $Root; changed = $true; disabled = $true }
        }
        $checkout = Get-ConsumerCheckoutBuild
        if ($ExpectedBuildSha -cne $checkout) { throw 'Consumer target build differs from its production checkout' }
        if (!(Test-BigQmtBridgeEnabled)) { throw 'Consumer is disabled; control does not change its configured state' }
        $existing = Get-ManagedProcess 'big_qmt_bridge'
        if ($existing) {
            $parts = ([IO.File]::ReadAllText((Get-ManagedPidPath 'big_qmt_bridge'))).Trim().Split('|', 4)
            if ($parts.Count -eq 4 -and $parts[2] -ceq 'run_big_qmt_bridge.py' -and $parts[3] -ceq $checkout) {
                $null = Assert-BigQmtConsumerIdentity $existing.Id ($existing.StartTime.ToUniversalTime().ToFileTimeUtc()) $checkout
                return @{ status = 'COMPLETE'; action = $Action; pid = $existing.Id; build_sha = $checkout; changed = $false }
            }
            if ($Action -ne 'Restart') { throw 'Existing consumer differs; exact Restart identity is required' }
            Stop-ManagedProcess 'big_qmt_bridge' -ExpectedProcessId $ExpectedProcessId `
                -ExpectedStartFileTimeUtc $ExpectedStartFileTimeUtc -ExpectedBuildSha $ExpectedProcessBuildSha
        }
        elseif ($Action -eq 'Restart') {
            throw 'Expected consumer already exited; inspect current identity before a new action'
        }
        $python = Join-Path $Root 'runtime\qmt-py313\Scripts\python.exe'
        if (!(Test-Path -LiteralPath $python)) { throw 'Consumer Python runtime is missing' }
        $null = Ensure-Process -PythonExe $python -ScriptName 'run_big_qmt_bridge.py' `
            -ArgLine 'tools/run_big_qmt_bridge.py' -ExpectedBuildSha $checkout `
            -StdOutPath (Join-Path $DataDir 'big_qmt_bridge.out.log') `
            -StdErrPath (Join-Path $DataDir 'big_qmt_bridge.err.log')
        $actual = Get-ManagedProcess 'big_qmt_bridge'
        if (!$actual) { throw 'Consumer process did not remain active after startup' }
        $birth = $actual.StartTime.ToUniversalTime().ToFileTimeUtc()
        $null = Assert-BigQmtConsumerIdentity $actual.Id $birth $checkout
        return @{ status = 'COMPLETE'; action = $Action; pid = $actual.Id; birth = $birth;
            build_sha = $checkout; changed = $true }
    }
    finally { Exit-BigQmtConsumerControl $control }
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
    $clientStartMutex = [System.Threading.Mutex]::new(
        $false,
        "Local\ProBigA.BigQmtStrategyRecovery"
    )
    $clientStartMutexOwned = $false
    try {
        try {
            $clientStartMutexOwned = $clientStartMutex.WaitOne(0)
        }
        catch [System.Threading.AbandonedMutexException] {
            $clientStartMutexOwned = $true
        }
        if (!$clientStartMutexOwned) {
            return
        }
        # A collection recovery may have started QMT since the first check.
        # Share its launch lock and leave every existing client untouched.
        if (@(Get-QmtProcesses).Count -gt 0) {
            return
        }
        Start-Process -FilePath $clientPath `
            -WorkingDirectory $workingDir `
            -WindowStyle Minimized
    }
    finally {
        if ($clientStartMutexOwned) {
            $clientStartMutex.ReleaseMutex()
        }
        $clientStartMutex.Dispose()
    }
    Start-Sleep -Seconds 15
}

if ($ServiceScope -eq 'big_qmt_bridge') {
    Invoke-BigQmtConsumerScope -Action $ConsumerAction -RegisteredRoot $RegisteredRoot `
        -ExpectedBuildSha $ExpectedBuildSha -ExpectedProcessId $ExpectedProcessId `
        -ExpectedStartFileTimeUtc $ExpectedStartFileTimeUtc `
        -ExpectedProcessBuildSha $ExpectedProcessBuildSha -DuplicateRoot $DuplicateRoot |
        ConvertTo-Json -Compress
    return
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
    $consumerControl = Enter-BigQmtConsumerControl
    try {
    if (Test-BigQmtBridgeEnabled) {
    $consumerBuildSha = Get-ConsumerCheckoutBuild
    $existingBridge = Get-BuildBoundConsumer -BuildSha $consumerBuildSha
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
                    $health.recovery_owner -ceq "CONSUMER"
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
                        $health.healthy
                    ) {
                        Write-QmtAlert `
                            "qmt_snapshot_consumer" `
                            "RECOVERED" `
                            "Consumer health recovered; restart guard reset."
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
        -PythonExe $qmtPython `
        -ScriptName "run_big_qmt_bridge.py" `
        -ArgLine "tools/run_big_qmt_bridge.py" `
        -ExpectedBuildSha $consumerBuildSha `
        -StdOutPath (Join-Path $DataDir "big_qmt_bridge.out.log") `
        -StdErrPath (Join-Path $DataDir "big_qmt_bridge.err.log")
    }
    }
    finally { Exit-BigQmtConsumerControl $consumerControl }

    if ((Test-BigQmtBridgeEnabled) -and (Test-BigQmtStrategyAutoRecover)) {
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
elseif (!(Test-LiveQuoteRuntimeEnabled)) {
    Stop-PublicQuoteService
}

# A source-profile switch must take effect in the running supervisor, not only
# on the next process start.  The public scheduler tasks are the sole market
# quote owners when the QMT embedded bridge is disabled.
if (!(Test-BigQmtBridgeEnabled)) {
    $consumerControl = Enter-BigQmtConsumerControl
    try {
        if (!(Test-BigQmtBridgeEnabled)) { Stop-ManagedProcess "big_qmt_bridge" }
    }
    finally { Exit-BigQmtConsumerControl $consumerControl }
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

# The release updater owns the production database forward because it is the
# only Windows controller bound to the protected release ledger. The local
# live supervisor only enforces retirement of the old reverse direction.
Stop-SupersededMysqlReverseTunnels

Stop-DuplicateProcesses
