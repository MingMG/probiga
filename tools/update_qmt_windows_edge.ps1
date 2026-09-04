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
$WScriptExe = Join-Path $WindowsRoot "System32\wscript.exe"
if ($RegisteredRoot -notmatch "^[A-Za-z]:[\\/]") {
    throw "QMT Windows edge registered root must be an absolute local path"
}
$ExpectedRoot = [System.IO.Path]::GetFullPath($RegisteredRoot)
$Root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
if ($Root -ine $ExpectedRoot) {
    throw "QMT Windows edge updater differs from its registered production root"
}
$RootItem = Get-Item -LiteralPath $Root -Force
if (
    !$RootItem.PSIsContainer -or
    ($RootItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0
) {
    throw "QMT Windows edge production root must be an ordinary directory"
}

$ProgramDataRoot = [System.IO.Path]::GetFullPath($env:ProgramData)
$SchedulerStateRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $ProgramDataRoot "ProBigA\scheduler")
)
$PythonExe = Join-Path $ExpectedRoot ".venv\Scripts\python.exe"
$QmtPythonExe = Join-Path $ExpectedRoot "runtime\qmt-py313\Scripts\python.exe"
$BootstrapTool = Join-Path $ExpectedRoot "tools\run_qmt_windows_edge_release_bootstrap.py"
$LocalHistoryMigrationTool = Join-Path $ExpectedRoot "tools\backfill_guojin_qmt_local_history.py"
$StrategyReloader = Join-Path $ExpectedRoot "tools\reload_big_qmt_strategy.ps1"
$Wrapper = Join-Path $ExpectedRoot "tools\run_local_scheduler_task.ps1"
$Updater = Join-Path $ExpectedRoot "tools\update_qmt_windows_edge.ps1"
$UpdaterLauncher = Join-Path $ExpectedRoot "tools\run_hidden_qmt_updater.vbs"
$EnvFile = Join-Path $ExpectedRoot ".env"
if (!$SchedulerStateRoot.StartsWith(
    $ProgramDataRoot + [System.IO.Path]::DirectorySeparatorChar
)) {
    throw "QMT Windows scheduler state root escapes ProgramData"
}
if (!(Test-Path -LiteralPath $SchedulerStateRoot -PathType Container)) {
    throw "QMT Windows scheduler state root was not installed"
}
foreach ($Path in @(
    $PythonExe,
    $QmtPythonExe,
    $BootstrapTool,
    $LocalHistoryMigrationTool,
    $StrategyReloader,
    $Wrapper,
    $Updater,
    $UpdaterLauncher,
    $EnvFile
)) {
    if (!(Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "QMT Windows edge bootstrap dependency is missing: $Path"
    }
    $DependencyItem = Get-Item -LiteralPath $Path -Force
    if (
        ($DependencyItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) `
            -ne 0
    ) {
        throw "QMT Windows edge dependency cannot be a reparse point: $Path"
    }
}
$StateItem = Get-Item -LiteralPath $SchedulerStateRoot -Force
if (($StateItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
    throw "QMT Windows scheduler state root cannot be a reparse point"
}

$LogPath = Join-Path $SchedulerStateRoot "edge-update.log"
$SchedulerRuntimePath = Join-Path $SchedulerStateRoot "scheduler-runtime.json"
$SchedulerShutdownRequestPath = Join-Path (
    $SchedulerStateRoot
) "scheduler-shutdown-request.json"
$SchedulerShutdownReceiptPath = Join-Path (
    $SchedulerStateRoot
) "scheduler-shutdown-receipt.json"
$LocalHistoryMigrationReceipt = Join-Path (
    $SchedulerStateRoot
) "local-history-schema.sha"
function Write-UpdateLog([string]$Message) {
    $Timestamp = Get-Date -Format "yyyy-MM-ddTHH:mm:ssK"
    Add-Content -LiteralPath $LogPath -Value "$Timestamp $Message" -Encoding UTF8
}

function Invoke-ReadOnlyStrategyPreflight([string]$BuildSha) {
    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $script:LASTEXITCODE = -1
        try {
            $PreflightOutput = & $PowerShellExe `
                -NoProfile -NonInteractive -ExecutionPolicy Bypass `
                -File $StrategyReloader `
                -RegisteredRoot $ExpectedRoot `
                -ExpectedBuildSha $BuildSha `
                -PreflightOnly 2>&1
            $PreflightExit = $script:LASTEXITCODE
        }
        catch {
            $PreflightOutput = @($_)
            $PreflightExit = -1
        }
    }
    finally {
        $ErrorActionPreference = $PreviousPreference
    }
    if ($PreflightExit -eq 0) {
        return "READY"
    }
    $PreflightText = ($PreflightOutput -join " ").Trim()
    if ($PreflightExit -eq 3) {
        $RecoveryRoute = ""
        try {
            $PreflightPayload = ($PreflightOutput -join "`n") |
                ConvertFrom-Json -ErrorAction Stop
            $ReasonCode = [string]$PreflightPayload.reason_code
            $ContractMatches = (
                [string]$PreflightPayload.schema -ceq `
                    "probiga.bigqmt-ui-release-reload.v1" -and
                [string]$PreflightPayload.mode -ceq "PREFLIGHT_ONLY" -and
                [string]$PreflightPayload.status -ceq "NEEDS_USER_ACTION" -and
                [string]$PreflightPayload.data_status -ceq "DATA_BLOCKED" -and
                [string]$PreflightPayload.expected_build_sha -ceq $BuildSha -and
                $ReasonCode -cin @(
                    "QMT_HEARTBEAT_PID_MISMATCH",
                    "QMT_COLD_START_RETRY_READY",
                    "QMT_COLD_START_RUNNING_FINALIZE_READY"
                ) -and
                $PreflightPayload.qmt_calls -eq $false -and
                $PreflightPayload.database_writes -eq $false -and
                $PreflightPayload.ui_actions_attempted -eq $false -and
                $PreflightPayload.authentication_attempted -eq $false -and
                $PreflightPayload.automatic_order_submission -eq $false -and
                $PreflightPayload.direct_python_strategy_execution -eq $false
            )
            if ($ContractMatches) {
                $RecoveryRoute = if (
                    $ReasonCode -cin @(
                        "QMT_COLD_START_RETRY_READY",
                        "QMT_COLD_START_RUNNING_FINALIZE_READY"
                    )
                ) {
                    "PERSISTED_RECOVERY_REQUIRED"
                }
                else {
                    "INITIAL_COLD_START_REQUIRED"
                }
            }
        }
        catch {
            $RecoveryRoute = ""
        }
        if ($RecoveryRoute) {
            Write-UpdateLog (
                "BigQMT read-only preflight requires controlled cold start " +
                "for ${BuildSha}: $PreflightText"
            )
            return $RecoveryRoute
        }
        Write-UpdateLog (
            "BigQMT read-only preflight NEEDS_USER_ACTION for " +
            "${BuildSha}: $PreflightText"
        )
        foreach ($Line in @($PreflightOutput)) {
            [Console]::Out.WriteLine([string]$Line)
        }
        exit 3
    }
    Write-UpdateLog (
        "BigQMT read-only preflight failed closed for " +
        "${BuildSha}: $PreflightText"
    )
    throw "BigQMT read-only strategy preflight failed closed"
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
        throw "git command failed: git $($Arguments -join ' ')"
    }
    return @($Output)
}

$SchedulerArgument = (
    "-WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass " +
    "-File `"$Wrapper`" -RegisteredRoot `"$ExpectedRoot`""
)
$UpdaterArgument = (
    "//B //NoLogo `"$UpdaterLauncher`" `"$ExpectedRoot`""
)
$Registered = Get-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
$RegisteredUpdater = Get-ScheduledTask -TaskName $UpdateTaskName -ErrorAction Stop
if (
    @($Registered.Actions).Count -ne 1 -or
    @($RegisteredUpdater.Actions).Count -ne 1 -or
    $Registered.Actions[0].Execute -ine $PowerShellExe -or
    $RegisteredUpdater.Actions[0].Execute -ine $WScriptExe -or
    $Registered.Actions[0].WorkingDirectory -ine $ExpectedRoot -or
    $RegisteredUpdater.Actions[0].WorkingDirectory -ine $ExpectedRoot -or
    $Registered.Actions[0].Arguments -cne $SchedulerArgument -or
    $RegisteredUpdater.Actions[0].Arguments -cne $UpdaterArgument
) {
    throw "QMT Windows edge registered production root binding differs"
}

function Stop-EdgeScheduler() {
    $Task = Get-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
    $Runtime = $null
    if (Test-Path -LiteralPath $SchedulerRuntimePath -PathType Leaf) {
        $RuntimeItem = Get-Item -LiteralPath $SchedulerRuntimePath -Force
        if (($RuntimeItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
            throw "QMT Windows scheduler runtime identity is a reparse point"
        }
        try {
            $Runtime = Get-Content -LiteralPath $SchedulerRuntimePath -Raw |
                ConvertFrom-Json -ErrorAction Stop
        } catch {
            throw "QMT Windows scheduler runtime identity is malformed"
        }
    }

    $TargetPid = 0
    $TargetInstance = ""
    $TargetBuild = ""
    if ($null -ne $Runtime) {
        $TargetPid = [int]$Runtime.pid
        $TargetInstance = [string]$Runtime.instance_id
        $TargetBuild = ([string]$Runtime.build_sha).ToLowerInvariant()
        $Heartbeat = [DateTimeOffset]::Parse([string]$Runtime.heartbeat_at_utc)
        $HeartbeatAge = ([DateTimeOffset]::UtcNow - $Heartbeat.ToUniversalTime()).TotalSeconds
        if (
            $TargetPid -le 0 -or
            $TargetInstance -notmatch "^[0-9a-fA-F-]{36}$" -or
            $TargetBuild -notmatch "^[0-9a-f]{40}$" -or
            $HeartbeatAge -lt -10 -or
            $HeartbeatAge -gt 15
        ) {
            throw "QMT Windows scheduler PID/instance/build/heartbeat proof is stale"
        }
        $TargetProcess = Get-CimInstance Win32_Process `
            -Filter "ProcessId = $TargetPid" -ErrorAction SilentlyContinue
        if (
            $null -eq $TargetProcess -or
            [string]$TargetProcess.CommandLine -notlike "*run_scheduler_daemon.py*"
        ) {
            throw "QMT Windows scheduler runtime PID is not the live daemon"
        }
        $RequestUid = [Guid]::NewGuid().ToString()
        $Request = [ordered]@{
            schema_version = 1
            request_uid = $RequestUid
            instance_id = $TargetInstance
            pid = $TargetPid
            build_sha = $TargetBuild
            requested_at_utc = [DateTimeOffset]::UtcNow.ToString("o")
        } | ConvertTo-Json -Compress
        $RequestTemp = "$SchedulerShutdownRequestPath.$PID.tmp"
        [System.IO.File]::WriteAllText(
            $RequestTemp,
            $Request + "`n",
            [System.Text.UTF8Encoding]::new($false)
        )
        Move-Item -LiteralPath $RequestTemp `
            -Destination $SchedulerShutdownRequestPath -Force

        $GracefulDeadline = (Get-Date).AddSeconds(60)
        do {
            $Alive = $null -ne (Get-Process -Id $TargetPid -ErrorAction SilentlyContinue)
            $TaskState = (Get-ScheduledTask -TaskName $SchedulerTaskName).State
            if (
                !$Alive -and
                $TaskState -ne "Running" -and
                (Test-Path -LiteralPath $SchedulerShutdownReceiptPath -PathType Leaf)
            ) {
                try {
                    $Receipt = Get-Content -LiteralPath $SchedulerShutdownReceiptPath -Raw |
                        ConvertFrom-Json -ErrorAction Stop
                } catch {
                    $Receipt = $null
                }
                if (
                    $null -ne $Receipt -and
                    [string]$Receipt.status -ceq "stopped" -and
                    [string]$Receipt.request_uid -ceq $RequestUid -and
                    [string]$Receipt.instance_id -ceq $TargetInstance -and
                    [int]$Receipt.pid -eq $TargetPid -and
                    ([string]$Receipt.build_sha).ToLowerInvariant() -ceq $TargetBuild
                ) {
                    return
                }
            }
            Start-Sleep -Seconds 1
        } while ((Get-Date) -lt $GracefulDeadline)
    } elseif ($Task.State -ne "Running") {
        return
    }

    # The wrapper owns a KILL_ON_JOB_CLOSE Job Object.  Stopping the scheduled
    # task therefore closes the complete daemon/QMT child tree even if the
    # graceful request could not finish.
    Stop-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
    $ForcedDeadline = (Get-Date).AddSeconds(60)
    do {
        $State = (Get-ScheduledTask -TaskName $SchedulerTaskName).State
        $Alive = $TargetPid -gt 0 -and (
            $null -ne (Get-Process -Id $TargetPid -ErrorAction SilentlyContinue)
        )
        if ($State -ne "Running" -and !$Alive) {
            return
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $ForcedDeadline)
    throw "QMT Windows edge scheduler process tree did not stop within 120 seconds"
}

function Start-EdgeScheduler() {
    $Task = Get-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
    if ($Task.State -ne "Running") {
        Start-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
    }
}

function Test-QmtReleaseActivationPayload(
    $Payload,
    [string]$ExpectedBuildSha
) {
    $TopLevelFields = @(
        "mode", "status", "build_sha", "deployment_attempt_id",
        "activation_granted", "reason_code", "hold", "grant",
        "database_writes"
    )
    $HoldFields = @(
        "schema", "build_sha", "deployment_attempt_id", "hold_run_uid",
        "request_run_uid", "requested_at", "real_order", "hold_hash"
    )
    $GrantFields = @(
        "schema", "build_sha", "deployment_attempt_id", "grant_run_uid",
        "hold_run_uid", "hold_hash", "granted_at",
        "schema_cutover_verified", "real_order", "grant_hash"
    )
    $ExactFields = {
        param($Value, [string[]]$ExpectedFields)
        if (
            $null -eq $Value -or
            $Value -isnot [System.Management.Automation.PSCustomObject]
        ) {
            return $false
        }
        $ActualFields = @($Value.PSObject.Properties.Name)
        if ($ActualFields.Count -ne $ExpectedFields.Count) {
            return $false
        }
        foreach ($Field in $ExpectedFields) {
            if ($ActualFields -cnotcontains $Field) {
                return $false
            }
        }
        return $true
    }
    $ParseTimestamp = {
        param($Value)
        if (
            $Value -isnot [string] -or
            $Value -cnotmatch (
                "^[0-9]{4}-[0-9]{2}-[0-9]{2}T" +
                "[0-9]{2}:[0-9]{2}:[0-9]{2}" +
                "(?:[+-][0-9]{2}:[0-9]{2})?$"
            )
        ) {
            return $null
        }
        try {
            return [DateTimeOffset]::ParseExact(
                $Value,
                [string[]]@(
                    "yyyy-MM-dd'T'HH:mm:ss",
                    "yyyy-MM-dd'T'HH:mm:sszzz"
                ),
                [Globalization.CultureInfo]::InvariantCulture,
                [Globalization.DateTimeStyles]::None
            )
        }
        catch {
            return $null
        }
    }
    $CanonicalTimestamp = {
        param($Value)
        if ($Value -is [string]) {
            if ($null -eq (& $ParseTimestamp $Value)) {
                return ""
            }
            return $Value
        }
        if ($Value -is [DateTimeOffset]) {
            return $Value.ToString(
                "yyyy-MM-dd'T'HH:mm:sszzz",
                [Globalization.CultureInfo]::InvariantCulture
            )
        }
        if ($Value -is [DateTime]) {
            if ($Value.Kind -eq [DateTimeKind]::Unspecified) {
                return $Value.ToString(
                    "yyyy-MM-dd'T'HH:mm:ss",
                    [Globalization.CultureInfo]::InvariantCulture
                )
            }
            return ([DateTimeOffset]$Value).ToString(
                "yyyy-MM-dd'T'HH:mm:sszzz",
                [Globalization.CultureInfo]::InvariantCulture
            )
        }
        return ""
    }
    $CanonicalDigest = {
        param([System.Collections.IDictionary]$Unsigned)
        $Json = $Unsigned | ConvertTo-Json -Depth 4 -Compress
        $Bytes = [System.Text.Encoding]::UTF8.GetBytes($Json)
        $Hasher = [System.Security.Cryptography.SHA256]::Create()
        try {
            return (($Hasher.ComputeHash($Bytes) | ForEach-Object {
                $_.ToString("x2")
            }) -join "")
        }
        finally {
            $Hasher.Dispose()
        }
    }
    $ValidateHold = {
        param($Hold, [string]$BuildSha, [string]$AttemptId)
        if (!(& $ExactFields $Hold $HoldFields)) {
            return $false
        }
        foreach ($Field in @(
            "schema", "build_sha", "deployment_attempt_id", "hold_run_uid",
            "request_run_uid", "hold_hash"
        )) {
            if ($Hold.$Field -isnot [string]) {
                return $false
            }
        }
        $RequestedAtText = & $CanonicalTimestamp $Hold.requested_at
        $RequestedAt = & $ParseTimestamp $RequestedAtText
        if (
            $Hold.schema -cne `
                "probiga.qmt-windows-edge-release-quiescence.v1" -or
            $Hold.build_sha -cne $BuildSha -or
            $Hold.deployment_attempt_id -cne $AttemptId -or
            $Hold.hold_run_uid -cne "qmt-edge-hold-${AttemptId}" -or
            $Hold.request_run_uid -cne "qmt-edge-request-${BuildSha}" -or
            $Hold.real_order -isnot [bool] -or
            $Hold.real_order -ne $false -or
            $null -eq $RequestedAt -or
            $Hold.hold_hash -cnotmatch "^[0-9a-f]{64}$"
        ) {
            return $false
        }
        $Unsigned = [ordered]@{
            build_sha = $Hold.build_sha
            deployment_attempt_id = $Hold.deployment_attempt_id
            hold_run_uid = $Hold.hold_run_uid
            real_order = $Hold.real_order
            request_run_uid = $Hold.request_run_uid
            requested_at = $RequestedAtText
            schema = $Hold.schema
        }
        return $Hold.hold_hash -ceq (& $CanonicalDigest $Unsigned)
    }
    $ValidateGrant = {
        param($Grant, $Hold, [string]$BuildSha, [string]$AttemptId)
        if (!(& $ExactFields $Grant $GrantFields)) {
            return $false
        }
        foreach ($Field in @(
            "schema", "build_sha", "deployment_attempt_id", "grant_run_uid",
            "hold_run_uid", "hold_hash", "grant_hash"
        )) {
            if ($Grant.$Field -isnot [string]) {
                return $false
            }
        }
        $GrantedAtText = & $CanonicalTimestamp $Grant.granted_at
        $RequestedAtText = & $CanonicalTimestamp $Hold.requested_at
        $GrantedAt = & $ParseTimestamp $GrantedAtText
        $RequestedAt = & $ParseTimestamp $RequestedAtText
        if (
            $Grant.schema -cne `
                "probiga.qmt-windows-edge-release-activation.v1" -or
            $Grant.build_sha -cne $BuildSha -or
            $Grant.deployment_attempt_id -cne $AttemptId -or
            $Grant.grant_run_uid -cne "qmt-edge-grant-${AttemptId}" -or
            $Grant.hold_run_uid -cne $Hold.hold_run_uid -or
            $Grant.hold_hash -cne $Hold.hold_hash -or
            $Grant.schema_cutover_verified -isnot [bool] -or
            $Grant.schema_cutover_verified -ne $true -or
            $Grant.real_order -isnot [bool] -or
            $Grant.real_order -ne $false -or
            $null -eq $GrantedAt -or
            $null -eq $RequestedAt -or
            $GrantedAt -lt $RequestedAt -or
            $Grant.grant_hash -cnotmatch "^[0-9a-f]{64}$"
        ) {
            return $false
        }
        $Unsigned = [ordered]@{
            build_sha = $Grant.build_sha
            deployment_attempt_id = $Grant.deployment_attempt_id
            grant_run_uid = $Grant.grant_run_uid
            granted_at = $GrantedAtText
            hold_hash = $Grant.hold_hash
            hold_run_uid = $Grant.hold_run_uid
            real_order = $Grant.real_order
            schema = $Grant.schema
            schema_cutover_verified = $Grant.schema_cutover_verified
        }
        return $Grant.grant_hash -ceq (& $CanonicalDigest $Unsigned)
    }

    if (!(& $ExactFields $Payload $TopLevelFields)) {
        return ""
    }
    foreach ($Field in @(
        "mode", "status", "build_sha", "deployment_attempt_id", "reason_code"
    )) {
        if ($Payload.$Field -isnot [string]) {
            return ""
        }
    }
    $ExpectedBuild = $ExpectedBuildSha.Trim().ToLowerInvariant()
    if (
        $Payload.mode -cne "check-activation" -or
        $Payload.build_sha -cne $ExpectedBuild -or
        $Payload.activation_granted -isnot [bool] -or
        $Payload.database_writes -isnot [bool] -or
        $Payload.database_writes -ne $false
    ) {
        return ""
    }
    $AttemptId = $Payload.deployment_attempt_id
    $ValidAttempt = (
        $AttemptId -cmatch "^[0-9a-f]{32}$" -and
        $AttemptId -cne ("0" * 32)
    )
    if ($Payload.status -ceq "PENDING") {
        if (
            $Payload.activation_granted -ne $false -or
            $Payload.reason_code -cne `
                "QMT_EDGE_RELEASE_ACTIVATION_PENDING" -or
            $null -ne $Payload.grant
        ) {
            return ""
        }
        if ($null -eq $Payload.hold) {
            if ($AttemptId -ceq "") {
                return "PENDING"
            }
            return ""
        }
        if (
            $ValidAttempt -and
            (& $ValidateHold $Payload.hold $ExpectedBuild $AttemptId)
        ) {
            return "PENDING"
        }
        return ""
    }
    if (
        $Payload.status -ceq "READY" -and
        $Payload.activation_granted -eq $true -and
        $Payload.reason_code -ceq "" -and
        $ValidAttempt -and
        (& $ValidateHold $Payload.hold $ExpectedBuild $AttemptId) -and
        (& $ValidateGrant `
            $Payload.grant $Payload.hold $ExpectedBuild $AttemptId)
    ) {
        return "READY"
    }
    return ""
}

function Confirm-QmtReleaseActivation([string]$ExpectedBuildSha) {
    $ActivationOutput = @()
    $ActivationExit = -1
    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $LASTEXITCODE = -1
        try {
            $ActivationOutput = & $PythonExe -P $BootstrapTool `
                --check-activation --expected-build-sha $ExpectedBuildSha `
                --compact 2>&1
            $ActivationExit = $LASTEXITCODE
        } catch {
            $ActivationOutput = @($_)
            $ActivationExit = -1
        }
    } finally {
        $ErrorActionPreference = $PreviousPreference
    }
    $ActivationPayload = $null
    try {
        $ActivationPayload = ($ActivationOutput -join "`n").Trim() |
            ConvertFrom-Json -ErrorAction Stop
    } catch {
        $ActivationPayload = $null
    }
    $ExpectedBuild = $ExpectedBuildSha.Trim().ToLowerInvariant()
    $ActivationState = Test-QmtReleaseActivationPayload `
        $ActivationPayload $ExpectedBuild
    if ($ActivationExit -eq 0 -and $ActivationState -ceq "READY") {
        return
    }
    if ($ActivationExit -eq 4 -and $ActivationState -ceq "PENDING") {
        Stop-EdgeScheduler
        Write-UpdateLog (
            "release activation remains pending for ${ExpectedBuildSha}; " +
            "QMT Windows edge stays stopped"
        )
        exit 0
    }
    try {
        Stop-EdgeScheduler
    } finally {
        Write-UpdateLog (
            "release activation proof failed for ${ExpectedBuildSha}: " +
            "$($ActivationOutput -join ' ')"
        )
    }
    throw "QMT Windows edge release activation proof failed closed"
}

$TopLevel = ((Invoke-Git @("rev-parse", "--show-toplevel")) -join "").Trim()
if ([System.IO.Path]::GetFullPath($TopLevel) -ine $ExpectedRoot) {
    throw "QMT Windows edge Git top level differs from registered production root"
}
$Origin = ((Invoke-Git @("remote", "get-url", "origin")) -join "").Trim()
if ($Origin -ine $ExpectedOrigin) {
    throw "QMT Windows edge origin differs from the production repository"
}
$env:QMT_PYTHON = $QmtPythonExe
$Branch = ((Invoke-Git @("symbolic-ref", "--short", "HEAD")) -join "").Trim()
if ($Branch -cne "main") {
    throw "QMT Windows edge checkout must remain on main"
}
$Dirty = ((Invoke-Git @("status", "--porcelain", "--untracked-files=normal")) -join "`n").Trim()
if ($Dirty) {
    throw "QMT Windows edge checkout is dirty; automatic update refused"
}

Invoke-Git @("fetch", "--prune", "origin", "main") | Out-Null
$CurrentSha = ((Invoke-Git @("rev-parse", "HEAD")) -join "").Trim().ToLowerInvariant()
$TargetSha = ((Invoke-Git @("rev-parse", "origin/main")) -join "").Trim().ToLowerInvariant()
if ($CurrentSha -notmatch "^[0-9a-f]{40}$" -or $TargetSha -notmatch "^[0-9a-f]{40}$") {
    throw "QMT Windows edge git identity is malformed"
}
if ($CurrentSha -cne $TargetSha) {
    & git -C $ExpectedRoot merge-base --is-ancestor HEAD origin/main 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "QMT Windows edge main diverged; automatic update refused"
    }
}

# Phase one is deliberately read-only and runs from the currently trusted
# checkout.  Linux appends this exact target-SHA request with a per-attempt hold
# before schema cutover; it authorizes only update/quiescence, never QMT use.
# A missing request and an unavailable proof are both non-authority: keep the
# existing scheduler/code untouched and retry later.
$env:PROBIGA_DEPLOYMENT_MODE = "production"
$env:PROBIGA_BUILD_COMMIT_SHA = $TargetSha
$env:PROBIGA_SCHEDULER_EXECUTOR_ROLE = "qmt_windows_edge"
$AuthorizationOutput = & $PythonExe -P $BootstrapTool `
    --check-request --expected-build-sha $TargetSha --compact 2>&1
$AuthorizationExit = $LASTEXITCODE
if ($AuthorizationExit -ne 0) {
    Write-UpdateLog "release request not authorized or unavailable for $TargetSha"
    exit 0
}

if ($CurrentSha -cne $TargetSha) {
    # A non-terminal recovery marker belongs to the current exact build and
    # must be interpreted by that build before any fast-forward. This check is
    # deliberately after target authorization, so an unauthorized target can
    # never stop the currently healthy writer edge.
    $CurrentRecoveryPreflight = Invoke-ReadOnlyStrategyPreflight $CurrentSha
    if ($CurrentRecoveryPreflight -ceq "PERSISTED_RECOVERY_REQUIRED") {
        Write-UpdateLog (
            "completing current-build QMT recovery before updating " +
            "${CurrentSha} -> ${TargetSha}"
        )
        Stop-EdgeScheduler
        $CurrentRecoveryOutput = & $PowerShellExe `
            -NoProfile -ExecutionPolicy Bypass `
            -File $StrategyReloader `
            -RegisteredRoot $ExpectedRoot `
            -ExpectedBuildSha $CurrentSha `
            -ColdStartRecovery 2>&1
        $CurrentRecoveryExit = $LASTEXITCODE
        if ($CurrentRecoveryExit -eq 3) {
            foreach ($Line in @($CurrentRecoveryOutput)) {
                [Console]::Out.WriteLine([string]$Line)
            }
            exit 3
        }
        if ($CurrentRecoveryExit -ne 0) {
            throw "current-build QMT cold-start recovery failed closed"
        }
        $CurrentRecoveryReadback = Invoke-ReadOnlyStrategyPreflight $CurrentSha
        if ($CurrentRecoveryReadback -cne "READY") {
            throw "current-build QMT recovery readback differs"
        }
        $CurrentStrategyOutput = & $PythonExe -P $BootstrapTool `
            --check-strategy --expected-build-sha $CurrentSha --compact 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "current-build QMT strategy proof failed after recovery"
        }
    }
    elseif ($CurrentRecoveryPreflight -cnotin @(
        "READY", "INITIAL_COLD_START_REQUIRED"
    )) {
        throw "current-build QMT recovery preflight returned an invalid status"
    }
}

# An equal-SHA retry can prove that the release is already complete without
# stopping anything.  A different checkout cannot make that claim because the
# live scheduler and Git identity still belong to the prior release.
if ($CurrentSha -ceq $TargetSha) {
    Confirm-QmtReleaseActivation $TargetSha
    $ReadyPreflightStatus = Invoke-ReadOnlyStrategyPreflight $TargetSha
    if ($ReadyPreflightStatus -ceq "READY") {
        $ReadyOutput = & $PythonExe -P $BootstrapTool `
            --check-ready --expected-build-sha $TargetSha `
            --expected-poll-seconds 60 --compact 2>&1
        $ReadyExit = $LASTEXITCODE
        if ($ReadyExit -eq 0) {
            Write-UpdateLog "release already exact-ready for $TargetSha; updater is a no-op"
            exit 0
        }
        if ($ReadyExit -ne 4) {
            # A read/probe outage is not authority to disturb an equal-SHA edge.
            Write-UpdateLog "exact release readiness probe unavailable for $TargetSha"
            exit $ReadyExit
        }
    }
    elseif ($ReadyPreflightStatus -cnotin @(
        "INITIAL_COLD_START_REQUIRED", "PERSISTED_RECOVERY_REQUIRED"
    )) {
        throw "BigQMT read-only strategy preflight returned an invalid status"
    }
}

# Phase two may quiesce the writer and switch code only after the exact remote
# target has a valid, immutable Linux release request.  The separate activation
# proof still gates every QMT or scheduler start after the code switch.
if ($CurrentSha -cne $TargetSha) {
    Stop-EdgeScheduler
    Invoke-Git @("merge", "--ff-only", "origin/main") | Out-Null
    $UpdatedSha = ((Invoke-Git @("rev-parse", "HEAD")) -join "").Trim().ToLowerInvariant()
    if ($UpdatedSha -cne $TargetSha) {
        throw "QMT Windows edge fast-forward readback differs"
    }
    Write-UpdateLog "updated $CurrentSha -> $UpdatedSha"
    $CurrentSha = $UpdatedSha
    Confirm-QmtReleaseActivation $CurrentSha
}

$env:PROBIGA_BUILD_COMMIT_SHA = $CurrentSha
$env:PROBIGA_SCHEDULER_EXECUTOR_ROLE = "qmt_windows_edge"

# The local schema receipt is written only after the runtime identity proves
# both frozen physical contracts read-only for this exact release.  Keeping it
# outside the Git checkout makes an interrupted post-fast-forward validation
# retryable even when HEAD already equals origin/main.  A real schema delta
# remains fail-closed until a separately provisioned privileged migration is
# completed; the updater never lends runtime credentials to persistent DDL.
$PreparedSha = ""
if (Test-Path -LiteralPath $LocalHistoryMigrationReceipt -PathType Leaf) {
    $ReceiptItem = Get-Item -LiteralPath $LocalHistoryMigrationReceipt -Force
    if (
        ($ReceiptItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) `
            -ne 0
    ) {
        throw "QMT Windows local history migration receipt cannot be a reparse point"
    }
    $PreparedSha = (
        Get-Content -LiteralPath $LocalHistoryMigrationReceipt -Raw
    ).Trim().ToLowerInvariant()
}
if ($PreparedSha -cne $CurrentSha) {
    Stop-EdgeScheduler
    # The fixed Windows option file is the least-privilege runtime identity.
    # Prove the complete existing physical contract before writing the local
    # release receipt; never hand that identity to a persistent-DDL path.
    $PreviousPreference = $ErrorActionPreference
    try {
        $ErrorActionPreference = "Continue"
        $SchemaValidationOutput = & $PythonExe -P $LocalHistoryMigrationTool `
            validate-schema --windows-local-option-file --json 2>&1
        $SchemaValidationExit = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $PreviousPreference
    }
    if ($SchemaValidationExit -ne 0) {
        Write-UpdateLog (
            "read-only local history schema validation failed for " +
            "${CurrentSha}; dedicated privileged migration or boundary " +
            "repair is required"
        )
        throw "QMT Windows local history schema is not release-ready"
    }
    $MigrationReceiptTemp = "$LocalHistoryMigrationReceipt.$PID.tmp"
    [System.IO.File]::WriteAllText(
        $MigrationReceiptTemp,
        "$CurrentSha`n",
        [System.Text.UTF8Encoding]::new($false)
    )
    Move-Item -LiteralPath $MigrationReceiptTemp `
        -Destination $LocalHistoryMigrationReceipt -Force
    Write-UpdateLog (
        "local history physical schema validated read-only and receipt " +
        "prepared for $CurrentSha"
    )
}

# A user may have completed the interactive reload after an earlier updater
# returned NEEDS_USER_ACTION.  Prove the live model first so the next retry can
# continue directly to scheduler bootstrap instead of stopping/reopening the
# already exact strategy a second time.
$StrategyPreflightStatus = Invoke-ReadOnlyStrategyPreflight $CurrentSha
$StrategyColdStartRequired = $StrategyPreflightStatus -cin @(
    "INITIAL_COLD_START_REQUIRED", "PERSISTED_RECOVERY_REQUIRED"
)
if ($StrategyColdStartRequired) {
    $StrategyAlreadyReady = $false
    Write-UpdateLog "BigQMT IPC probe skipped; controlled cold start is required"
}
elseif ($StrategyPreflightStatus -ceq "READY") {
    $StrategyProbeOutput = & $PythonExe -P $BootstrapTool `
        --check-strategy --expected-build-sha $CurrentSha --compact 2>&1
    $StrategyProbeExit = $LASTEXITCODE
    $StrategyAlreadyReady = $StrategyProbeExit -eq 0
    if ($StrategyAlreadyReady) {
        Write-UpdateLog "BigQMT exact strategy already loaded for $CurrentSha"
    }
    elseif ($StrategyProbeExit -ne 4) {
        Write-UpdateLog "BigQMT strategy preflight unavailable for ${CurrentSha}: $($StrategyProbeOutput -join ' ')"
        throw "BigQMT strategy preflight failed closed"
    }
}
else {
    throw "BigQMT read-only strategy preflight returned an invalid status"
}

if (!$StrategyAlreadyReady) {
    # Keep the database-writing edge stopped while the interactive QMT control
    # plane atomically installs, stops, reopens and starts only the exact bridge
    # model.  The reloader verifies the new model's own frozen build/source/
    # artifact identity and restores the previous artifact/model on failure.
    Stop-EdgeScheduler
    $StrategyReloadArguments = @(
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", $StrategyReloader,
        "-RegisteredRoot", $ExpectedRoot,
        "-ExpectedBuildSha", $CurrentSha
    )
    if ($StrategyColdStartRequired) {
        $StrategyReloadArguments += "-ColdStartRecovery"
    }
    $StrategyReloadOutput = & $PowerShellExe @StrategyReloadArguments 2>&1
    $StrategyReloadExit = $LASTEXITCODE
    if ($StrategyReloadExit -eq 3) {
        Write-UpdateLog "BigQMT strategy reload NEEDS_USER_ACTION for ${CurrentSha}: $($StrategyReloadOutput -join ' ')"
        # Login expiry, broker CAPTCHA and interactive confirmations cannot be
        # bypassed.  Preserve the explicit exit status for Task Scheduler while
        # leaving the writer edge stopped and the prior model untouched/restored.
        foreach ($Line in @($StrategyReloadOutput)) {
            [Console]::Out.WriteLine([string]$Line)
        }
        exit 3
    }
    if ($StrategyReloadExit -ne 0) {
        Write-UpdateLog "BigQMT strategy reload failed closed for ${CurrentSha}: $($StrategyReloadOutput -join ' ')"
        throw "BigQMT strategy release reload failed closed"
    }
    Write-UpdateLog "BigQMT exact strategy reloaded and identity-bound for $CurrentSha"
}

Confirm-QmtReleaseActivation $CurrentSha
Start-EdgeScheduler
$BootstrapExit = -1
$BootstrapOutput = @()
$PreviousPreference = $ErrorActionPreference
try {
    $ErrorActionPreference = "Continue"
    # A native launch failure can otherwise leave the prior successful exit
    # code in this automatic variable.  Reset it before invoking the child so
    # every launch/traceback/non-zero path reaches the fail-closed branch.
    $LASTEXITCODE = -1
    try {
        $BootstrapOutput = & $PythonExe -P $BootstrapTool `
            --bootstrap --expected-build-sha $CurrentSha `
            --expected-poll-seconds 60 --heartbeat-timeout-seconds 240 `
            --compact 2>&1
        $BootstrapExit = $LASTEXITCODE
    } catch {
        $BootstrapOutput = @($_)
        $BootstrapExit = -1
    }
} finally {
    $ErrorActionPreference = $PreviousPreference
}
if ($BootstrapExit -ne 0) {
    try {
        Stop-EdgeScheduler
    } finally {
        # A bootstrap failure must make the next equal-SHA updater repeat the
        # read-only schema validation.  The receipt is local/recoverable
        # metadata; removing it never changes market history rows.
        Remove-Item -LiteralPath $LocalHistoryMigrationReceipt `
            -Force -ErrorAction SilentlyContinue
    }
    Write-UpdateLog "release bootstrap failed for ${CurrentSha}: $($BootstrapOutput -join ' ')"
    throw "QMT Windows edge release bootstrap failed"
}
Write-UpdateLog "release bootstrap ready for $CurrentSha"
