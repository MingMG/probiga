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
$LocalHistoryMigrationReceipt = Join-Path (
    $SchedulerStateRoot
) "local-history-schema.sha"
function Write-UpdateLog([string]$Message) {
    $Timestamp = Get-Date -Format "yyyy-MM-ddTHH:mm:ssK"
    Add-Content -LiteralPath $LogPath -Value "$Timestamp $Message" -Encoding UTF8
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
    "-WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass " +
    "-File `"$Updater`" -RegisteredRoot `"$ExpectedRoot`""
)
$Registered = Get-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
$RegisteredUpdater = Get-ScheduledTask -TaskName $UpdateTaskName -ErrorAction Stop
if (
    @($Registered.Actions).Count -ne 1 -or
    @($RegisteredUpdater.Actions).Count -ne 1 -or
    $Registered.Actions[0].Execute -ine $PowerShellExe -or
    $RegisteredUpdater.Actions[0].Execute -ine $PowerShellExe -or
    $Registered.Actions[0].WorkingDirectory -ine $ExpectedRoot -or
    $RegisteredUpdater.Actions[0].WorkingDirectory -ine $ExpectedRoot -or
    $Registered.Actions[0].Arguments -cne $SchedulerArgument -or
    $RegisteredUpdater.Actions[0].Arguments -cne $UpdaterArgument
) {
    throw "QMT Windows edge registered production root binding differs"
}

function Stop-EdgeScheduler() {
    $Task = Get-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
    if ($Task.State -ne "Running") {
        return
    }
    Stop-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
    $Deadline = (Get-Date).AddSeconds(120)
    do {
        $State = (Get-ScheduledTask -TaskName $SchedulerTaskName).State
        if ($State -ne "Running") {
            return
        }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $Deadline)
    throw "QMT Windows edge scheduler did not stop before release hold"
}

function Start-EdgeScheduler() {
    $Task = Get-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
    if ($Task.State -ne "Running") {
        Start-ScheduledTask -TaskName $SchedulerTaskName -ErrorAction Stop
    }
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
# checkout.  Linux appends this exact target-SHA request only after its schema
# cutover is complete.  A missing request and an unavailable proof are both
# non-authority: keep the existing scheduler/code untouched and retry later.
$env:PROBIGA_BUILD_COMMIT_SHA = $TargetSha
$env:PROBIGA_SCHEDULER_EXECUTOR_ROLE = "qmt_windows_edge"
$AuthorizationOutput = & $PythonExe -P $BootstrapTool `
    --check-request --expected-build-sha $TargetSha --compact 2>&1
$AuthorizationExit = $LASTEXITCODE
if ($AuthorizationExit -ne 0) {
    Write-UpdateLog "release request not authorized or unavailable for $TargetSha"
    exit 0
}

# An equal-SHA retry can prove that the release is already complete without
# stopping anything.  A different checkout cannot make that claim because the
# live scheduler and Git identity still belong to the prior release.
if ($CurrentSha -ceq $TargetSha) {
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

# Phase two may quiesce the writer and switch code only after the exact remote
# target has a valid, immutable Linux release request.
if ($CurrentSha -cne $TargetSha) {
    Stop-EdgeScheduler
    Invoke-Git @("merge", "--ff-only", "origin/main") | Out-Null
    $UpdatedSha = ((Invoke-Git @("rev-parse", "HEAD")) -join "").Trim().ToLowerInvariant()
    if ($UpdatedSha -cne $TargetSha) {
        throw "QMT Windows edge fast-forward readback differs"
    }
    Write-UpdateLog "updated $CurrentSha -> $UpdatedSha"
    $CurrentSha = $UpdatedSha
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

if (!$StrategyAlreadyReady) {
    # Keep the database-writing edge stopped while the interactive QMT control
    # plane atomically installs, stops, reopens and starts only the exact bridge
    # model.  The reloader verifies the new model's own frozen build/source/
    # artifact identity and restores the previous artifact/model on failure.
    Stop-EdgeScheduler
    $StrategyReloadOutput = & $PowerShellExe `
        -NoProfile -ExecutionPolicy Bypass `
        -File $StrategyReloader `
        -RegisteredRoot $ExpectedRoot `
        -ExpectedBuildSha $CurrentSha 2>&1
    $StrategyReloadExit = $LASTEXITCODE
    if ($StrategyReloadExit -eq 3) {
        Write-UpdateLog "BigQMT strategy reload NEEDS_USER_ACTION for ${CurrentSha}: $($StrategyReloadOutput -join ' ')"
        # Login expiry, broker CAPTCHA and interactive confirmations cannot be
        # bypassed.  Preserve the explicit exit status for Task Scheduler while
        # leaving the writer edge stopped and the prior model untouched/restored.
        exit 3
    }
    if ($StrategyReloadExit -ne 0) {
        Write-UpdateLog "BigQMT strategy reload failed closed for ${CurrentSha}: $($StrategyReloadOutput -join ' ')"
        throw "BigQMT strategy release reload failed closed"
    }
    Write-UpdateLog "BigQMT exact strategy reloaded and identity-bound for $CurrentSha"
}

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
