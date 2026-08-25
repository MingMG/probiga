$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$SchedulerTaskName = "ProBigA QMT Windows Edge Scheduler"
$ExpectedRoot = [System.IO.Path]::GetFullPath("E:\My Code\ProBigA")
$Root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
if ($Root -ine $ExpectedRoot) {
    throw "QMT Windows edge updater is outside the production workspace"
}

$ProgramDataRoot = [System.IO.Path]::GetFullPath($env:ProgramData)
$SchedulerStateRoot = [System.IO.Path]::GetFullPath(
    (Join-Path $ProgramDataRoot "ProBigA\scheduler")
)
$PythonExe = Join-Path $ExpectedRoot ".venv\Scripts\python.exe"
$BootstrapTool = Join-Path $ExpectedRoot "tools\run_qmt_windows_edge_release_bootstrap.py"
$LocalHistoryMigrationTool = Join-Path $ExpectedRoot "tools\backfill_guojin_qmt_local_history.py"
if (!$SchedulerStateRoot.StartsWith(
    $ProgramDataRoot + [System.IO.Path]::DirectorySeparatorChar
)) {
    throw "QMT Windows scheduler state root escapes ProgramData"
}
if (!(Test-Path -LiteralPath $SchedulerStateRoot -PathType Container)) {
    throw "QMT Windows scheduler state root was not installed"
}
foreach ($Path in @($PythonExe, $BootstrapTool, $LocalHistoryMigrationTool)) {
    if (!(Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "QMT Windows edge bootstrap dependency is missing: $Path"
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
    $Output = & git -C $ExpectedRoot @Arguments 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "git command failed: git $($Arguments -join ' ')"
    }
    return @($Output)
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
    # Stop before changing code and remain stopped until Linux appends the
    # post-schema, build-bound release request.  This drains the shared writer
    # heartbeat before the privileged schema cutover.
    Stop-EdgeScheduler
    Invoke-Git @("merge", "--ff-only", "origin/main") | Out-Null
    $UpdatedSha = ((Invoke-Git @("rev-parse", "HEAD")) -join "").Trim().ToLowerInvariant()
    if ($UpdatedSha -cne $TargetSha) {
        throw "QMT Windows edge fast-forward readback differs"
    }
    Write-UpdateLog "updated $CurrentSha -> $UpdatedSha"
    $CurrentSha = $UpdatedSha
}

# The migration receipt is written only after the idempotent privileged init
# succeeds for this exact release.  Keeping it outside the Git checkout makes
# an interrupted post-fast-forward migration retryable even when HEAD already
# equals origin/main, while avoiding a DDL window every five minutes during a
# healthy steady state.
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
    $MigrationOutput = & $PythonExe -P $LocalHistoryMigrationTool `
        init --windows-local-option-file --json 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-UpdateLog "local history schema migration failed for ${CurrentSha}: $($MigrationOutput -join ' ')"
        throw "QMT Windows local history schema migration failed"
    }
    $MigrationReceiptTemp = "$LocalHistoryMigrationReceipt.$PID.tmp"
    [System.IO.File]::WriteAllText(
        $MigrationReceiptTemp,
        "$CurrentSha`n",
        [System.Text.UTF8Encoding]::new($false)
    )
    Move-Item -LiteralPath $MigrationReceiptTemp `
        -Destination $LocalHistoryMigrationReceipt -Force
    Write-UpdateLog "local history schema prepared for $CurrentSha"
}

$env:PROBIGA_BUILD_COMMIT_SHA = $CurrentSha
$env:PROBIGA_SCHEDULER_EXECUTOR_ROLE = "qmt_windows_edge"
$RequestOutput = & $PythonExe -P $BootstrapTool `
    --check-request --expected-build-sha $CurrentSha --compact 2>&1
$RequestExit = $LASTEXITCODE
if ($RequestExit -ne 0) {
    Stop-EdgeScheduler
    Write-UpdateLog "release request not ready for $CurrentSha"
    exit 0
}

Start-EdgeScheduler
$BootstrapOutput = & $PythonExe -P $BootstrapTool `
    --bootstrap --expected-build-sha $CurrentSha `
    --expected-poll-seconds 60 --heartbeat-timeout-seconds 240 `
    --compact 2>&1
$BootstrapExit = $LASTEXITCODE
if ($BootstrapExit -ne 0) {
    Stop-EdgeScheduler
    # A schema validation failure must make the next equal-SHA updater run the
    # idempotent migration again.  The receipt is local/recoverable metadata;
    # removing it never changes market history rows.
    Remove-Item -LiteralPath $LocalHistoryMigrationReceipt `
        -Force -ErrorAction SilentlyContinue
    Write-UpdateLog "release bootstrap failed for ${CurrentSha}: $($BootstrapOutput -join ' ')"
    throw "QMT Windows edge release bootstrap failed"
}
Write-UpdateLog "release bootstrap ready for $CurrentSha"
