[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$PipelineStatus,

    [Parameter(Mandatory = $true)]
    [string]$WorkRoot,

    [Parameter(Mandatory = $true)]
    [string]$RollbackRoot,

    [string]$SeedBinlogCheckpoint,

    [string]$AcceptedWorkRoot,

    [Parameter(Mandatory = $true)]
    [string]$Ack
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$completionAck = "I_CONFIRM_PROBIGA_WRITERS_MAY_BE_FROZEN_AND_MYSQL84_CUTOVER_MAY_RUN"
if ($Ack -cne $completionAck) {
    throw "Exact end-to-end upgrade completion acknowledgement is required"
}

$windowsIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
$windowsPrincipal = [Security.Principal.WindowsPrincipal]::new($windowsIdentity)
if (-not $windowsPrincipal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Final MySQL service registration requires an elevated Administrator process"
}

$workspace = (Resolve-Path -LiteralPath (Split-Path -Parent $PSScriptRoot)).Path
$python = Join-Path $workspace ".venv\Scripts\python.exe"
$manifestPython = "C:\Users\Administrator\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe"
$manifestSitePackages = Join-Path $workspace ".runtime\py312-manifest"
$tools = Join-Path $workspace "tools"
$sourceOptions = "D:\MySQL84\config\source-client.ini"
$mysqlHome = "D:\MySQL84\software\mysql-8.4.11-winx64\bin"
$mysql = Join-Path $mysqlHome "mysql.exe"
$mysqladmin = Join-Path $mysqlHome "mysqladmin.exe"
$mysqlbinlog = Join-Path $mysqlHome "mysqlbinlog.exe"
$legacyMysql = "C:\Program Files\MySQL\MySQL Server 5.5\bin\mysql.exe"
$formalCa = "D:\MySQL84\certs\ca.pem"
$runtimeOptions = "D:\MySQL84\config\mysql84-runtime-client.ini"
$stagedEnv = "D:\MySQL84\config\probiga.env.mysql84.staged"
$activeEnv = Join-Path $workspace ".env"
$targetConfig = "D:\MySQL84\config\my-rehearsal-12.ini"
$targetAdmin = "F:\ProBigA-MySQL-Upgrade-20260806\rehearsal\mysql84-state-12\mysql84-admin-client.ini"
$targetPythonAdmin = "F:\ProBigA-MySQL-Upgrade-20260806\rehearsal\mysql84-state-12\mysql84-restore-client.ini"
$targetCa = "F:\ProBigA-MySQL-Upgrade-20260806\rehearsal\mysql84-certs-12\ca.pem"
$binlogDir = "E:\MySQL Datafiles\binlog"

function Get-AbsolutePath {
    param([Parameter(Mandatory = $true)][string]$Path, [bool]$MustExist = $true)
    # Windows PowerShell 5.1 runs on .NET Framework, where
    # Path.IsPathFullyQualified is unavailable. Reject drive-relative paths
    # such as C:foo explicitly while accepting drive-rooted and UNC paths.
    $isDriveRooted = $Path -match '^[A-Za-z]:[\\/]'
    $isUncRooted = $Path -match '^\\\\[^\\/]'
    if (-not ($isDriveRooted -or $isUncRooted)) { throw "Path must be absolute: $Path" }
    if ($MustExist) { return (Resolve-Path -LiteralPath $Path).Path }
    return [IO.Path]::GetFullPath($Path)
}

function Write-AtomicJson {
    param([string]$Path, $Value, [bool]$Replace)
    $resolved = Get-AbsolutePath -Path $Path -MustExist $false
    if ((Test-Path -LiteralPath $resolved) -and -not $Replace) { throw "Artifact already exists: $resolved" }
    $parent = Split-Path -Parent $resolved
    New-Item -ItemType Directory -Path $parent -Force | Out-Null
    $partial = Join-Path $parent ("." + [IO.Path]::GetFileName($resolved) + "." + [guid]::NewGuid().ToString("N") + ".partial")
    try {
        [IO.File]::WriteAllText($partial, (($Value | ConvertTo-Json -Depth 20) + [Environment]::NewLine), [Text.UTF8Encoding]::new($false))
        Move-Item -LiteralPath $partial -Destination $resolved -Force:$Replace
    }
    finally {
        if (Test-Path -LiteralPath $partial) { Remove-Item -LiteralPath $partial -Force }
    }
}

function Get-DirectoryBytes {
    param([Parameter(Mandatory = $true)][string]$Path)
    $sum = (Get-ChildItem -LiteralPath $Path -File -Recurse -Force | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $sum) { return [int64]0 }
    return [int64]$sum
}

function Get-MySQLDataUuid {
    param([Parameter(Mandatory = $true)][string]$DataDirectory)
    $autoCnf = Join-Path $DataDirectory "auto.cnf"
    if (-not (Test-Path -LiteralPath $autoCnf -PathType Leaf)) {
        throw "Staged MySQL 8.4 data has no auto.cnf"
    }
    $match = Select-String -LiteralPath $autoCnf -Pattern '^server-uuid=([0-9a-f-]+)$' | Select-Object -First 1
    if ($null -eq $match) { throw "Staged MySQL 8.4 auto.cnf has no server UUID" }
    return ([string]$match.Matches[0].Groups[1].Value).ToLowerInvariant()
}

function Confirm-StageMySQL84LayoutEvidence {
    param(
        [Parameter(Mandatory = $true)][string]$EvidencePath,
        [Parameter(Mandatory = $true)][string]$ExpectedTargetUuid,
        [Parameter(Mandatory = $true)][string]$ExpectedTargetSourceData,
        [Parameter(Mandatory = $true)][string]$ExpectedRollbackRoot
    )

    if (-not (Test-Path -LiteralPath $EvidencePath -PathType Leaf)) {
        throw "Cold data-layout transition did not publish evidence"
    }
    $layout = Get-Content -LiteralPath $EvidencePath -Raw | ConvertFrom-Json
    $expectedFormal = [IO.Path]::GetFullPath("E:\MySQL84\Data")
    $expectedSource = [IO.Path]::GetFullPath($ExpectedTargetSourceData)
    $expectedRollback = [IO.Path]::GetFullPath($ExpectedRollbackRoot)
    $expectedLegacyIbdata = Join-Path $expectedRollback "innodb\ibdata1"
    $formal = [IO.Path]::GetFullPath([string]$layout.mysql84_formal.datadir)
    $source = [IO.Path]::GetFullPath([string]$layout.preflight.target_source_data)
    $rollbackRoot = [IO.Path]::GetFullPath([string]$layout.legacy_rollback.root)
    $rollbackIbdata = [IO.Path]::GetFullPath([string]$layout.legacy_rollback.ibdata)

    if ([int]$layout.schema_version -ne 1 -or
        [string]$layout.tool -cne "transition_mysql84_data_layout" -or
        [string]$layout.status -cne "passed" -or
        [string]$layout.mode -cne "StageMySQL84" -or
        $layout.recoverable -ne $true -or
        $layout.mysql84_formal.source_preserved_on_f -ne $true -or
        $layout.mysql84_formal.full_file_manifest_verified -ne $true -or
        $layout.legacy_rollback.source_ibdata_removed_after_verified_copy -ne $true -or
        -not $formal.Equals($expectedFormal, [StringComparison]::OrdinalIgnoreCase) -or
        -not $source.Equals($expectedSource, [StringComparison]::OrdinalIgnoreCase) -or
        -not $rollbackRoot.Equals($expectedRollback, [StringComparison]::OrdinalIgnoreCase) -or
        -not $rollbackIbdata.Equals($expectedLegacyIbdata, [StringComparison]::OrdinalIgnoreCase) -or
        ([string]$layout.mysql84_formal.server_uuid).ToLowerInvariant() -ne $ExpectedTargetUuid -or
        ([string]$layout.preflight.target_uuid).ToLowerInvariant() -ne $ExpectedTargetUuid) {
        throw "Cold data-layout transition evidence has invalid identity or safety fields"
    }
    if (-not (Test-Path -LiteralPath $formal -PathType Container) -or
        -not (Test-Path -LiteralPath $source -PathType Container) -or
        -not (Test-Path -LiteralPath $rollbackIbdata -PathType Leaf) -or
        (Test-Path -LiteralPath "E:\MySQL Datafiles\ibdata1" -PathType Leaf)) {
        throw "Cold data-layout transition evidence does not match the live disk layout"
    }

    $sourceBytes = Get-DirectoryBytes -Path $source
    $formalBytes = Get-DirectoryBytes -Path $formal
    $evidenceBytes = [int64]$layout.mysql84_formal.bytes
    if ($sourceBytes -ne $evidenceBytes -or
        $formalBytes -ne $evidenceBytes -or
        [int64]$layout.preflight.target_source_bytes -ne $evidenceBytes -or
        (Get-MySQLDataUuid -DataDirectory $formal) -ne $ExpectedTargetUuid -or
        (Get-Item -LiteralPath $rollbackIbdata).Length -ne [int64]$layout.legacy_rollback.ibdata_bytes) {
        throw "Cold data-layout transition evidence does not match staged byte counts or UUID"
    }
    return $layout
}

$pipelinePath = Get-AbsolutePath -Path $PipelineStatus
$work = Get-AbsolutePath -Path $WorkRoot -MustExist $false
$rollback = Get-AbsolutePath -Path $RollbackRoot -MustExist $false
$acceptedWork = $null
if (-not [string]::IsNullOrWhiteSpace($AcceptedWorkRoot)) {
    $acceptedWork = Get-AbsolutePath -Path $AcceptedWorkRoot
}
if (-not $work.StartsWith("F:\", [StringComparison]::OrdinalIgnoreCase)) { throw "WorkRoot must be on F:" }
if (-not $rollback.StartsWith("F:\", [StringComparison]::OrdinalIgnoreCase)) { throw "RollbackRoot must be on F:" }
if ($null -ne $acceptedWork -and -not $acceptedWork.StartsWith("F:\", [StringComparison]::OrdinalIgnoreCase)) {
    throw "AcceptedWorkRoot must be on F:"
}
if ($null -ne $acceptedWork -and $acceptedWork.Equals($work, [StringComparison]::OrdinalIgnoreCase)) {
    throw "AcceptedWorkRoot must differ from WorkRoot"
}
if (Test-Path -LiteralPath $work) { throw "WorkRoot must be new" }

$statusPath = Join-Path $work "completion-status.json"
$finalEvidence = Join-Path $work "completion-evidence.json"
$stageHistory = [Collections.Generic.List[object]]::new()
function Set-CompletionStatus {
    param([string]$Status, [string]$Stage, [string]$Message)
    $entry = [ordered]@{
        at_utc = [DateTime]::UtcNow.ToString("o")
        stage = $Stage
        status = $Status
        message = $Message
    }
    $stageHistory.Add($entry)
    Write-AtomicJson -Path $statusPath -Value ([ordered]@{
        schema_version = 1
        tool = "complete_mysql84_upgrade"
        status = $Status
        stage = $Stage
        message = $Message
        updated_at_utc = [DateTime]::UtcNow.ToString("o")
        history = $stageHistory
    }) -Replace (Test-Path -LiteralPath $statusPath)
}

foreach ($required in @($python, $manifestPython, (Join-Path $manifestSitePackages "pymysql\__init__.py"), $sourceOptions, $mysql, $mysqladmin, $mysqlbinlog, $legacyMysql, $formalCa, $activeEnv, $targetConfig, $targetAdmin, $targetPythonAdmin, $targetCa, $binlogDir)) {
    Get-AbsolutePath -Path $required | Out-Null
}

$pipeline = Get-Content -LiteralPath $pipelinePath -Raw | ConvertFrom-Json
if ($pipeline.status -ne "success" -or $pipeline.stage -ne "restore-complete") {
    throw "Seed restore pipeline is not complete"
}
$targetUuid = ([string]$pipeline.target_uuid).ToLowerInvariant()
$targetPort = [int]$pipeline.target_port
$targetData = Get-AbsolutePath -Path ([string]$pipeline.target_datadir)
$dumpManifest = Get-AbsolutePath -Path ([string]$pipeline.dump_manifest)
$sanitizerManifest = Get-AbsolutePath -Path ([string]$pipeline.sanitizer_manifest)
$restoreEvidence = Get-AbsolutePath -Path ([string]$pipeline.restore_evidence)
if ($targetPort -eq 3306 -or $targetPort -ne 33090) { throw "Unexpected restored target port" }
$restore = Get-Content -LiteralPath $restoreEvidence -Raw | ConvertFrom-Json
if ($restore.status -ne "success" -or ([string]$restore.target_after.server_uuid).ToLowerInvariant() -ne $targetUuid) {
    throw "Seed restore evidence is not successful for the expected target"
}
$sanitizer = Get-Content -LiteralPath $sanitizerManifest -Raw | ConvertFrom-Json
$restoredArtifactSha = ([string]$sanitizer.output_sha256).ToLowerInvariant()
if ($restoredArtifactSha -notmatch '^[0-9a-f]{64}$') { throw "Sanitized restore SHA-256 is invalid" }
$seedCheckpointPath = $null
$seedCheckpointSha256 = $null
if (-not [string]::IsNullOrWhiteSpace($SeedBinlogCheckpoint)) {
    $seedCheckpointPath = Get-AbsolutePath -Path $SeedBinlogCheckpoint
    $seedCheckpoint = Get-Content -LiteralPath $seedCheckpointPath -Raw | ConvertFrom-Json
    $dumpManifestSha256 = (Get-FileHash -LiteralPath $dumpManifest -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($seedCheckpoint.format -ne "probiga.mysql55_to_mysql84.binlog_checkpoint" -or
        $seedCheckpoint.status -ne "success" -or
        ([string]$seedCheckpoint.target_server_uuid).ToLowerInvariant() -ne $targetUuid -or
        ([string]$seedCheckpoint.snapshot_manifest_sha256).ToLowerInvariant() -ne $dumpManifestSha256 -or
        [string]::IsNullOrWhiteSpace([string]$seedCheckpoint.cursor.file) -or
        [int64]$seedCheckpoint.cursor.position -lt 4) {
        throw "Seed binlog checkpoint is not bound to the restored snapshot and target"
    }
    $seedCheckpointSha256 = (Get-FileHash -LiteralPath $seedCheckpointPath -Algorithm SHA256).Hash.ToLowerInvariant()
}

New-Item -ItemType Directory -Path $work | Out-Null
New-Item -ItemType Directory -Path (Join-Path $work "secrets") | Out-Null

$writerState = Join-Path $work "writer-freeze.state.json"
$writerEvidence = Join-Path $work "writer-freeze.json"
$freezeReady = Join-Path $work "mysql55-freeze-ready.json"
$freezeHeartbeat = Join-Path $work "mysql55-freeze-heartbeat.json"
$freezeStop = Join-Path $work "mysql55-freeze.stop"
$freezeFinal = Join-Path $work "mysql55-freeze-final.json"
$freezeStdout = Join-Path $work "mysql55-freeze.stdout.log"
$freezeStderr = Join-Path $work "mysql55-freeze.stderr.log"
$dataConfig = Join-Path $work "data-manifest.final.json"
$acceptanceDir = Join-Path $work "acceptance"
$migrationOptions = Join-Path $work "secrets\mysql84-migration-client.ini"
$runtimeProvisionEvidence = Join-Path $work "runtime-provision.json"
$layoutEvidence = Join-Path $work "data-layout.json"
$layoutStdout = Join-Path $work "data-layout.stdout.log"
$layoutStderr = Join-Path $work "data-layout.stderr.log"
$cutoverEvidence = Join-Path $work "service-cutover.json"
$envBackup = Join-Path $work "probiga.env.mysql55.backup"
$automaticRollbackLayout = Join-Path $work "automatic-rollback-data-layout.json"
$automaticRollbackService = Join-Path $work "automatic-rollback-service.json"
$automaticRollbackArchive = Join-Path $rollback (
    "mysql84-interrupted-" + $targetUuid + "-" + [IO.Path]::GetFileName($work)
)
$legacyEnvSha256 = (Get-FileHash -LiteralPath $activeEnv -Algorithm SHA256).Hash.ToLowerInvariant()
$acceptanceEvidence = $null
$acceptedAcceptanceState = $null
$acceptedBinlogEvidence = $null
$acceptedMigrationOptions = $null
$acceptedRuntimeProvisionEvidence = $null
if ($null -ne $acceptedWork) {
    $acceptanceEvidence = Get-AbsolutePath -Path (Join-Path $acceptedWork "acceptance\final-acceptance.json")
    $acceptedAcceptanceState = Get-AbsolutePath -Path (Join-Path $acceptedWork "acceptance\final-acceptance.state.json")
    $acceptedBinlogEvidence = Get-AbsolutePath -Path (Join-Path $acceptedWork "acceptance\01-binlog-final.json")
    $acceptedMigrationOptions = Get-AbsolutePath -Path (Join-Path $acceptedWork "secrets\mysql84-migration-client.ini")
    $acceptedRuntimeProvisionEvidence = Get-AbsolutePath -Path (Join-Path $acceptedWork "runtime-provision.json")
}
if ($null -ne $seedCheckpointPath) {
    New-Item -ItemType Directory -Path $acceptanceDir | Out-Null
    $seedCheckpointTarget = Join-Path $acceptanceDir "binlog-catchup.checkpoint.json"
    Copy-Item -LiteralPath $seedCheckpointPath -Destination $seedCheckpointTarget
    if ((Get-FileHash -LiteralPath $seedCheckpointTarget -Algorithm SHA256).Hash.ToLowerInvariant() -ne $seedCheckpointSha256) {
        throw "Seed binlog checkpoint copy verification failed"
    }
}

$guardian = $null
$legacyStopped = $false
try {
    Set-CompletionStatus "running" "freeze-writers" "stopping known writers and disabling ProBigA scheduled tasks"
    & powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File (Join-Path $tools "freeze_probiga_business_writers.ps1") `
        -Mode Freeze -State $writerState -Evidence $writerEvidence `
        -Ack "I_CONFIRM_PROBIGA_BUSINESS_WRITERS_MAY_BE_STOPPED"
    if ($LASTEXITCODE -ne 0) { throw "Business writer freeze failed" }

    Set-CompletionStatus "running" "freeze-source" "acquiring and continuously guarding the MySQL 5.5 global read lock"
    $guardianScript = Join-Path $tools "hold_mysql55_cutover_lock.py"
    $guardianArgs = @(
        ('"' + $guardianScript + '"'),
        "--source-option-file", $sourceOptions,
        "--ready-evidence", $freezeReady,
        "--heartbeat", $freezeHeartbeat,
        "--stop-file", $freezeStop,
        "--final-evidence", $freezeFinal,
        "--heartbeat-seconds", "5",
        "--ack", "I_CONFIRM_BUSINESS_WRITERS_STOPPED_AND_ACQUIRE_GLOBAL_READ_LOCK"
    )
    $guardian = Start-Process -FilePath $python -ArgumentList $guardianArgs -WindowStyle Hidden -RedirectStandardOutput $freezeStdout -RedirectStandardError $freezeStderr -PassThru
    $deadline = (Get-Date).AddSeconds(180)
    while (-not (Test-Path -LiteralPath $freezeReady) -or -not (Test-Path -LiteralPath $freezeHeartbeat)) {
        if ($guardian.HasExited) { throw "MySQL 5.5 freeze guardian exited before becoming ready" }
        if ((Get-Date) -ge $deadline) { throw "MySQL 5.5 freeze guardian did not become ready" }
        Start-Sleep -Seconds 1
    }
    $freezeReport = Get-Content -LiteralPath $freezeReady -Raw | ConvertFrom-Json
    if ($freezeReport.status -ne "locked" -or $freezeReport.global_read_lock_held -ne $true) {
        throw "MySQL 5.5 global read lock was not established"
    }

    if ($null -ne $acceptedWork) {
        Set-CompletionStatus "running" "resume-accepted" "verifying the sealed acceptance, unchanged source coordinate and isolated target"
        $acceptedState = Get-Content -LiteralPath $acceptedAcceptanceState -Raw | ConvertFrom-Json
        $acceptedFinal = Get-Content -LiteralPath $acceptanceEvidence -Raw | ConvertFrom-Json
        if ($acceptedState.status -ne "complete" -or $acceptedState.cutover_ready -ne $true -or
            $acceptedFinal.status -ne "passed" -or $acceptedFinal.cutover_ready -ne $true -or
            [string]$acceptedState.plan_sha256 -ne [string]$acceptedFinal.plan_sha256) {
            throw "Accepted work root does not contain matching cutover-ready acceptance evidence"
        }
        $requiredAcceptedSteps = @(
            "final_binlog_catchup", "provision_migration_account", "schema_semantic_audit",
            "repair_fractional_datetime_compatibility", "materialize_datetime_defaults",
            "capture_frozen_source_data", "capture_quiescent_target_data",
            "compare_business_data", "materialize_check_constraints",
            "v2_v3_v4_migrations", "read_only_business_smoke"
        )
        foreach ($stepName in $requiredAcceptedSteps) {
            $step = @($acceptedState.steps | Where-Object { $_.name -eq $stepName })
            if ($step.Count -ne 1 -or $step[0].status -ne "passed") {
                throw "Accepted evidence is missing passed step: $stepName"
            }
        }
        foreach ($step in $acceptedState.steps) {
            foreach ($output in @($step.outputs)) {
                $recordedPath = [string]$output.path
                $acceptanceMarker = "\acceptance\"
                $secretsMarker = "\secrets\"
                $acceptanceIndex = $recordedPath.IndexOf($acceptanceMarker, [StringComparison]::OrdinalIgnoreCase)
                $secretsIndex = $recordedPath.IndexOf($secretsMarker, [StringComparison]::OrdinalIgnoreCase)
                if ($acceptanceIndex -ge 0) {
                    $relative = $recordedPath.Substring($acceptanceIndex + 1)
                }
                elseif ($secretsIndex -ge 0) {
                    $relative = $recordedPath.Substring($secretsIndex + 1)
                }
                else {
                    throw "Accepted output path cannot be safely rebound: $recordedPath"
                }
                $rebound = Get-AbsolutePath -Path (Join-Path $acceptedWork $relative)
                $actualSha256 = (Get-FileHash -LiteralPath $rebound -Algorithm SHA256).Hash.ToLowerInvariant()
                if ($actualSha256 -ne ([string]$output.sha256).ToLowerInvariant()) {
                    throw "Accepted output hash changed: $relative"
                }
            }
        }
        $acceptedTargetData = ([string]$acceptedFinal.target.datadir).TrimEnd("\")
        if (([string]$acceptedFinal.target.server_uuid).ToLowerInvariant() -ne $targetUuid -or
            [int]$acceptedFinal.target.port -ne $targetPort -or
            $acceptedTargetData -ne $targetData.TrimEnd("\") -or
            [string]$acceptedFinal.target.version -ne "8.4.11" -or
            [string]::IsNullOrWhiteSpace([string]$acceptedFinal.target.tls_cipher)) {
            throw "Accepted target identity does not match the restored target"
        }

        $acceptedBinlog = Get-Content -LiteralPath $acceptedBinlogEvidence -Raw | ConvertFrom-Json
        if ($acceptedBinlog.status -ne "success" -or $acceptedBinlog.mode -ne "final-frozen") {
            throw "Accepted final binlog evidence is invalid"
        }
        $sourceRows = @(& $mysql "--defaults-file=$sourceOptions" --protocol=tcp --host=127.0.0.1 --port=3306 --batch --skip-column-names -e "SHOW MASTER STATUS; SELECT @@version, @@hostname, @@server_id, @@port;")
        if ($LASTEXITCODE -ne 0 -or $sourceRows.Count -lt 2) { throw "Could not revalidate the frozen MySQL 5.5 identity" }
        $masterParts = ([string]$sourceRows[0]) -split "`t"
        $sourceParts = ([string]$sourceRows[1]) -split "`t"
        if ($masterParts.Count -lt 2 -or $sourceParts.Count -lt 4 -or
            $sourceParts[0] -ne [string]$acceptedBinlog.source.version -or
            $sourceParts[1] -ne [string]$acceptedBinlog.source.hostname -or
            [int]$sourceParts[2] -ne [int]$acceptedBinlog.source.server_id -or
            [int]$sourceParts[3] -ne 3306) {
            throw "Source changed after the accepted frozen snapshot; acceptance cannot be resumed"
        }
        $sourceCoordinateUnchanged = (
            $masterParts[0] -eq [string]$acceptedBinlog.source.master.file -and
            [int64]$masterParts[1] -eq [int64]$acceptedBinlog.source.master.position
        )
        if (-not $sourceCoordinateUnchanged) {
            Set-CompletionStatus "running" "resume-source-restart-tail" "proving the post-acceptance source advance contains restart metadata only"
            $restartTailEvidence = Join-Path $acceptanceDir "resume-source-restart-tail.json"
            & $python (Join-Path $tools "verify_mysql55_restart_only_binlog_tail.py") `
                --source-option-file $sourceOptions `
                --accepted-binlog-evidence $acceptedBinlogEvidence `
                --evidence $restartTailEvidence
            if ($LASTEXITCODE -ne 0) {
                throw "Source advanced after acceptance and is not proven restart-only"
            }
            $restartTail = Get-Content -LiteralPath $restartTailEvidence -Raw | ConvertFrom-Json
            if ($restartTail.status -ne "passed" -or
                [int]$restartTail.business_or_unknown_event_count -ne 0 -or
                [string]$restartTail.current_coordinate.file -ne $masterParts[0] -or
                [int64]$restartTail.current_coordinate.position -ne [int64]$masterParts[1]) {
                throw "Restart-only source tail evidence is invalid"
            }
        }

        $targetRows = @(& $mysql "--defaults-file=$targetAdmin" --ssl-mode=VERIFY_CA "--ssl-ca=$targetCa" --protocol=tcp --host=127.0.0.1 "--port=$targetPort" --batch --raw --skip-column-names -e "SELECT VERSION(), @@server_uuid, @@port, @@datadir; SHOW STATUS LIKE 'Ssl_cipher';")
        if ($LASTEXITCODE -ne 0 -or $targetRows.Count -lt 2) { throw "Could not revalidate the accepted MySQL 8.4 target" }
        $targetParts = ([string]$targetRows[0]) -split "`t"
        $sslParts = ([string]$targetRows[1]) -split "`t"
        if ($targetParts.Count -lt 4 -or $sslParts.Count -lt 2 -or
            $targetParts[0] -ne "8.4.11" -or $targetParts[1].ToLowerInvariant() -ne $targetUuid -or
            [int]$targetParts[2] -ne $targetPort -or $targetParts[3].TrimEnd("\") -ne $targetData.TrimEnd("\") -or
            [string]::IsNullOrWhiteSpace($sslParts[1])) {
            throw "Live target identity or TLS differs from the accepted target"
        }

        Copy-Item -LiteralPath $acceptedMigrationOptions -Destination $migrationOptions
        if ((Get-FileHash -LiteralPath $migrationOptions -Algorithm SHA256).Hash.ToLowerInvariant() -ne
            (Get-FileHash -LiteralPath $acceptedMigrationOptions -Algorithm SHA256).Hash.ToLowerInvariant()) {
            throw "Accepted migration option-file copy verification failed"
        }
        New-Item -ItemType Directory -Path $acceptanceDir -Force | Out-Null
        $resumeSmoke = Join-Path $acceptanceDir "resume-business-smoke.json"
        & $python (Join-Path $tools "mysql84_restored_business_smoke.py") `
            --admin-option-file $targetPythonAdmin `
            --ssl-ca $targetCa `
            --expected-server-uuid $targetUuid `
            --expected-server-port $targetPort `
            --expected-datadir $targetData `
            --evidence $resumeSmoke
        if ($LASTEXITCODE -ne 0) { throw "Resumed target business smoke failed" }
        $resumeSmokeReport = Get-Content -LiteralPath $resumeSmoke -Raw | ConvertFrom-Json
        if ($resumeSmokeReport.status -ne "ok" -or $resumeSmokeReport.read_only_transaction -ne $true) {
            throw "Resumed target business smoke evidence is invalid"
        }

        $acceptedRuntime = Get-Content -LiteralPath $acceptedRuntimeProvisionEvidence -Raw | ConvertFrom-Json
        if ($acceptedRuntime.status -ne "success" -or
            ([string]$acceptedRuntime.target.server_uuid).ToLowerInvariant() -ne $targetUuid -or
            [int]$acceptedRuntime.target.port -ne $targetPort -or
            (Get-AbsolutePath -Path ([string]$acceptedRuntime.runtime_option_file)) -ne (Get-AbsolutePath -Path $runtimeOptions) -or
            (Get-AbsolutePath -Path ([string]$acceptedRuntime.staged_env)) -ne (Get-AbsolutePath -Path $stagedEnv) -or
            (Get-FileHash -LiteralPath $stagedEnv -Algorithm SHA256).Hash.ToLowerInvariant() -ne ([string]$acceptedRuntime.staged_env_sha256).ToLowerInvariant()) {
            throw "Previously provisioned runtime artifacts are not reusable"
        }
        $runtimeProvisionEvidence = $acceptedRuntimeProvisionEvidence
    }
    else {
        Set-CompletionStatus "running" "build-data-policy" "pinning live source/target identities and table-level comparison tiers"
        & $python (Join-Path $tools "build_mysql84_final_data_manifest_config.py") `
        --source-option-file $sourceOptions `
        --target-option-file $targetPythonAdmin `
        --target-ssl-ca $targetCa `
        --expected-target-uuid $targetUuid `
        --expected-target-port $targetPort `
        --expected-target-datadir $targetData `
        --max-workers 2 `
        --output $dataConfig
    if ($LASTEXITCODE -ne 0) { throw "Final data-manifest policy build failed" }

    $lockedAt = [string]$freezeReport.locked_at_utc
    $snapshotId = "mysql84-final-" + (Get-Date -Format "yyyyMMddTHHmmss") + "-" + $targetUuid.Substring(0, 8)
    $changeId = "MYSQL84-FINAL-" + (Get-Date -Format "yyyyMMdd-HHmmss")
    Set-CompletionStatus "running" "final-acceptance" "running final catch-up, all-table comparison, DDL, migrations and read-only smoke"
    & $python (Join-Path $tools "run_mysql84_final_acceptance.py") `
        --manifest-python $manifestPython `
        --manifest-site-packages $manifestSitePackages `
        --source-option-file $sourceOptions `
        --dump-manifest $dumpManifest `
        --binlog-dir $binlogDir `
        --mysqlbinlog $mysqlbinlog `
        --mysql $mysql `
        --target-admin-option-file $targetPythonAdmin `
        --target-ssl-ca $targetCa `
        --target-migration-option-file $migrationOptions `
        --freeze-ready-evidence $freezeReady `
        --freeze-heartbeat $freezeHeartbeat `
        --expected-target-uuid $targetUuid `
        --expected-target-port $targetPort `
        --expected-target-datadir $targetData `
        --data-manifest-config $dataConfig `
        --snapshot-id $snapshotId `
        --writes-frozen-at $lockedAt `
        --restored-artifact-sha256 $restoredArtifactSha `
        --change-id $changeId `
        --output-dir $acceptanceDir `
        --workers 2 `
        --execute-ack "I_CONFIRM_SOURCE_WRITES_ARE_FROZEN_FOR_FINAL_ACCEPTANCE"
    if ($LASTEXITCODE -ne 0) { throw "Final MySQL 8.4 acceptance failed" }
    $acceptanceEvidence = Join-Path $acceptanceDir "final-acceptance.json"
    $acceptance = Get-Content -LiteralPath $acceptanceEvidence -Raw | ConvertFrom-Json
    if ($acceptance.status -ne "passed" -or $acceptance.cutover_ready -ne $true) {
        throw "Final acceptance did not authorize cutover"
    }

    }

    if ($null -eq $acceptedWork) {
        Set-CompletionStatus "running" "provision-runtime" "creating TLS runtime account and staging the production env"
        & $python (Join-Path $tools "provision_mysql84_runtime.py") `
        --target-admin-option-file $targetPythonAdmin `
        --target-ssl-ca $targetCa `
        --expected-target-uuid $targetUuid `
        --expected-target-port $targetPort `
        --expected-target-datadir $targetData `
        --runtime-option-file $runtimeOptions `
        --source-env $activeEnv `
        --staged-env $stagedEnv `
        --formal-ca $formalCa `
        --evidence $runtimeProvisionEvidence `
        --apply-ack "I_CONFIRM_ISOLATED_MYSQL84_RUNTIME_PROVISIONING"
        if ($LASTEXITCODE -ne 0) { throw "Runtime account/env staging failed" }
    }
    else {
        Set-CompletionStatus "running" "reuse-runtime" "reusing the verified TLS runtime account and staged production env"
    }

    Set-CompletionStatus "running" "stop-target" "cleanly stopping the accepted 33090 target before cold copy"
    & $mysqladmin "--defaults-file=$targetAdmin" --ssl-mode=VERIFY_CA "--ssl-ca=$targetCa" --protocol=tcp --host=127.0.0.1 "--port=$targetPort" shutdown
    if ($LASTEXITCODE -ne 0) { throw "Accepted MySQL 8.4 target did not shut down cleanly" }
    $deadline = (Get-Date).AddSeconds(180)
    while (Get-NetTCPConnection -State Listen -LocalPort $targetPort -ErrorAction SilentlyContinue) {
        if ((Get-Date) -ge $deadline) { throw "Accepted target port did not close" }
        Start-Sleep -Seconds 2
    }

    Set-CompletionStatus "running" "stop-source" "preannouncing and stopping the globally locked MySQL 5.5 service"
    [IO.File]::WriteAllText($freezeStop, "MYSQL55_SERVICE_STOPPED`n", [Text.Encoding]::ASCII)
    try {
        Stop-Service -Name "MySQL" -ErrorAction Stop
    }
    catch {
        # Codex Desktop can lose its elevated SCM token even for an
        # Administrator account. A privileged MySQL client can still request
        # the same clean server shutdown, after which Windows marks the service
        # process as stopped.
        & $mysqladmin "--defaults-file=$sourceOptions" --protocol=tcp --host=127.0.0.1 --port=3306 shutdown
        if ($LASTEXITCODE -ne 0) {
            throw "Legacy MySQL service stop failed through both SCM and mysqladmin: $($_.Exception.Message)"
        }
    }
    $deadline = (Get-Date).AddSeconds(180)
    while ((Get-Service -Name "MySQL").Status -ne "Stopped") {
        if ((Get-Date) -ge $deadline) { throw "Legacy MySQL service did not stop" }
        Start-Sleep -Seconds 2
    }
    $legacyStopped = $true
    if (-not $guardian.WaitForExit(180000)) { throw "Freeze guardian did not confirm the legacy service stop" }
    # Start-Process can expose a stale/non-zero ExitCode on Windows after the
    # guarded mysqld connection is severed by the intentional service stop.
    # The guardian writes its final evidence only after observing both the
    # preannouncement and the expected connection loss, so validate that
    # evidence before treating the process exit code as authoritative.
    $guardian.Refresh()
    $guardianExitCode = $guardian.ExitCode
    $freezeFinalReport = Get-Content -LiteralPath $freezeFinal -Raw | ConvertFrom-Json
    if ($freezeFinalReport.status -ne "service_stopped" -or $freezeFinalReport.service_stop_was_preannounced -ne $true) {
        if ($guardianExitCode -ne 0) { throw "Freeze guardian reported an unsafe lock loss" }
        throw "Freeze guardian final evidence is invalid"
    }

    Set-CompletionStatus "running" "data-layout" "preserving MySQL 5.5 on F and copying the accepted MySQL 8.4 target to E"
    $layoutInvocationFailure = $null
    $layoutExitCode = $null
    try {
        & powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File (Join-Path $tools "transition_mysql84_data_layout.ps1") `
            -Mode StageMySQL84 `
            -TargetUuid $targetUuid `
            -TargetSourceData $targetData `
            -RollbackRoot $rollback `
            -Evidence $layoutEvidence `
            -Ack "I_CONFIRM_ALL_MYSQL_SERVERS_STOPPED_AND_LEGACY_COPY_MAY_BE_MOVED" `
            1> $layoutStdout 2> $layoutStderr
        $layoutExitCode = $LASTEXITCODE
    }
    catch {
        # A removable F: device can transiently fail the parent PowerShell's
        # native stdout/stderr plumbing after the child has already completed
        # and atomically published its passed evidence. Never discard that
        # durable result solely because the wrapper invocation raised.
        $layoutInvocationFailure = $_.Exception.Message
    }

    try {
        $layout = Confirm-StageMySQL84LayoutEvidence `
            -EvidencePath $layoutEvidence `
            -ExpectedTargetUuid $targetUuid `
            -ExpectedTargetSourceData $targetData `
            -ExpectedRollbackRoot $rollback
    }
    catch {
        $layoutEvidenceFailure = $_.Exception.Message
        $layoutFailure = @(Get-Content -LiteralPath $layoutStderr -Tail 20 -ErrorAction SilentlyContinue) -join " | "
        throw "Cold data-layout transition failed: invocation=$layoutInvocationFailure; exit=$layoutExitCode; evidence=$layoutEvidenceFailure; stderr=$layoutFailure"
    }
    Set-CompletionStatus "running" "data-layout-verified" "verified the durable E/F layout evidence and accepted target identity"

    Set-CompletionStatus "running" "service-cutover" "registering MySQL 8.4 on 3306 and atomically promoting the staged env"
    & $python (Join-Path $tools "cutover_mysql84_production.py") `
        --mode apply `
        --runtime-option-file $runtimeOptions `
        --active-env $activeEnv `
        --staged-env $stagedEnv `
        --active-env-backup $envBackup `
        --provision-evidence $runtimeProvisionEvidence `
        --acceptance-evidence $acceptanceEvidence `
        --expected-target-uuid $targetUuid `
        --evidence $cutoverEvidence `
        --ack "I_CONFIRM_WRITES_FROZEN_AND_MYSQL84_ACCEPTED"
    if ($LASTEXITCODE -ne 0) { throw "MySQL 8.4 production service cutover failed" }

    $cutover = Get-Content -LiteralPath $cutoverEvidence -Raw | ConvertFrom-Json
    if ($cutover.status -ne "passed" -or $cutover.verification.server_uuid -ne $targetUuid) {
        throw "Production service cutover evidence is invalid"
    }

    Set-CompletionStatus "running" "production-business-smoke" "verifying production TLS and read-only business queries on MySQL 8.4/3306"
    $productionBusinessSmoke = Join-Path $work "production-business-smoke.json"
    & $python (Join-Path $tools "mysql84_restored_business_smoke.py") `
        --admin-option-file $runtimeOptions `
        --ssl-ca $formalCa `
        --expected-server-uuid $targetUuid `
        --expected-server-port 3306 `
        --expected-datadir "E:\MySQL84\Data" `
        --evidence $productionBusinessSmoke `
        --production-ack "I_CONFIRM_READ_ONLY_MYSQL84_PRODUCTION_SMOKE"
    if ($LASTEXITCODE -ne 0) { throw "Production TLS/business smoke failed" }
    $productionSmoke = Get-Content -LiteralPath $productionBusinessSmoke -Raw | ConvertFrom-Json
    if ($productionSmoke.status -ne "ok" -or
        ([string]$productionSmoke.target.server_uuid).ToLowerInvariant() -ne $targetUuid -or
        [int]$productionSmoke.target.port -ne 3306 -or
        [string]::IsNullOrWhiteSpace([string]$productionSmoke.target.tls_cipher)) {
        throw "Production TLS/business smoke evidence is invalid"
    }
    $result = [ordered]@{
        schema_version = 1
        tool = "complete_mysql84_upgrade"
        status = "passed"
        finished_at_utc = [DateTime]::UtcNow.ToString("o")
        target_uuid = $targetUuid
        target_version = "8.4.11"
        production_port = 3306
        final_acceptance = $acceptanceEvidence
        runtime_provision = $runtimeProvisionEvidence
        data_layout = $layoutEvidence
        service_cutover = $cutoverEvidence
        production_business_smoke = $productionBusinessSmoke
        old_database_deleted = $false
        legacy_rollback_root = $rollback
        business_writers_remain_frozen = $true
        scheduled_tasks_remain_disabled = $true
        binlog_checkpoint_seeded = ($null -ne $seedCheckpointPath)
        seed_binlog_checkpoint_sha256 = $seedCheckpointSha256
        production_trading_activation_changed = $false
    }
    Write-AtomicJson -Path $finalEvidence -Value $result -Replace $false
    Set-CompletionStatus "passed" "complete" "MySQL 8.4 database cutover passed; business writers intentionally remain frozen"
    $result | ConvertTo-Json -Depth 20
    exit 0
}
catch {
    $failure = $_.Exception.Message
    if (-not $legacyStopped -and $null -ne $guardian -and -not $guardian.HasExited) {
        try {
            if (-not (Test-Path -LiteralPath $freezeStop)) {
                [IO.File]::WriteAllText($freezeStop, "ABORT`n", [Text.Encoding]::ASCII)
            }
            $guardian.WaitForExit(30000) | Out-Null
        }
        catch {}
    }
    $rollbackFailure = $null
    if ($legacyStopped) {
        try {
            # A transient F: failure must never prevent recovery of the E:-hosted
            # legacy service. Status and evidence writes are best effort here.
            try {
                Set-CompletionStatus "running" "automatic-rollback" "cutover failed after the legacy stop; preserving MySQL 8.4 and restoring MySQL 5.5"
            }
            catch {}
            $newService = Get-Service -Name "ProBigA-MySQL84" -ErrorAction SilentlyContinue
            if ($null -ne $newService -and $newService.Status -ne "Stopped") {
                Stop-Service -Name "ProBigA-MySQL84" -ErrorAction Stop
                $newService.WaitForStatus("Stopped", [TimeSpan]::FromMinutes(3))
            }
            if (@(Get-Process -Name mysqld -ErrorAction SilentlyContinue).Count -ne 0) {
                throw "Automatic rollback requires every mysqld process to be stopped"
            }

            $formalData = "E:\MySQL84\Data"
            $stagingData = "E:\MySQL84\Data.staging-$targetUuid"
            $legacyIbdata = "E:\MySQL Datafiles\ibdata1"
            $layoutRecoveryRequired = (Test-Path -LiteralPath $formalData -PathType Container) -or `
                (Test-Path -LiteralPath $stagingData -PathType Container) -or `
                -not (Test-Path -LiteralPath $legacyIbdata -PathType Leaf)
            if ($layoutRecoveryRequired) {
                & powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File (Join-Path $tools "transition_mysql84_data_layout.ps1") `
                    -Mode PrepareRollback `
                    -TargetUuid $targetUuid `
                    -TargetSourceData $targetData `
                    -RollbackRoot $rollback `
                    -Evidence $automaticRollbackLayout `
                    -Mysql84RollbackArchive $automaticRollbackArchive `
                    -Ack "I_CONFIRM_ALL_MYSQL_SERVERS_STOPPED_AND_MYSQL84_MAY_BE_ARCHIVED"
                if ($LASTEXITCODE -ne 0) { throw "Automatic data-layout rollback failed" }
            }
            else {
                try {
                    Write-AtomicJson -Path $automaticRollbackLayout -Value ([ordered]@{
                        schema_version = 1
                        status = "passed"
                        mode = "legacy-layout-already-intact"
                        finished_at_utc = [DateTime]::UtcNow.ToString("o")
                        legacy_ibdata_present = $true
                        mysql84_tree_on_e_present = $false
                        destructive_action_performed = $false
                    }) -Replace $false
                }
                catch {}
            }

            if (Test-Path -LiteralPath $envBackup -PathType Leaf) {
                & $python (Join-Path $tools "cutover_mysql84_production.py") `
                    --mode rollback `
                    --runtime-option-file $runtimeOptions `
                    --active-env $activeEnv `
                    --staged-env $stagedEnv `
                    --active-env-backup $envBackup `
                    --provision-evidence $runtimeProvisionEvidence `
                    --acceptance-evidence $acceptanceEvidence `
                    --expected-target-uuid $targetUuid `
                    --evidence $automaticRollbackService `
                    --ack "I_CONFIRM_WRITES_FROZEN_AND_MYSQL55_PHYSICAL_DATA_RESTORED"
                if ($LASTEXITCODE -ne 0) { throw "Automatic legacy service/env rollback failed" }
            }
            else {
                $currentEnvSha256 = (Get-FileHash -LiteralPath $activeEnv -Algorithm SHA256).Hash.ToLowerInvariant()
                if ($currentEnvSha256 -cne $legacyEnvSha256) {
                    throw "Active env changed without a recoverable legacy backup"
                }
                & sc.exe config MySQL start= auto | Out-Null
                if ($LASTEXITCODE -ne 0) { throw "Could not restore legacy service start mode" }
                Start-Service -Name "MySQL" -ErrorAction Stop
                (Get-Service -Name "MySQL").WaitForStatus("Running", [TimeSpan]::FromMinutes(3))
                $legacyIdentity = @(& $legacyMysql "--defaults-file=$sourceOptions" --batch --skip-column-names --execute="SELECT @@version, @@port, @@datadir" 2>$null)
                if ($LASTEXITCODE -ne 0 -or $legacyIdentity.Count -ne 1 -or $legacyIdentity[0] -notmatch '^5\.5\.20-log\s+3306\s+') {
                    throw "Restored legacy service identity verification failed"
                }
                try {
                    Write-AtomicJson -Path $automaticRollbackService -Value ([ordered]@{
                        schema_version = 1
                        status = "passed"
                        mode = "legacy-reactivation-with-unchanged-env"
                        finished_at_utc = [DateTime]::UtcNow.ToString("o")
                        legacy_service_running = $true
                        legacy_identity_verified = $true
                        active_env_unchanged = $true
                    }) -Replace $false
                }
                catch {}
            }
            try {
                Set-CompletionStatus "failed" "rolled-back" ($failure + "; MySQL 5.5 was automatically restored and verified")
            }
            catch {}
        }
        catch {
            $rollbackFailure = $_.Exception.Message
            try {
                Set-CompletionStatus "failed" "rollback-halted" ($failure + "; automatic rollback failed: " + $rollbackFailure)
            }
            catch {}
        }
    }
    else {
        try {
            Set-CompletionStatus "failed" "halted" $failure
        }
        catch {}
    }
    Write-Error $failure
    exit 2
}
