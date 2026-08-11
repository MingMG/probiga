[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("Preflight", "StageMySQL84", "PrepareRollback")]
    [string]$Mode,

    [Parameter(Mandatory = $true)]
    [ValidatePattern("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")]
    [string]$TargetUuid,

    [Parameter(Mandatory = $true)]
    [string]$TargetSourceData,

    [Parameter(Mandatory = $true)]
    [string]$RollbackRoot,

    [Parameter(Mandatory = $true)]
    [string]$Evidence,

    [string]$Mysql84RollbackArchive,
    [string]$Ack
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$stageAck = "I_CONFIRM_ALL_MYSQL_SERVERS_STOPPED_AND_LEGACY_COPY_MAY_BE_MOVED"
$rollbackAck = "I_CONFIRM_ALL_MYSQL_SERVERS_STOPPED_AND_MYSQL84_MAY_BE_ARCHIVED"
$legacyService = "MySQL"
$newService = "ProBigA-MySQL84"
$legacyIbdata = "E:\MySQL Datafiles\ibdata1"
$legacyBinlog = "E:\MySQL Datafiles\binlog"
$legacyData = "C:\ProgramData\MySQL\MySQL Server 5.5\Data"
$legacyConfig = "C:\Program Files\MySQL\MySQL Server 5.5\my.ini"
$formalParent = "E:\MySQL84"
$formalData = "E:\MySQL84\Data"

function Get-NormalizedPath {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [bool]$MustExist = $true
    )
    # Windows PowerShell 5.1 uses .NET Framework, which does not expose
    # Path.IsPathFullyQualified. Accept only drive-rooted or UNC paths so
    # drive-relative values such as C:foo remain rejected.
    $isDriveRooted = $Path -match '^[A-Za-z]:[\\/]'
    $isUncRooted = $Path -match '^\\\\[^\\/]'
    if (-not ($isDriveRooted -or $isUncRooted)) { throw "Path must be absolute: $Path" }
    if ($MustExist) {
        return (Resolve-Path -LiteralPath $Path -ErrorAction Stop).Path.TrimEnd("\")
    }
    return [System.IO.Path]::GetFullPath($Path).TrimEnd("\")
}

function Assert-ExactPath {
    param(
        [Parameter(Mandatory = $true)][string]$Actual,
        [Parameter(Mandatory = $true)][string]$Expected,
        [bool]$MustExist = $true
    )
    $actualPath = Get-NormalizedPath -Path $Actual -MustExist $MustExist
    $expectedPath = Get-NormalizedPath -Path $Expected -MustExist $false
    if (-not $actualPath.Equals($expectedPath, [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "Unexpected path: $actualPath (expected $expectedPath)"
    }
    return $actualPath
}

function Assert-Administrator {
    $identity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = [Security.Principal.WindowsPrincipal]::new($identity)
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        throw "An elevated Windows administrator token is required"
    }
}

function Get-ServiceSnapshot {
    param([Parameter(Mandatory = $true)][string]$Name)
    $service = Get-CimInstance -ClassName Win32_Service -Filter "Name='$Name'" -ErrorAction SilentlyContinue
    if ($null -eq $service) {
        return [ordered]@{ exists = $false; name = $Name }
    }
    return [ordered]@{
        exists = $true
        name = $Name
        state = [string]$service.State
        start_mode = [string]$service.StartMode
        start_name = [string]$service.StartName
        path_name = [string]$service.PathName
        process_id = [int]$service.ProcessId
    }
}

function Assert-AllMysqlStopped {
    $legacy = Get-ServiceSnapshot -Name $legacyService
    if (-not $legacy.exists -or $legacy.state -ne "Stopped") {
        throw "Legacy MySQL service must exist and be stopped"
    }
    $new = Get-ServiceSnapshot -Name $newService
    if ($new.exists -and $new.state -ne "Stopped") {
        throw "MySQL 8.4 service must be stopped"
    }
    $running = @(Get-Process -Name mysqld -ErrorAction SilentlyContinue)
    if ($running.Count -ne 0) {
        throw "Every mysqld process must be stopped before the cold data transition"
    }
    return [ordered]@{ legacy = $legacy; mysql84 = $new }
}

function Get-DirectoryBytes {
    param([Parameter(Mandatory = $true)][string]$Path)
    $sum = (Get-ChildItem -LiteralPath $Path -File -Recurse -Force | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $sum) { return [int64]0 }
    return [int64]$sum
}

function Get-FileSha256 {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [int]$Attempts = 3,
        [int]$DelaySeconds = 15
    )
    $lastFailure = $null
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        try {
            $hash = (Get-FileHash -LiteralPath $Path -Algorithm SHA256 -ErrorAction Stop).Hash.ToLowerInvariant()
            if ($hash -notmatch '^[0-9a-f]{64}$') {
                throw "SHA-256 returned an invalid digest"
            }
            return $hash
        }
        catch {
            $lastFailure = $_.Exception.Message
            if ($attempt -lt $Attempts) {
                Start-Sleep -Seconds $DelaySeconds
            }
        }
    }
    throw "SHA-256 read failed after $Attempts attempts for $Path`: $lastFailure"
}

function Get-DirectoryManifest {
    param([Parameter(Mandatory = $true)][string]$Path)
    $root = Get-NormalizedPath -Path $Path
    $files = [ordered]@{}
    foreach ($file in Get-ChildItem -LiteralPath $root -File -Recurse -Force | Sort-Object FullName) {
        $prefix = $root.TrimEnd("\") + "\"
        if (-not $file.FullName.StartsWith($prefix, [System.StringComparison]::OrdinalIgnoreCase)) {
            throw "Manifest file escaped its root: $($file.FullName)"
        }
        $relative = $file.FullName.Substring($prefix.Length).Replace("\", "/")
        $files[$relative] = [ordered]@{
            bytes = [int64]$file.Length
            sha256 = Get-FileSha256 -Path $file.FullName
        }
    }
    return $files
}

function Get-StringSha256 {
    param([Parameter(Mandatory = $true)][string]$Value)
    $algorithm = [Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [Text.Encoding]::UTF8.GetBytes($Value)
        return ([BitConverter]::ToString($algorithm.ComputeHash($bytes))).Replace("-", "").ToLowerInvariant()
    }
    finally {
        $algorithm.Dispose()
    }
}

function Assert-ManifestsEqual {
    param(
        [Parameter(Mandatory = $true)]$Source,
        [Parameter(Mandatory = $true)]$Target,
        [Parameter(Mandatory = $true)][string]$Label
    )
    $sourceJson = $Source | ConvertTo-Json -Depth 8 -Compress
    $targetJson = $Target | ConvertTo-Json -Depth 8 -Compress
    if ($sourceJson -cne $targetJson) {
        throw "$Label file manifest differs after copy"
    }
}

function Invoke-RobocopyVerified {
    param(
        [Parameter(Mandatory = $true)][string]$Source,
        [Parameter(Mandatory = $true)][string]$Destination
    )
    $sourcePath = Get-NormalizedPath -Path $Source
    $destinationPath = Get-NormalizedPath -Path $Destination -MustExist $false
    New-Item -ItemType Directory -Path $destinationPath -Force | Out-Null
    & robocopy.exe $sourcePath $destinationPath /E /COPY:DAT /DCOPY:DAT /R:2 /W:3 /MT:8 /J /XJ /NFL /NDL /NP
    $code = $LASTEXITCODE
    if ($code -gt 7) {
        throw "Robocopy failed with exit code $code"
    }
    return $code
}

function Write-AtomicJson {
    param(
        [Parameter(Mandatory = $true)][string]$Path,
        [Parameter(Mandatory = $true)]$Value
    )
    $resolved = Get-NormalizedPath -Path $Path -MustExist $false
    if (Test-Path -LiteralPath $resolved) {
        throw "Evidence already exists: $resolved"
    }
    $parent = Split-Path -Parent $resolved
    New-Item -ItemType Directory -Path $parent -Force | Out-Null
    $partial = Join-Path $parent ("." + [System.IO.Path]::GetFileName($resolved) + "." + [guid]::NewGuid().ToString("N") + ".partial")
    try {
        $json = $Value | ConvertTo-Json -Depth 20
        [System.IO.File]::WriteAllText($partial, $json + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))
        Move-Item -LiteralPath $partial -Destination $resolved
    }
    finally {
        if (Test-Path -LiteralPath $partial) {
            Remove-Item -LiteralPath $partial -Force
        }
    }
}

function Get-AutoUuid {
    param([Parameter(Mandatory = $true)][string]$DataDirectory)
    $auto = Join-Path $DataDirectory "auto.cnf"
    if (-not (Test-Path -LiteralPath $auto -PathType Leaf)) {
        throw "Target data directory has no auto.cnf"
    }
    $match = Select-String -LiteralPath $auto -Pattern '^server-uuid=([0-9a-f-]+)$' | Select-Object -First 1
    if ($null -eq $match) {
        throw "Target auto.cnf has no server UUID"
    }
    return $match.Matches[0].Groups[1].Value.ToLowerInvariant()
}

function Get-DriveFreeBytes {
    param([Parameter(Mandatory = $true)][string]$DriveLetter)
    $drive = Get-PSDrive -Name $DriveLetter -PSProvider FileSystem
    return [int64]$drive.Free
}

Assert-Administrator
$sourceData = Get-NormalizedPath -Path $TargetSourceData
$rollback = Get-NormalizedPath -Path $RollbackRoot -MustExist $false
$evidencePath = Get-NormalizedPath -Path $Evidence -MustExist $false
$expectedFormal = Get-NormalizedPath -Path $formalData -MustExist $false
$expectedParent = Get-NormalizedPath -Path $formalParent -MustExist $false

if (-not $sourceData.StartsWith("F:\", [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "The removable rehearsal target must be on F:"
}
if (-not $rollback.StartsWith("F:\", [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "The legacy rollback root must be on F:"
}
if ($sourceData.StartsWith($rollback + "\", [System.StringComparison]::OrdinalIgnoreCase) -or
    $rollback.StartsWith($sourceData + "\", [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Target source and rollback root must be separate trees"
}
if ((Get-AutoUuid -DataDirectory $sourceData) -ne $TargetUuid) {
    throw "Target source data UUID mismatch"
}

$legacyIbdataPath = Get-NormalizedPath -Path $legacyIbdata -MustExist $false
$legacyDataPath = Get-NormalizedPath -Path $legacyData
$legacyConfigPath = Get-NormalizedPath -Path $legacyConfig
$services = Assert-AllMysqlStopped
$targetBytes = Get-DirectoryBytes -Path $sourceData
$rollbackIbdataCandidate = Join-Path (Join-Path $rollback "innodb") "ibdata1"
$legacyIbdataExists = Test-Path -LiteralPath $legacyIbdataPath -PathType Leaf
$rollbackIbdataExists = Test-Path -LiteralPath $rollbackIbdataCandidate -PathType Leaf
if ($Mode -ne "PrepareRollback" -and -not $legacyIbdataExists -and -not $rollbackIbdataExists) {
    throw "Neither active nor rollback legacy ibdata exists"
}
$legacyBytes = if ($legacyIbdataExists) {
    [int64](Get-Item -LiteralPath $legacyIbdataPath).Length
}
elseif ($rollbackIbdataExists) {
    [int64](Get-Item -LiteralPath $rollbackIbdataCandidate).Length
}
else {
    [int64]0
}
$releaseableLegacyBytes = if ($legacyIbdataExists) { $legacyBytes } else { [int64]0 }
$legacyDataBytes = Get-DirectoryBytes -Path $legacyDataPath
$eFree = Get-DriveFreeBytes -DriveLetter "E"
$fFree = Get-DriveFreeBytes -DriveLetter "F"

$preflight = [ordered]@{
    mode = $Mode
    target_uuid = $TargetUuid
    target_source_data = $sourceData
    target_source_bytes = $targetBytes
    formal_data = $expectedFormal
    legacy_ibdata = $legacyIbdataPath
    legacy_ibdata_bytes = $legacyBytes
    legacy_datadir = $legacyDataPath
    legacy_datadir_bytes = $legacyDataBytes
    rollback_root = $rollback
    e_free_bytes = $eFree
    f_free_bytes = $fFree
    services = $services
    no_mysql_processes = $true
}

if ($Mode -eq "Preflight") {
    $result = [ordered]@{
        schema_version = 1
        tool = "transition_mysql84_data_layout"
        status = "passed"
        finished_at_utc = [DateTime]::UtcNow.ToString("o")
        preflight = $preflight
        destructive_action_performed = $false
    }
    Write-AtomicJson -Path $evidencePath -Value $result
    $result | ConvertTo-Json -Depth 20
    exit 0
}

if ($Mode -eq "StageMySQL84") {
    if ($Ack -cne $stageAck) {
        throw "Exact StageMySQL84 acknowledgement is required"
    }
    if (Test-Path -LiteralPath $expectedFormal -PathType Container) {
        if ($legacyIbdataExists -or -not $rollbackIbdataExists) {
            throw "Formal data exists without an unambiguous released legacy rollback state"
        }
        if ((Get-AutoUuid -DataDirectory $expectedFormal) -ne $TargetUuid) {
            throw "Existing formal data UUID mismatch"
        }
        $sourceManifest = Get-DirectoryManifest -Path $sourceData
        $formalManifest = Get-DirectoryManifest -Path $expectedFormal
        Assert-ManifestsEqual -Source $sourceManifest -Target $formalManifest -Label "Existing formal MySQL 8.4 target"
        $recoveredResult = [ordered]@{
            schema_version = 1
            tool = "transition_mysql84_data_layout"
            status = "passed"
            mode = "StageMySQL84"
            finished_at_utc = [DateTime]::UtcNow.ToString("o")
            preflight = $preflight
            recovered_after_interrupted_evidence_publish = $true
            legacy_rollback = [ordered]@{
                root = $rollback
                ibdata = $rollbackIbdataCandidate
                ibdata_bytes = [int64](Get-Item -LiteralPath $rollbackIbdataCandidate).Length
                ibdata_sha256 = Get-FileSha256 -Path $rollbackIbdataCandidate
                source_ibdata_removed_after_verified_copy = $true
            }
            mysql84_formal = [ordered]@{
                datadir = $expectedFormal
                server_uuid = $TargetUuid
                bytes = (Get-DirectoryBytes -Path $expectedFormal)
                source_preserved_on_f = $true
                full_file_manifest_verified = $true
            }
            deleted_assets = @($legacyIbdataPath)
            recoverable = $true
        }
        Write-AtomicJson -Path $evidencePath -Value $recoveredResult
        $recoveredResult | ConvertTo-Json -Depth 20
        exit 0
    }
    if (($eFree + $releaseableLegacyBytes) -lt [int64]($targetBytes * 1.10)) {
        throw "E: will not have enough space after preserving legacy ibdata"
    }
    # Keep enough headroom to quarantine an existing rollback file and make a
    # fresh unbuffered copy if its durable seal is absent or fails validation.
    $additionalRollbackBytes = if ($legacyIbdataExists) {
        [int64]($legacyBytes + $legacyDataBytes)
    }
    else {
        [int64]$legacyDataBytes
    }
    if ($fFree -lt [int64]($additionalRollbackBytes * 1.05)) {
        throw "F: does not have enough space for the legacy physical rollback copy"
    }
    New-Item -ItemType Directory -Path $rollback -Force | Out-Null
    $rollbackIbdataDir = Join-Path $rollback "innodb"
    $rollbackIbdata = Join-Path $rollbackIbdataDir "ibdata1"
    $rollbackIbdataSeal = Join-Path $rollbackIbdataDir "ibdata1.seal.json"
    $rollbackDatadir = Join-Path $rollback "legacy-datadir"
    $rollbackBinlog = Join-Path $rollback "binlog"
    $rollbackConfig = Join-Path $rollback "my.ini"
    New-Item -ItemType Directory -Path $rollbackIbdataDir -Force | Out-Null

    if ($legacyIbdataExists) {
        $reuseRollbackIbdata = $false
        $ibdataSourceHash = $null
        $ibdataTargetHash = $null
        if ((Test-Path -LiteralPath $rollbackIbdata -PathType Leaf) -and
            (Test-Path -LiteralPath $rollbackIbdataSeal -PathType Leaf)) {
            $seal = Get-Content -LiteralPath $rollbackIbdataSeal -Raw | ConvertFrom-Json
            if ($seal.status -eq "passed" -and
                [string]$seal.copy_mode -eq "robocopy-unbuffered-sha256" -and
                [int64]$seal.bytes -eq (Get-Item -LiteralPath $rollbackIbdata).Length -and
                ([string]$seal.sha256).ToLowerInvariant() -match '^[0-9a-f]{64}$') {
                $existingRollbackHash = Get-FileSha256 -Path $rollbackIbdata
                $reuseRollbackIbdata = $existingRollbackHash -ceq ([string]$seal.sha256).ToLowerInvariant()
                if ($reuseRollbackIbdata) {
                    $ibdataSourceHash = ([string]$seal.source_sha256).ToLowerInvariant()
                    $ibdataTargetHash = $existingRollbackHash
                }
            }
        }
        if (-not $reuseRollbackIbdata) {
            $suffix = [guid]::NewGuid().ToString("N")
            if (Test-Path -LiteralPath $rollbackIbdata -PathType Leaf) {
                Move-Item -LiteralPath $rollbackIbdata -Destination (Join-Path $rollbackIbdataDir ("ibdata1.unverified-" + $suffix))
            }
            if (Test-Path -LiteralPath $rollbackIbdataSeal -PathType Leaf) {
                Move-Item -LiteralPath $rollbackIbdataSeal -Destination (Join-Path $rollbackIbdataDir ("ibdata1.seal.unverified-" + $suffix + ".json"))
            }

            $ibdataSourceHash = Get-FileSha256 -Path $legacyIbdataPath
            $ibdataBytes = [int64](Get-Item -LiteralPath $legacyIbdataPath).Length
            $legacyIbdataName = Split-Path -Leaf $legacyIbdataPath
            $verifiedPartial = $null
            foreach ($candidateRoot in @(Get-ChildItem -LiteralPath $rollbackIbdataDir -Directory -Force |
                    Where-Object { $_.Name -like ".ibdata1.copying-*" } |
                    Sort-Object LastWriteTime -Descending)) {
                $candidateFile = Join-Path $candidateRoot.FullName $legacyIbdataName
                if ((Test-Path -LiteralPath $candidateFile -PathType Leaf) -and
                    [int64](Get-Item -LiteralPath $candidateFile).Length -eq $ibdataBytes) {
                    $candidateHash = Get-FileSha256 -Path $candidateFile
                    if ($candidateHash -ceq $ibdataSourceHash) {
                        $verifiedPartial = [ordered]@{
                            root = $candidateRoot.FullName
                            file = $candidateFile
                            hash = $candidateHash
                        }
                        break
                    }
                }
            }

            if ($null -ne $verifiedPartial) {
                $ibdataTargetHash = [string]$verifiedPartial.hash
                Move-Item -LiteralPath ([string]$verifiedPartial.file) -Destination $rollbackIbdata
                Write-AtomicJson -Path $rollbackIbdataSeal -Value ([ordered]@{
                    schema_version = 1
                    status = "passed"
                    copy_mode = "robocopy-unbuffered-sha256"
                    sealed_at_utc = [DateTime]::UtcNow.ToString("o")
                    source = $legacyIbdataPath
                    target = $rollbackIbdata
                    bytes = $ibdataBytes
                    source_sha256 = $ibdataSourceHash
                    sha256 = $ibdataTargetHash
                    resumed_from_verified_partial = $true
                })
                if (@(Get-ChildItem -LiteralPath ([string]$verifiedPartial.root) -Force).Count -eq 0) {
                    Remove-Item -LiteralPath ([string]$verifiedPartial.root) -Force
                }
            }
            else {
                $copyRoot = Join-Path $rollbackIbdataDir (".ibdata1.copying-" + [guid]::NewGuid().ToString("N"))
                New-Item -ItemType Directory -Path $copyRoot | Out-Null
                $copyCompleted = $false
                try {
                    $legacyIbdataParent = Split-Path -Parent $legacyIbdataPath
                    & robocopy.exe $legacyIbdataParent $copyRoot $legacyIbdataName /COPY:DAT /DCOPY:DAT /R:2 /W:3 /J /NFL /NDL /NP
                    $copyCode = $LASTEXITCODE
                    if ($copyCode -gt 7) { throw "Legacy ibdata unbuffered copy failed with exit code $copyCode" }
                    $rollbackPartial = Join-Path $copyRoot $legacyIbdataName
                    $ibdataTargetHash = Get-FileSha256 -Path $rollbackPartial
                    if ($ibdataSourceHash -cne $ibdataTargetHash -or
                        $ibdataBytes -ne (Get-Item -LiteralPath $rollbackPartial).Length) {
                        throw "Legacy ibdata unbuffered rollback copy failed hash/length verification"
                    }
                    Move-Item -LiteralPath $rollbackPartial -Destination $rollbackIbdata
                    Write-AtomicJson -Path $rollbackIbdataSeal -Value ([ordered]@{
                        schema_version = 1
                        status = "passed"
                        copy_mode = "robocopy-unbuffered-sha256"
                        sealed_at_utc = [DateTime]::UtcNow.ToString("o")
                        source = $legacyIbdataPath
                        target = $rollbackIbdata
                        bytes = $ibdataBytes
                        source_sha256 = $ibdataSourceHash
                        sha256 = $ibdataTargetHash
                        resumed_from_verified_partial = $false
                    })
                    $copyCompleted = $true
                }
                finally {
                    if ($copyCompleted -and
                        (Test-Path -LiteralPath $copyRoot -PathType Container) -and
                        @(Get-ChildItem -LiteralPath $copyRoot -Force).Count -eq 0) {
                        Remove-Item -LiteralPath $copyRoot -Force
                    }
                }
            }
        }
        if (-not $reuseRollbackIbdata -and -not (Test-Path -LiteralPath $rollbackIbdataSeal -PathType Leaf)) {
            throw "Legacy ibdata durable seal was not published"
        }
    }
    else {
        $ibdataTargetHash = Get-FileSha256 -Path $rollbackIbdata
        $ibdataSourceHash = $ibdataTargetHash
    }

    Invoke-RobocopyVerified -Source $legacyDataPath -Destination $rollbackDatadir | Out-Null
    $legacySourceManifest = Get-DirectoryManifest -Path $legacyDataPath
    $legacyTargetManifest = Get-DirectoryManifest -Path $rollbackDatadir
    Assert-ManifestsEqual -Source $legacySourceManifest -Target $legacyTargetManifest -Label "Legacy datadir"
    Copy-Item -LiteralPath $legacyConfigPath -Destination $rollbackConfig -ErrorAction Stop
    $configSourceHash = Get-FileSha256 -Path $legacyConfigPath
    $configTargetHash = Get-FileSha256 -Path $rollbackConfig
    if ($configSourceHash -cne $configTargetHash) {
        throw "Legacy my.ini rollback copy failed hash verification"
    }
    if (Test-Path -LiteralPath $legacyBinlog -PathType Container) {
        Invoke-RobocopyVerified -Source $legacyBinlog -Destination $rollbackBinlog | Out-Null
        Assert-ManifestsEqual -Source (Get-DirectoryManifest -Path $legacyBinlog) -Target (Get-DirectoryManifest -Path $rollbackBinlog) -Label "Legacy binlog"
    }

    # This is the only legacy-file removal.  It occurs after a byte-length and
    # SHA-256 verified F: copy exists, while every mysqld process is stopped.
    if ($legacyIbdataExists) {
        Assert-ExactPath -Actual $legacyIbdataPath -Expected $legacyIbdata | Out-Null
        Remove-Item -LiteralPath $legacyIbdataPath -Force
        if (Test-Path -LiteralPath $legacyIbdataPath) {
            throw "Legacy ibdata was not released from E:"
        }
    }

    New-Item -ItemType Directory -Path $expectedParent -Force | Out-Null
    $staging = Join-Path $expectedParent ("Data.staging-" + $TargetUuid)
    Invoke-RobocopyVerified -Source $sourceData -Destination $staging | Out-Null
    $targetSourceManifest = Get-DirectoryManifest -Path $sourceData
    $targetStagingManifest = Get-DirectoryManifest -Path $staging
    Assert-ManifestsEqual -Source $targetSourceManifest -Target $targetStagingManifest -Label "MySQL 8.4 target"
    if ((Get-AutoUuid -DataDirectory $staging) -ne $TargetUuid) {
        throw "Staged formal target UUID mismatch"
    }
    Move-Item -LiteralPath $staging -Destination $expectedFormal
    & icacls.exe $expectedFormal /inheritance:e /grant:r 'SYSTEM:(OI)(CI)F' 'Administrators:(OI)(CI)F' | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "Could not grant the formal datadir ACL"
    }

    $result = [ordered]@{
        schema_version = 1
        tool = "transition_mysql84_data_layout"
        status = "passed"
        mode = "StageMySQL84"
        finished_at_utc = [DateTime]::UtcNow.ToString("o")
        preflight = $preflight
        legacy_rollback = [ordered]@{
            root = $rollback
            ibdata = $rollbackIbdata
            ibdata_bytes = [int64](Get-Item -LiteralPath $rollbackIbdata).Length
            ibdata_sha256 = $ibdataTargetHash
            datadir = $rollbackDatadir
            datadir_manifest_sha256 = Get-StringSha256 -Value ($legacyTargetManifest | ConvertTo-Json -Depth 8 -Compress)
            config = $rollbackConfig
            config_sha256 = $configTargetHash
            source_ibdata_removed_after_verified_copy = $true
        }
        mysql84_formal = [ordered]@{
            datadir = $expectedFormal
            server_uuid = (Get-AutoUuid -DataDirectory $expectedFormal)
            bytes = (Get-DirectoryBytes -Path $expectedFormal)
            source_preserved_on_f = (Test-Path -LiteralPath $sourceData -PathType Container)
            full_file_manifest_verified = $true
        }
        deleted_assets = @($legacyIbdataPath)
        recoverable = $true
    }
    Write-AtomicJson -Path $evidencePath -Value $result
    $result | ConvertTo-Json -Depth 20
    exit 0
}

if ($Mode -eq "PrepareRollback") {
    if ($Ack -cne $rollbackAck) {
        throw "Exact PrepareRollback acknowledgement is required"
    }
    if ([string]::IsNullOrWhiteSpace($Mysql84RollbackArchive)) {
        throw "Mysql84RollbackArchive is required for PrepareRollback"
    }
    $archive = Get-NormalizedPath -Path $Mysql84RollbackArchive -MustExist $false
    if (-not $archive.StartsWith("F:\", [System.StringComparison]::OrdinalIgnoreCase)) {
        throw "MySQL 8.4 rollback archive must be on F:"
    }
    $rollbackIbdata = Join-Path (Join-Path $rollback "innodb") "ibdata1"
    if (-not (Test-Path -LiteralPath $rollbackIbdata -PathType Leaf)) {
        throw "Verified legacy rollback ibdata is missing"
    }
    $formalCandidate = Get-NormalizedPath -Path $formalData -MustExist $false
    $stagingCandidate = Get-NormalizedPath -Path (Join-Path $expectedParent ("Data.staging-" + $TargetUuid)) -MustExist $false
    $formalExists = Test-Path -LiteralPath $formalCandidate -PathType Container
    $stagingExists = Test-Path -LiteralPath $stagingCandidate -PathType Container
    if ($formalExists -and $stagingExists) {
        throw "Both formal and staging MySQL 8.4 data directories exist; rollback source is ambiguous"
    }

    $mysql84OnE = if ($formalExists) { $formalCandidate } elseif ($stagingExists) { $stagingCandidate } else { $null }
    $mysql84Archive = $null
    $mysql84ArchiveUuid = $null
    $removedAssets = @()
    if ($null -ne $mysql84OnE) {
        if (Test-Path -LiteralPath $archive) {
            throw "MySQL 8.4 rollback archive already exists"
        }
        $activeBytes = Get-DirectoryBytes -Path $mysql84OnE
        if ((Get-DriveFreeBytes -DriveLetter "F") -lt [int64]($activeBytes * 1.05)) {
            throw "F: does not have enough space to archive the interrupted MySQL 8.4 data tree"
        }
        Invoke-RobocopyVerified -Source $mysql84OnE -Destination $archive | Out-Null
        $activeManifest = Get-DirectoryManifest -Path $mysql84OnE
        $archiveManifest = Get-DirectoryManifest -Path $archive
        Assert-ManifestsEqual -Source $activeManifest -Target $archiveManifest -Label "MySQL 8.4 rollback archive"
        $mysql84Archive = $archive
        $archiveAutoCnf = Join-Path $archive "auto.cnf"
        $mysql84ArchiveUuid = if (Test-Path -LiteralPath $archiveAutoCnf -PathType Leaf) {
            Get-AutoUuid -DataDirectory $archive
        }
        else {
            $null
        }

        # The exact formal or interrupted staging tree is removed only after
        # its full F: archive has been hash verified.  The accepted data-12
        # source also remains on F:, so no MySQL 8.4 generation is lost.
        $formalParentResolved = Get-NormalizedPath -Path $formalParent
        if (-not $mysql84OnE.StartsWith($formalParentResolved + "\", [System.StringComparison]::OrdinalIgnoreCase)) {
            throw "MySQL 8.4 data removal escaped E:\MySQL84"
        }
        if (-not ($mysql84OnE.Equals($formalCandidate, [System.StringComparison]::OrdinalIgnoreCase) -or
            $mysql84OnE.Equals($stagingCandidate, [System.StringComparison]::OrdinalIgnoreCase))) {
            throw "MySQL 8.4 rollback source is not an approved exact path"
        }
        Remove-Item -LiteralPath $mysql84OnE -Recurse -Force
        if (Test-Path -LiteralPath $mysql84OnE) {
            throw "MySQL 8.4 data tree was not released"
        }
        $removedAssets = @($mysql84OnE)
    }

    $legacyParent = Split-Path -Parent $legacyIbdata
    New-Item -ItemType Directory -Path $legacyParent -Force | Out-Null
    $sourceHash = Get-FileSha256 -Path $rollbackIbdata
    if (Test-Path -LiteralPath $legacyIbdata -PathType Leaf) {
        $restoredHash = Get-FileSha256 -Path $legacyIbdata
        if ($sourceHash -cne $restoredHash -or
            (Get-Item -LiteralPath $rollbackIbdata).Length -ne (Get-Item -LiteralPath $legacyIbdata).Length) {
            throw "Existing legacy ibdata does not match the verified rollback copy"
        }
    }
    else {
        $restorePartial = Join-Path $legacyParent (".ibdata1.restore-" + [guid]::NewGuid().ToString("N"))
        Copy-Item -LiteralPath $rollbackIbdata -Destination $restorePartial
        $restoredHash = Get-FileSha256 -Path $restorePartial
        if ($sourceHash -cne $restoredHash -or
            (Get-Item -LiteralPath $rollbackIbdata).Length -ne (Get-Item -LiteralPath $restorePartial).Length) {
            throw "Restored legacy ibdata failed hash/length verification"
        }
        Move-Item -LiteralPath $restorePartial -Destination $legacyIbdata
    }

    $result = [ordered]@{
        schema_version = 1
        tool = "transition_mysql84_data_layout"
        status = "passed"
        mode = "PrepareRollback"
        finished_at_utc = [DateTime]::UtcNow.ToString("o")
        preflight = $preflight
        mysql84_archive = [ordered]@{
            path = $mysql84Archive
            server_uuid = $mysql84ArchiveUuid
            full_file_manifest_verified = ($null -ne $mysql84Archive)
            source_on_f_preserved = (Test-Path -LiteralPath $sourceData -PathType Container)
        }
        legacy_restore = [ordered]@{
            ibdata = $legacyIbdata
            bytes = [int64](Get-Item -LiteralPath $legacyIbdata).Length
            sha256 = $restoredHash
            source_preserved_on_f = $true
        }
        deleted_assets = $removedAssets
        recoverable = $true
        service_activation_performed = $false
    }
    Write-AtomicJson -Path $evidencePath -Value $result
    $result | ConvertTo-Json -Depth 20
    exit 0
}

throw "Unsupported mode"
