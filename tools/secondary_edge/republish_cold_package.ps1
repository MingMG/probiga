#Requires -Version 5.1
[CmdletBinding()]
param(
    [string]$PackageRoot='', [string]$JournalRoot='', [string]$ExpectedPreviousBuild='',
    [string]$StateRoot='F:\ProBigA-Source-Pause-20261001', [string]$CodeRoot='',
    [string]$ExpectedUserSid='', [switch]$TargetInstallationNotStarted, [switch]$Elevated,
    [string]$QmtHome='', [string]$SourceProjectRoot='E:\My Code\ProBigA',
    [string]$SourceProductionRoot='E:\My Code\ProBigA-qmt-production',
    [string]$Python314='E:\My Code\ProBigA\.venv\Scripts\python.exe'
)
if (-not $CodeRoot) { $CodeRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent }
. (Join-Path $PSScriptRoot 'export_cold_package.ps1') -CodeRoot $CodeRoot -QmtHome $QmtHome `
    -SourceProjectRoot $SourceProjectRoot -SourceProductionRoot $SourceProductionRoot -Python314 $Python314

function Get-ColdReleaseCodeFiles {
    return @('code.bundle','package_common.ps1','migrate_target.ps1','target_entry.ps1','start_target_migration.cmd','COLD_README.txt')
}

function Get-ColdReleaseAllowedChanges {
    # Permanent narrow boundary, not a bypass flag. No dependency, schema,
    # acquisition, identity protocol or Linux changes can reuse these assets.
    return @('tools/secondary_edge/package_common.ps1','tools/secondary_edge/export_cold_package.ps1',
        'tools/secondary_edge/republish_cold_package.ps1','tools/secondary_edge/package_metadata.py',
        'tools/secondary_edge/migrate_target.ps1','tools/secondary_edge/target_entry.ps1',
        'tools/secondary_edge/start_target_migration.cmd','tools/secondary_edge/COLD_README.txt',
        'tools/secondary_edge/README.txt','tests/test_secondary_edge_package.py',
        'tests/test_secondary_edge_cold_export.py','tests/test_secondary_edge_target_entry.py',
        'tests/test_secondary_edge_power.py','tests/test_secondary_edge_cold_republish.py',
        'tests/test_secondary_edge_package_metadata.py')
}

function Assert-ColdReleaseCodeBoundary([string]$Root, [string]$Previous, [string]$Build) {
    Assert-ColdPublicationCode $Root $Build
    if ($Previous -notmatch '^[0-9a-f]{40}$' -or $Previous -eq $Build) { throw 'RELEASE_PREVIOUS_BUILD_INVALID' }
    & git -C $Root merge-base --is-ancestor $Previous $Build
    if ($LASTEXITCODE -ne 0) { throw 'RELEASE_PREVIOUS_BUILD_NOT_ANCESTOR' }
    $changes = @(& git -C $Root diff --name-status --no-renames $Previous $Build)
    if ($LASTEXITCODE -ne 0 -or -not $changes.Count) { throw 'RELEASE_DIFF_INVALID' }
    $allowed = @(Get-ColdReleaseAllowedChanges)
    foreach ($change in $changes) {
        $parts = ([string]$change).Split([char]9)
        if ($parts.Count -ne 2 -or $parts[0] -notin @('A','M') -or $parts[1] -cnotin $allowed) {
            throw 'RELEASE_CHANGE_OUTSIDE_WINDOWS_INSTALLATION_BOUNDARY'
        }
    }
    foreach ($path in @('deploy/qmt_windows_requirements.lock','deploy/windows_app_requirements.txt')) {
        $old = & git -C $Root rev-parse ($Previous+':'+$path)
        if ($LASTEXITCODE -ne 0) { throw 'RELEASE_REQUIREMENTS_BLOB_MISSING' }
        $new = & git -C $Root rev-parse ($Build+':'+$path)
        if ($LASTEXITCODE -ne 0 -or ([string]$old).Trim() -cne ([string]$new).Trim()) { throw 'RELEASE_REQUIREMENTS_CHANGED' }
    }
}

function Assert-ColdReleaseBundle([string]$Root, [string]$Bundle, [string]$Build) {
    Invoke-Checked 'git' @('-C',$Root,'bundle','verify',$Bundle)
    $heads = @(& git -C $Root bundle list-heads $Bundle)
    if ($LASTEXITCODE -ne 0 -or $heads.Count -ne 1 -or ([string]$heads[0]).Trim() -cne ($Build+' refs/heads/main')) {
        throw 'RELEASE_BUNDLE_BUILD_MISMATCH'
    }
}

function Assert-ColdReleaseSoftware([string]$Root) {
    # PS5 ConvertFrom-Json emits a JSON array as one pipeline object.
    $decoded = Get-Content -LiteralPath (Join-Path $Root 'audit\official-software.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    $receipts = @($decoded)
    $specs = @(Get-ColdExportArtifacts)
    if ($receipts.Count -ne $specs.Count) { throw 'RELEASE_SOFTWARE_RECEIPT_SET_INVALID' }
    foreach ($spec in $specs) {
        $matches = @($receipts | Where-Object { $_.name -ceq $spec.name })
        $path = Join-Path $Root ('software\installers\'+$spec.name)
        if ($matches.Count -ne 1 -or -not (Test-Path -LiteralPath $path -PathType Leaf)) { throw 'RELEASE_SOFTWARE_RECEIPT_MISSING' }
        $receipt = $matches[0]
        if ($receipt.url -cne $spec.url -or $receipt.publisher -cne $spec.publisher -or
            $receipt.sha256 -ine (Get-Sha256 $path) -or ($spec.sha256 -and $receipt.sha256 -ine $spec.sha256)) {
            throw 'RELEASE_SOFTWARE_RECEIPT_MISMATCH'
        }
        Get-SignedArtifact $spec.url $path $spec.publisher $spec.sha256
    }
}

function Assert-ColdReleaseJournal([string]$Journal, [string[]]$ProtectedRoots) {
    $full = [IO.Path]::GetFullPath($Journal).TrimEnd('\')
    if ($full -notmatch '^[A-Za-z]:\\' -or $full -eq [IO.Path]::GetPathRoot($full).TrimEnd('\') -or
        (Test-Path -LiteralPath $full)) { throw 'RELEASE_JOURNAL_MUST_BE_A_NEW_LOCAL_DIRECTORY' }
    Assert-ColdExportPlainAncestors (Split-Path $full -Parent)
    foreach ($root in $ProtectedRoots) {
        $other = [IO.Path]::GetFullPath($root).TrimEnd('\')
        if (($full+'\').StartsWith(($other+'\'),[StringComparison]::OrdinalIgnoreCase) -or
            ($other+'\').StartsWith(($full+'\'),[StringComparison]::OrdinalIgnoreCase)) { throw 'RELEASE_JOURNAL_OVERLAPS_PROTECTED_SOURCE' }
    }
    $volume = Get-Volume -DriveLetter $full.Substring(0,1)
    if ($volume.FileSystem -ne 'NTFS' -or $volume.SizeRemaining -lt 1GB) { throw 'RELEASE_JOURNAL_STORAGE_INVALID' }
}

function Invoke-ColdReleaseMetadata([string]$Root, [string]$Repo, [string]$Python) {
    Push-Location $Repo
    try { Invoke-Checked $Python @('-B','-m','tools.secondary_edge.package_metadata','--package',$Root) }
    finally { Pop-Location }
}

function New-ColdReleaseSafeLog([string]$Path) {
    # A raw PS5 transcript records caught provider errors before sanitization.
    # This permanent event log never receives native/provider/error text.
    $stream = New-Object IO.FileStream($Path,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,
        [IO.FileShare]::Read,4096,[IO.FileOptions]::WriteThrough)
    try { return New-Object IO.StreamWriter($stream,(New-Object Text.UTF8Encoding($false))) }
    catch { $stream.Dispose(); throw 'RELEASE_SAFE_LOG_INITIALIZATION_FAILED' }
}

function Get-ColdReleaseSafeReasons {
    return @('SOURCE_RELEASE_VERIFICATION_FAILED','COLD_PACKAGE_INTEGRITY_VALIDATION_FAILED',
        'NATIVE_OFFLINE_VALIDATION_COMMAND_FAILED','RELEASE_SAFE_LOG_INITIALIZATION_FAILED',
        'RELEASE_SAFE_LOG_VALUE_INVALID','RELEASE_TARGET_INSTALLATION_MUST_NOT_HAVE_STARTED',
        'RELEASE_ARGUMENT_INVALID','RELEASE_PREVIOUS_BUILD_INVALID','RELEASE_PREVIOUS_BUILD_NOT_ANCESTOR',
        'RELEASE_DIFF_INVALID','RELEASE_CHANGE_OUTSIDE_WINDOWS_INSTALLATION_BOUNDARY',
        'RELEASE_REQUIREMENTS_BLOB_MISSING','RELEASE_REQUIREMENTS_CHANGED',
        'RELEASE_JOURNAL_MUST_BE_A_NEW_LOCAL_DIRECTORY','RELEASE_JOURNAL_OVERLAPS_PROTECTED_SOURCE',
        'RELEASE_JOURNAL_STORAGE_INVALID','RELEASE_SOURCE_USER_OR_HOST_MISMATCH',
        'RELEASE_SOURCE_JOURNAL_NOT_PRIVATE','RELEASE_QMT_ROOT_AMBIGUOUS',
        'RELEASE_PREVIOUS_PUBLISHER_INTERRUPTED','RELEASE_PACKAGE_ALREADY_BEING_PUBLISHED',
        'RELEASE_PACKAGE_OWNER_OR_BUILD_MISMATCH','RELEASE_BUNDLE_BUILD_MISMATCH',
        'RELEASE_SOFTWARE_RECEIPT_SET_INVALID','RELEASE_SOFTWARE_RECEIPT_MISSING',
        'RELEASE_SOFTWARE_RECEIPT_MISMATCH','RELEASE_SOURCE_RECEIPTS_CHANGED',
        'RELEASE_OLD_SEAL_CHANGED_BEFORE_WITHDRAWAL','RELEASE_CODE_COPY_CORRUPT',
        'PUBLICATION_BUILD_CHANGED','PUBLICATION_ORIGIN_INVALID',
        'PUBLICATION_READY_ALREADY_EXISTS','PUBLICATION_SOURCE_GUARD_REQUIRED',
        'PUBLICATION_READY_APPEARED_DURING_VALIDATION','PUBLICATION_MANIFEST_CHANGED',
        'MIGRATION_AC_POWER_REQUIRED','MIGRATION_POWER_POLICY_REFUSES_SYSTEM_REQUESTS',
        'MIGRATION_POWER_STATUS_API_FAILED','MIGRATION_POWER_SCHEME_API_FAILED',
        'MIGRATION_POWER_POLICY_API_FAILED','MIGRATION_POWER_SCHEME_RELEASE_FAILED',
        'MIGRATION_POWER_CREATE_API_FAILED','MIGRATION_POWER_SET_API_FAILED',
        'MIGRATION_POWER_REQUEST_NOT_ACQUIRED')
}

function Write-ColdReleaseSafeEvent {
    param($Log,
        [ValidateSet('helper-started','old-sha-started','old-sha-verified','ready-withdrawn',
            'new-sha-started','new-sha-verified','published','blocked','cleanup-finished','cleanup-failed')]
        [string]$Event, [string]$Build='', [string]$Seal='', [string]$Reason='')
    if (-not $Log) { return }
    if (($Build -and $Build -cnotmatch '^[0-9a-f]{40}$') -or
        ($Seal -and $Seal -notmatch '^[0-9A-Fa-f]{64}$') -or
        ($Reason -and $Reason -cnotin @(Get-ColdReleaseSafeReasons))) { throw 'RELEASE_SAFE_LOG_VALUE_INVALID' }
    $row = [ordered]@{format='probiga.cold-package-log.v1';event=$Event;helper_pid=$PID;
        at_utc=(Get-Date).ToUniversalTime().ToString('o')}
    if ($Build) { $row.build_sha=$Build }
    if ($Seal) { $row.manifest_sha256=$Seal }
    if ($Reason) { $row.reason=$Reason }
    $Log.WriteLine(($row | ConvertTo-Json -Compress)); $Log.Flush()
}

function Invoke-ColdPackageRepublication {
    $lease = $null; $journalCreated = $false; $readyWithdrawn = $false
    $mutex = $null; $ownsMutex = $false
    $safeLog = $null; $operationFailed = $false
    try {
    Assert-Administrator
    if (-not $TargetInstallationNotStarted) { throw 'RELEASE_TARGET_INSTALLATION_MUST_NOT_HAVE_STARTED' }
    foreach ($value in @($PackageRoot,$JournalRoot,$StateRoot,$CodeRoot,$Python314,$ExpectedUserSid)) {
        if (-not $value -or $value -match '["\x00-\x1f]') { throw 'RELEASE_ARGUMENT_INVALID' }
    }
    if ($ExpectedPreviousBuild -notmatch '^[0-9a-f]{40}$') { throw 'RELEASE_PREVIOUS_BUILD_INVALID' }
    $identitySid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    Assert-ColdExportPlainAncestors $StateRoot
    Assert-ColdExportPlainAncestors (Join-Path $StateRoot 'original-state.json')
    $original = Get-Content -LiteralPath (Join-Path $StateRoot 'original-state.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($original.format -ne 'probiga.pause-original-state.v1' -or $original.user_sid -ne $ExpectedUserSid -or
        $identitySid -ne $ExpectedUserSid -or $original.source_host -ine $env:COMPUTERNAME) { throw 'RELEASE_SOURCE_USER_OR_HOST_MISMATCH' }
    $allowedJournalSids = @('S-1-5-18','S-1-5-32-544',$identitySid)
    foreach ($rule in (Get-Acl -LiteralPath $StateRoot).Access) {
        if ($rule.AccessControlType -eq 'Allow' -and
            $rule.IdentityReference.Translate([Security.Principal.SecurityIdentifier]).Value -notin $allowedJournalSids) {
            throw 'RELEASE_SOURCE_JOURNAL_NOT_PRIVATE'
        }
    }
    $PackageRoot = [IO.Path]::GetFullPath($PackageRoot).TrimEnd('\')
    $JournalRoot = [IO.Path]::GetFullPath($JournalRoot).TrimEnd('\')
    Assert-ColdExportPlainAncestors $PackageRoot
    $sourceLayout = Join-Path $StateRoot 'source-layout.json'
    $sourcePause = Join-Path $StateRoot 'source-pause.json'
    foreach ($path in @($sourceLayout,$sourcePause,$Python314)) { Assert-ColdExportPlainAncestors $path }
    $layout = Get-Content -LiteralPath $sourceLayout -Raw -Encoding UTF8 | ConvertFrom-Json
    $pause = Get-Content -LiteralPath $sourcePause -Raw -Encoding UTF8 | ConvertFrom-Json
    $layoutHash = Get-Sha256 $sourceLayout; $pauseHash = Get-Sha256 $sourcePause
    if (-not $QmtHome) {
        $candidates = @(Get-ChildItem -LiteralPath 'D:\' -Directory | Where-Object {
            Test-Path -LiteralPath (Join-Path $_.FullName 'bin.x64\XtItClient.exe') -PathType Leaf })
        if ($candidates.Count -ne 1) { throw 'RELEASE_QMT_ROOT_AMBIGUOUS' }
        $QmtHome = $candidates[0].FullName
    }
    $protected = @($PackageRoot,$StateRoot,$CodeRoot,$QmtHome,$MySqlHome,$CodexBundle,$SourceProjectRoot,$SourceProductionRoot)
    foreach ($property in $layout.roots.PSObject.Properties) { $protected += [string]$property.Value }
    Assert-ColdReleaseJournal $JournalRoot $protected
    Assert-ColdExportPythonIdentity $Python314 'Python 3.14.3'
    Assert-MergedMain $CodeRoot
    $build = (& git -C $CodeRoot rev-parse HEAD).Trim()
    Assert-ColdReleaseCodeBoundary $CodeRoot $ExpectedPreviousBuild $build
    $guard = {
        Assert-ColdExportPause $layout $pause $env:COMPUTERNAME
        Assert-ColdExportQmtStopped $QmtHome
        foreach ($tree in @($SourceProjectRoot,$SourceProductionRoot)) { Assert-ColdExportSourceBrowserStopped $tree }
        if ((Get-Sha256 $sourceLayout) -cne $layoutHash -or (Get-Sha256 $sourcePause) -cne $pauseHash -or
            (Get-Sha256 (Join-Path $PackageRoot 'audit\source-layout.json')) -cne $layoutHash -or
            (Get-Sha256 (Join-Path $PackageRoot 'audit\source-pause.json')) -cne $pauseHash) { throw 'RELEASE_SOURCE_RECEIPTS_CHANGED' }
        Assert-ColdPublicationCode $CodeRoot $build
    }
    & $guard
        $logName = 'republish-'+(Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss')+'-'+[Guid]::NewGuid().ToString('N')+'.log'
        $logPath = Join-Path $StateRoot $logName
        $safeLog = New-ColdReleaseSafeLog $logPath
        Write-ColdReleaseSafeEvent $safeLog 'helper-started' -Build $build
        Write-Host ('SOURCE RELEASE HELPER PID: '+$PID+'; source remains paused; log: '+$logPath)
        $pathHasher = [Security.Cryptography.SHA256]::Create()
        try { $lockId = [BitConverter]::ToString($pathHasher.ComputeHash(
            [Text.Encoding]::UTF8.GetBytes($PackageRoot.ToUpperInvariant()))).Replace('-','') }
        finally { $pathHasher.Dispose() }
        $mutex = New-Object Threading.Mutex($false,('Local\ProBigAColdRelease-'+$lockId))
        try { $ownsMutex = $mutex.WaitOne(0) } catch [Threading.AbandonedMutexException] {
            $ownsMutex = $true
            throw 'RELEASE_PREVIOUS_PUBLISHER_INTERRUPTED'
        }
        if (-not $ownsMutex) { throw 'RELEASE_PACKAGE_ALREADY_BEING_PUBLISHED' }
        $lease = New-ColdMigrationPowerLease
        Write-Host 'Verify the entire successful old package. No database bytes will be recopied.'
        Write-ColdReleaseSafeEvent $safeLog 'old-sha-started' -Build $ExpectedPreviousBuild
        $oldManifest = Assert-ColdPackage $PackageRoot
        if ($oldManifest.build_sha -cne $ExpectedPreviousBuild -or $oldManifest.source_host -ine $env:COMPUTERNAME -or
            $oldManifest.git_origin_fetch_url -cne 'https://github.com/MingMG/probiga.git') { throw 'RELEASE_PACKAGE_OWNER_OR_BUILD_MISMATCH' }
        $oldSeal = Get-Sha256 (Join-Path $PackageRoot 'manifest.json')
        Write-ColdReleaseSafeEvent $safeLog 'old-sha-verified' -Build $ExpectedPreviousBuild -Seal $oldSeal
        Assert-ColdReleaseBundle $CodeRoot (Join-Path $PackageRoot 'code.bundle') $ExpectedPreviousBuild
        Assert-ColdReleaseSoftware $PackageRoot
        Invoke-ColdReleaseMetadata $PackageRoot $CodeRoot $Python314
        & $guard
        # All rollback evidence is external. Never add unsealed files to payload.
        New-Item -ItemType Directory -Path $JournalRoot -ErrorAction Stop | Out-Null
        $journalCreated = $true
        Protect-LocalPath $JournalRoot
        $backupRoot = Join-Path $JournalRoot 'previous-release'
        New-Item -ItemType Directory -Path $backupRoot | Out-Null
        $backups = @()
        foreach ($name in @((Get-ColdReleaseCodeFiles) + @('manifest.json','READY'))) {
            $source = Join-Path $PackageRoot $name; $copy = Join-Path $backupRoot $name
            $hash = Get-Sha256 $source
            Copy-ColdExportExactFile $source $copy $hash | Out-Null
            $backups += [ordered]@{path=$name;sha256=$hash;bytes=[long](Get-Item -LiteralPath $copy).Length}
        }
        $releaseState = [ordered]@{format='probiga.cold-package-release.v1';status='backed-up';
            source_host=$env:COMPUTERNAME;source_user_sid=$identitySid;package_root=$PackageRoot;
            previous_build=$ExpectedPreviousBuild;previous_manifest_sha256=$oldSeal;new_build=$build;
            source_layout_sha256=$layoutHash;source_pause_sha256=$pauseHash;backups=$backups;
            production_activation=$false;restore_requested=$false;automatic_rollback=$false;
            created_at_utc=(Get-Date).ToUniversalTime().ToString('o')}
        $statePath = Join-Path $JournalRoot 'release-state.json'
        Write-Utf8 $statePath ($releaseState | ConvertTo-Json -Depth 12)
        & $guard
        if ((Get-Sha256 (Join-Path $PackageRoot 'manifest.json')) -cne $oldSeal -or
            (Get-Sha256 (Join-Path $PackageRoot 'READY')) -cne (@($backups | Where-Object path -ceq 'READY')[0].sha256)) {
            throw 'RELEASE_OLD_SEAL_CHANGED_BEFORE_WITHDRAWAL'
        }
        $releaseState.status='replacing-code'; Write-Utf8 $statePath ($releaseState | ConvertTo-Json -Depth 12)
        Remove-Item -LiteralPath (Join-Path $PackageRoot 'READY') -Force
        $readyWithdrawn = $true
        Write-ColdReleaseSafeEvent $safeLog 'ready-withdrawn' -Build $ExpectedPreviousBuild -Seal $oldSeal
        # Build externally first, then replace only six owned code artifacts.
        $newCode = Join-Path $JournalRoot 'new-release-code'
        New-Item -ItemType Directory -Path $newCode | Out-Null
        Invoke-Checked 'git' @('-C',$CodeRoot,'bundle','create',(Join-Path $newCode 'code.bundle'),'main')
        Assert-ColdReleaseBundle $CodeRoot (Join-Path $newCode 'code.bundle') $build
        foreach ($name in @(Get-ColdReleaseCodeFiles | Where-Object { $_ -ne 'code.bundle' })) {
            Copy-ColdExportExactFile (Join-Path $CodeRoot ('tools\secondary_edge\'+$name)) (Join-Path $newCode $name) | Out-Null
        }
        & $guard
        $codeFiles = @()
        foreach ($name in @(Get-ColdReleaseCodeFiles)) {
            $source = Join-Path $newCode $name; $target = Join-Path $PackageRoot $name
            Copy-Item -LiteralPath $source -Destination $target -Force
            $hash = Get-Sha256 $source
            if ((Get-Sha256 $target) -cne $hash) { throw 'RELEASE_CODE_COPY_CORRUPT' }
            $codeFiles += [ordered]@{path=$name;bytes=[long](Get-Item -LiteralPath $target).Length;sha256=$hash}
        }
        $mutable = @(Get-ColdReleaseCodeFiles)
        $preserved = @($oldManifest.files | Where-Object { ([string]$_.path).Replace('\','/') -notin $mutable })
        $files = @($preserved + $codeFiles)
        $manifest = New-ColdExportManifest $build $oldManifest.source_host $oldManifest.git_origin_fetch_url `
            $oldManifest.ai_server_url $oldManifest.database $files
        $releaseState.status='validating-new-release'; Write-Utf8 $statePath ($releaseState | ConvertTo-Json -Depth 12)
        # Contents validator authenticates every preserved file against OLD SHA,
        # not a new digest that could legitimize changed database/software bytes.
        Write-ColdReleaseSafeEvent $safeLog 'new-sha-started' -Build $build
        Publish-ColdPackage $PackageRoot $manifest $guard
        $releaseState.status='published'; $releaseState.new_manifest_sha256=Get-Sha256 (Join-Path $PackageRoot 'manifest.json')
        Write-ColdReleaseSafeEvent $safeLog 'new-sha-verified' -Build $build -Seal $releaseState.new_manifest_sha256
        $releaseState.completed_at_utc=(Get-Date).ToUniversalTime().ToString('o')
        Write-Utf8 $statePath ($releaseState | ConvertTo-Json -Depth 12)
        Write-ColdReleaseSafeEvent $safeLog 'published' -Build $build -Seal $releaseState.new_manifest_sha256
        Write-Host 'New merged-main package published. Original physical snapshot and all non-code bytes unchanged; source remains paused.'
    } catch {
        # Only controlled reason codes enter the private log: malformed JSON or
        # provider errors must never print their input contents/credential data.
        $operationFailed = $true
        $blockedReason = 'SOURCE_RELEASE_VERIFICATION_FAILED'
        $failureMessage = [string]$_.Exception.Message
        $safeReasons = @(Get-ColdReleaseSafeReasons)
        if ($failureMessage -match '^([A-Z][A-Z0-9_]{2,})(?:[:\s]|$)' -and $Matches[1] -cin $safeReasons) {
            $blockedReason = $Matches[1]
        }
        elseif ($failureMessage.StartsWith('Cold package')) { $blockedReason = 'COLD_PACKAGE_INTEGRITY_VALIDATION_FAILED' }
        elseif ($failureMessage.StartsWith('Command failed:')) { $blockedReason = 'NATIVE_OFFLINE_VALIDATION_COMMAND_FAILED' }
        if ($readyWithdrawn -and (Test-Path -LiteralPath (Join-Path $PackageRoot 'READY'))) {
            Remove-Item -LiteralPath (Join-Path $PackageRoot 'READY') -Force
        }
        if ($journalCreated) {
            Write-Utf8 (Join-Path $JournalRoot 'release-failed.json') ([ordered]@{
                status='failed';ready_withdrawn=$readyWithdrawn;automatic_rollback=$false;
                reason=$blockedReason;production_activation=$false;at_utc=(Get-Date).ToUniversalTime().ToString('o')} | ConvertTo-Json)
        }
        try { Write-ColdReleaseSafeEvent $safeLog 'blocked' -Reason $blockedReason }
        catch { Write-Warning 'SOURCE_RELEASE_SAFE_LOG_WRITE_REQUIRES_ATTENTION' }
        Write-Host ('SOURCE RELEASE BLOCKED: '+$blockedReason+'. No automatic source/database restart or rollback. Inspect this private log and preserved release journal.')
        # Do not propagate the provider's original ErrorRecord, TargetObject or
        # InnerException: formatting it could disclose malformed input values.
        throw ('SOURCE_RELEASE_BLOCKED: '+$blockedReason)
    } finally {
        $cleanupFailed = $false
        try { Remove-ColdMigrationPowerLease $lease }
        catch { $cleanupFailed = $true; Write-Warning 'SOURCE_RELEASE_POWER_CLEANUP_REQUIRES_ATTENTION' }
        finally {
            try { if ($ownsMutex) { $mutex.ReleaseMutex() } }
            catch { $cleanupFailed = $true; Write-Warning 'SOURCE_RELEASE_MUTEX_CLEANUP_REQUIRES_ATTENTION' }
            finally {
                try { if ($mutex) { $mutex.Dispose() } }
                catch { $cleanupFailed = $true; Write-Warning 'SOURCE_RELEASE_MUTEX_DISPOSAL_REQUIRES_ATTENTION' }
                finally {
                    if ($safeLog) {
                        try { Write-ColdReleaseSafeEvent $safeLog $(if ($cleanupFailed) { 'cleanup-failed' } else { 'cleanup-finished' }) }
                        catch { $cleanupFailed=$true; Write-Warning 'SOURCE_RELEASE_SAFE_LOG_WRITE_REQUIRES_ATTENTION' }
                        finally {
                            try { $safeLog.Dispose() }
                            catch { $cleanupFailed=$true; Write-Warning 'SOURCE_RELEASE_SAFE_LOG_CLOSE_REQUIRES_ATTENTION' }
                        }
                    }
                }
            }
        }
        if ($cleanupFailed -and -not $operationFailed) { throw 'SOURCE_RELEASE_CLEANUP_REQUIRES_ATTENTION' }
    }
}

function Start-ColdPackageRepublication {
    $env:PSModulePath = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\Modules;$env:ProgramFiles\WindowsPowerShell\Modules"
    Import-Module Microsoft.PowerShell.Management,Microsoft.PowerShell.Utility,Microsoft.PowerShell.Security -ErrorAction Stop
    if (-not $ExpectedUserSid) { $ExpectedUserSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value }
    foreach ($value in @($PackageRoot,$JournalRoot,$StateRoot,$CodeRoot,$ExpectedPreviousBuild,$ExpectedUserSid,
        $QmtHome,$SourceProjectRoot,$SourceProductionRoot,$Python314)) {
        if ($value -match '["\x00-\x1f]') { throw 'RELEASE_ARGUMENT_INVALID' }
    }
    if (-not $TargetInstallationNotStarted) { throw 'RELEASE_TARGET_INSTALLATION_MUST_NOT_HAVE_STARTED' }
    $principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
    if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
        if ($Elevated) { throw 'Windows administrator elevation was not granted.' }
        $native = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
        $arguments = '-NoProfile -ExecutionPolicy Bypass -File "'+$PSCommandPath+'" -Elevated -TargetInstallationNotStarted'
        foreach ($pair in @(@('PackageRoot',$PackageRoot),@('JournalRoot',$JournalRoot),@('StateRoot',$StateRoot),
            @('CodeRoot',$CodeRoot),@('ExpectedPreviousBuild',$ExpectedPreviousBuild),@('ExpectedUserSid',$ExpectedUserSid),
            @('QmtHome',$QmtHome),@('SourceProjectRoot',$SourceProjectRoot),@('SourceProductionRoot',$SourceProductionRoot),@('Python314',$Python314))) {
            $arguments += ' -'+$pair[0]+' "'+$pair[1]+'"'
        }
        $helper = Start-Process -FilePath $native -ArgumentList $arguments -Verb RunAs -WindowStyle Hidden -PassThru -Wait
        exit $helper.ExitCode
    }
    Invoke-ColdPackageRepublication
}

if ($MyInvocation.InvocationName -ne '.') { Start-ColdPackageRepublication }
