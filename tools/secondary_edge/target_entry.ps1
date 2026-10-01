#Requires -Version 5.1
[CmdletBinding()]
param([string]$InstallRoot='',[string]$PackageRoot=$PSScriptRoot)

$env:PSModulePath = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\Modules;$env:ProgramFiles\WindowsPowerShell\Modules"
Import-Module Microsoft.PowerShell.Management,Microsoft.PowerShell.Utility,Microsoft.PowerShell.Security,ScheduledTasks -ErrorAction Stop
. (Join-Path $PSScriptRoot 'package_common.ps1')
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$originalSid=[Security.Principal.WindowsIdentity]::GetCurrent().User.Value
$script:MigrationMutex=$null
$script:MigrationLockHeld=$false
$script:EntryStage='package-validation'

function Write-UserReceipt($Value) {
    $partial=$statusPath+'.partial'
    Write-Utf8 $partial ($Value | ConvertTo-Json -Depth 8)
    Move-Item -LiteralPath $partial -Destination $statusPath -Force
}

function Resolve-PackageLocation([string]$Root) {
    if (Test-Path -LiteralPath (Join-Path $Root 'manifest.json')) { return [IO.Path]::GetFullPath($Root).TrimEnd('\') }
    if ([string]::IsNullOrWhiteSpace($InstallRoot)) { throw 'MIGRATION_DISK_REQUIRED' }
    $locationFile=Join-Path $InstallRoot 'migration-control\package-location.json'
    if (-not (Test-Path -LiteralPath $locationFile)) { throw 'MIGRATION_DISK_REQUIRED' }
    $location=Get-Content -LiteralPath $locationFile -Raw -Encoding UTF8 | ConvertFrom-Json
    $volumes=@(Get-Volume | Where-Object {$_.UniqueId -eq $location.volume_unique_id -and $_.DriveLetter})
    if ($volumes.Count -ne 1) { throw 'MIGRATION_DISK_REQUIRED' }
    $resolved=Join-Path ($volumes[0].DriveLetter.ToString()+':\') $location.relative_path
    if ((Get-Sha256 (Join-Path $resolved 'manifest.json')) -ne $location.manifest_sha256) { throw 'MIGRATION_DISK_MISMATCH' }
    return $resolved
}

function Read-PublicManifest([string]$Root) {
    # NTFS payload ACLs belong to the source SID. The original target user may
    # read only these public, non-secret seals; UAC performs the full payload check.
    $manifestPath=Join-Path $Root 'manifest.json'
    $public=Get-Content -LiteralPath $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($public.format -ne 'probiga.windows-cold-migration.v2' -or $public.build_sha -notmatch '^[0-9a-f]{40}$' -or
        -not $public.source_host -or $public.source_paused -ne $true -or $public.production_activation -ne $false -or
        $public.restore_requested -ne $false) { throw 'PUBLIC_PACKAGE_IDENTITY_INVALID' }
    $expected=$public.build_sha+' '+(Get-Sha256 $manifestPath)
    if ((Get-Content -LiteralPath (Join-Path $Root 'READY') -Raw -Encoding UTF8).Trim() -cne $expected) {
        throw 'PUBLIC_PACKAGE_SEAL_MISMATCH'
    }
    return $public
}

function Select-TargetInstallRoot($Manifest,[string]$ManifestSha256) {
    [long]$requiredBytes=$Manifest.minimum_target_free_bytes
    if ($requiredBytes -lt 250GB) { throw 'TARGET_CAPACITY_CONTRACT_INVALID' }
    $owned=New-Object 'Collections.Generic.List[string]'
    $available=New-Object 'Collections.Generic.List[object]'
    foreach ($volume in @(Get-Volume -ErrorAction Stop)) {
        if (-not $volume.DriveLetter -or $volume.FileSystem -ne 'NTFS' -or $volume.HealthStatus -ne 'Healthy' -or
            $volume.DriveType -ne 'Fixed') { continue }
        try {
            $partitions=@(Get-Partition -DriveLetter $volume.DriveLetter -ErrorAction Stop)
            if ($partitions.Count -ne 1) { continue }
            $disks=@(Get-Disk -Number $partitions[0].DiskNumber -ErrorAction Stop)
            # A fixed-volume label alone is insufficient: USB disks may report it.
            # Only known internal storage buses are eligible; removable, unknown,
            # network and file-backed/virtual disks are never selected.
            if ($disks.Count -ne 1 -or $disks[0].BusType -notin @('ATA','SATA','SAS','SCSI','RAID','NVMe','SCM') -or
                $disks[0].HealthStatus -ne 'Healthy' -or $disks[0].IsOffline -or $disks[0].IsReadOnly) { continue }
            $root=$volume.DriveLetter.ToString().ToUpperInvariant()+':\ProBigA'
            if (Test-Path -LiteralPath $root) {
                $directory=Get-Item -LiteralPath $root -Force -ErrorAction Stop
                if (-not $directory.PSIsContainer -or $directory.Attributes -band [IO.FileAttributes]::ReparsePoint) { continue }
                $marker=Join-Path $root 'installation.json'
                if (-not (Test-Path -LiteralPath $marker -PathType Leaf) -or
                    (Get-Item -LiteralPath $marker -Force -ErrorAction Stop).Attributes -band [IO.FileAttributes]::ReparsePoint) { continue }
                $identity=Get-Content -LiteralPath $marker -Raw -Encoding UTF8 -ErrorAction Stop | ConvertFrom-Json
                if ($identity.format -eq 'probiga.windows-cold-installation.v1' -and $identity.host -eq $env:COMPUTERNAME -and
                    $identity.original_user_sid -ceq $originalSid -and $identity.manifest_sha256 -ceq $ManifestSha256 -and
                    $identity.build_sha -ceq $Manifest.build_sha -and $identity.install_root -ieq $root -and
                    $identity.source_host -eq $Manifest.source_host -and $identity.production_active -eq $false -and
                    $identity.restore_requested -eq $false) { $owned.Add($root) }
                # Unknown or mismatched directories remain completely untouched.
                continue
            }
            if ($volume.SizeRemaining -ge $requiredBytes) {
                $available.Add([pscustomobject]@{root=$root;free_bytes=[long]$volume.SizeRemaining})
            }
        } catch { continue } # Unreadable/unsupported storage is not a safe candidate.
    }
    if ($owned.Count -gt 1) { throw 'TARGET_MULTIPLE_OWNED_INSTALLATIONS' }
    if ($owned.Count -eq 1) { return $owned[0] }
    if (-not $available.Count) { throw 'TARGET_NO_SAFE_INTERNAL_VOLUME' }
    return ($available | Sort-Object -Property @{Expression='free_bytes';Descending=$true},@{Expression='root';Descending=$false} | Select-Object -First 1).root
}

function Invoke-AuthProbe([string]$Program,[string]$Script,[string[]]$Parameters) {
    # Only fixed readiness codes are accepted; no account, provider text or secrets are logged.
    & $Program -B -c $Script @Parameters *> $null
    return $LASTEXITCODE
}

function Assert-TargetPaused {
    $serviceName='ProBigA-MySQL84'
    $mysqld=Join-Path $InstallRoot 'mysql84\bin\mysqld.exe'
    $myIni=Join-Path $InstallRoot 'my.ini'
    $exePattern=[Regex]::Escape($mysqld)
    $iniPattern=[Regex]::Escape($myIni)
    $servicePattern='^(?:"'+$exePattern+'"|'+$exePattern+')\s+(?:"--defaults-file='+$iniPattern+'"|--defaults-file="'+$iniPattern+'"|--defaults-file='+$iniPattern+')\s+'+$serviceName+'$'
    $service=Get-CimInstance Win32_Service -Filter "Name='$serviceName'" -ErrorAction Stop
    if (-not $service -or $service.PathName -notmatch $servicePattern -or $service.State -ne 'Stopped' -or
        $service.StartMode -ne 'Disabled' -or $service.ProcessId -ne 0 -or
        (Get-NetTCPConnection -LocalPort 3306 -State Listen -ErrorAction SilentlyContinue)) { throw 'TARGET_DATABASE_NOT_PAUSED' }
    if (Get-CimInstance Win32_Process -Filter "Name='XtItClient.exe'" -ErrorAction Stop) { throw 'QMT_MUST_REMAIN_STOPPED' }
}

function Remove-OwnedContinuation {
    $continuation=Get-ScheduledTask -TaskName 'ProBigA Cold Migration Continue' -ErrorAction SilentlyContinue
    if (-not $continuation) { return }
    $location=Get-Content -LiteralPath (Join-Path $InstallRoot 'migration-control\package-location.json') -Raw -Encoding UTF8 | ConvertFrom-Json
    $entry=Join-Path $InstallRoot 'migration-control\target_entry.ps1'
    $expected="-NoProfile -ExecutionPolicy Bypass -File `"$entry`" -InstallRoot `"$InstallRoot`" -PackageRoot `"$($location.package_root)`""
    $expectedExe=Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
    if (@($continuation.Actions).Count -ne 1 -or @($continuation.Actions)[0].Arguments -ne $expected -or
        @($continuation.Actions)[0].Execute -ne $expectedExe -or $continuation.Principal.RunLevel -ne 'Limited') {
        throw 'RESUME_TASK_OWNER_MISMATCH'
    }
    Unregister-ScheduledTask -TaskName 'ProBigA Cold Migration Continue' -Confirm:$false
}

$deepseekProbe=@'
import sys
from pathlib import Path
from tools.run_codex_web_bridge import DeepSeekChromeSession, CdpConnection, COMPOSER_STATE_SCRIPT
try:
    page = DeepSeekChromeSession(Path(sys.argv[1])).page()
    connection = CdpConnection(page["webSocketDebuggerUrl"])
    try:
        state = connection.evaluate(COMPOSER_STATE_SCRIPT) or {}
        sys.exit(0 if state.get("ready") and not state.get("captcha") else 10)
    finally:
        connection.close()
except Exception:
    sys.exit(20)
'@

try {
    if (-not [string]::IsNullOrWhiteSpace($InstallRoot)) {
        $InstallRoot=[IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
        if ($InstallRoot -notmatch '^[A-Za-z]:\\' -or $InstallRoot -eq [IO.Path]::GetPathRoot($InstallRoot).TrimEnd('\') -or
            $InstallRoot -match '[\s"\x00-\x1f]') { throw 'TARGET_PATH_INVALID' }
    }
    $PackageRoot=Resolve-PackageLocation $PackageRoot
    Write-Host 'Checking the public migration identity. Windows permission approval precedes the complete protected-payload verification.'
    $manifest=Read-PublicManifest $PackageRoot
    if ($env:COMPUTERNAME -eq $manifest.source_host) { throw 'SOURCE_COMPUTER_BLOCKED' }
    if (-not [Environment]::Is64BitOperatingSystem -or -not [Environment]::Is64BitProcess) { throw 'WIN64_REQUIRED' }
    if ([int](Get-CimInstance Win32_OperatingSystem).BuildNumber -lt 22000) { throw 'WINDOWS11_REQUIRED' }
    $seal=Get-Sha256 (Join-Path $PackageRoot 'manifest.json')
    if ([string]::IsNullOrWhiteSpace($InstallRoot)) {
        $script:EntryStage='target-storage-selection'
        $InstallRoot=Select-TargetInstallRoot $manifest $seal
        Write-Host "Selected safe internal installation root: $InstallRoot"
        $script:EntryStage='package-validation'
    }
    $script:MigrationMutex=New-Object Threading.Mutex($false,"Local\ProBigA.ColdMigration.$originalSid")
    try { $script:MigrationLockHeld=$script:MigrationMutex.WaitOne(0) }
    catch [Threading.AbandonedMutexException] { $script:MigrationLockHeld=$true }
    if (-not $script:MigrationLockHeld) { Write-Host 'This migration is already running in your session.'; exit 0 }
    $marker=Join-Path $InstallRoot 'installation.json'
    $softwarePath=Join-Path $InstallRoot 'migration-control\software-status.json'
    if (Test-Path -LiteralPath $InstallRoot) {
        if (-not (Test-Path -LiteralPath $marker)) { throw 'TARGET_UNOWNED_DIRECTORY' }
        $identity=Get-Content -LiteralPath $marker -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($identity.format -ne 'probiga.windows-cold-installation.v1' -or $identity.host -ne $env:COMPUTERNAME -or
            $identity.original_user_sid -ne $originalSid -or $identity.manifest_sha256 -ne $seal -or
            $identity.install_root -ine $InstallRoot -or $identity.production_active -ne $false -or
            $identity.restore_requested -ne $false) { throw 'TARGET_OWNER_MISMATCH' }
    }
    $installed=$false
    if (Test-Path -LiteralPath $softwarePath) {
        $prior=Get-Content -LiteralPath $softwarePath -Raw -Encoding UTF8 | ConvertFrom-Json
        $installed=$prior.status -eq 'paused-installed' -and $prior.manifest_sha256 -eq $seal
    }
    if (-not $installed) {
        $script:EntryStage='software-installation'
        Write-Host 'Approve the Windows permission request. Software and the stopped database will be installed without starting production.'
        $powershell=Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
        $helper=Join-Path $PackageRoot 'migrate_target.ps1'
        $arguments="-NoProfile -ExecutionPolicy Bypass -File `"$helper`" -InstallRoot `"$InstallRoot`" -PackageRoot `"$PackageRoot`" -OriginalUserSid `"$originalSid`""
        $elevated=Start-Process -FilePath $powershell -ArgumentList $arguments -Verb RunAs -PassThru -Wait -WindowStyle Hidden
        if ($elevated.ExitCode -eq 3010) {
            Write-Host 'A Windows restart is required. The same migration continues automatically when you next sign in.'
            exit 3010
        }
        if ($elevated.ExitCode -ne 0) { throw 'TARGET_ADMIN_STAGE_FAILED' }
    }
    $software=Get-Content -LiteralPath $softwarePath -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($software.status -ne 'paused-installed' -or $software.manifest_sha256 -ne $seal -or
        $software.database_stopped -ne $true -or $software.database_start_mode -ne 'Disabled') { throw 'TARGET_INSTALL_NOT_PAUSED' }
    Assert-TargetPaused
    $authRoot=Join-Path $InstallRoot 'auth'
    $statusPath=Join-Path $authRoot 'migration-status.json'
    $code=Join-Path $InstallRoot 'code'
    $qmtPython=Join-Path $code 'runtime\qmt-py313\Scripts\python.exe'
    $appPython=Join-Path $code '.venv\Scripts\python.exe'
    $qmtHome=Join-Path $InstallRoot 'qmt'
    $codex=Join-Path $InstallRoot 'codex\codex.exe'
    $deepseek=Join-Path $authRoot 'deepseek-profile'
    $codexHome=Join-Path $authRoot 'codex-home'
    if (Test-Path -LiteralPath $statusPath) {
        $completed=Get-Content -LiteralPath $statusPath -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($completed.status -eq 'paused-installed' -and $completed.manifest_sha256 -eq $seal -and
            $completed.authentication_pending -eq $false -and $completed.production_active -eq $false) {
            Remove-OwnedContinuation
            Write-Host 'The sealed migration is already installed and paused. No application is started.'
            exit 0
        }
    }
    New-Item -ItemType Directory -Path $codexHome,$deepseek -Force | Out-Null
    # A stopped physical QMT image is checked without launching the vendor client.
    # Its saved model configuration can auto-run when opened; login is deferred
    # until the separately authorized production-restoration workflow.
    $qmtExe=Join-Path $qmtHome 'bin.x64\XtItClient.exe'
    $script:EntryStage='stopped-qmt-file-verification'
    if ((Get-AuthenticodeSignature -LiteralPath $qmtExe).Status -ne 'Valid' -or
        (Get-Item -LiteralPath $qmtExe).VersionInfo.FileVersion -ne '2.1.19.0') { throw 'QMT_VENDOR_IDENTITY_INVALID' }
    foreach ($dependency in @('python36.dll','python36.zip','Qt5Core.dll','Qt5Gui.dll','Qt5Widgets.dll','Qt5Network.dll')) {
        if (-not (Test-Path -LiteralPath (Join-Path $qmtHome "bin.x64\$dependency") -PathType Leaf)) { throw 'QMT_DEPENDENCY_MISSING' }
    }
    $oldCodexHome=$env:CODEX_HOME
    $oldChrome=$env:PROBIGA_DEEPSEEK_CHROME_EXE
    $env:CODEX_HOME=$codexHome
    $env:PROBIGA_DEEPSEEK_CHROME_EXE=Join-Path $env:ProgramFiles 'Google\Chrome\Application\chrome.exe'
    Write-UserReceipt ([ordered]@{status='authenticating';host=$env:COMPUTERNAME;manifest_sha256=$seal;
        production_active=$false;restore_requested=$false;database_stopped=$true;authentication_pending=$true})
    Push-Location $code
    try {
        Write-Host 'QMT vendor files/dependencies are present. The client and its strategies remain stopped; broker login is deferred until production restoration.'
        $script:EntryStage='codex-account-login'
        & $codex login status *> $null
        if ($LASTEXITCODE -ne 0) {
            Write-Host 'Complete the official Codex account login in the browser. This target has its own login cache.'
            & $codex login
            if ($LASTEXITCODE -ne 0) { throw 'CODEX_AUTHENTICATION_NOT_COMPLETED' }
        }
        & $codex login status *> $null
        if ($LASTEXITCODE -ne 0) { throw 'CODEX_AUTHENTICATION_NOT_VERIFIED' }
        Write-Host 'Complete DeepSeek login/verification in its dedicated Chrome window. The installer continues automatically.'
        $script:EntryStage='deepseek-web-account-login'
        $deadline=[DateTime]::UtcNow.AddMinutes(30)
        do {
            $deepseekStatus=Invoke-AuthProbe $appPython $deepseekProbe @($deepseek)
            if ($deepseekStatus -eq 0) { break }
            if ($deepseekStatus -ne 10) { throw 'DEEPSEEK_AUTHENTICATION_NOT_VERIFIABLE' }
            Start-Sleep -Seconds 2
        } while ([DateTime]::UtcNow -lt $deadline)
        if ($deepseekStatus -ne 0) { throw 'DEEPSEEK_AUTHENTICATION_NOT_COMPLETED' }
    } finally { Pop-Location; $env:CODEX_HOME=$oldCodexHome; $env:PROBIGA_DEEPSEEK_CHROME_EXE=$oldChrome }
    $script:EntryStage='final-pause-verification'
    Assert-TargetPaused
    # Read-only auth readiness is not production/history-continuity acceptance.
    Write-UserReceipt ([ordered]@{status='paused-installed';host=$env:COMPUTERNAME;build_sha=$manifest.build_sha;
        manifest_sha256=$seal;production_active=$false;restore_requested=$false;database_stopped=$true;
        authentication_pending=$false;qmt_client_started=$false;qmt_authentication='deferred-until-restoration';
        codex_authentication='local-login-confirmed';deepseek_authentication='webpage-login-ready';
        ai_generation_verified=$false;
        codex_history_continuity='not-verified';production_tunnel_started=$false;production_workers_started=$false;
        completed_at=[DateTime]::UtcNow.ToString('o')})
    Write-Host "Migration installation and account readiness are complete: $statusPath"
    Remove-OwnedContinuation
    Write-Host 'PAUSED-INSTALLED. Database is stopped/Disabled. No QMT client/strategy, collector, AI queue worker or production tunnel is started.'
    Write-Host 'Project production remains paused. Linux services were not moved or restarted. Restoring production requires a separate coordinated operation.'
    exit 0
} catch {
    if ($script:EntryStage -eq 'target-storage-selection') {
        Write-Host 'Automatic storage selection is blocked: a unique owned installation or a healthy internal NTFS disk with sufficient free space is required. Existing directories and disk partitions were not changed.'
    }
    if (Get-Variable statusPath -ErrorAction SilentlyContinue) {
        $installedStatus=if($script:EntryStage -in @('codex-account-login','deepseek-web-account-login')){'paused-installed'}else{'needs-attention'}
        try { Write-UserReceipt ([ordered]@{status=$installedStatus;stage=$script:EntryStage;host=$env:COMPUTERNAME;manifest_sha256=$seal;
            production_active=$false;restore_requested=$false;authentication_pending=$true;
            qmt_authentication='deferred-until-restoration';ai_generation_verified=$false}) } catch { }
    }
    Write-Host 'MIGRATION NEEDS ATTENTION. Nothing is restored to production automatically.'
    exit 1
} finally {
    if ($script:MigrationLockHeld) { $script:MigrationMutex.ReleaseMutex() }
    if ($script:MigrationMutex) { $script:MigrationMutex.Dispose() }
}
