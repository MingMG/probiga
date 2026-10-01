#Requires -Version 5.1
[CmdletBinding()]
param(
    [string]$InstallRoot = 'C:\ProBigA',
    [Parameter(Mandatory=$true)][string]$PackageRoot,
    [Parameter(Mandatory=$true)][ValidatePattern('^S-1-5-21-[0-9-]+$')][string]$OriginalUserSid
)

# Do not inherit PowerShell 7's module directories into Windows PowerShell 5.1.
$env:PSModulePath = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\Modules;$env:ProgramFiles\WindowsPowerShell\Modules"
Import-Module Microsoft.PowerShell.Management,Microsoft.PowerShell.Utility,Microsoft.PowerShell.Security,ScheduledTasks -ErrorAction Stop
. (Join-Path $PSScriptRoot 'package_common.ps1')
Assert-Administrator
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script:BootstrapProcess = $null
$script:ServiceOwned = $false
$script:NeedsRestart = $false
$script:InstallStage = 'package-validation'
$serviceName = 'ProBigA-MySQL84'
$taskName = 'ProBigA Cold Migration Continue'
$powershell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'

function Write-Receipt([string]$Path, $Value) {
    $partial = $Path + '.partial'
    Write-Utf8 $partial ($Value | ConvertTo-Json -Depth 12)
    Move-Item -LiteralPath $partial -Destination $Path -Force
}

function Assert-PlainPath([string]$Path) {
    $cursor = [IO.Path]::GetFullPath($Path)
    while ($cursor) {
        if (Test-Path -LiteralPath $cursor) {
            if ((Get-Item -LiteralPath $cursor -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
                throw 'TARGET_REPARSE_PATH_REFUSED'
            }
        }
        $parent = [IO.Directory]::GetParent($cursor)
        if (-not $parent) { break }
        $cursor = $parent.FullName
    }
}

function Protect-AdministratorPath([string]$Path, [switch]$UserRead, [switch]$UserTraverse) {
    $acl = Get-Acl -LiteralPath $Path
    $acl.SetAccessRuleProtection($true,$false)
    foreach ($entry in @($acl.Access)) { [void]$acl.RemoveAccessRuleSpecific($entry) }
    foreach ($sid in @('S-1-5-18','S-1-5-32-544')) {
        $rule = New-Object Security.AccessControl.FileSystemAccessRule(
            (New-Object Security.Principal.SecurityIdentifier($sid)),
            'FullControl','ContainerInherit,ObjectInherit','None','Allow')
        [void]$acl.AddAccessRule($rule)
    }
    if ($UserRead) {
        $rule = New-Object Security.AccessControl.FileSystemAccessRule(
            (New-Object Security.Principal.SecurityIdentifier($OriginalUserSid)),
            'ReadAndExecute','ContainerInherit,ObjectInherit','None','Allow')
        [void]$acl.AddAccessRule($rule)
    }
    if ($UserTraverse) {
        # These directory-only, non-inheriting rights expose no file contents.
        $rights = [Security.AccessControl.FileSystemRights]::Traverse -bor
            [Security.AccessControl.FileSystemRights]::ReadAttributes -bor
            [Security.AccessControl.FileSystemRights]::ReadPermissions
        $rule = New-Object Security.AccessControl.FileSystemAccessRule(
            (New-Object Security.Principal.SecurityIdentifier($OriginalUserSid)),
            $rights,'None','None','Allow')
        [void]$acl.AddAccessRule($rule)
    }
    Set-Acl -LiteralPath $Path -AclObject $acl
}

function Grant-UserWrite([string]$Path) {
    $acl = Get-Acl -LiteralPath $Path
    $rule = New-Object Security.AccessControl.FileSystemAccessRule(
        (New-Object Security.Principal.SecurityIdentifier($OriginalUserSid)),
        'Modify','ContainerInherit,ObjectInherit','None','Allow')
    [void]$acl.AddAccessRule($rule)
    Set-Acl -LiteralPath $Path -AclObject $acl
}

function Copy-SealedSourceState {
    $prefix='audit/source-project-state/'
    $entries=@($manifest.files | Where-Object {$_.path.StartsWith($prefix,[StringComparison]::Ordinal)})
    if (-not $entries.Count) { throw 'SOURCE_STATE_ARCHIVE_INVENTORY_MISSING' }
    $archive=Join-Path $InstallRoot 'source-state-archive'
    $copyReceipt=Join-Path $control 'copy-source-state.json'
    Assert-PlainPath $archive
    if (Test-Path -LiteralPath $copyReceipt) {
        $prior=Get-Content -LiteralPath $copyReceipt -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($prior.manifest_sha256 -ne $seal -or $prior.status -ne 'copied') { throw 'SOURCE_STATE_ARCHIVE_OWNER_MISMATCH' }
    } else {
        New-Item -ItemType Directory -Path $archive -Force | Out-Null
        Protect-AdministratorPath $archive
        Copy-Tree (Join-Path $PackageRoot 'audit\source-project-state') $archive
    }
    Protect-AdministratorPath $archive
    $expected=New-Object 'Collections.Generic.HashSet[string]' ([StringComparer]::OrdinalIgnoreCase)
    [long]$totalBytes=0
    foreach ($entry in $entries) {
        $relative=$entry.path.Substring($prefix.Length).Replace('/', '\')
        if (-not $expected.Add($relative)) { throw 'SOURCE_STATE_ARCHIVE_DUPLICATE_PATH' }
        $file=Join-Path $archive $relative
        Assert-PlainPath $file
        if (-not (Test-Path -LiteralPath $file -PathType Leaf) -or (Get-Item -LiteralPath $file -Force).Length -ne $entry.bytes -or
            (Get-Sha256 $file) -ne $entry.sha256) { throw 'SOURCE_STATE_ARCHIVE_CONTENT_MISMATCH' }
        $totalBytes+=[long]$entry.bytes
    }
    $actual=@(Get-ChildItem -LiteralPath $archive -Recurse -Force -File)
    if ($actual.Count -ne $entries.Count) { throw 'SOURCE_STATE_ARCHIVE_EXTRA_OR_MISSING_FILE' }
    foreach ($file in $actual) {
        Assert-PlainPath $file.FullName
        if (-not $expected.Contains($file.FullName.Substring($archive.Length+1))) { throw 'SOURCE_STATE_ARCHIVE_UNSEALED_FILE' }
    }
    Write-Receipt $copyReceipt ([ordered]@{status='copied';manifest_sha256=$seal;
        source_inventory='audit/source-project-state';file_count=$entries.Count;bytes=$totalBytes;
        runtime_activation=$false;authentication_reuse=$false;verified_at=[DateTime]::UtcNow.ToString('o')})
}

function Install-Artifact([string]$File,[string]$Arguments,[string]$Name,[string]$PayloadFile=$File) {
    $signature = Get-AuthenticodeSignature -LiteralPath $File
    if ($signature.Status -ne 'Valid') { throw 'INSTALLER_SIGNATURE_INVALID' }
    if ((Get-AuthenticodeSignature -LiteralPath $PayloadFile).Status -ne 'Valid') { throw 'INSTALLER_SIGNATURE_INVALID' }
    $receiptPath = Join-Path $control "installer-$Name.json"
    if (Test-Path -LiteralPath $receiptPath) {
        $prior = Get-Content -LiteralPath $receiptPath -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($prior.sha256 -ne (Get-Sha256 $PayloadFile)) { throw 'INSTALLER_RECEIPT_MISMATCH' }
        if ($prior.exit_code -in @(3010,1641) -and $prior.boot_time -eq $bootTime) {
            $script:NeedsRestart = $true
        }
        return
    }
    $vcSatisfied = $false
    if ($Name -in @('vc-x64','vc-x86')) {
        $architecture = $Name.Substring(3)
        $required = [version]((Get-Item -LiteralPath $File).VersionInfo.FileVersion -replace '[^0-9.].*$','')
        foreach ($registry in @("HKLM:\SOFTWARE\Microsoft\VisualStudio\14.0\VC\Runtimes\$architecture",
            "HKLM:\SOFTWARE\WOW6432Node\Microsoft\VisualStudio\14.0\VC\Runtimes\$architecture")) {
            $installed = Get-ItemProperty -LiteralPath $registry -ErrorAction SilentlyContinue
            if ($installed -and $installed.Installed -eq 1 -and ([version]$installed.Version.TrimStart('v')) -ge $required) {
                $vcSatisfied = $true
            }
        }
    }
    if ($vcSatisfied) {
        Write-Receipt $receiptPath ([ordered]@{name=$Name;sha256=(Get-Sha256 $PayloadFile);exit_code=0;boot_time=$bootTime;existing_runtime_satisfied=$true})
        return
    }
    $result = Start-Process -FilePath $File -ArgumentList $Arguments -Wait -PassThru -WindowStyle Hidden
    if ($result.ExitCode -notin @(0,3010,1641)) { throw 'INSTALLER_FAILED' }
    Write-Receipt $receiptPath ([ordered]@{name=$Name;sha256=(Get-Sha256 $PayloadFile);exit_code=$result.ExitCode;boot_time=$bootTime})
    if ($result.ExitCode -in @(3010,1641)) { $script:NeedsRestart = $true }
}

function Assert-OwnedService {
    $service = Get-CimInstance Win32_Service -Filter "Name='$serviceName'" -ErrorAction Stop
    if (-not $service) { return $null }
    if ($service.PathName -notmatch $servicePattern) { throw 'MYSQL_SERVICE_OWNER_MISMATCH' }
    $script:ServiceOwned = $true
    return $service
}

function Stop-OwnedService {
    $service = Assert-OwnedService
    if (-not $service) { return }
    Set-Service -Name $serviceName -StartupType Disabled
    if ($service.State -ne 'Stopped') {
        # SCM stop is graceful. Never force-kill a database process.
        $controller = New-Object ServiceProcess.ServiceController($serviceName)
        try {
            $controller.Stop()
            $controller.WaitForStatus([ServiceProcess.ServiceControllerStatus]::Stopped,[TimeSpan]::FromSeconds(120))
        } finally { $controller.Dispose() }
    }
    $after = Assert-OwnedService
    if ($after.State -ne 'Stopped' -or $after.StartMode -ne 'Disabled' -or $after.ProcessId -ne 0) {
        throw 'MYSQL_STOP_NOT_CONFIRMED'
    }
    if (Get-NetTCPConnection -LocalPort 3306 -State Listen -ErrorAction SilentlyContinue) {
        throw 'MYSQL_LISTENER_REMAINS'
    }
}

function Stop-Bootstrap {
    if (-not $script:BootstrapProcess) { return }
    $script:BootstrapProcess.Refresh()
    if (-not $script:BootstrapProcess.HasExited) {
        # Password is read by MySQL from an administrator-only option file.
        & $mysqladmin "--defaults-file=$InstallRoot\root-client.ini" --no-login-paths --protocol=MEMORY --shared-memory-base-name=ProBigA-Cold-Admin shutdown *> $null
        if (-not $script:BootstrapProcess.WaitForExit(120000)) { throw 'MYSQL_BOOTSTRAP_STOP_NOT_CONFIRMED' }
    }
    $script:BootstrapProcess.Dispose()
    $script:BootstrapProcess = $null
}

function Assert-OwnedMysqlListener {
    $owner=Assert-OwnedService
    if (-not $owner -or $owner.State -ne 'Running' -or $owner.ProcessId -le 0) { return $false }
    $listeners=@(Get-NetTCPConnection -LocalPort 3306 -State Listen -ErrorAction SilentlyContinue)
    if (-not $listeners.Count) { return $false }
    $serviceProcess=Get-CimInstance Win32_Process -Filter "ProcessId=$($owner.ProcessId)"
    if (-not $serviceProcess -or $serviceProcess.ExecutablePath -ine $mysqld) { throw 'MYSQL_PROCESS_OWNER_MISMATCH' }
    foreach ($listener in $listeners) {
        if ($listener.LocalAddress -ne '127.0.0.1') { throw 'MYSQL_LISTENER_OWNER_MISMATCH' }
        $cursor=Get-CimInstance Win32_Process -Filter "ProcessId=$($listener.OwningProcess)"
        $seen=New-Object 'Collections.Generic.HashSet[uint32]'
        $proved=$false
        for ($depth=0;$depth -lt 16 -and $cursor;$depth++) {
            if (-not $seen.Add([uint32]$cursor.ProcessId) -or $cursor.ExecutablePath -ine $mysqld -or
                $cursor.CreationDate -lt $serviceProcess.CreationDate) { throw 'MYSQL_LISTENER_OWNER_MISMATCH' }
            if ($cursor.ProcessId -eq $owner.ProcessId) {
                if ($cursor.CreationDate -ne $serviceProcess.CreationDate) { throw 'MYSQL_PROCESS_ID_REUSED' }
                $proved=$true
                break
            }
            $childCreated=$cursor.CreationDate
            $parent=Get-CimInstance Win32_Process -Filter "ProcessId=$($cursor.ParentProcessId)"
            if (-not $parent -or $parent.CreationDate -gt $childCreated) { throw 'MYSQL_PROCESS_PARENT_NOT_PROVEN' }
            $cursor=$parent
        }
        if (-not $proved) { throw 'MYSQL_LISTENER_OWNER_MISMATCH' }
    }
    $after=Assert-OwnedService
    if ($after.State -ne 'Running' -or $after.ProcessId -ne $owner.ProcessId) { throw 'MYSQL_SERVICE_OWNER_CHANGED' }
    return $true
}

function Invoke-DatabaseVerification([string]$Phase) {
    $deadline = [DateTime]::UtcNow.AddSeconds(120)
    do {
        if ($Phase -eq 'tcp' -and -not (Assert-OwnedMysqlListener)) { Start-Sleep -Seconds 2; continue }
        $command = if ($Phase -eq 'memory') { 'verify' } else { 'verify-tls' }
        & $qmtPython -B -m tools.secondary_edge.cold_database $command --root $InstallRoot --package $PackageRoot *> $null
        if ($LASTEXITCODE -eq 0) {
            if ($Phase -eq 'tcp' -and -not (Assert-OwnedMysqlListener)) { throw 'MYSQL_LISTENER_NOT_CONFIRMED' }
            return
        }
        if ($script:BootstrapProcess) {
            $script:BootstrapProcess.Refresh()
            if ($script:BootstrapProcess.HasExited) { throw 'MYSQL_BOOTSTRAP_EXITED' }
        }
        Start-Sleep -Seconds 2
    } while ([DateTime]::UtcNow -lt $deadline)
    throw 'MYSQL_VERIFICATION_FAILED'
}

try {
    # All sealed files are checked before any target directory/service change.
    $PackageRoot = [IO.Path]::GetFullPath($PackageRoot).TrimEnd('\')
    $InstallRoot = [IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
    if ($InstallRoot -eq [IO.Path]::GetPathRoot($InstallRoot).TrimEnd('\') -or
        $InstallRoot -notmatch '^[A-Za-z]:\\' -or $InstallRoot -match '[\s"\x00-\x1f]') { throw 'TARGET_PATH_INVALID' }
    Assert-PlainPath $PackageRoot
    Assert-PlainPath $InstallRoot
    $manifest = Assert-ColdPackage $PackageRoot
    if ($env:COMPUTERNAME -eq $manifest.source_host) { throw 'SOURCE_COMPUTER_BLOCKED' }
    if (-not [Environment]::Is64BitOperatingSystem -or -not [Environment]::Is64BitProcess) { throw 'WIN64_REQUIRED' }
    $os = Get-CimInstance Win32_OperatingSystem
    if ([int]$os.BuildNumber -lt 22000) { throw 'WINDOWS11_REQUIRED' }
    if ((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory -lt 15GB) { throw 'MEMORY_INSUFFICIENT' }
    $bootTime = $os.LastBootUpTime.ToUniversalTime().ToString('o')
    $seal = Get-Sha256 (Join-Path $PackageRoot 'manifest.json')
    $marker = Join-Path $InstallRoot 'installation.json'
    $control = Join-Path $InstallRoot 'migration-control'
    $softwareReceipt = Join-Path $control 'software-status.json'
    $alreadyAllocated = [long]0
    if (Test-Path -LiteralPath $InstallRoot) {
        if (-not (Test-Path -LiteralPath $marker)) { throw 'TARGET_UNOWNED_DIRECTORY' }
        $identity = Get-Content -LiteralPath $marker -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($identity.format -ne 'probiga.windows-cold-installation.v1' -or $identity.host -ne $env:COMPUTERNAME -or
            $identity.manifest_sha256 -ne $seal -or $identity.original_user_sid -ne $OriginalUserSid -or
            $identity.build_sha -ne $manifest.build_sha -or $identity.install_root -ine $InstallRoot -or
            $identity.production_active -ne $false -or $identity.restore_requested -ne $false) { throw 'TARGET_OWNER_MISMATCH' }
        $alreadyAllocated = [long]((Get-ChildItem -LiteralPath $InstallRoot -File -Recurse | Measure-Object Length -Sum).Sum)
    }
    $volume = Get-Volume -DriveLetter $InstallRoot.Substring(0,1)
    if ($volume.FileSystem -ne 'NTFS' -or $volume.SizeRemaining -lt [Math]::Max([long]30GB,[long]$manifest.minimum_target_free_bytes-$alreadyAllocated)) {
        throw 'TARGET_CAPACITY_INSUFFICIENT'
    }
    $mysqld = Join-Path $InstallRoot 'mysql84\bin\mysqld.exe'
    $mysqladmin = Join-Path $InstallRoot 'mysql84\bin\mysqladmin.exe'
    $myIni = Join-Path $InstallRoot 'my.ini'
    $exePattern = [Regex]::Escape($mysqld)
    $iniPattern = [Regex]::Escape($myIni)
    $servicePattern = '^(?:"' + $exePattern + '"|' + $exePattern + ')\s+(?:"--defaults-file=' + $iniPattern + '"|--defaults-file="' + $iniPattern + '"|--defaults-file=' + $iniPattern + ')\s+' + $serviceName + '$'
    $existingService = Assert-OwnedService
    if ($existingService -and $existingService.State -ne 'Stopped') { throw 'TARGET_MYSQL_ALREADY_RUNNING' }
    if (Get-NetTCPConnection -LocalPort 3306 -State Listen -ErrorAction SilentlyContinue) { throw 'TARGET_PORT_IN_USE' }
    if (-not (Test-Path -LiteralPath $InstallRoot)) {
        New-Item -ItemType Directory -Path $InstallRoot | Out-Null
        Protect-AdministratorPath $InstallRoot -UserTraverse
        Write-Receipt $marker ([ordered]@{format='probiga.windows-cold-installation.v1';host=$env:COMPUTERNAME;
            source_host=$manifest.source_host;build_sha=$manifest.build_sha;manifest_sha256=$seal;install_root=$InstallRoot;
            original_user_sid=$OriginalUserSid;production_active=$false;restore_requested=$false})
        Protect-AdministratorPath $marker -UserRead
    }
    New-Item -ItemType Directory -Path $control -Force | Out-Null
    Protect-AdministratorPath $control -UserRead
    foreach ($name in @('target_entry.ps1','migrate_target.ps1','package_common.ps1','start_target_migration.cmd')) {
        Copy-Item -LiteralPath (Join-Path $PackageRoot $name) -Destination (Join-Path $control $name) -Force
    }
    $packageVolume = Get-Volume -DriveLetter $PackageRoot.Substring(0,1)
    Write-Receipt (Join-Path $control 'package-location.json') ([ordered]@{manifest_sha256=$seal;package_root=$PackageRoot;
        volume_unique_id=$packageVolume.UniqueId;relative_path=$PackageRoot.Substring(3)})
    # The task runs in the original user's desktop after THEY log in, never as SYSTEM.
    $resumeArguments = "-NoProfile -ExecutionPolicy Bypass -File `"$control\target_entry.ps1`" -InstallRoot `"$InstallRoot`" -PackageRoot `"$PackageRoot`""
    $existingTask = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    if ($existingTask -and (@($existingTask.Actions)[0].Execute -ne $powershell -or @($existingTask.Actions)[0].Arguments -ne $resumeArguments)) {
        throw 'RESUME_TASK_OWNER_MISMATCH'
    }
    $action = New-ScheduledTaskAction -Execute $powershell -Argument $resumeArguments -WorkingDirectory $control
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $OriginalUserSid
    $principal = New-ScheduledTaskPrincipal -UserId $OriginalUserSid -LogonType Interactive -RunLevel Limited
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -ExecutionTimeLimit ([TimeSpan]::Zero) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
    # The original desktop user may remove only this limited, no-password continuation task.
    $taskService = New-Object -ComObject 'Schedule.Service'
    $taskService.Connect()
    $registeredTask = $taskService.GetFolder('\').GetTask($taskName)
    $registeredTask.SetSecurityDescriptor("D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;$OriginalUserSid)",0)
    if (Test-Path -LiteralPath $softwareReceipt) {
        $complete = Get-Content -LiteralPath $softwareReceipt -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($complete.status -eq 'paused-installed' -and $complete.manifest_sha256 -eq $seal) {
            Stop-OwnedService
            exit 0
        }
    }
    Write-Receipt $softwareReceipt ([ordered]@{status='installing';manifest_sha256=$seal;production_active=$false;restore_requested=$false})
    $script:InstallStage = 'offline-software'
    $installers = Join-Path $PackageRoot 'software\installers'
    Install-Artifact (Join-Path $installers 'vc_x64.exe') '/install /quiet /norestart' 'vc-x64'
    Install-Artifact (Join-Path $installers 'vc_x86.exe') '/install /quiet /norestart' 'vc-x86'
    $py313 = Join-Path $InstallRoot 'Python313\python.exe'
    $py314 = Join-Path $InstallRoot 'Python314\python.exe'
    Install-Artifact (Join-Path $installers 'python313.exe') ("/quiet InstallAllUsers=0 Include_launcher=0 Include_test=0 Include_pip=1 PrependPath=0 TargetDir=`"$InstallRoot\Python313`"") 'python313'
    Install-Artifact (Join-Path $installers 'python314.exe') ("/quiet InstallAllUsers=0 Include_launcher=0 Include_test=0 Include_pip=1 PrependPath=0 TargetDir=`"$InstallRoot\Python314`"") 'python314'
    $gitRoot = Join-Path $env:ProgramFiles 'Git'
    $git = Join-Path $gitRoot 'cmd\git.exe'
    Install-Artifact (Join-Path $installers 'git.exe') ("/VERYSILENT /NORESTART /SP- /DIR=`"$gitRoot`"") 'git'
    $chrome = Join-Path $env:ProgramFiles 'Google\Chrome\Application\chrome.exe'
    $chromeMsi = Join-Path $installers 'chrome.msi'
    if ((Get-AuthenticodeSignature -LiteralPath $chromeMsi).Status -ne 'Valid') { throw 'INSTALLER_SIGNATURE_INVALID' }
    Install-Artifact (Join-Path $env:SystemRoot 'System32\msiexec.exe') ("/i `"$chromeMsi`" /qn /norestart") 'chrome' $chromeMsi
    if ($script:NeedsRestart) {
        Write-Receipt $softwareReceipt ([ordered]@{status='needs-restart';manifest_sha256=$seal;boot_time=$bootTime;production_active=$false;restore_requested=$false})
        Write-Host 'Restart Windows when convenient. Migration resumes automatically after the original user signs in.'
        exit 3010
    }
    if ((& $py313 --version) -ne 'Python 3.13.14' -or (& $py314 --version) -ne 'Python 3.14.3' -or -not (Test-Path -LiteralPath $chrome)) { throw 'SOFTWARE_IDENTITY_INVALID' }
    $env:PATH = "$(Split-Path $git -Parent);$env:PATH"
    $code = Join-Path $InstallRoot 'code'
    $script:InstallStage = 'exact-build-and-python-environments'
    if (-not (Test-Path -LiteralPath $code)) { Invoke-Checked $git @('clone','--branch','main',(Join-Path $PackageRoot 'code.bundle'),$code) }
    if ((& $git -C $code rev-parse HEAD).Trim() -ne $manifest.build_sha -or (& $git -C $code branch --show-current).Trim() -ne 'main') { throw 'CODE_BUILD_MISMATCH' }
    if (@(& $git -C $code status --porcelain --untracked-files=normal).Count) { throw 'CODE_WORKTREE_NOT_CLEAN' }
    if ($manifest.git_origin_fetch_url -ne 'https://github.com/MingMG/probiga.git') { throw 'CODE_REMOTE_INVALID' }
    Invoke-Checked $git @('-C',$code,'remote','set-url','origin',$manifest.git_origin_fetch_url)
    $qmtPython = Join-Path $code 'runtime\qmt-py313\Scripts\python.exe'
    $appPython = Join-Path $code '.venv\Scripts\python.exe'
    if (-not (Test-Path -LiteralPath $qmtPython)) { Invoke-Checked $py313 @('-m','venv',(Join-Path $code 'runtime\qmt-py313')) }
    if (-not (Test-Path -LiteralPath $appPython)) { Invoke-Checked $py314 @('-m','venv',(Join-Path $code '.venv')) }
    Invoke-Checked $qmtPython @('-m','pip','install','--no-index','--find-links',(Join-Path $PackageRoot 'wheels313'),'--only-binary=:all:','--require-hashes','-r',(Join-Path $code 'deploy\qmt_windows_requirements.lock'))
    Invoke-Checked $appPython @('-m','pip','install','--no-index','--find-links',(Join-Path $PackageRoot 'wheels314'),'--only-binary=:all:','-r',(Join-Path $code 'deploy\windows_app_requirements.txt'))
    Invoke-Checked $qmtPython @('-m','pip','check')
    Invoke-Checked $appPython @('-m','pip','check')
    foreach ($folder in @($code,(Join-Path $InstallRoot 'Python313'),(Join-Path $InstallRoot 'Python314'))) {
        Protect-AdministratorPath $folder -UserRead
    }
    foreach ($name in @('qmt','mysql84','codex')) {
        $script:InstallStage = 'copy-' + $name
        $copyReceipt = Join-Path $control "copy-$name.json"
        if (-not (Test-Path -LiteralPath $copyReceipt)) {
            Copy-Tree (Join-Path $PackageRoot "software\$name") (Join-Path $InstallRoot $name)
            Write-Receipt $copyReceipt ([ordered]@{manifest_sha256=$seal;name=$name;status='copied'})
        } elseif ((Get-Content -LiteralPath $copyReceipt -Raw -Encoding UTF8 | ConvertFrom-Json).manifest_sha256 -ne $seal) { throw 'COPY_RECEIPT_MISMATCH' }
    }
    $auth = Join-Path $InstallRoot 'auth'
    New-Item -ItemType Directory -Path $auth -Force | Out-Null
    Protect-AdministratorPath $auth -UserRead
    Grant-UserWrite $auth
    foreach ($folder in @((Join-Path $InstallRoot 'qmt'),(Join-Path $InstallRoot 'codex'))) {
        Protect-AdministratorPath $folder -UserRead
    }
    # Preserve all stopped project data (including SQLite, caches and reports)
    # as a protected, hash-verified archive. Never activate archived credentials,
    # machine-bound state, legacy runtimes or source ownership grants.
    $script:InstallStage='sealed-source-state-archive'
    Copy-SealedSourceState
    # Histories are preserved as archives. This is not a claim of Codex import/continuity.
    $histories = Join-Path $PackageRoot 'audit\codex-production-threads'
    if (Test-Path -LiteralPath $histories) { Copy-Tree $histories (Join-Path $auth 'source-codex-history-archive') }
    Push-Location $code
    try {
        $script:InstallStage = 'cold-database-materialization'
        Invoke-Checked $qmtPython @('-B','-m','tools.secondary_edge.cold_database','restore','--root',$InstallRoot,'--package',$PackageRoot)
        $bootstrapReceipt = Get-Content -LiteralPath (Join-Path $InstallRoot 'database-status.json') -Raw -Encoding UTF8 | ConvertFrom-Json
        if ($bootstrapReceipt.status -ne 'paused-ready') {
            $bootstrapArguments = "--defaults-file=`"$myIni`" --init-file=`"$InstallRoot\bootstrap-init.sql`" --persisted-globals-load=OFF --skip-networking --shared-memory --shared-memory-base-name=ProBigA-Cold-Admin"
            $script:BootstrapProcess = Start-Process -FilePath $mysqld -ArgumentList $bootstrapArguments -PassThru -WindowStyle Hidden
            $script:InstallStage = 'private-database-bootstrap-verification'
            Invoke-DatabaseVerification 'memory'
            Stop-Bootstrap
            # The registered, normal command never includes init-file or authentication material.
            # Registration itself is manual, eliminating any reboot/autostart
            # window between creation and the controlled verification lifecycle.
            if (-not (Assert-OwnedService)) { Invoke-Checked $mysqld @('--install-manual',$serviceName,"--defaults-file=$myIni") }
            [void](Assert-OwnedService)
            Set-Service -Name $serviceName -StartupType Manual
            $script:InstallStage = 'owned-service-tls-verification'
            Start-Service -Name $serviceName
            [void](Assert-OwnedService)
            Invoke-DatabaseVerification 'tcp'
            Stop-OwnedService
            $init = Join-Path $InstallRoot 'bootstrap-init.sql'
            if (Test-Path -LiteralPath $init) { Remove-Item -LiteralPath $init -Force }
        } else { Stop-OwnedService }
    } finally {
        try { Stop-Bootstrap } finally { if ($script:ServiceOwned) { Stop-OwnedService }; Pop-Location }
    }
    $stopped = Assert-OwnedService
    if (-not $stopped -or $stopped.State -ne 'Stopped' -or $stopped.StartMode -ne 'Disabled') { throw 'MYSQL_PAUSE_NOT_CONFIRMED' }
    Write-Receipt $softwareReceipt ([ordered]@{status='paused-installed';manifest_sha256=$seal;host=$env:COMPUTERNAME;
        build_sha=$manifest.build_sha;database_stopped=$true;database_start_mode='Disabled';production_active=$false;
        restore_requested=$false;authentication_pending=$true;windows_time_zone_id=(Get-TimeZone -ErrorAction Stop).Id;
        clock_calibration='NOT_VERIFIED_REQUIRES_PRODUCTION_RESUME_GATE';completed_at=[DateTime]::UtcNow.ToString('o')})
    exit 0
} catch {
    # Never echo native provider output, database statements or secret-bearing exception text.
    if ($script:ServiceOwned) { try { Stop-OwnedService } catch { Write-Host 'TARGET_DATABASE_STOP_REQUIRES_ATTENTION' } }
    if (Get-Variable softwareReceipt -ErrorAction SilentlyContinue) {
        try { Write-Receipt $softwareReceipt ([ordered]@{status='blocked';stage=$script:InstallStage;
            production_active=$false;restore_requested=$false;manifest_sha256=$seal}) } catch { }
    }
    Write-Host 'TARGET_INSTALLATION_BLOCKED. Source production remains paused; no automatic recovery is performed.'
    exit 1
}
