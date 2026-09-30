#Requires -Version 5.1
[CmdletBinding()]
param([string]$InstallRoot = 'C:\ProBigAEdge', [string]$PackageRoot = $PSScriptRoot)
. (Join-Path $PSScriptRoot 'package_common.ps1')
Assert-Administrator
$PackageRoot = [IO.Path]::GetFullPath($PackageRoot)
$InstallRoot = [IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
if ($InstallRoot -eq [IO.Path]::GetPathRoot($InstallRoot).TrimEnd('\')) { throw 'Do not install at a drive root.' }
if ($InstallRoot.Contains('"') -or $InstallRoot.Contains("`n")) { throw 'Unsafe install path.' }
if ($InstallRoot -match '\s') { throw 'Use an installation path without whitespace, e.g. C:\ProBigAEdge.' }
Write-Host 'Verify offline package hashes before making changes. This may take time.'
$manifest = Assert-Package $PackageRoot
if ($env:COMPUTERNAME -eq $manifest.source_host) { throw 'SOURCE COMPUTER BLOCKED: install only on the old computer.' }
if (-not [Environment]::Is64BitOperatingSystem) { throw '64-bit Windows is required.' }
$os = Get-CimInstance Win32_OperatingSystem
if ([int]$os.BuildNumber -lt 22000) { throw 'This package was prepared for Windows 11.' }
$memory = (Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
if ($memory -lt 15GB) { throw 'At least 16 GB installed RAM is required.' }
$volume = Get-Volume -DriveLetter ([IO.Path]::GetPathRoot($InstallRoot).Substring(0,1))
$marker = Join-Path $InstallRoot 'installation.json'
$alreadyAllocated = [long]0
if (Test-Path -LiteralPath $InstallRoot) {
    if (-not (Test-Path -LiteralPath $marker)) { throw 'Existing unowned directory: choose a new InstallRoot.' }
    $identity = Get-Content -LiteralPath $marker -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($identity.build_sha -ne $manifest.build_sha -or $identity.host -ne $env:COMPUTERNAME) { throw 'Installation identity mismatch.' }
    $alreadyAllocated = [long]((Get-ChildItem -LiteralPath $InstallRoot -File -Recurse | Measure-Object Length -Sum).Sum)
}
$remainingBudget = [Math]::Max([long]30GB, ([long]$manifest.minimum_target_free_bytes - $alreadyAllocated))
if ($volume.FileSystem -ne 'NTFS' -or $volume.SizeRemaining -lt $remainingBudget) {
    throw 'Target needs NTFS and 450 GiB initial capacity, less already installed files, with 30 GiB working headroom.'
}
if (-not (Test-Path -LiteralPath $InstallRoot)) {
    New-Item -ItemType Directory -Path $InstallRoot | Out-Null
    Protect-LocalPath $InstallRoot
    Write-Utf8 $marker ([ordered]@{host=$env:COMPUTERNAME;build_sha=$manifest.build_sha;production_active=$false} | ConvertTo-Json)
}
Write-Host '1/5 Install VC, Python 3.13/3.14, Git and Chrome (offline, no forced restart).'
$installers = Join-Path $PackageRoot 'software\installers'
function Install-Exe([string]$File, [string]$ArgumentLine) {
    $signature = Get-AuthenticodeSignature -LiteralPath $File
    if ($signature.Status -ne 'Valid') { throw "Installer signature invalid: $File" }
    $result = Start-Process -FilePath $File -ArgumentList $ArgumentLine -Wait -PassThru -WindowStyle Hidden
    if ($result.ExitCode -notin @(0,3010,1641)) { throw "Installation failed: $File (exit $($result.ExitCode))" }
    if ($result.ExitCode -ne 0) { $script:NeedsRestart = $true }
}
$NeedsRestart = $false
Install-Exe (Join-Path $installers 'vc_x64.exe') '/install /quiet /norestart'
Install-Exe (Join-Path $installers 'vc_x86.exe') '/install /quiet /norestart'
$py313 = Join-Path $InstallRoot 'Python313\python.exe'
$py314 = Join-Path $InstallRoot 'Python314\python.exe'
if (-not (Test-Path -LiteralPath $py313)) {
    Install-Exe (Join-Path $installers 'python313.exe') ("/quiet InstallAllUsers=0 Include_launcher=0 Include_test=0 Include_pip=1 PrependPath=0 TargetDir=`"$InstallRoot\Python313`"")
}
if (-not (Test-Path -LiteralPath $py314)) {
    Install-Exe (Join-Path $installers 'python314.exe') ("/quiet InstallAllUsers=0 Include_launcher=0 Include_test=0 Include_pip=1 PrependPath=0 TargetDir=`"$InstallRoot\Python314`"")
}
$gitRoot = Join-Path $env:ProgramFiles 'Git'
$git = Join-Path $gitRoot 'cmd\git.exe'
if (-not (Test-Path -LiteralPath $git)) {
    # The trusted release classifier deliberately resolves this standard path,
    # not caller-controlled PATH. Keep the final deployment layout compatible.
    Install-Exe (Join-Path $installers 'git.exe') ("/VERYSILENT /NORESTART /SP- /DIR=`"$gitRoot`"")
}
if ((Get-AuthenticodeSignature -LiteralPath (Join-Path $installers 'chrome.msi')).Status -ne 'Valid') { throw 'Invalid Chrome MSI signature.' }
Install-Exe (Join-Path $env:SystemRoot 'System32\msiexec.exe') ("/i `"$(Join-Path $installers 'chrome.msi')`" /qn /norestart")
if ($NeedsRestart) { Write-Host 'RESTART REQUIRED. Restart the old PC, then rerun this script; source PC is unchanged.'; exit 3010 }
$env:PATH = "$(Split-Path $git -Parent);$env:PATH"
$userPath = [string][Environment]::GetEnvironmentVariable('Path','User')
$gitCmd = Split-Path $git -Parent
if ($gitCmd -notin @($userPath -split ';')) {
    [Environment]::SetEnvironmentVariable('Path', (($userPath.TrimEnd(';') + ';' + $gitCmd).TrimStart(';')), 'User')
}
if ((& $py313 --version) -ne 'Python 3.13.14' -or (& $py314 --version) -ne 'Python 3.14.3') { throw 'Installed Python identity mismatch.' }
Write-Host '2/5 Restore Git main, rebuild both venvs with ONLY bundled wheels.'
$code = Join-Path $InstallRoot 'code'
if (-not (Test-Path -LiteralPath $code)) {
    Invoke-Checked $git @('clone','--branch','main',(Join-Path $PackageRoot 'code.bundle'),$code)
}
if ((& $git -C $code rev-parse HEAD).Trim() -ne $manifest.build_sha) { throw 'Installed Git build differs from package.' }
if ($manifest.git_origin_fetch_url -ne 'https://github.com/MingMG/probiga.git') { throw 'Unexpected project update remote.' }
Invoke-Checked $git @('-C',$code,'remote','set-url','origin',$manifest.git_origin_fetch_url)
$qmtPython = Join-Path $code 'runtime\qmt-py313\Scripts\python.exe'
$appPython = Join-Path $code '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $qmtPython)) { Invoke-Checked $py313 @('-m','venv',(Join-Path $code 'runtime\qmt-py313')) }
if (-not (Test-Path -LiteralPath $appPython)) { Invoke-Checked $py314 @('-m','venv',(Join-Path $code '.venv')) }
Invoke-Checked $qmtPython @('-m','pip','install','--no-index','--find-links',(Join-Path $PackageRoot 'wheels313'),
    '--only-binary=:all:','--require-hashes','-r',(Join-Path $code 'deploy\qmt_windows_requirements.lock'))
Invoke-Checked $appPython @('-m','pip','install','--no-index','--find-links',(Join-Path $PackageRoot 'wheels314'),
    '--only-binary=:all:','-r',(Join-Path $code 'deploy\windows_app_requirements.txt'))
Invoke-Checked $qmtPython @('-m','pip','check')
Invoke-Checked $appPython @('-m','pip','check')
Write-Host '3/5 Copy QMT/MySQL/Codex. Configure a PRIVATE local MySQL instance.'
foreach ($name in @('qmt','mysql84','codex')) {
    $destination = Join-Path $InstallRoot $name
    $receipt = Join-Path $InstallRoot "copy-$name.complete"
    if (-not (Test-Path -LiteralPath $receipt)) {
        Copy-Tree (Join-Path $PackageRoot "software\$name") $destination
        Write-Utf8 $receipt $manifest.build_sha
    }
}
$data = Join-Path $InstallRoot 'mysql-data'
$logs = Join-Path $InstallRoot 'mysql-logs'
$mysqld = Join-Path $InstallRoot 'mysql84\bin\mysqld.exe'
$serviceName = 'ProBigA-MySQL84-Edge'
$myIni = Join-Path $InstallRoot 'my.ini'
$exePattern = [Regex]::Escape($mysqld)
$iniPattern = [Regex]::Escape($myIni)
$servicePattern = '^(?:"' + $exePattern + '"|' + $exePattern + ')\s+(?:"--defaults-file=' + $iniPattern + '"|--defaults-file="' + $iniPattern + '"|--defaults-file=' + $iniPattern + ')\s+' + $serviceName + '$'
$service = Get-CimInstance Win32_Service -Filter "Name='$serviceName'"
if ($service -and $service.PathName -notmatch $servicePattern) {
    throw 'Existing MySQL service is owned by a different installation.'
}
$listeners = @(Get-NetTCPConnection -LocalPort 33085 -State Listen -ErrorAction SilentlyContinue)
if ($listeners.Count -and (-not $service -or @($listeners | Where-Object { $_.OwningProcess -ne $service.ProcessId -or $_.LocalAddress -ne '127.0.0.1' }).Count)) {
    throw 'Port 33085 is owned by another process or an unsafe listener.'
}
if (-not (Test-Path -LiteralPath $data)) {
    if (Get-NetTCPConnection -LocalPort 33085 -State Listen -ErrorAction SilentlyContinue) { throw 'Port 33085 is already in use.' }
    New-Item -ItemType Directory -Path $data,$logs,(Join-Path $InstallRoot 'mysql-tmp') | Out-Null
    Invoke-Checked $mysqld @('--initialize-insecure',"--basedir=$InstallRoot\mysql84","--datadir=$data",'--lower-case-table-names=1')
}
$baseUnix = $InstallRoot.Replace('\','/')
$expectedMyIni = @"
[mysqld]
basedir="$baseUnix/mysql84"
datadir="$baseUnix/mysql-data"
port=33085
bind-address=127.0.0.1
mysqlx=OFF
skip-name-resolve=ON
lower_case_table_names=1
character-set-server=utf8mb4
collation-server=utf8mb4_general_ci
sql-mode=STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ZERO_DATE,NO_ZERO_IN_DATE,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY
default-time-zone=+08:00
require-secure-transport=ON
tls-version=TLSv1.2,TLSv1.3
ssl-ca="$baseUnix/mysql-data/ca.pem"
ssl-cert="$baseUnix/mysql-data/server-cert.pem"
ssl-key="$baseUnix/mysql-data/server-key.pem"
innodb-buffer-pool-size=4G
innodb-redo-log-capacity=2G
innodb-flush-log-at-trx-commit=1
sync-binlog=1
log-bin="$baseUnix/mysql-logs/mysql-bin"
binlog-format=ROW
binlog-row-image=FULL
binlog-expire-logs-seconds=259200
server-id=84012
max-allowed-packet=256M
max-connections=40
event-scheduler=OFF
local-infile=OFF
secure-file-priv=NULL
tmpdir="$baseUnix/mysql-tmp"
log-error="$baseUnix/mysql-logs/mysql.err"
"@
if (Test-Path -LiteralPath $myIni) {
    if ((Get-Content -LiteralPath $myIni -Raw -Encoding UTF8).Replace("`r`n","`n").Trim() -ne $expectedMyIni.Replace("`r`n","`n").Trim()) {
        throw 'Existing my.ini differs from the private TLS database contract; review before retry.'
    }
} else { Write-Utf8 $myIni $expectedMyIni }
if (-not $service) { Invoke-Checked $mysqld @('--install',$serviceName,"--defaults-file=$myIni") }
Set-Service -Name $serviceName -StartupType Automatic
Start-Service -Name $serviceName
$service = Get-CimInstance Win32_Service -Filter "Name='$serviceName'"
if ($service.PathName -notmatch $servicePattern) { throw 'Registered service command changed.' }
$listeners = @()
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    $listeners = @(Get-NetTCPConnection -LocalPort 33085 -State Listen -ErrorAction SilentlyContinue)
    if ($listeners.Count) { break }
    Start-Sleep -Milliseconds 500
}
if (-not $listeners.Count -or @($listeners | Where-Object { $_.OwningProcess -ne $service.ProcessId -or $_.LocalAddress -ne '127.0.0.1' }).Count) {
    throw 'Private MySQL listener ownership could not be verified.'
}
Write-Host '4/5 Restore consistent business schemas, recreate restricted local test accounts.'
Push-Location $code
try {
    Invoke-Checked $qmtPython @('-m','tools.secondary_edge.target_database','--root',$InstallRoot,'--package',$PackageRoot)
    $env:BIG_QMT_HOME = Join-Path $InstallRoot 'qmt'
    Invoke-Checked $qmtPython @('tools\run_big_qmt_bridge.py','--install-strategy','--install-only','--expected-build-sha',$manifest.build_sha,'--json')
} finally { Pop-Location }
Write-Host '5/5 Installed, NOT promoted. No production scheduler, updater, tunnel or AI queue worker was started.'
Write-Host "Manually open $InstallRoot\qmt\bin.x64\XtItClient.exe, login, and start the ProBigA native read-only strategy."
Write-Host "Then run verify_target.ps1 -InstallRoot `"$InstallRoot`". Read README.txt for Codex/DeepSeek login and stability gates."
