#Requires -Version 5.1
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$StateRoot,
    [Parameter(Mandatory=$true)][string]$ExpectedHost,
    [Parameter(Mandatory=$true)][string]$ExpectedUserSid,
    [string[]]$SourceRoots = @('E:\My Code\ProBigA','E:\My Code\ProBigA-qmt-production'),
    [string]$MySqlHome = 'D:\MySQL84\software\mysql-8.4.11-winx64',
    [string]$ClientFile = 'D:\MySQL84\config\mysql84-runtime-client.ini',
    [string]$ConfigRoot = 'D:\MySQL84\config',
    [string]$CertRoot = 'D:\MySQL84\certs',
    [string]$DataRoot = 'E:\MySQL84\Data',
    [string]$LogRoot = 'E:\MySQL84\Logs',
    [string]$QmtHome,
    [switch]$InspectOnly
)
$env:PSModulePath = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\Modules;$env:ProgramFiles\WindowsPowerShell\Modules"
Import-Module Microsoft.PowerShell.Management,Microsoft.PowerShell.Utility,Microsoft.PowerShell.Security,ScheduledTasks -ErrorAction Stop
. (Join-Path $PSScriptRoot 'package_common.ps1')
Assert-Administrator
if (-not $InspectOnly) { Assert-MergedMain (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent) }
if ($env:COMPUTERNAME -ine $ExpectedHost -or
    [Security.Principal.WindowsIdentity]::GetCurrent().User.Value -ne $ExpectedUserSid) {
    throw 'Source host/user mismatch; no changes made.'
}
function Assert-PausePlainPath([string]$Path) {
    $cursor = [IO.Path]::GetFullPath($Path)
    while ($cursor) {
        if ((Test-Path -LiteralPath $cursor) -and
            ((Get-Item -LiteralPath $cursor -Force).Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw 'Source pause refuses a reparse-point path.'
        }
        $parent = [IO.Directory]::GetParent($cursor)
        if (-not $parent) { break }
        $cursor = $parent.FullName
    }
}
$StateRoot = [IO.Path]::GetFullPath($StateRoot).TrimEnd('\')
Assert-PausePlainPath $StateRoot
if ($StateRoot -notmatch '^[A-Za-z]:\\[^\\]+\\?' -or
    $StateRoot -eq [IO.Path]::GetPathRoot($StateRoot).TrimEnd('\')) { throw 'Use a dedicated absolute state directory.' }
foreach ($root in @($SourceRoots) + @($MySqlHome,$ConfigRoot,$CertRoot,$DataRoot,$LogRoot)) {
    $full = [IO.Path]::GetFullPath($root).TrimEnd('\')
    Assert-PausePlainPath $full
    if ($StateRoot -ieq $full -or ($StateRoot+'\').StartsWith($full+'\',[StringComparison]::OrdinalIgnoreCase)) {
        throw 'Pause journal must be outside all source directories.'
    }
    if ((Get-Item -LiteralPath $full -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
        throw 'Source root cannot be a reparse point.'
    }
}
if (-not $QmtHome) {
    $homes = @(Get-ChildItem -LiteralPath 'D:\' -Directory | Where-Object {
        Test-Path -LiteralPath (Join-Path $_.FullName 'bin.x64\XtItClient.exe')
    })
    if ($homes.Count -ne 1) { throw 'Select exactly one QMT home.' }
    $QmtHome = $homes[0].FullName
}
Assert-PausePlainPath $QmtHome
$qmtPrefix = [IO.Path]::GetFullPath($QmtHome).TrimEnd('\') + '\'
if (($StateRoot+'\').StartsWith($qmtPrefix,[StringComparison]::OrdinalIgnoreCase)) { throw 'Pause journal must not be inside QMT.' }
$qmtExe = [IO.Path]::GetFullPath((Join-Path $QmtHome 'bin.x64\XtItClient.exe'))
$serviceName = 'ProBigA-MySQL84'
$service = Get-CimInstance Win32_Service -Filter "Name='ProBigA-MySQL84'"
$expectedExe = Join-Path $MySqlHome 'bin\mysqld.exe'
$expectedConfig = Join-Path $ConfigRoot 'my.ini'
if (-not $service -or $service.PathName -notlike "*$expectedExe*" -or
    $service.PathName -notlike "*--defaults-file=$expectedConfig*") { throw 'Unexpected source MySQL service configuration.' }
$taskNames = @('ProBigA QMT Windows Edge Scheduler','ProBigA QMT Windows Edge Updater')
$tasks = @($taskNames | ForEach-Object { Get-ScheduledTask -TaskName $_ -TaskPath '\' })
foreach ($task in $tasks) {
    $action = @($task.Actions)[0]
    $expectedActionExe = if ($task.TaskName -eq $taskNames[0]) { Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe' }
        else { Join-Path $env:SystemRoot 'System32\wscript.exe' }
    $expectedActionScript = if ($task.TaskName -eq $taskNames[0]) { Join-Path $SourceRoots[1] 'tools\run_local_scheduler_task.ps1' }
        else { Join-Path $SourceRoots[1] 'tools\run_hidden_qmt_updater.vbs' }
    if (@($task.Actions).Count -ne 1 -or $action.Arguments -notlike ('*'+$expectedActionScript+'*') -or
        $action.Execute -ine $expectedActionExe -or $action.WorkingDirectory -ine $SourceRoots[1]) {
        throw 'Scheduled task ownership mismatch.'
    }
}
$runPath = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Run'
$runName = 'ProBigA Local Live Services'
function Get-SourceStartupRunValue([string]$Path, [string]$Name) {
    # PS5 Get-ItemPropertyValue throws for an absent value even with
    # SilentlyContinue. Absence is the expected state after a completed pause;
    # check names directly and keep genuine registry access failures fatal.
    if (-not (Test-Path -LiteralPath $Path -ErrorAction Stop)) { return $null }
    $key = Get-Item -LiteralPath $Path -ErrorAction Stop
    if ($Name -notin $key.GetValueNames()) { return $null }
    return $key.GetValue($Name, $null, [Microsoft.Win32.RegistryValueOptions]::DoNotExpandEnvironmentNames)
}
$runValue = Get-SourceStartupRunValue $runPath $runName
if ($runValue -and ($runValue -notlike ('*'+$SourceRoots[1]+'\tools\launch_local_live_supervisor.ps1*'))) {
    throw 'Startup registry ownership mismatch.'
}
$startup = [Environment]::GetFolderPath('Startup')
$startupNames = @('ProBigA AI Bridge.lnk','ProBigA Local Live Services.cmd')
$startupCmd = Join-Path $startup 'ProBigA Local Live Services.cmd'
if (Test-Path -LiteralPath $startupCmd) {
    $cmdText = (Get-Content -LiteralPath $startupCmd -Raw).Trim()
    $expectedCmd = '@echo off' + "`r`n" + 'powershell.exe -WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File "' +
        $SourceRoots[1] + '\tools\launch_local_live_supervisor.ps1"'
    if ($cmdText.Replace("`r`n","`n") -cne $expectedCmd.Replace("`r`n","`n")) {
        throw 'Startup CMD ownership mismatch.'
    }
}
$startupLink = Join-Path $startup 'ProBigA AI Bridge.lnk'
if (Test-Path -LiteralPath $startupLink) {
    # Read shortcut metadata only; no Windows UI automation or launch occurs.
    $shortcutReader = New-Object -ComObject WScript.Shell
    $shortcut = $shortcutReader.CreateShortcut($startupLink)
    if ($shortcut.TargetPath -ine (Join-Path $SourceRoots[0] '.venv\Scripts\pythonw.exe') -or
        $shortcut.Arguments -ine ('"'+(Join-Path $SourceRoots[0] 'tools\run_codex_web_bridge.py')+'"') -or
        $shortcut.WorkingDirectory -ine $SourceRoots[0]) { throw 'Startup shortcut ownership mismatch.' }
}
$scriptNames = @('run_local_live_supervisor.ps1','launch_local_live_supervisor.ps1',
    'start_local_live_services.ps1','run_local_scheduler_task.ps1','run_scheduler_daemon.py',
    'run_codex_web_bridge.py','run_big_qmt_bridge.py','run_guojin_qmt_gateway.py',
    'run_qmt_live_runtime.py','run_remote_qmt_tunnel.py','run_production_mysql_forward.py','run_remote_mysql_tunnel.py',
    'update_qmt_windows_edge.ps1','run_hidden_qmt_updater.vbs','ensure_big_qmt_strategy_running.ps1')
function Get-OwnedProcesses {
    $all = @(Get-CimInstance Win32_Process)
    $excluded = @([uint32]$PID)
    $walk = $all | Where-Object ProcessId -eq $PID | Select-Object -First 1
    while ($walk -and $walk.ParentProcessId -and $walk.ParentProcessId -notin $excluded) {
        $excluded += [uint32]$walk.ParentProcessId
        $walk = $all | Where-Object ProcessId -eq $walk.ParentProcessId | Select-Object -First 1
    }
    $owned = @($all | Where-Object {
        $candidate = $_
        if ($candidate.ProcessId -in $excluded -or $candidate.Name -notin
            @('python.exe','pythonw.exe','powershell.exe','pwsh.exe','wscript.exe') -or
            -not $candidate.CommandLine -or -not $candidate.ExecutablePath) { return $false }
        $inScope = $false
        foreach ($root in $SourceRoots) {
            if ($candidate.CommandLine.IndexOf($root,[StringComparison]::OrdinalIgnoreCase) -ge 0) { $inScope = $true }
        }
        if (-not $inScope) { return $false }
        $exeToken = '^\s*(?:"[^"]+"|\S+)\s+'
        if ($candidate.Name -in @('powershell.exe','pwsh.exe')) {
            $pattern = $exeToken + '(?:-(?:NoProfile|NoLogo|NonInteractive|WindowStyle\s+\S+|ExecutionPolicy\s+\S+)\s+)*-File\s+(?:"(?<script>[^"]+)"|(?<script>\S+))'
        } elseif ($candidate.Name -eq 'wscript.exe') {
            $pattern = $exeToken + '(?://\S+\s+)*(?:"(?<script>[^"]+)"|(?<script>\S+))'
        } else {
            $pattern = $exeToken + '(?:(?:-B|-u)\s+)*(?:"(?<script>[^"]+)"|(?<script>\S+))'
        }
        # The actual script argument must match; -Command/-c diagnostic text is
        # never interpreted as an invoked producer merely because it names one.
        if ($candidate.CommandLine -notmatch $pattern) { return $false }
        $invoked = $Matches['script'].Replace('/','\')
        $leaf = [IO.Path]::GetFileName($invoked)
        if ($leaf -notin $scriptNames) { return $false }
        if ($invoked -ieq ('tools\'+$leaf)) { return $true }
        foreach ($root in $SourceRoots) {
            if ($invoked -ieq (Join-Path $root ('tools\'+$leaf))) { return $true }
        }
        return $false
    })
    # Children of an owned producer belong to that producer (not this Codex app).
    $ids = @($owned | ForEach-Object ProcessId)
    for ($depth=0; $depth -lt 12; $depth++) {
        $children = @($all | Where-Object {$_.ParentProcessId -in $ids -and $_.ProcessId -notin $ids -and $_.ProcessId -notin $excluded})
        if (-not $children.Count) { break }
        $owned += $children
        $ids += @($children | ForEach-Object ProcessId)
    }
    if (@($owned|Where-Object Name -eq 'mysqld.exe').Count) { throw 'Never terminate a database process as a producer child.' }
    return @($owned)
}
if ($InspectOnly) {
    [ordered]@{source_host=$env:COMPUTERNAME;service=$service.State;tasks=@($tasks|ForEach-Object TaskName);
        owned_processes=@(Get-OwnedProcesses|Select-Object ProcessId,Name,CreationDate);
        qmt_running=@(Get-CimInstance Win32_Process -Filter "Name='XtItClient.exe'"|Where-Object ExecutablePath -ieq $qmtExe).Count -gt 0;
        mutation=$false} | ConvertTo-Json -Depth 6
    exit 0
}
if (-not (Test-Path -LiteralPath $StateRoot)) { New-Item -ItemType Directory -Path $StateRoot | Out-Null }
if ((Get-Item -LiteralPath $StateRoot -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) { throw 'Unsafe pause journal root.' }
Protect-LocalPath $StateRoot
$journalPath = Join-Path $StateRoot 'original-state.json'
$layoutPath = Join-Path $StateRoot 'source-layout.json'
$receiptPath = Join-Path $StateRoot 'source-pause.json'
if (-not (Test-Path -LiteralPath $journalPath)) {
    if ($service.State -ne 'Running') { throw 'First pause must capture the running source identity; do not invent it.' }
    $clientArgs = @("--defaults-file=$ClientFile",'--no-login-paths','--protocol=TCP','--host=127.0.0.1','--port=3306',
        '--ssl-mode=VERIFY_CA',('--ssl-ca='+ (Join-Path $CertRoot 'ca.pem')),'--batch','--raw','--skip-column-names')
    $identityRows = @(& (Join-Path $MySqlHome 'bin\mysql.exe') @clientArgs '--execute=SELECT @@server_uuid,@@hostname,@@version;' 2>$null)
    if ($LASTEXITCODE -ne 0 -or $identityRows.Count -ne 1) { throw 'Could not read authenticated source database identity.' }
    $identityFields = $identityRows[0].Split("`t")
    if ($identityFields.Count -ne 3 -or $identityFields[0] -notmatch '^[0-9a-f-]{36}$' -or
        $identityFields[1] -ine $ExpectedHost -or $identityFields[2] -ne '8.4.11') { throw 'Source identity mismatch.' }
    $inventorySql = "SELECT JSON_OBJECT('schemas',(SELECT JSON_ARRAYAGG(SCHEMA_NAME) FROM information_schema.SCHEMATA WHERE SCHEMA_NAME NOT IN ('mysql','sys','information_schema','performance_schema')),'tables',(SELECT JSON_ARRAYAGG(JSON_OBJECT('schema',TABLE_SCHEMA,'name',TABLE_NAME,'type',TABLE_TYPE,'estimated_rows',TABLE_ROWS)) FROM information_schema.TABLES WHERE TABLE_SCHEMA NOT IN ('mysql','sys','information_schema','performance_schema')));"
    $inventoryRows = @(& (Join-Path $MySqlHome 'bin\mysql.exe') @clientArgs ('--execute='+$inventorySql) 2>$null)
    if ($LASTEXITCODE -ne 0 -or $inventoryRows.Count -ne 1) { throw 'Could not read source object inventory.' }
    $inventory = $inventoryRows[0] | ConvertFrom-Json
    $layout = [ordered]@{format='probiga.cold-source-layout.v1';source=[ordered]@{
        hostname=$identityFields[1];server_uuid=$identityFields[0];version=$identityFields[2];service_name=$serviceName;
        datadir=$DataRoot;logsdir=$LogRoot;inventory=$inventory};roots=[ordered]@{data=$DataRoot;logs=$LogRoot;config=$ConfigRoot;certs=$CertRoot}}
    Write-Utf8 $layoutPath ($layout|ConvertTo-Json -Depth 12)
    $original = [ordered]@{format='probiga.pause-original-state.v1';source_host=$ExpectedHost;user_sid=$ExpectedUserSid;
        service_name=$serviceName;service_start_mode=$service.StartMode;service_state=$service.State;
        run_value=$runValue;startup_root=$startup;startup_names=$startupNames;
        captured_at_utc=(Get-Date).ToUniversalTime().ToString('o');automatic_restore=$false;
        tasks=@($tasks | Select-Object TaskName,TaskPath,State,@{n='enabled';e={$_.Settings.Enabled}})}
    foreach ($task in $tasks) { Write-Utf8 (Join-Path $StateRoot ($task.TaskName+'.xml')) (Export-ScheduledTask -TaskName $task.TaskName -TaskPath $task.TaskPath) }
    Write-Utf8 $journalPath ($original|ConvertTo-Json -Depth 6)
} else {
    $original = Get-Content -LiteralPath $journalPath -Raw -Encoding UTF8|ConvertFrom-Json
    if ($original.source_host -ine $ExpectedHost -or $original.user_sid -ne $ExpectedUserSid -or
        -not (Test-Path -LiteralPath $layoutPath)) { throw 'Existing journal does not belong to this source.' }
}
$layout = Get-Content -LiteralPath $layoutPath -Raw -Encoding UTF8|ConvertFrom-Json
Write-Utf8 $receiptPath ([ordered]@{format='probiga.source-pause.v1';status='pausing';source_host=$ExpectedHost;
    source_automatically_resume=$false;production_activation=$false}|ConvertTo-Json)
try {
    # Capture descendants before Stop-ScheduledTask can orphan them. A saved PID
    # is acted on only after its exact image and creation time are rechecked.
    $initialOwned = @(Get-OwnedProcesses)
    foreach ($task in $tasks) {
        Disable-ScheduledTask -TaskName $task.TaskName -TaskPath $task.TaskPath | Out-Null
        Stop-ScheduledTask -TaskName $task.TaskName -TaskPath $task.TaskPath
    }
    if ($runValue) { Remove-ItemProperty -LiteralPath $runPath -Name $runName }
    $startupBackup = Join-Path $StateRoot 'startup'
    if (-not (Test-Path -LiteralPath $startupBackup)) { New-Item -ItemType Directory -Path $startupBackup | Out-Null }
    foreach ($name in $startupNames) {
        $from = Join-Path $startup $name
        $to = Join-Path $startupBackup $name
        if (Test-Path -LiteralPath $from) {
            if (Test-Path -LiteralPath $to) { throw 'Startup backup already exists while startup is still active.' }
            Move-Item -LiteralPath $from -Destination $to
        }
    }
    $stopped = @()
    # Stop supervisors first so they cannot respawn children during the pause.
    for ($pass=0; $pass -lt 5; $pass++) {
        $owned = @(@(Get-OwnedProcesses) + $initialOwned | Sort-Object ProcessId -Unique |
            Sort-Object @{Expression={if($_.Name -in @('powershell.exe','pwsh.exe','wscript.exe')){0}else{1}}})
        if (-not $owned.Count) { break }
        foreach ($proc in $owned) {
            $current = Get-CimInstance Win32_Process -Filter ('ProcessId='+$proc.ProcessId) -ErrorAction SilentlyContinue
            if ($current -and $current.CreationDate -eq $proc.CreationDate -and $current.ExecutablePath -ieq $proc.ExecutablePath) {
                Stop-Process -Id $current.ProcessId -ErrorAction Stop
                $stopped += [ordered]@{pid=$current.ProcessId;name=$current.Name;created_at=$current.CreationDate.ToString('o')}
            }
        }
        $initialOwned = @($initialOwned | Where-Object {
            $live = Get-CimInstance Win32_Process -Filter ('ProcessId='+$_.ProcessId) -ErrorAction SilentlyContinue
            $live -and $live.CreationDate -eq $_.CreationDate -and $live.ExecutablePath -ieq $_.ExecutablePath
        })
        Start-Sleep -Milliseconds 500
    }
    if (@(Get-OwnedProcesses).Count -or $initialOwned.Count) { throw 'An owned source producer remains running.' }
    Set-Service -Name $serviceName -StartupType Disabled
    $shutdownStart = (Get-Date).ToUniversalTime()
    $svc = Get-Service -Name $serviceName
    $wasRunning = $svc.Status -ne 'Stopped'
    if ($svc.Status -ne 'Stopped') {
        Stop-Service -Name $serviceName -ErrorAction Stop
        $svc.WaitForStatus([ServiceProcess.ServiceControllerStatus]::Stopped,[TimeSpan]::FromSeconds(120))
    }
    $errorLog = Join-Path $LogRoot 'mysql84.err'
    $shutdownLines = @(Get-Content -LiteralPath $errorLog -Tail 50 | Where-Object {$_ -match '\[MY-010910\].*Shutdown complete'})
    if (-not $shutdownLines.Count) { throw 'No MySQL clean Shutdown complete evidence; no cold backup permitted.' }
    $shutdownTime = [datetime]::Parse($shutdownLines[-1].Split(' ')[0]).ToUniversalTime()
    if ($shutdownTime -lt [datetime]::Parse($original.captured_at_utc).ToUniversalTime()) { throw 'Shutdown evidence predates this pause.' }
    if ($wasRunning -and $shutdownTime -lt $shutdownStart) { throw 'No fresh shutdown evidence for the stopped source instance.' }
    $remainingServer = @(Get-CimInstance Win32_Process -Filter "Name='mysqld.exe'"|Where-Object ExecutablePath -ieq $expectedExe)
    if ($remainingServer.Count -or (Get-Service -Name $serviceName).StartType -ne 'Disabled') { throw 'Source database is not durably stopped.' }
    $qmtRunning = @(Get-CimInstance Win32_Process -Filter "Name='XtItClient.exe'"|Where-Object ExecutablePath -ieq $qmtExe).Count -gt 0
    $receipt = [ordered]@{format='probiga.source-pause.v1';status=$(if($qmtRunning){'qmt-exit-required'}else{'paused'});
        source_host=$ExpectedHost;source_service_name=$serviceName;source_server_uuid=$layout.source.server_uuid;
        source_service_state='Stopped';source_service_startup='Disabled';source_processes_running=$false;
        source_qmt_running=$qmtRunning;shutdown_complete=$true;shutdown_completed_at_utc=$shutdownTime.ToString('o');
        source_automatically_resume=$false;production_activation=$false;completed_at_utc=(Get-Date).ToUniversalTime().ToString('o');
        stopped_processes=$stopped;linux_deployed=$false}
    Write-Utf8 $receiptPath ($receipt|ConvertTo-Json -Depth 8)
    Write-Host ('Source pause status: '+$receipt.status+'. Database Stopped/Disabled. No automatic resume.')
    if ($qmtRunning) { exit 2 }
} catch {
    Write-Utf8 (Join-Path $StateRoot 'pause-failed.json') ([ordered]@{status='failed';source_automatically_resume=$false;
        at_utc=(Get-Date).ToUniversalTime().ToString('o');reason='SOURCE_PAUSE_INCOMPLETE'}|ConvertTo-Json)
    throw
}
