$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$TaskName = "ProBigA QMT Windows Edge Scheduler"
$UpdateTaskName = "ProBigA QMT Windows Edge Updater"
$ExpectedRoot = [System.IO.Path]::GetFullPath("E:\My Code\ProBigA")
$Root = [System.IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot))
if ($Root -ine $ExpectedRoot) {
    throw "QMT Windows edge installer is outside the production workspace"
}

$Wrapper = Join-Path $Root "tools\run_local_scheduler_task.ps1"
$Updater = Join-Path $Root "tools\update_qmt_windows_edge.ps1"
$Daemon = Join-Path $Root "tools\run_scheduler_daemon.py"
$PythonExe = Join-Path $Root ".venv\Scripts\python.exe"
foreach ($Path in @($Wrapper, $Updater, $Daemon, $PythonExe)) {
    if (!(Test-Path -LiteralPath $Path -PathType Leaf)) {
        throw "QMT Windows edge dependency is missing: $Path"
    }
}

$UserName = "$env:USERDOMAIN\$env:USERNAME"
$ProgramDataRoot = [System.IO.Path]::GetFullPath($env:ProgramData)
$Identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
function Initialize-ProtectedStateDirectory([string]$RelativePath) {
    $StateRoot = [System.IO.Path]::GetFullPath(
        (Join-Path $ProgramDataRoot $RelativePath)
    )
    if (!$StateRoot.StartsWith(
        $ProgramDataRoot + [System.IO.Path]::DirectorySeparatorChar
    )) {
        throw "QMT Windows state root escapes ProgramData"
    }
    New-Item -ItemType Directory -Path $StateRoot -Force | Out-Null
    $StateItem = Get-Item -LiteralPath $StateRoot -Force
    if (($StateItem.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "QMT Windows state root cannot be a reparse point"
    }
    $Acl = [System.Security.AccessControl.DirectorySecurity]::new()
    $Acl.SetAccessRuleProtection($true, $false)
    $Inheritance = [System.Security.AccessControl.InheritanceFlags](
        [System.Security.AccessControl.InheritanceFlags]::ContainerInherit -bor
        [System.Security.AccessControl.InheritanceFlags]::ObjectInherit
    )
    $Propagation = [System.Security.AccessControl.PropagationFlags]::None
    foreach ($SidValue in @(
        "S-1-5-18", "S-1-5-32-544", $Identity.User.Value
    )) {
        $Sid = [System.Security.Principal.SecurityIdentifier]::new($SidValue)
        $Rule = [System.Security.AccessControl.FileSystemAccessRule]::new(
            $Sid,
            [System.Security.AccessControl.FileSystemRights]::FullControl,
            $Inheritance,
            $Propagation,
            [System.Security.AccessControl.AccessControlType]::Allow
        )
        $Acl.AddAccessRule($Rule)
    }
    Set-Acl -LiteralPath $StateRoot -AclObject $Acl
    return $StateRoot
}

$StateRoots = @(
    (Initialize-ProtectedStateDirectory "ProBigA\qmt-local-gap-repair"),
    (Initialize-ProtectedStateDirectory "ProBigA\scheduler"),
    (Initialize-ProtectedStateDirectory "ProBigA\jobs")
)

$Action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument (
        "-WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass " +
        "-File `"$Wrapper`""
    )
$Trigger = New-ScheduledTaskTrigger -AtLogOn -User $UserName
$Principal = New-ScheduledTaskPrincipal `
    -UserId $UserName `
    -LogonType Interactive `
    -RunLevel Limited
$Settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -RestartCount 20 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $Action `
    -Trigger $Trigger `
    -Principal $Principal `
    -Settings $Settings `
    -Force | Out-Null

$UpdateAction = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument (
        "-WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass " +
        "-File `"$Updater`""
    )
$UpdateTrigger = New-ScheduledTaskTrigger `
    -Once `
    -At ((Get-Date).AddMinutes(1)) `
    -RepetitionInterval (New-TimeSpan -Minutes 5)
$UpdateSettings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 45)
Register-ScheduledTask `
    -TaskName $UpdateTaskName `
    -Action $UpdateAction `
    -Trigger $UpdateTrigger `
    -Principal $Principal `
    -Settings $UpdateSettings `
    -Force | Out-Null

Start-ScheduledTask -TaskName $TaskName

$Registered = Get-ScheduledTask -TaskName $TaskName -ErrorAction Stop
$RegisteredUpdater = Get-ScheduledTask -TaskName $UpdateTaskName -ErrorAction Stop
if (
    $Registered.TaskName -ne $TaskName -or
    $RegisteredUpdater.TaskName -ne $UpdateTaskName
) {
    throw "QMT Windows edge scheduled task registration differs"
}
Write-Host "Registered scheduler/updater and started: $TaskName"
