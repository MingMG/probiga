$ErrorActionPreference = "Stop"

$TaskName = "ProBigA Local Live Services"
$ScriptPath = Join-Path (Split-Path -Parent $PSScriptRoot) "tools\launch_local_live_supervisor.ps1"
$UserName = "$env:USERDOMAIN\$env:USERNAME"
$StartupDir = [Environment]::GetFolderPath("Startup")
$StartupCmd = Join-Path $StartupDir "ProBigA Local Live Services.cmd"
$RunKeyPath = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Run"
$RunValueName = "ProBigA Local Live Services"
$AutoRunCommand = "powershell.exe -WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`""

$trigger = New-ScheduledTaskTrigger -AtLogOn -User $UserName
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew

$principal = New-ScheduledTaskPrincipal `
    -UserId $UserName `
    -LogonType Interactive `
    -RunLevel Limited

try {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -Principal $principal `
        -Force | Out-Null
    Write-Host "Registered scheduled task: $TaskName"
}
catch {
    Write-Host "Scheduled task registration failed: $($_.Exception.Message)"
}

$content = "@echo off`r`npowershell.exe -WindowStyle Hidden -NoProfile -ExecutionPolicy Bypass -File `"$ScriptPath`"`r`n"
Set-Content -Path $StartupCmd -Value $content -Encoding ASCII
if (!(Test-Path $RunKeyPath)) {
    New-Item -Path $RunKeyPath -Force | Out-Null
}
Set-ItemProperty -Path $RunKeyPath -Name $RunValueName -Value $AutoRunCommand
Write-Host "Created startup launcher: $StartupCmd"
Write-Host "Created HKCU Run auto-start: $RunValueName"
