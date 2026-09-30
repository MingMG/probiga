#Requires -Version 5.1
[CmdletBinding()]
param([string]$InstallRoot = 'C:\ProBigAEdge', [switch]$Observe, [switch]$Ai, [switch]$LoginCodex)
. (Join-Path $PSScriptRoot 'package_common.ps1')
$InstallRoot = [IO.Path]::GetFullPath($InstallRoot).TrimEnd('\')
$marker = Get-Content -LiteralPath (Join-Path $InstallRoot 'installation.json') -Raw -Encoding UTF8 | ConvertFrom-Json
if ($marker.host -ne $env:COMPUTERNAME) { throw 'Wrong candidate host.' }
$configPath = Join-Path $InstallRoot 'config.json'
$config = Get-Content -LiteralPath $configPath -Raw -Encoding UTF8 | ConvertFrom-Json
if ($config.mysql.host -ne '127.0.0.1' -or $config.mysql.port -ne 33085 -or $config.mysql.database -ne 'probiga_secondary') {
    throw 'Not a private candidate database.'
}
if ((Get-TimeZone).Id -ne 'China Standard Time') { throw 'Set Windows timezone to China Standard Time before QMT acceptance.' }
$code = Join-Path $InstallRoot 'code'
$appPython = Join-Path $code '.venv\Scripts\python.exe'
$qmtPython = Join-Path $code 'runtime\qmt-py313\Scripts\python.exe'
$env:PATH = "$(Join-Path $env:ProgramFiles 'Git\cmd');$env:PATH"
if ($LoginCodex) {
    $oldCodexHome = $env:CODEX_HOME
    try {
        $env:CODEX_HOME = Join-Path (Split-Path $config.ai.profile_dir -Parent) 'codex-home'
        New-Item -ItemType Directory -Path $env:CODEX_HOME -Force | Out-Null
        # Official login is interactive; credentials are NOT copied from source.
        Invoke-Checked $config.ai.codex_exe @('login','--device-auth')
    } finally { $env:CODEX_HOME = $oldCodexHome }
}
Push-Location $code
try {
    & $qmtPython -m tools.secondary_edge.collector --root $InstallRoot --once
    $quoteExit = $LASTEXITCODE
    if ($Ai) {
        & $appPython tools/secondary_edge/ai_probe.py --config $configPath
        $aiExit = $LASTEXITCODE
    } else { $aiExit = -1 }
} finally { Pop-Location }
if ($Observe) {
    Assert-Administrator
    $taskName = 'ProBigA Edge Acceptance'
    $existing = Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
    $pythonw = Join-Path $code 'runtime\qmt-py313\Scripts\pythonw.exe'
    $arguments = "-m tools.secondary_edge.collector --root `"$InstallRoot`""
    if ($existing -and (@($existing.Actions)[0].Execute -ne $pythonw -or @($existing.Actions)[0].Arguments -ne $arguments)) {
        throw 'Existing acceptance task belongs to another installation.'
    }
    $action = New-ScheduledTaskAction -Execute $pythonw -Argument $arguments -WorkingDirectory $code
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User ([Security.Principal.WindowsIdentity]::GetCurrent().Name)
    $principal = New-ScheduledTaskPrincipal -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType Interactive -RunLevel Limited
    $settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings -Force | Out-Null
    Start-ScheduledTask -TaskName $taskName
    Write-Host 'Continuous independent acceptance collector started; no production task ownership changed.'
}
Write-Host "QMT result exit=$quoteExit; AI result exit=$aiExit (-1 means not requested)."
Write-Host "Reports: $InstallRoot\status.json, database-status.json, ai-status.json."
Write-Host 'Do NOT shut down the original production PC. Send the reports for coordinated handoff review.'
if ($quoteExit -ne 0 -or ($Ai -and $aiExit -ne 0)) { Write-Host 'ACCEPTANCE NOT PASSED (see reports; market closed is not a live-market pass).'; exit 2 }
exit 0
