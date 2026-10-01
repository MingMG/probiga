#Requires -Version 5.1
[CmdletBinding()]
param(
    [string]$OutputRoot='F:\ProBigA-OldPC-Package',
    [string]$StateRoot='F:\ProBigA-Source-Pause-20261001',
    [string]$CodeRoot='',
    [string]$ExpectedHost='WIN-20260322RGF',
    [string]$ExpectedUserSid='',
    [switch]$Elevated
)
if (-not $CodeRoot) { $CodeRoot = Split-Path (Split-Path $PSScriptRoot -Parent) -Parent }
$env:PSModulePath = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\Modules;$env:ProgramFiles\WindowsPowerShell\Modules"
Import-Module Microsoft.PowerShell.Management,Microsoft.PowerShell.Utility,Microsoft.PowerShell.Security,ScheduledTasks -ErrorAction Stop
. (Join-Path $PSScriptRoot 'package_common.ps1')
if ($env:COMPUTERNAME -ine $ExpectedHost) { throw 'This preparation entry is SOURCE-PC ONLY.' }
if (-not $ExpectedUserSid) { $ExpectedUserSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value }
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = New-Object Security.Principal.WindowsPrincipal($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    if ($Elevated) { throw 'Windows administrator elevation was not granted.' }
    $native = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
    $arguments = '-NoProfile -ExecutionPolicy Bypass -File "'+$PSCommandPath+'" -Elevated -CodeRoot "'+$CodeRoot+
        '" -OutputRoot "'+$OutputRoot+'" -StateRoot "'+$StateRoot+'" -ExpectedHost "'+$ExpectedHost+'" -ExpectedUserSid "'+$ExpectedUserSid+'"'
    $helper = Start-Process -FilePath $native -ArgumentList $arguments -Verb RunAs -WindowStyle Hidden -PassThru -Wait
    exit $helper.ExitCode
}
Assert-Administrator
Assert-MergedMain $CodeRoot
& (Join-Path $PSScriptRoot 'pause_source.ps1') -StateRoot $StateRoot -ExpectedHost $ExpectedHost -ExpectedUserSid $ExpectedUserSid
if ($LASTEXITCODE -eq 2) {
    Write-Host 'QMT normal exit is required. Source database stays stopped; no automatic resume.'
    exit 2
}
$receipt = Get-Content -LiteralPath (Join-Path $StateRoot 'source-pause.json') -Raw -Encoding UTF8|ConvertFrom-Json
if ($receipt.status -ne 'paused') { throw 'A full source pause receipt is required.' }
& (Join-Path $PSScriptRoot 'export_cold_package.ps1') -OutputRoot $OutputRoot -CodeRoot $CodeRoot `
    -SourceLayout (Join-Path $StateRoot 'source-layout.json') -PauseReceipt (Join-Path $StateRoot 'source-pause.json')
