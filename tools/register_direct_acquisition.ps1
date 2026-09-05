# Run manually only AFTER isolated acceptance and single-writer cutover.
# Does not start QMT, authenticate a broker, read old .env, or install a service.
param(
    [Parameter(Mandatory = $true)][string]$RegisteredRoot,
    [Parameter(Mandatory = $true)][string]$ConfigPath,
    [Parameter(Mandatory = $true)][string]$PythonExe
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

function Resolve-ExplicitPath([string]$Value, [bool]$Directory) {
    if (![System.IO.Path]::IsPathRooted($Value)) {
        throw "An explicit absolute installation/config/runtime path is required"
    }
    $Resolved = [System.IO.Path]::GetFullPath($Value)
    $Kind = if ($Directory) { "Container" } else { "Leaf" }
    if (!(Test-Path -LiteralPath $Resolved -PathType $Kind)) {
        throw "An explicit installation/config/runtime path does not exist"
    }
    $Item = Get-Item -LiteralPath $Resolved -Force
    if (($Item.Attributes -band [System.IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw "Installation/config/runtime target cannot be a reparse point"
    }
    return $Resolved
}

function Quote-PowerShellLiteral([string]$Value) {
    return "'" + $Value.Replace("'", "''") + "'"
}

$InstallRoot = Resolve-ExplicitPath $RegisteredRoot $true
$ConfigFile = Resolve-ExplicitPath $ConfigPath $false
$RuntimeExe = Resolve-ExplicitPath $PythonExe $false
if (!(Test-Path -LiteralPath (Join-Path $InstallRoot "acquisition\__main__.py") -PathType Leaf)) {
    throw "The selected root does not contain the new acquisition CLI"
}
$Config = Get-Content -LiteralPath $ConfigFile -Raw | ConvertFrom-Json
if ($null -eq $Config.PSObject.Properties["write_enabled"] -or $Config.write_enabled -ne $true) {
    throw "Configuration writes are disabled; finish isolated acceptance/cutover before registering writers"
}

$Identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
# InteractiveToken is deliberate: full QMT must be logged in and its read-only
# model loaded in this user's session. AtLogOn catches a reboot after login;
# this script does not promise operation while nobody is logged in.
$Principal = New-ScheduledTaskPrincipal -UserId $Identity -LogonType Interactive -RunLevel Limited
$Periodic = New-ScheduledTaskTrigger -Once -At (Get-Date).AddSeconds(30) `
    -RepetitionInterval (New-TimeSpan -Minutes 5)
$OnLogon = New-ScheduledTaskTrigger -AtLogOn -User $Identity
$ShellExe = Join-Path $PSHOME "powershell.exe"
if (!(Test-Path -LiteralPath $ShellExe -PathType Leaf)) {
    $ShellExe = (Get-Command powershell.exe -ErrorAction Stop).Source
}

foreach ($Definition in @(
    @{ Name = "ProBigA Direct Acquisition Daily"; Command = "daily"; Limit = 22 * 60 },
    @{ Name = "ProBigA Direct Acquisition Live"; Command = "live --duration-seconds 295"; Limit = 300 }
)) {
    $Command = "& " + (Quote-PowerShellLiteral $RuntimeExe) + " -B -m acquisition --config " + `
        (Quote-PowerShellLiteral $ConfigFile) + " " + $Definition.Command + "; exit `$LASTEXITCODE"
    $Action = New-ScheduledTaskAction -Execute $ShellExe `
        -Argument ("-NoLogo -NoProfile -NonInteractive -WindowStyle Hidden -Command `"" + $Command + "`"") `
        -WorkingDirectory $InstallRoot
    $Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew `
        -ExecutionTimeLimit ([TimeSpan]::FromSeconds($Definition.Limit)) `
        -RestartCount 2 -RestartInterval (New-TimeSpan -Minutes 1) `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
    Register-ScheduledTask -TaskName $Definition.Name -Action $Action -Trigger @($Periodic, $OnLogon) `
        -Principal $Principal -Settings $Settings -Force | Out-Null
    Write-Output ("Registered " + $Definition.Name + "; full QMT requires this user's logged-in session.")
}
# The next scheduled/logon trigger starts these entry points. No immediate run
# and no old-task stop is performed; disable old writers during the documented cutover.
