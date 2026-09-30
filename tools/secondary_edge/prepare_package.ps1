#Requires -Version 5.1
[CmdletBinding()]
param([string]$OutputRoot = 'F:\ProBigA-OldPC-Package', [string]$AiServerUrl = '',
    [string]$CodeRoot = (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent))
. (Join-Path $PSScriptRoot 'package_common.ps1')
Assert-Administrator
# Prompt on the SOURCE computer. Never put the password in command-line args,
# in chat, or in the transferable package.
$credential = Get-Credential -UserName 'root' -Message 'Current MySQL 8.4 backup administrator (NOT Windows login)'
if (-not $credential) { throw 'Database authentication cancelled.' }
$temporaryRoot = Join-Path ([IO.Path]::GetTempPath()) ('probiga-db-auth-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $temporaryRoot | Out-Null
Protect-LocalPath $temporaryRoot
$credentialFile = Join-Path $temporaryRoot 'client.ini'
try {
    $password = $credential.GetNetworkCredential().Password
    $username = $credential.UserName
    if ($username -notmatch '^[A-Za-z0-9_]+$') { throw 'Use a MySQL account name, not DOMAIN\user.' }
    $escaped = $password.Replace('\','\\').Replace('"','\"').Replace("`r",'\r').Replace("`n",'\n')
    Write-Utf8 $credentialFile ("[client]`nuser=$username`npassword=`"$escaped`"`nhost=127.0.0.1`nport=3306`n")
    $password = $null
    $escaped = $null
    & (Join-Path $PSScriptRoot 'export_package.ps1') -OutputRoot $OutputRoot -AdminClientFile $credentialFile -AiServerUrl $AiServerUrl -CodeRoot $CodeRoot
} finally {
    if (Test-Path -LiteralPath $credentialFile) { Remove-Item -LiteralPath $credentialFile -Force }
    Remove-Item -LiteralPath $temporaryRoot -Force
    $credential = $null
}
