[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$RegisteredRoot,
    [Parameter(Mandatory=$true)][ValidatePattern('^[0-9a-f]{64}$')][string]$ExpectedWorkerSha256,
    [Parameter(Mandatory=$true)][string]$ServerUrl,
    [Parameter(Mandatory=$true)][string]$StateRoot
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$root = [IO.Path]::GetFullPath($RegisteredRoot).TrimEnd('\')
$derived = [IO.Path]::GetFullPath((Split-Path -Parent $PSScriptRoot)).TrimEnd('\')
if ($root -ine $derived -or $root -notmatch '^[A-Za-z]:\\') {
    throw 'Research worker wrapper differs from registered production root.'
}
$worker = Join-Path $root 'tools\run_codex_web_bridge.py'
$python = Join-Path $root '.venv\Scripts\python.exe'
foreach ($path in @($root, $worker, $python)) {
    $item = Get-Item -LiteralPath $path -Force -ErrorAction Stop
    if ($item.Attributes -band [IO.FileAttributes]::ReparsePoint) {
        throw 'Research worker code boundary cannot be a reparse point.'
    }
}
if ((Get-FileHash -LiteralPath $worker -Algorithm SHA256).Hash.ToLowerInvariant() -cne $ExpectedWorkerSha256) {
    throw 'Research worker content differs from registered artifact identity.'
}
$uri = [uri]$ServerUrl
if (-not $uri.IsAbsoluteUri -or $uri.Scheme -notin @('http','https') -or $uri.UserInfo -or $uri.Query -or $uri.Fragment) {
    throw 'Research server must be a credential-free absolute HTTP origin.'
}
$state = [IO.Path]::GetFullPath($StateRoot).TrimEnd('\')
if ($state -eq [IO.Path]::GetPathRoot($state).TrimEnd('\') -or $state -eq $root -or
    ($state+'\').StartsWith($root+'\',[StringComparison]::OrdinalIgnoreCase)) {
    throw 'Research worker state must be outside its immutable code checkout.'
}
New-Item -ItemType Directory -Path $state -Force | Out-Null
if ((Get-Item -LiteralPath $state -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) {
    throw 'Research worker state cannot be a reparse point.'
}
$env:PYTHONDONTWRITEBYTECODE = '1'
$env:PYTHONUTF8 = '1'
Set-Location -LiteralPath $root
# Foreground ownership lets Task Scheduler observe the actual exit and restart
# only this independent research worker. It never starts or stops QMT.
& $python -B $worker --research-only --server-url $ServerUrl --state-dir $state
exit $LASTEXITCODE
