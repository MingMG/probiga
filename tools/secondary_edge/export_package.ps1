#Requires -Version 5.1
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$OutputRoot,
    [Parameter(Mandatory=$true)][string]$AdminClientFile,
    [string]$CodeRoot = (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent),
    [string]$QmtHome = '',
    [string]$MySqlHome = 'D:\MySQL84\software\mysql-8.4.11-winx64',
    [string]$SourceCa = 'D:\MySQL84\certs\ca.pem',
    [string]$Python313 = 'C:\Users\Administrator\AppData\Local\Programs\Python\Python313\python.exe',
    [string]$Python314 = 'E:\My Code\ProBigA\.venv\Scripts\python.exe',
    [string]$CodexBundle = 'C:\Users\Administrator\AppData\Local\OpenAI\Codex\bin\c6fe824d725f02d7',
    [string]$AiServerUrl = ''
)
. (Join-Path $PSScriptRoot 'package_common.ps1')
Assert-Administrator
$CodeRoot = [IO.Path]::GetFullPath($CodeRoot)
$OutputRoot = [IO.Path]::GetFullPath($OutputRoot)
if (-not $AiServerUrl) {
    $configuredHost = [Environment]::GetEnvironmentVariable('PROBIGA_REMOTE_SSH_HOST','User')
    if (-not $configuredHost) { $configuredHost = $env:PROBIGA_REMOTE_SSH_HOST }
    if (-not $configuredHost -or $configuredHost -match '[/\s@]') { throw 'Specify the production website -AiServerUrl.' }
    $AiServerUrl = 'http://' + $configuredHost
}
if (-not $QmtHome) {
    $homes = @(Get-ChildItem -LiteralPath 'D:\' -Directory | Where-Object {
        Test-Path -LiteralPath (Join-Path $_.FullName 'bin.x64\XtItClient.exe')
    })
    if ($homes.Count -ne 1) { throw 'Specify -QmtHome; exactly one standard QMT directory must be selected.' }
    $QmtHome = $homes[0].FullName
}
foreach ($sourceTree in @($CodeRoot,$QmtHome,$MySqlHome,$CodexBundle)) {
    $sourcePrefix = [IO.Path]::GetFullPath($sourceTree).TrimEnd('\') + '\'
    if (($OutputRoot + '\').StartsWith($sourcePrefix, [StringComparison]::OrdinalIgnoreCase)) {
        throw 'Output must not be inside a source tree.'
    }
}
if (Test-Path -LiteralPath $OutputRoot) { throw 'Use a NEW empty output directory; previous exports are never overwritten.' }
$branch = (& git -C $CodeRoot branch --show-current).Trim()
$head = (& git -C $CodeRoot rev-parse HEAD).Trim()
$main = (& git -C $CodeRoot rev-parse origin/main).Trim()
if ($branch -ne 'main' -or $head -ne $main -or (& git -C $CodeRoot status --porcelain)) {
    throw 'Export code only from clean merged production main (fetch origin first).'
}
foreach ($path in @($AdminClientFile, $SourceCa, $Python313, $Python314)) {
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { throw "Missing prerequisite: $path" }
}
if ((& $Python313 -c 'import sys;print("%d.%d"%sys.version_info[:2])').Trim() -ne '3.13' -or
    (& $Python314 -c 'import sys;print("%d.%d"%sys.version_info[:2])').Trim() -ne '3.14') { throw 'Source Python version mismatch.' }
# Allow enough space for SQL expansion, QMT and wheels; final measured sizes are sealed.
$volume = Get-Volume -DriveLetter ([IO.Path]::GetPathRoot($OutputRoot).Substring(0,1))
if ($volume.SizeRemaining -lt 450GB -or $volume.FileSystem -ne 'NTFS') {
    throw 'Export needs an NTFS volume with at least 450 GiB free (156 GiB source database).'
}
New-Item -ItemType Directory -Path $OutputRoot | Out-Null
Protect-LocalPath $OutputRoot
foreach ($name in @('software', 'software\installers', 'wheels313', 'wheels314', 'database', 'audit')) {
    New-Item -ItemType Directory -Path (Join-Path $OutputRoot $name) | Out-Null
}
Write-Host '1/6 Official signed offline installers (before the expensive database export).'
$installers = Join-Path $OutputRoot 'software\installers'
Get-SignedArtifact 'https://www.python.org/ftp/python/3.13.14/python-3.13.14-amd64.exe' (Join-Path $installers 'python313.exe') 'Python Software Foundation' 'c54d9b9bbb8a36e6489363ddd01139707fd781d72f1f9e90c7ec65d0061368e0'
Get-SignedArtifact 'https://www.python.org/ftp/python/3.14.3/python-3.14.3-amd64.exe' (Join-Path $installers 'python314.exe') 'Python Software Foundation' 'b68ad91421afbbd1a628105199c8c5f6179b21ba799067a8d8c0bbac3b7defb0'
Get-SignedArtifact 'https://github.com/git-for-windows/git/releases/download/v2.53.0.windows.2/Git-2.53.0.2-64-bit.exe' (Join-Path $installers 'git.exe') 'Johannes Schindelin' '194362cf24cd0db4b573096108460a34c7f80a20c5f2aa60d06ef817be9f73a1'
Get-SignedArtifact 'https://aka.ms/vc14/vc_redist.x64.exe' (Join-Path $installers 'vc_x64.exe') 'Microsoft Corporation'
Get-SignedArtifact 'https://aka.ms/vc14/vc_redist.x86.exe' (Join-Path $installers 'vc_x86.exe') 'Microsoft Corporation'
Get-SignedArtifact 'https://dl.google.com/dl/chrome/install/googlechromestandaloneenterprise64.msi' (Join-Path $installers 'chrome.msi') 'Google'
Write-Host '2/6 Complete Windows wheelhouses. No Linux wheels and no source builds.'
Invoke-Checked $Python313 @('-m','pip','download','--index-url','https://pypi.org/simple','--only-binary=:all:',
    '--require-hashes','-r',(Join-Path $CodeRoot 'deploy\qmt_windows_requirements.lock'),'-d',(Join-Path $OutputRoot 'wheels313'))
Invoke-Checked $Python314 @('-m','pip','download','--index-url','https://pypi.org/simple','--only-binary=:all:',
    '-r',(Join-Path $CodeRoot 'deploy\windows_app_requirements.txt'),'-d',(Join-Path $OutputRoot 'wheels314'))
Write-Host '3/6 Copy installed QMT, MySQL binaries, Codex CLI and exact Git main.'
Copy-Tree $QmtHome (Join-Path $OutputRoot 'software\qmt') @('/XF','*.lock','*.pid')
Copy-Tree $MySqlHome (Join-Path $OutputRoot 'software\mysql84')
Copy-Tree $CodexBundle (Join-Path $OutputRoot 'software\codex')
Invoke-Checked 'git' @('-C',$CodeRoot,'bundle','create',(Join-Path $OutputRoot 'code.bundle'),'main')
foreach ($name in @('package_common.ps1','install_target.ps1','verify_target.ps1','README.txt')) {
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot $name) -Destination (Join-Path $OutputRoot $name)
}
# Preserve source-only state for audit/controlled handoff; never activate it on another host.
$audit = Join-Path $OutputRoot 'audit'
$tasks = Get-ScheduledTask | Where-Object TaskName -Like '*ProBigA*' |
    Select-Object TaskName,TaskPath,State,@{n='Actions';e={$_.Actions | Select-Object Execute,Arguments,WorkingDirectory}}
Write-Utf8 (Join-Path $audit 'source-tasks.json') ($tasks | ConvertTo-Json -Depth 8)
Write-Utf8 (Join-Path $audit 'source-host.txt') $env:COMPUTERNAME
New-Item -ItemType Directory -Path (Join-Path $audit 'codex-production-threads') | Out-Null
foreach ($thread in @('019fbe02-0390-7663-a7ba-bd150e063fe7','019fbe02-0a70-7c62-9bf2-9ab439bea770')) {
    $sessionRoot = Join-Path $env:USERPROFILE '.codex\sessions'
    $rollouts = @(Get-ChildItem -LiteralPath $sessionRoot -File -Recurse | Where-Object Name -Like "*$thread.jsonl")
    if ($rollouts.Count -ne 1) { throw 'A production Codex legacy history archive is missing or ambiguous.' }
    $rollout = $rollouts[0]
    $before = Get-Sha256 $rollout.FullName
    $copy = Join-Path (Join-Path $audit 'codex-production-threads') $rollout.Name
    Copy-Item -LiteralPath $rollout.FullName -Destination $copy
    if ((Get-Sha256 $copy) -ne $before -or (Get-Sha256 $rollout.FullName) -ne $before) {
        throw 'Codex history changed during archive copy; preserve this attempt and retry at a quiet time.'
    }
}
# Credentials, SSH private keys, personal CODEX_HOME, live SQLite/WAL, and inherited
# scheduler grants are deliberately not transplanted. Copying them is NOT enrollment.
Write-Host '4/6 Consistent online database export. This can take hours; original services remain running.'
Push-Location $CodeRoot
try {
    Invoke-Checked $Python314 @('-m','tools.secondary_edge.database_export',
        '--client-option-file',$AdminClientFile,'--mysqldump',(Join-Path $MySqlHome 'bin\mysqldump.exe'),
        '--ssl-ca',$SourceCa,'--output-dir',(Join-Path $OutputRoot 'database'))
} finally { Pop-Location }
Write-Host '5/6 Seal all files (SQL/QMT hashing can take substantial time).'
$database = Get-Content -LiteralPath (Join-Path $OutputRoot 'database\metadata.json') -Raw -Encoding UTF8 | ConvertFrom-Json
if ($database.status -ne 'ready') { throw 'No successful database export receipt.' }
$files = @(Get-ChildItem -LiteralPath $OutputRoot -Recurse -File | ForEach-Object {
    [ordered]@{path=$_.FullName.Substring($OutputRoot.Length+1);bytes=$_.Length;sha256=(Get-Sha256 $_.FullName)}
})
$manifest = [ordered]@{
    format='probiga.windows-edge-offline.v1'; build_sha=$head; source_host=$env:COMPUTERNAME;
    created_at=(Get-Date).ToUniversalTime().ToString('o'); ai_server_url=$AiServerUrl;
    git_origin_fetch_url=(& git -C $CodeRoot remote get-url origin).Trim();
    database=$database; files=$files; production_activation=$false;
    minimum_target_free_bytes=([long]450GB);
    manual_gates=@('QMT broker login and native strategy start','Codex official login','DeepSeek login',
        'Codex stock/general history continuity validation','new host enrollment and database seal',
        'fresh data catch-up and coordinated production handoff')
}
Write-Utf8 (Join-Path $OutputRoot 'manifest.json') ($manifest | ConvertTo-Json -Depth 20)
Write-Utf8 (Join-Path $OutputRoot 'READY') $head
Write-Host "6/6 COMPLETE OFFLINE INSTALL PACKAGE: $OutputRoot"
Write-Host 'Protect this disk: the database/QMT files contain private business and account data.'
Write-Host 'Copy the entire directory to the mobile disk. Run install_target.ps1 on OLD PC only.'
