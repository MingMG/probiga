param(
    [Parameter(Mandatory = $true)] [string]$RegisteredRoot,
    [Parameter(Mandatory = $true)] [ValidatePattern('^[0-9a-f]{40}$')] [string]$ExpectedBuildSha,
    [ValidateRange(5, 300)] [int]$GitTimeoutSeconds = 45,
    [string]$GitHubProxy = ''
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
. (Join-Path $PSScriptRoot 'deploy_preflight.ps1')
Assert-DeployProxy $GitHubProxy
$ProductionRoot = [System.IO.Path]::GetFullPath($RegisteredRoot)
function Invoke-CandidateGit([string[]]$Arguments) {
    return @(Invoke-DeployGit -Root $ProductionRoot -Arguments $Arguments `
        -Stage "candidate.$($Arguments[0])" -TimeoutSeconds $GitTimeoutSeconds -GitHubProxy $GitHubProxy)
}
Assert-DeployRepositoryIdle $ProductionRoot $GitTimeoutSeconds
$PriorSha = ((Invoke-CandidateGit @('rev-parse', 'HEAD')) -join '').Trim()
$Origin = ((Invoke-CandidateGit @('remote', 'get-url', 'origin')) -join '').Trim()
if ($Origin -cne 'https://github.com/MingMG/probiga.git') { throw 'Candidate origin differs' }
Invoke-CandidateGit @('merge-base', '--is-ancestor', $PriorSha, $ExpectedBuildSha) | Out-Null
Invoke-CandidateGit @('merge-base', '--is-ancestor', $ExpectedBuildSha, 'origin/main') | Out-Null
$CandidateParent = Join-Path $ProductionRoot 'runtime\release-candidates'
foreach ($Directory in @($ProductionRoot, (Join-Path $ProductionRoot 'runtime'), $CandidateParent)) {
    if (!(Test-Path -LiteralPath $Directory)) {
        New-Item -ItemType Directory -Path $Directory | Out-Null
    }
    $Item = Get-Item -LiteralPath $Directory -Force
    if (!$Item.PSIsContainer -or ($Item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
        throw 'Candidate staging path must be an ordinary directory'
    }
}
$CandidateRoot = Join-Path $CandidateParent $ExpectedBuildSha
if (!(Test-Path -LiteralPath $CandidateRoot)) {
    Invoke-CandidateGit @('worktree', 'add', '--detach', $CandidateRoot, $ExpectedBuildSha) | Out-Null
}
$CandidateItem = Get-Item -LiteralPath $CandidateRoot -Force
if (!$CandidateItem.PSIsContainer -or ($CandidateItem.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
    throw 'Candidate worktree must be an ordinary directory'
}
$Python = Join-Path $ProductionRoot '.venv\Scripts\python.exe'
$Validator = Join-Path $CandidateRoot 'tools\validate_windows_release_candidate.py'
$env:PYTHONDONTWRITEBYTECODE = '1'
$Output = & $Python -P $Validator --validate --runtime-root $ProductionRoot `
    --expected-build-sha $ExpectedBuildSha --prior-build-sha $PriorSha
$ValidationExit = $LASTEXITCODE
$Payload = ($Output -join "`n") | ConvertFrom-Json -ErrorAction Stop
if ($ValidationExit -ne 0 -or $Payload.status -cne 'READY' -or
    $Payload.schema -cne 'probiga.windows-release-candidate.v1' -or
    $Payload.candidate.build_sha -cne $ExpectedBuildSha -or
    $Payload.candidate.prior_build_sha -cne $PriorSha -or
    $Payload.activation_granted -ne $false) {
    [Console]::Out.WriteLine(($Output -join "`n"))
    throw 'Windows candidate validation failed; existing services retained'
}
[Console]::Out.WriteLine(($Output -join "`n"))
