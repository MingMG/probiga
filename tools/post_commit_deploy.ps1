# Runs after a successful git commit. Config is stored in local git config:
#   probiga.postCommitDeploy.enabled
#   probiga.postCommitDeploy.mode        push | local | off
#   probiga.postCommitDeploy.branch      main
#   probiga.postCommitDeploy.remote      origin
#   probiga.postCommitDeploy.deployScript deploy/deploy.ps1

$ErrorActionPreference = "Stop"

function Get-RepoConfig {
    param(
        [string]$Key,
        [string]$DefaultValue
    )

    $value = (& git config --get $Key 2>$null)
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($value)) {
        return $DefaultValue
    }

    return $value.Trim()
}

try {
    $root = (& git rev-parse --show-toplevel).Trim()
    Set-Location $root

    $enabled = Get-RepoConfig "probiga.postCommitDeploy.enabled" "true"
    if ($enabled -notin @("true", "1", "yes", "on")) {
        Write-Host "[post-commit] ProBigA auto deploy disabled."
        exit 0
    }

    $branch = (& git rev-parse --abbrev-ref HEAD).Trim()
    $targetBranch = Get-RepoConfig "probiga.postCommitDeploy.branch" "main"
    if ($branch -ne $targetBranch) {
        Write-Host "[post-commit] Current branch '$branch' is not '$targetBranch'; skip deploy."
        exit 0
    }

    $mode = Get-RepoConfig "probiga.postCommitDeploy.mode" "push"
    if ($mode -eq "off") {
        Write-Host "[post-commit] ProBigA auto deploy mode is off."
        exit 0
    }

    $commit = (& git rev-parse --short HEAD).Trim()
    Write-Host "[post-commit] ProBigA auto deploy starting for $branch@$commit ..."

    if ($mode -eq "push") {
        $remote = Get-RepoConfig "probiga.postCommitDeploy.remote" "origin"
        git push $remote "HEAD:$targetBranch"
        if ($LASTEXITCODE -ne 0) {
            throw "git push failed; deployment workflow was not triggered."
        }

        Write-Host "[post-commit] Pushed to $remote/$targetBranch. GitHub Actions will run .github/workflows/deploy.yml."
        exit 0
    }

    if ($mode -eq "local") {
        $deployScript = Get-RepoConfig "probiga.postCommitDeploy.deployScript" "deploy/deploy.ps1"
        $deployScriptPath = Join-Path $root $deployScript
        if (-not (Test-Path $deployScriptPath)) {
            throw "Deploy script not found: $deployScriptPath"
        }

        & powershell -NoProfile -ExecutionPolicy Bypass -File $deployScriptPath
        if ($LASTEXITCODE -ne 0) {
            throw "Local deploy script failed."
        }

        Write-Host "[post-commit] Local deploy finished."
        exit 0
    }

    throw "Unknown ProBigA post-commit deploy mode: $mode"
} catch {
    Write-Host "[post-commit] ProBigA auto deploy failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

