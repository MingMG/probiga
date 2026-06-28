# Install the local Git post-commit deployment hook for ProBigA.
# Usage:
#   powershell -ExecutionPolicy Bypass -File .\tools\install_post_commit_deploy.ps1
#   powershell -ExecutionPolicy Bypass -File .\tools\install_post_commit_deploy.ps1 -Mode local
#   powershell -ExecutionPolicy Bypass -File .\tools\install_post_commit_deploy.ps1 -Disable

param(
    [ValidateSet("push", "local", "off")]
    [string]$Mode = "push",

    [string]$Branch = "main",

    [string]$Remote = "origin",

    [string]$DeployScript = "deploy/deploy.ps1",

    [switch]$Disable
)

$ErrorActionPreference = "Stop"

$root = (& git rev-parse --show-toplevel).Trim()
if (-not $root) {
    throw "Not inside a Git repository."
}

Set-Location $root

git config core.hooksPath tools/git-hooks

if ($Disable) {
    git config probiga.postCommitDeploy.enabled false
    Write-Host "ProBigA post-commit auto deploy disabled." -ForegroundColor Yellow
    exit 0
}

git config probiga.postCommitDeploy.enabled true
git config probiga.postCommitDeploy.mode $Mode
git config probiga.postCommitDeploy.branch $Branch
git config probiga.postCommitDeploy.remote $Remote
git config probiga.postCommitDeploy.deployScript $DeployScript

Write-Host "ProBigA post-commit auto deploy installed." -ForegroundColor Green
Write-Host "  hooksPath : tools/git-hooks"
Write-Host "  mode      : $Mode"
Write-Host "  branch    : $Branch"
Write-Host "  remote    : $Remote"
if ($Mode -eq "push") {
    Write-Host "Next successful commit on '$Branch' will push to '$Remote' and trigger .github/workflows/deploy.yml."
} elseif ($Mode -eq "local") {
    Write-Host "Next successful commit on '$Branch' will run '$DeployScript' locally."
} else {
    Write-Host "Mode is off; the hook is installed but will not deploy."
}

