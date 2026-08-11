# Upload portfolio profit calculation fixes and restart probiga.
# Usage: cd <repo>; .\deploy\upload_portfolio_profit_fix.ps1

$ErrorActionPreference = "Stop"
$LegacyDeployOverride = "I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES"
if ($env:PROBIGA_ALLOW_LEGACY_DEPLOY -cne $LegacyDeployOverride) {
    throw "Legacy deploy blocked. Set PROBIGA_ALLOW_LEGACY_DEPLOY=$LegacyDeployOverride to override."
}

$RemoteHost = $env:PROBIGA_REMOTE_SSH_HOST
if (-not $RemoteHost) { throw "Set PROBIGA_REMOTE_SSH_HOST first." }
$RemoteUser = $env:PROBIGA_REMOTE_SSH_USER
if (-not $RemoteUser) { throw "Set PROBIGA_REMOTE_SSH_USER to a named deploy account first." }
if ($RemoteUser -ieq "root") { throw "Root production deploy is forbidden." }
$KnownHosts = $env:PROBIGA_SSH_KNOWN_HOSTS
if (-not $KnownHosts -or -not (Test-Path -LiteralPath $KnownHosts -PathType Leaf)) {
    throw "Set PROBIGA_SSH_KNOWN_HOSTS to an existing pinned known-hosts file."
}
$KeyFile = $env:PROBIGA_REMOTE_SSH_KEY_FILE
if (-not $KeyFile -or -not (Test-Path -LiteralPath $KeyFile -PathType Leaf)) {
    throw "Set PROBIGA_REMOTE_SSH_KEY_FILE to an existing deploy key."
}
$SshOptions = @(
    "-o", "BatchMode=yes",
    "-o", "PasswordAuthentication=no",
    "-o", "StrictHostKeyChecking=yes",
    "-o", "UserKnownHostsFile=$KnownHosts",
    "-i", $KeyFile
)
$RemoteRoot = $env:PROBIGA_REMOTE_ROOT
if (-not $RemoteRoot) { throw "Set PROBIGA_REMOTE_ROOT first." }
$Server = "$RemoteUser@$RemoteHost"
$Remote = $RemoteRoot
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

$Files = @(
    @{ Local = "server\api\routers\hot_data.py"; Remote = "server/api/routers/hot_data.py" },
    @{ Local = "server\api\routers\portfolio_math.py"; Remote = "server/api/routers/portfolio_math.py" },
    @{ Local = "tools\fetch_sector_heat_east_daily.py"; Remote = "tools/fetch_sector_heat_east_daily.py" },
    @{ Local = "server\static\index.html"; Remote = "server/static/index.html" },
    @{ Local = "server\static\js\app.js"; Remote = "server/static/js/app.js" },
    @{ Local = "server\static\css\style.css"; Remote = "server/static/css/style.css" }
)

Write-Host "Prepare remote directories ..." -ForegroundColor Yellow
ssh @SshOptions $Server "mkdir -p $Remote/tools $Remote/server/api/routers $Remote/server/static $Remote/server/static/js $Remote/server/static/css"
if ($LASTEXITCODE -ne 0) { throw "Failed to prepare remote directories" }

foreach ($File in $Files) {
    $LocalPath = Join-Path $Root $File.Local
    Write-Host "Upload $($File.Local) ..." -ForegroundColor Yellow
    scp @SshOptions $LocalPath "${Server}:${Remote}/$($File.Remote)"
    if ($LASTEXITCODE -ne 0) { throw "Failed to upload $($File.Local)" }
}

Write-Host "Restart probiga ..." -ForegroundColor Yellow
ssh @SshOptions $Server "systemctl restart probiga; sleep 2; systemctl is-active probiga; systemctl status probiga --no-pager -l | head -8"
if ($LASTEXITCODE -ne 0) { throw "Failed to restart probiga" }

Write-Host "Done. Hard-refresh browser (Ctrl+F5), then reload the portfolio tab." -ForegroundColor Green
