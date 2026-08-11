# Upload hot-data fixes and restart probiga on server
# Usage: cd <repo>; .\deploy\upload_and_restart.ps1

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

Write-Host "Prepare remote directories ..." -ForegroundColor Yellow
ssh @SshOptions $SERVER "mkdir -p $REMOTE/tools $REMOTE/server/api/routers $REMOTE/server/static/js $REMOTE/server/static/css"
if ($LASTEXITCODE -ne 0) { throw "Failed to prepare remote directories" }

Write-Host "Upload hot_data.py ..." -ForegroundColor Yellow
scp @SshOptions "$ROOT\server\api\routers\hot_data.py" "${SERVER}:${REMOTE}/server/api/routers/hot_data.py"
if ($LASTEXITCODE -ne 0) { throw "Failed to upload hot_data.py" }

Write-Host "Upload fetch_sector_heat_east_daily.py ..." -ForegroundColor Yellow
scp @SshOptions "$ROOT\tools\fetch_sector_heat_east_daily.py" "${SERVER}:${REMOTE}/tools/fetch_sector_heat_east_daily.py"
if ($LASTEXITCODE -ne 0) { throw "Failed to upload fetch_sector_heat_east_daily.py" }

Write-Host "Upload index.html ..." -ForegroundColor Yellow
scp @SshOptions "$ROOT\server\static\index.html" "${SERVER}:${REMOTE}/server/static/index.html"
if ($LASTEXITCODE -ne 0) { throw "Failed to upload index.html" }

Write-Host "Upload app.js ..." -ForegroundColor Yellow
scp @SshOptions "$ROOT\server\static\js\app.js" "${SERVER}:${REMOTE}/server/static/js/app.js"
if ($LASTEXITCODE -ne 0) { throw "Failed to upload app.js" }

Write-Host "Upload style.css ..." -ForegroundColor Yellow
scp @SshOptions "$ROOT\server\static\css\style.css" "${SERVER}:${REMOTE}/server/static/css/style.css"
if ($LASTEXITCODE -ne 0) { throw "Failed to upload style.css" }

Write-Host "Restart probiga ..." -ForegroundColor Yellow
ssh @SshOptions $SERVER "systemctl restart probiga; sleep 2; systemctl is-active probiga; systemctl status probiga --no-pager -l | head -8"
if ($LASTEXITCODE -ne 0) { throw "Failed to restart probiga" }

Write-Host "Done. Hard-refresh browser (Ctrl+F5), then reload the page." -ForegroundColor Green
