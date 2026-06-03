# Upload hot-data fixes and restart probiga on server
# Usage: cd "E:\My Code\ProBigA"; .\deploy\upload_and_restart.ps1

$ErrorActionPreference = "Stop"
$SERVER = "root@47.113.123.190"
$REMOTE = "/opt/ProBigA"
$ROOT = "E:\My Code\ProBigA"

Write-Host "Prepare remote directories ..." -ForegroundColor Yellow
ssh $SERVER "mkdir -p $REMOTE/tools $REMOTE/server/api/routers $REMOTE/server/static/js $REMOTE/server/static/css"
if ($LASTEXITCODE -ne 0) { throw "Failed to prepare remote directories" }

Write-Host "Upload hot_data.py ..." -ForegroundColor Yellow
scp "$ROOT\server\api\routers\hot_data.py" "${SERVER}:${REMOTE}/server/api/routers/hot_data.py"
if ($LASTEXITCODE -ne 0) { throw "Failed to upload hot_data.py" }

Write-Host "Upload fetch_sector_heat_east_daily.py ..." -ForegroundColor Yellow
scp "$ROOT\tools\fetch_sector_heat_east_daily.py" "${SERVER}:${REMOTE}/tools/fetch_sector_heat_east_daily.py"
if ($LASTEXITCODE -ne 0) { throw "Failed to upload fetch_sector_heat_east_daily.py" }

Write-Host "Upload index.html ..." -ForegroundColor Yellow
scp "$ROOT\server\static\index.html" "${SERVER}:${REMOTE}/server/static/index.html"
if ($LASTEXITCODE -ne 0) { throw "Failed to upload index.html" }

Write-Host "Upload app.js ..." -ForegroundColor Yellow
scp "$ROOT\server\static\js\app.js" "${SERVER}:${REMOTE}/server/static/js/app.js"
if ($LASTEXITCODE -ne 0) { throw "Failed to upload app.js" }

Write-Host "Upload style.css ..." -ForegroundColor Yellow
scp "$ROOT\server\static\css\style.css" "${SERVER}:${REMOTE}/server/static/css/style.css"
if ($LASTEXITCODE -ne 0) { throw "Failed to upload style.css" }

Write-Host "Restart probiga ..." -ForegroundColor Yellow
ssh $SERVER "systemctl restart probiga; sleep 2; systemctl is-active probiga; systemctl status probiga --no-pager -l | head -8"
if ($LASTEXITCODE -ne 0) { throw "Failed to restart probiga" }

Write-Host "Done. Hard-refresh browser (Ctrl+F5), then reload the page." -ForegroundColor Green
