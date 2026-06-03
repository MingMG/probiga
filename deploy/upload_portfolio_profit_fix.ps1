# Upload portfolio profit calculation fixes and restart probiga.
# Usage: cd "E:\My Code\ProBigA"; .\deploy\upload_portfolio_profit_fix.ps1

$ErrorActionPreference = "Stop"
$Server = "root@47.113.123.190"
$Remote = "/opt/ProBigA"
$Root = "E:\My Code\ProBigA"

$Files = @(
    @{ Local = "server\api\routers\hot_data.py"; Remote = "server/api/routers/hot_data.py" },
    @{ Local = "server\api\routers\portfolio_math.py"; Remote = "server/api/routers/portfolio_math.py" },
    @{ Local = "tools\fetch_sector_heat_east_daily.py"; Remote = "tools/fetch_sector_heat_east_daily.py" },
    @{ Local = "server\static\index.html"; Remote = "server/static/index.html" },
    @{ Local = "server\static\js\app.js"; Remote = "server/static/js/app.js" },
    @{ Local = "server\static\css\style.css"; Remote = "server/static/css/style.css" }
)

Write-Host "Prepare remote directories ..." -ForegroundColor Yellow
ssh $Server "mkdir -p $Remote/tools $Remote/server/api/routers $Remote/server/static $Remote/server/static/js $Remote/server/static/css"
if ($LASTEXITCODE -ne 0) { throw "Failed to prepare remote directories" }

foreach ($File in $Files) {
    $LocalPath = Join-Path $Root $File.Local
    Write-Host "Upload $($File.Local) ..." -ForegroundColor Yellow
    scp $LocalPath "${Server}:${Remote}/$($File.Remote)"
    if ($LASTEXITCODE -ne 0) { throw "Failed to upload $($File.Local)" }
}

Write-Host "Restart probiga ..." -ForegroundColor Yellow
ssh $Server "systemctl restart probiga; sleep 2; systemctl is-active probiga; systemctl status probiga --no-pager -l | head -8"
if ($LASTEXITCODE -ne 0) { throw "Failed to restart probiga" }

Write-Host "Done. Hard-refresh browser (Ctrl+F5), then reload the portfolio tab." -ForegroundColor Green
