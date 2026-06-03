# ProBigA 一键部署脚本
# 用法：右键 → "使用 PowerShell 运行"，或在终端执行 .\deploy\deploy.ps1
$ErrorActionPreference = "Stop"

$SERVER = "root@47.113.123.190"
$TAR_FILE = "$env:TEMP\probiga_deploy.tar"

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  ProBigA 一键部署 → 47.113.123.190" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# 第1步：打包
Write-Host "`n[1/4] 打包项目文件..." -ForegroundColor Yellow
Push-Location "E:\My Code\ProBigA"
try {
    tar -cf $TAR_FILE `
        server/api/main.py `
        server/api/routers/*.py `
        server/static/ `
        tools/*.py `
        tools/03_scheduled_tasks.sql `
        biz/stock_market/sync_stock_market.py `
        2>$null
    Write-Host "  ✅ 打包完成" -ForegroundColor Green
} finally {
    Pop-Location
}

# 第2步：上传
Write-Host "`n[2/4] 上传到服务器（需要输入服务器密码: ProBigA@2026）..." -ForegroundColor Yellow
scp $TAR_FILE "${SERVER}:/root/probiga_deploy.tar"
if ($LASTEXITCODE -ne 0) { throw "上传失败" }
Write-Host "  ✅ 上传完成" -ForegroundColor Green

# 第3步：解压 + 重启
Write-Host "`n[3/4] 服务器解压并重启服务..." -ForegroundColor Yellow
ssh $SERVER "cd /opt/ProBigA && tar -xf /root/probiga_deploy.tar && systemctl restart probiga && systemctl status probiga --no-pager -l | head -8"
if ($LASTEXITCODE -ne 0) { throw "部署失败" }

# 第4步：验证
Write-Host "`n[4/4] 验证..." -ForegroundColor Yellow
Start-Sleep -Seconds 2
$result = ssh $SERVER "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1/"
Write-Host "  服务状态: HTTP $result" -ForegroundColor Green

# 清理
Remove-Item $TAR_FILE -Force -ErrorAction SilentlyContinue

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "  ✅ 部署完成！" -ForegroundColor Green
Write-Host "  访问: http://47.113.123.190" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
pause
