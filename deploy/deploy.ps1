# ProBigA 一键部署脚本
# 用法：右键 → "使用 PowerShell 运行"，或在终端执行 .\deploy\deploy.ps1
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
$SERVER = "$RemoteUser@$RemoteHost"
$TAR_FILE = "$env:TEMP\probiga_deploy.tar"
$ROOT = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  ProBigA 一键部署 → $RemoteHost" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# 第1步：打包
Write-Host "`n[1/4] 打包项目文件..." -ForegroundColor Yellow
Push-Location $ROOT
try {
    tar -cf $TAR_FILE `
        server/api/main.py `
        server/api/routers/*.py `
        server/api/scheduler_runtime.py `
        server/common/batch_db.py `
        server/common/config.py `
        server/common/mysql_lock.py `
        server/common/process_env.py `
        server/common/scheduler_args.py `
        server/common/scheduler_runner.py `
        server/common/scheduler_tasks.py `
        server/common/scheduler_validation.py `
        server/db/data_integrity.py `
        server/db/migrations.py `
        server/static/ `
        tools/*.py `
        tools/03_scheduled_tasks.sql `
        biz/notice/sync_notice_em.py `
        biz/stock_market/sql/02_sm_stock_market_tables.sql `
        biz/stock_market/sync_stock_market.py `
        2>$null
    Write-Host "  ✅ 打包完成" -ForegroundColor Green
} finally {
    Pop-Location
}

# 第2步：上传
Write-Host "`n[2/4] 上传到服务器（请使用 SSH 密钥或手动输入服务器密码）..." -ForegroundColor Yellow
scp @SshOptions $TAR_FILE "${SERVER}:/root/probiga_deploy.tar"
if ($LASTEXITCODE -ne 0) { throw "上传失败" }
Write-Host "  ✅ 上传完成" -ForegroundColor Green

# 第3步：解压 + 重启
Write-Host "`n[3/4] 服务器解压并重启服务..." -ForegroundColor Yellow
ssh @SshOptions $SERVER "cd $RemoteRoot && tar -xf /root/probiga_deploy.tar && systemctl restart probiga && systemctl status probiga --no-pager -l | head -8"
if ($LASTEXITCODE -ne 0) { throw "部署失败" }

# 第4步：验证
Write-Host "`n[4/4] 验证..." -ForegroundColor Yellow
Start-Sleep -Seconds 2
$result = ssh @SshOptions $SERVER "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1/"
Write-Host "  服务状态: HTTP $result" -ForegroundColor Green

# 清理
Remove-Item $TAR_FILE -Force -ErrorAction SilentlyContinue

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "  Deploy complete." -ForegroundColor Green
Write-Host "  访问: http://$RemoteHost" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
if ($env:PROBIGA_NONINTERACTIVE -ne "1") {
    pause
}
