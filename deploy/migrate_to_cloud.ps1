#!/bin/bash
# ==========================================
# 数据迁移脚本：将本地 MySQL 数据迁移到云服务器
# 在本地 PowerShell 中执行
# ==========================================

echo "======================================"
echo "  ProBigA 数据迁移到云服务器"
echo "======================================"

# ---------- 设置参数 ----------
read -p "云服务器 IP: " SERVER_IP
read -p "云服务器 SSH 端口 (默认22): " SSH_PORT
SSH_PORT=${SSH_PORT:-22}

# ---------- 1. 本地导出数据库 ----------
echo ""
echo "[1/4] 本地导出数据库..."
if (-not $env:MYSQL_PWD) {
    Write-Host "请先通过 MYSQL_PWD 设置本地 MySQL 导出密码。" -ForegroundColor Red
    exit 1
}
mysqldump -h localhost -P 3306 -u root --databases probiga --skip-lock-tables --single-transaction --quick -r probiga_dump.sql 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "mysqldump 可能不在 PATH，尝试用 python 导出..." -ForegroundColor Yellow
    python -c "
import subprocess, os
url = os.environ.get('MYSQL_URL', '')
print(f'请手动执行: mysqldump -h localhost -P 3306 -u root -p probiga > probiga_dump.sql')
print('然后输入你的本地 MySQL 密码')
"
    exit 1
}

# ---------- 2. 上传到云服务器 ----------
echo ""
echo "[2/4] 上传 SQL 文件到云服务器..."
scp -P ${SSH_PORT} probiga_dump.sql root@${SERVER_IP}:/root/

# ---------- 3. 上传项目代码 ----------
echo ""
echo "[3/4] 上传项目代码..."
$projectRoot = Split-Path -Parent $PSScriptRoot
scp -P ${SSH_PORT} -r "${projectRoot}" root@${SERVER_IP}:/opt/ 2>$null

# ---------- 4. 远程导入数据库 ----------
echo ""
echo "[4/4] 在云服务器导入数据库..."
ssh -p ${SSH_PORT} root@${SERVER_IP} "mysql -u probiga -pProBigA@2024 probiga < /root/probiga_dump.sql && echo '✅ 数据库导入成功' || echo '❌ 导入失败'"

echo ""
echo "======================================"
echo "  迁移完成！"
echo "  访问 http://${SERVER_IP} 查看"
echo "======================================"
