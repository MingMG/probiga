#!/bin/bash
set -e

LEGACY_DEPLOY_OVERRIDE="I_ACKNOWLEDGE_LEGACY_DEPLOY_BYPASSES_RELEASE_GATES"
if [ "${PROBIGA_ALLOW_LEGACY_DEPLOY:-}" != "${LEGACY_DEPLOY_OVERRIDE}" ]; then
    echo "Legacy deploy blocked. Set PROBIGA_ALLOW_LEGACY_DEPLOY=${LEGACY_DEPLOY_OVERRIDE} to override." >&2
    exit 64
fi

echo "======================================"
echo "  ProBigA 一键部署脚本"
echo "  适用: Ubuntu 22.04 / CentOS 7+"
echo "======================================"

# ---------- 1. 系统更新 & 安装依赖 ----------
echo "[1/8] 安装系统依赖..."
if command -v apt &>/dev/null; then
    apt update -y
    apt install -y python3 python3-pip python3-venv git mysql-server nginx nodejs npm
elif command -v yum &>/dev/null; then
    yum install -y epel-release
    yum install -y python3 python3-pip git mysql-server nginx nodejs npm
fi

# ---------- 2. 配置 MySQL（低内存模式） ----------
echo "[2/8] 配置 MySQL（2G内存优化模式）..."
if command -v systemctl &>/dev/null; then
    systemctl start mysqld 2>/dev/null || systemctl start mysql 2>/dev/null || true
    systemctl enable mysqld 2>/dev/null || systemctl enable mysql 2>/dev/null || true
fi

# 创建 MySQL 低内存配置
mkdir -p /etc/mysql/conf.d
cat > /etc/mysql/conf.d/probiga.cnf << 'SQLEOF'
[mysqld]
# 2G 内存优化配置
innodb_buffer_pool_size = 512M
innodb_log_buffer_size = 16M
max_connections = 50
tmp_table_size = 32M
max_heap_table_size = 32M
query_cache_type = 0
thread_cache_size = 8
skip-name-resolve
character-set-server = utf8mb4
collation-server = utf8mb4_unicode_ci
SQLEOF

systemctl restart mysqld 2>/dev/null || systemctl restart mysql 2>/dev/null || true

# 创建数据库和用户
MYSQL_ROOT_PASS="ProBigA@$(date +%s | tail -c 6)"
mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED BY '${MYSQL_ROOT_PASS}';" 2>/dev/null || true
mysql -e "CREATE DATABASE IF NOT EXISTS probiga DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;" 2>/dev/null || true
mysql -e "CREATE USER IF NOT EXISTS 'probiga'@'localhost' IDENTIFIED BY 'ProBigA@2024';" 2>/dev/null || true
mysql -e "GRANT ALL PRIVILEGES ON probiga.* TO 'probiga'@'localhost';" 2>/dev/null || true
mysql -e "FLUSH PRIVILEGES;" 2>/dev/null || true

echo "  MySQL root密码: ${MYSQL_ROOT_PASS}"
echo "  应用数据库: probiga / probiga:ProBigA@2024"

# ---------- 3. 克隆/上传项目 ----------
echo "[3/8] 部署项目代码..."
cd /opt
if [ -d "ProBigA" ]; then
    cd ProBigA && git pull
else
    # 如果没有git仓库，手动创建说明
    echo "  请手动将项目文件上传到 /opt/ProBigA"
    echo "  可以使用 scp 或 sftp 上传"
    mkdir -p /opt/ProBigA
fi
cd /opt/ProBigA

# ---------- 4. 创建 Python 虚拟环境 ----------
echo "[4/8] 创建 Python 虚拟环境..."
python3 -m venv venv
source venv/bin/activate

# 安装 adata
pip install --upgrade pip
pip install -e ./adata
pip install -r requirements-platform.txt

# ---------- 5. 配置 MySQL 连接 ----------
echo "[5/8] 配置环境变量..."
# 如果没有 .env 文件则创建
if [ ! -f ".env" ]; then
    cat > .env << 'ENVEOF'
MYSQL_URL=mysql+pymysql://probiga:ProBigA@2024@localhost:3306/probiga?charset=utf8mb4
ENVEOF
fi

# ---------- 6. 配置 Nginx 反向代理 ----------
echo "[6/8] 配置 Nginx..."
cat > /etc/nginx/conf.d/probiga.conf << 'NGINXEOF'
limit_req_zone $binary_remote_addr zone=probiga_api:10m rate=10r/s;
limit_req_zone $binary_remote_addr zone=probiga_admin:10m rate=1r/s;

server {
    listen 80;
    server_name _;

    client_max_body_size 10m;

    location ~ ^/(api/deploy|api/scheduler|api/datasource|api/commentary|api/jq/minute|api/notify|deploy) {
        limit_req zone=probiga_admin burst=20 nodelay;
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /api/ {
        limit_req zone=probiga_api burst=60 nodelay;
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }

    location /static/ {
        alias /opt/ProBigA/server/static/;
        expires 1d;
    }
}
NGINXEOF

# 删除默认站点
rm -f /etc/nginx/sites-enabled/default 2>/dev/null || true
nginx -t && systemctl restart nginx

# ---------- 7. 配置 Systemd 服务 ----------
echo "[7/8] 配置开机自启服务..."
cat > /etc/systemd/system/probiga.service << 'SERVICEEOF'
[Unit]
Description=ProBigA Data Dashboard
After=network.target mysql.service mariadb.service

[Service]
Type=simple
User=root
WorkingDirectory=/opt/ProBigA
Environment=PYTHONPATH=/opt/ProBigA:/opt/ProBigA/adata
EnvironmentFile=/opt/ProBigA/.env
Environment=API_EMBEDDED_SCHEDULER_ENABLED=true
ExecStart=/opt/ProBigA/venv/bin/python -m uvicorn server.api.main:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
SERVICEEOF

systemctl daemon-reload
systemctl enable probiga.service
systemctl start probiga.service

# ---------- 8. 验证部署 ----------
echo "[8/8] 验证部署..."
sleep 3
if systemctl is-active --quiet probiga.service; then
    echo "======================================"
    echo "  ✅ 部署成功！"
    echo "  访问地址: http://你的服务器公网IP"
    echo ""
    echo "  MySQL 连接信息:"
    echo "    地址: localhost:3306"
    echo "    数据库: probiga"
    echo "    用户: probiga"
    echo "    密码: ProBigA@2024"
    echo "  MySQL root密码: ${MYSQL_ROOT_PASS}"
    echo ""
    echo "  常用管理命令:"
    echo "    查看状态: systemctl status probiga"
    echo "    重启服务: systemctl restart probiga"
    echo "    查看日志: journalctl -u probiga -f"
    echo "======================================"
else
    echo "❌ 服务启动失败，请检查日志: journalctl -u probiga -f"
fi
