#!/bin/bash
# 在服务器上执行：重启 ProBigA API 并查看状态
set -e
systemctl restart probiga
sleep 2
systemctl status probiga --no-pager -l | head -12
curl -s -o /dev/null -w "API HTTP %{http_code}\n" http://127.0.0.1:8000/docs || true
