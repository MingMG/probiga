#!/usr/bin/env python3
import paramiko
import os

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('47.113.123.190', username='root', password='ProBigA@2026', look_for_keys=False, allow_agent=False, timeout=10)

stdin, stdout, stderr = ssh.exec_command(
    "ps aux | grep 'sync_stock_market' | grep -v grep | awk '{print $2}' | xargs kill 2>/dev/null; echo 'killed all sync processes'",
    timeout=10
)
stdout.channel.settimeout(10)
print(stdout.read().decode().strip())

sftp = ssh.open_sftp()
local_path = os.path.join(os.path.dirname(__file__), '..', 'biz', 'stock_market', 'sync_stock_market.py')
remote_path = '/opt/ProBigA/biz/stock_market/sync_stock_market.py'
sftp.put(os.path.abspath(local_path), remote_path)

shell_script = '''#!/bin/bash
echo "$(date) - Waiting 10 minutes for rate limit cooldown..."
sleep 600
echo "$(date) - Starting K-line incremental sync..."
cd /opt/ProBigA
source venv/bin/activate
export PYTHONPATH=/opt/ProBigA:/opt/ProBigA/adata
export SM_MAX_STOCKS=0
export SM_MAX_WORKERS=1
export SM_REQUEST_SLEEP=0.5
python -m biz.stock_market.sync_stock_market --only stock_kline \\
    --kline-start 2026-04-28 --kline-end 2026-05-08 \\
    --kline-incremental
echo "$(date) - K-line sync done."
'''
with sftp.open('/tmp/run_kline_delayed.sh', 'w') as f:
    f.write(shell_script)
sftp.close()

chan = ssh.get_transport().open_session()
chan.settimeout(15)
chan.exec_command('nohup bash /tmp/run_kline_delayed.sh > /tmp/kline_incremental.log 2>&1 & echo "PID=$!"')
import time; time.sleep(3)
out = chan.recv(4096).decode().strip()
print(out)
chan.close()
ssh.close()
print('Delayed K-line sync scheduled (10 min cooldown)')
