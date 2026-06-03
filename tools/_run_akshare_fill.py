#!/usr/bin/env python3
import paramiko
import os

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('47.113.123.190', username='root', password='ProBigA@2026', look_for_keys=False, allow_agent=False, timeout=10)

sftp = ssh.open_sftp()
local = os.path.join(os.path.dirname(__file__), '_kline_fill_akshare.py')
sftp.put(os.path.abspath(local), '/tmp/kline_fill_akshare.py')
sftp.close()

chan = ssh.get_transport().open_session()
chan.settimeout(15)
chan.exec_command('nohup bash -c "cd /opt/ProBigA && source venv/bin/activate && python /tmp/kline_fill_akshare.py" > /tmp/kline_akshare.log 2>&1 & echo "PID=$!"')
import time; time.sleep(3)
out = chan.recv(4096).decode().strip()
print(out)
chan.close()
ssh.close()
print('K-line akshare fill started.')
