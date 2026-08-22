#!/usr/bin/env python3
from remote_support import production_ssh_client, production_ssh_connect_kwargs
import os

ssh = production_ssh_client()
ssh.connect(**production_ssh_connect_kwargs())

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
