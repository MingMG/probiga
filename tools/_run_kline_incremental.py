#!/usr/bin/env python3
from remote_support import production_ssh_client, production_ssh_connect_kwargs
import os
import time

ssh = production_ssh_client()
ssh.connect(**production_ssh_connect_kwargs())

sftp = ssh.open_sftp()
local_path = os.path.join(os.path.dirname(__file__), '..', 'biz', 'stock_market', 'sync_stock_market.py')
remote_path = '/opt/ProBigA/biz/stock_market/sync_stock_market.py'
sftp.put(os.path.abspath(local_path), remote_path)

shell_script = '''#!/bin/bash
cd /opt/ProBigA
source venv/bin/activate
export PYTHONPATH=/opt/ProBigA:/opt/ProBigA/adata
export SM_MAX_STOCKS=0
export SM_MAX_WORKERS=1
export SM_REQUEST_SLEEP=0.2
nohup python -m biz.stock_market.sync_stock_market --only stock_kline \
    --kline-start 2026-04-28 --kline-end 2026-05-08 \
    --kline-incremental > /tmp/kline_incremental.log 2>&1 &
echo "KLINE_PID=$!"
'''
with sftp.open('/tmp/run_kline.sh', 'w') as f:
    f.write(shell_script)
sftp.close()

chan = ssh.get_transport().open_session()
chan.settimeout(15)
chan.exec_command('bash /tmp/run_kline.sh')
time.sleep(5)
out = chan.recv(4096).decode().strip()
print(out)
chan.close()
ssh.close()
print('K-line incremental sync launched with SM_MAX_WORKERS=1')
