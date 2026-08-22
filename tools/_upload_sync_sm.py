#!/usr/bin/env python3
from remote_support import production_ssh_client, production_ssh_connect_kwargs
import os

ssh = production_ssh_client()
ssh.connect(**production_ssh_connect_kwargs())

sftp = ssh.open_sftp()

local_path = os.path.join(os.path.dirname(__file__), '..', 'biz', 'stock_market', 'sync_stock_market.py')
remote_path = '/opt/ProBigA/biz/stock_market/sync_stock_market.py'

print(f'Uploading {local_path} -> {remote_path}')
sftp.put(os.path.abspath(local_path), remote_path)
print('Upload done.')
sftp.close()

stdin, stdout, stderr = ssh.exec_command(
    'cd /opt/ProBigA && source venv/bin/activate && python -c "import biz.stock_market.sync_stock_market; print(\'import OK\')"',
    timeout=30
)
out = stdout.read().decode()
err = stderr.read().decode()
print(out)
if err:
    print('ERR:', err[:1000])
ssh.close()
