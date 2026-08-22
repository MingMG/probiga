#!/usr/bin/env python3
from remote_support import production_ssh_client, production_ssh_connect_kwargs

ssh = production_ssh_client()
ssh.connect(**production_ssh_connect_kwargs())

cmds = [
    'kill 61752 2>/dev/null; echo "killed"',
    "python3 -c \"import akshare as ak; df = ak.stock_zh_a_hist(symbol='000001', period='daily', start_date='20260428', end_date='20260508', adjust='qfq'); print(df.shape); print(df.head())\" 2>&1 | head -10 || echo 'akshare test failed'",
]
for cmd in cmds:
    stdin, stdout, stderr = ssh.exec_command(f'cd /opt/ProBigA && source venv/bin/activate && {cmd}', timeout=30)
    stdout.channel.settimeout(30)
    print(stdout.read().decode().strip())
    print()

ssh.close()
