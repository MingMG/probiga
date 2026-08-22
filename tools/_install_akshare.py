#!/usr/bin/env python3
from remote_support import production_ssh_client, production_ssh_connect_kwargs

ssh = production_ssh_client()
ssh.connect(**production_ssh_connect_kwargs())

cmd = 'cd /opt/ProBigA && source venv/bin/activate && pip install akshare -q 2>&1 | tail -5'
stdin, stdout, stderr = ssh.exec_command(cmd, timeout=120)
stdout.channel.settimeout(120)
print(stdout.read().decode().strip())
err = stderr.read().decode().strip()
if err:
    print('STDERR:', err[-500:])
ssh.close()
