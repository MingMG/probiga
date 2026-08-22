#!/usr/bin/env python3
from remote_support import production_ssh_client, production_ssh_connect_kwargs

ssh = production_ssh_client()
ssh.connect(**production_ssh_connect_kwargs())

cmds = [
    'kill 60753 2>/dev/null; echo "killed 60753"',
    'tail -5 /tmp/kline_incremental.log 2>/dev/null || echo "no log"',
    'wc -l /tmp/kline_incremental.log 2>/dev/null',
]
for cmd in cmds:
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=10)
    stdout.channel.settimeout(10)
    print(stdout.read().decode().strip())

ssh.close()
